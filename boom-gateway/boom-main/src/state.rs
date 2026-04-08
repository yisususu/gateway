use arc_swap::ArcSwap;
use boom_auth::DbAuthenticator;
use boom_config::Config;
use boom_core::provider::Authenticator;
use boom_limiter::{PlanStore, RateLimitPlan, ScheduleSlot, SlidingWindowLimiter};
use boom_routing::{AliasStore, DeploymentStore, InFlightTracker, KeyAffinityPolicy, Router, RoundRobinPolicy, SchedulePolicy};
use boom_provider;
use sqlx::PgPool;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

/// Shared application state.
///
/// `inner` is wrapped in `ArcSwap` for lock-free atomic hot-swap:
///   - New requests immediately see the reloaded config.
///   - In-flight requests keep using the old state until done.
///   - Zero downtime, no races.
///
/// `db_pool`, `limiter`, `plan_store`, `deployment_store`, `alias_store`
/// live at this level — they survive reloads so DB connections,
/// rate-limit counters, deployments, and aliases are preserved.
#[derive(Clone)]
pub struct AppState {
    /// Config file path (stored for reload).
    pub config_path: String,
    /// Hot-swappable inner state (config + auth + health only).
    pub inner: Arc<ArcSwap<AppStateInner>>,
    /// DB pool survives reloads (avoids reconnection).
    pub db_pool: Option<PgPool>,
    /// Limiter survives reloads (preserves in-flight counters).
    pub limiter: Arc<SlidingWindowLimiter>,
    /// Plan store survives reloads (preserves plan definitions and key assignments).
    pub plan_store: Arc<PlanStore>,
    /// Deployment store survives reloads (preserves model deployments).
    pub deployment_store: Arc<DeploymentStore>,
    /// Alias store survives reloads (preserves model aliases).
    pub alias_store: Arc<AliasStore>,
    /// Router owns deployment + alias stores for routing decisions.
    pub router: Arc<Router>,
    /// In-flight request tracker (per-model count + input chars).
    pub inflight: Arc<InFlightTracker>,
    /// Request counter for periodic summary logging.
    pub request_count: Arc<AtomicU64>,
}

/// The state that gets swapped on config reload.
/// Only contains config, auth, and health — deployments/aliases live in stores.
pub struct AppStateInner {
    pub config: Config,
    pub auth: Arc<dyn Authenticator>,
    pub health: HealthStatus,
}

#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub last_reload_at: chrono::DateTime<chrono::Utc>,
    pub db_connected: bool,
    pub reload_count: u64,
}

impl AppState {
    /// Build state from config. Called once at startup.
    ///
    /// Unified YAML-priority flow:
    ///   1. Build deployments/aliases/plans from YAML → memory stores
    ///   2. sync_yaml_to_db() → persist YAML to DB, handle same-name conflicts
    ///   3. load_db_only_*() → load source='db' records from DB on top
    ///   4. Restore runtime state (assignments, counters)
    pub async fn from_config(config: Config, config_path: String) -> anyhow::Result<Self> {
        // 1. Connect to database (optional).
        let db_pool = match &config.general_settings.database_url {
            Some(url) => {
                tracing::info!("Connecting to database...");
                let pool = sqlx::postgres::PgPoolOptions::new()
                    .max_connections(30)
                    .acquire_timeout(std::time::Duration::from_secs(10))
                    .idle_timeout(std::time::Duration::from_secs(600))
                    .max_lifetime(std::time::Duration::from_secs(1800))
                    .connect(url)
                    .await?;
                tracing::info!("Database connected");
                Some(pool)
            }
            None => {
                tracing::warn!("No database URL — running in master-key-only auth mode");
                None
            }
        };

        // 2. Limiter survives across reloads.
        let limiter = Arc::new(SlidingWindowLimiter::new());

        // 3. Plan store survives across reloads.
        let plan_store = Arc::new(PlanStore::new());

        // 4. Deployment store & alias store survive across reloads.
        let deployment_store = Arc::new(DeploymentStore::new());
        let alias_store = Arc::new(AliasStore::new());

        // In-flight tracker survives across reloads — must be created before policy.
        let inflight = Arc::new(InFlightTracker::new());

        // Create scheduling policy from config (may reference inflight).
        let policy = create_policy(&config, &inflight);

        // Router wraps stores + policy for routing decisions.
        let router = Arc::new(Router::new(deployment_store.clone(), alias_store.clone(), policy));

        // 5. Build from YAML first, then layer DB-only records on top.
        build_deployments_from_config(&config, &deployment_store);
        build_aliases_from_config(&config, &alias_store, &deployment_store);
        load_plans_from_config(&plan_store, &config);

        if let Some(ref pool) = db_pool {
            // Run migrations (all tables).
            if let Err(e) = boom_dashboard::migrations::run_migrations(pool).await {
                tracing::error!("Failed to run migrations: {}", e);
            }

            // Sync YAML config to DB (upsert source='yaml', handle conflicts).
            if let Err(e) = sync_yaml_to_db(pool, &config).await {
                tracing::error!("Failed to sync YAML to DB: {}", e);
            }

            // Load source='db' records on top of YAML-built stores.
            load_db_only_deployments(pool, &deployment_store).await;
            load_db_only_aliases(pool, &alias_store).await;
            load_db_only_plans(pool, &plan_store).await;

            // Restore runtime state.
            restore_assignments_from_db(pool, &plan_store).await;
            restore_counters_from_db(pool, &limiter).await;
        }

        // 6. Build inner state (config + auth + health).
        let inner = Self::build_inner(config, &db_pool, chrono::Utc::now(), 0)?;

        Ok(Self {
            config_path,
            inner: Arc::new(ArcSwap::from_pointee(inner)),
            db_pool,
            limiter,
            plan_store,
            deployment_store,
            alias_store,
            router,
            inflight,
            request_count: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Hot-reload: re-read config file and update state.
    ///
    /// Unified YAML-priority flow (same as startup, minus DB reconnection):
    ///   1. Rebuild deployments/aliases/plans from YAML → memory stores
    ///   2. sync_yaml_to_db() → persist YAML to DB, handle conflicts
    ///   3. load_db_only_*() → load source='db' records on top
    ///   4. Clean up orphaned assignments
    pub async fn reload(&self) -> anyhow::Result<String> {
        tracing::info!("Hot-reloading config from {}...", self.config_path);

        // 1. Re-read config.
        let new_config = boom_config::load_config(&self.config_path)?;

        // 2. Snapshot old state to get counts.
        let old_guard = self.inner.load();
        let old_started_at = old_guard.health.started_at;
        let old_reload_count = old_guard.health.reload_count;
        let old_db_url = old_guard.config.general_settings.database_url.clone();
        drop(old_guard);

        // 3. Check if DB URL changed.
        let db_pool = if old_db_url != new_config.general_settings.database_url {
            tracing::info!("Database URL changed, reconnecting...");
            match &new_config.general_settings.database_url {
                Some(url) => Some(
                    sqlx::postgres::PgPoolOptions::new()
                        .max_connections(30)
                        .acquire_timeout(std::time::Duration::from_secs(10))
                        .idle_timeout(std::time::Duration::from_secs(600))
                        .max_lifetime(std::time::Duration::from_secs(1800))
                        .connect(url)
                        .await?,
                ),
                None => None,
            }
        } else {
            self.db_pool.clone()
        };

        let new_reload_count = old_reload_count + 1;

        // 4. Rebuild stores: YAML first, then DB-only on top.
        self.deployment_store.clear();
        build_deployments_from_config(&new_config, &self.deployment_store);

        self.alias_store.clear();
        build_aliases_from_config(&new_config, &self.alias_store, &self.deployment_store);

        self.plan_store.clear_plans();
        load_plans_from_config(&self.plan_store, &new_config);

        // Recreate policy (fresh counters etc.) — router reuses same stores.
        let new_policy = create_policy(&new_config, &self.inflight);
        self.router.set_policy(new_policy);

        if let Some(ref pool) = db_pool {
            // Sync YAML config to DB (upsert source='yaml', handle conflicts).
            if let Err(e) = sync_yaml_to_db(pool, &new_config).await {
                tracing::error!("Failed to sync YAML to DB: {}", e);
            }

            // Load source='db' records on top of YAML-built stores.
            load_db_only_deployments(pool, &self.deployment_store).await;
            load_db_only_aliases(pool, &self.alias_store).await;
            load_db_only_plans(pool, &self.plan_store).await;
        }

        // Clean up assignments pointing to plans that no longer exist.
        self.plan_store.cleanup_assignments();

        // 5. Build new inner state.
        let new_inner =
            Self::build_inner(new_config, &db_pool, old_started_at, new_reload_count)?;

        // 6. Atomic swap.
        self.inner.store(Arc::new(new_inner));

        let model_count = self.deployment_store.len();
        let summary = format!(
            "Reloaded: {} model(s), reload #{}",
            model_count, new_reload_count,
        );
        tracing::info!("{}", summary);
        Ok(summary)
    }

    /// Build AppStateInner from config.
    fn build_inner(
        config: Config,
        db_pool: &Option<PgPool>,
        started_at: chrono::DateTime<chrono::Utc>,
        reload_count: u64,
    ) -> Result<AppStateInner, anyhow::Error> {
        // Build authenticator.
        let auth: Arc<dyn Authenticator> = Arc::new(DbAuthenticator::new(
            db_pool.clone(),
            config.general_settings.master_key.clone(),
        ));

        let health = HealthStatus {
            started_at,
            last_reload_at: chrono::Utc::now(),
            db_connected: db_pool.is_some(),
            reload_count,
        };

        Ok(AppStateInner {
            config,
            auth,
            health,
        })
    }

    /// Dump current runtime config (models, aliases, plans) to a timestamped YAML snapshot.
    /// Best-effort: errors are logged but not propagated.
    pub async fn dump_config_snapshot(&self) {
        let pool = match &self.db_pool {
            Some(p) => p,
            None => return,
        };

        let config_value = match build_config_snapshot_value(pool).await {
            Ok(v) => v,
            Err(e) => {
                tracing::error!("Failed to build config snapshot: {}", e);
                return;
            }
        };

        let yaml_str = match serde_yaml::to_string(&config_value) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to serialize config snapshot to YAML: {}", e);
                return;
            }
        };

        let timestamp = chrono::Local::now().format("%Y%m%d%H%M%S");
        let snapshot_path = format!("{}.{}", self.config_path, timestamp);

        match tokio::fs::write(&snapshot_path, &yaml_str).await {
            Ok(_) => {
                tracing::info!(path = %snapshot_path, "Config snapshot saved");
            }
            Err(e) => {
                tracing::error!(path = %snapshot_path, "Failed to write config snapshot: {}", e);
            }
        }
    }
}

// ═══════════════════════════════════════════════════════════
// YAML → DB sync (replaces seed_from_yaml + reseed_yaml_in_db)
// ═══════════════════════════════════════════════════════════

/// Sync YAML config to DB: replace source='yaml' rows, handle same-name conflicts.
///
/// For each table (deployments, aliases, plans):
///   1. DELETE source='yaml' rows → INSERT from current YAML
///   2. DELETE source='db' rows that conflict with YAML names (aliases & plans)
///   3. Clean up orphaned assignments
async fn sync_yaml_to_db(pool: &PgPool, config: &Config) -> Result<(), sqlx::Error> {
    // ── Deployments ──
    sqlx::query(r#"DELETE FROM boom_model_deployment WHERE source = 'yaml'"#)
        .execute(pool)
        .await?;

    for entry in &config.model_list {
        let p = &entry.litellm_params;
        let headers_json = serde_json::to_value(&p.headers).unwrap_or(serde_json::json!({}));
        let deployment_id = entry.model_info.as_ref().and_then(|mi| mi.id.clone());
        sqlx::query(
            r#"INSERT INTO boom_model_deployment
               (model_name, litellm_model, api_key, api_key_env, api_base, api_version,
                aws_region_name, aws_access_key_id, aws_secret_access_key,
                rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source, deployment_id)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, true, 'yaml', $16)"#,
        )
        .bind(&entry.model_name)
        .bind(&p.model)
        .bind(&p.api_key)
        .bind(false) // api_key already resolved by load_config
        .bind(&p.api_base)
        .bind(&p.api_version)
        .bind(&p.aws_region_name)
        .bind(&p.aws_access_key_id)
        .bind(&p.aws_secret_access_key)
        .bind(p.rpm.map(|v| v as i64))
        .bind(p.tpm.map(|v| v as i64))
        .bind(p.timeout as i64)
        .bind(&headers_json)
        .bind(p.temperature)
        .bind(p.max_tokens.map(|v| v as i32))
        .bind(&deployment_id)
        .execute(pool)
        .await?;
    }
    tracing::info!("Synced {} deployment(s) from YAML to DB", config.model_list.len());

    // ── Aliases ──
    sqlx::query(r#"DELETE FROM boom_model_alias WHERE source = 'yaml'"#)
        .execute(pool)
        .await?;

    for (alias, alias_cfg) in &config.router_settings.model_group_alias {
        sqlx::query(
            r#"INSERT INTO boom_model_alias (alias_name, target_model, hidden, source)
               VALUES ($1, $2, $3, 'yaml')"#,
        )
        .bind(alias)
        .bind(alias_cfg.target_model())
        .bind(alias_cfg.is_hidden())
        .execute(pool)
        .await?;
    }

    // Delete source='db' aliases that conflict with YAML alias names.
    if !config.router_settings.model_group_alias.is_empty() {
        let yaml_alias_names: Vec<String> =
            config.router_settings.model_group_alias.keys().cloned().collect();
        let result = sqlx::query(
            r#"DELETE FROM boom_model_alias WHERE source = 'db' AND alias_name = ANY($1)"#,
        )
        .bind(&yaml_alias_names)
        .execute(pool)
        .await?;
        if result.rows_affected() > 0 {
            tracing::info!(
                "Removed {} conflicting source='db' alias(es)",
                result.rows_affected()
            );
        }
    }
    tracing::info!(
        "Synced {} alias(es) from YAML to DB",
        config.router_settings.model_group_alias.len()
    );

    // ── Plans ──
    // 1. Delete all source='yaml' rows (stale YAML plans from previous run).
    sqlx::query(r#"DELETE FROM boom_rate_limit_plan WHERE source = 'yaml'"#)
        .execute(pool)
        .await?;

    // 2. Delete source='db' plans that conflict with YAML names BEFORE inserting.
    //    This prevents unique-key conflicts on the next INSERT.
    if !config.plan_settings.plans.is_empty() {
        let yaml_plan_names: Vec<String> = config.plan_settings.plans.keys().cloned().collect();
        let result = sqlx::query(
            r#"DELETE FROM boom_rate_limit_plan WHERE source = 'db' AND name = ANY($1)"#,
        )
        .bind(&yaml_plan_names)
        .execute(pool)
        .await?;
        if result.rows_affected() > 0 {
            tracing::info!(
                "Removed {} conflicting source='db' plan(s)",
                result.rows_affected()
            );
        }
    }

    // 3. Insert current YAML plans as source='yaml'.
    for (name, pc) in &config.plan_settings.plans {
        let window_limits_json = serde_json::to_value(&pc.window_limits).unwrap_or(serde_json::json!([]));
        let schedule_json = serde_json::to_value(
            pc.schedule
                .iter()
                .map(|s| serde_json::json!({
                    "hours": s.hours,
                    "concurrency_limit": s.concurrency_limit,
                    "rpm_limit": s.rpm_limit,
                    "window_limits": s.window_limits,
                }))
                .collect::<Vec<_>>(),
        )
        .unwrap_or(serde_json::json!([]));

        let is_default = config.plan_settings.default_plan.as_deref() == Some(name.as_str());

        sqlx::query(
            r#"INSERT INTO boom_rate_limit_plan
               (name, concurrency_limit, rpm_limit, window_limits, schedule, is_default, source)
               VALUES ($1, $2, $3, $4, $5, $6, 'yaml')"#,
        )
        .bind(name)
        .bind(pc.concurrency_limit.map(|v| v as i32))
        .bind(pc.rpm_limit.map(|v| v as i64))
        .bind(&window_limits_json)
        .bind(&schedule_json)
        .bind(is_default)
        .execute(pool)
        .await?;
    }

    tracing::info!("Synced {} plan(s) from YAML to DB", config.plan_settings.plans.len());

    // ── Clean up orphaned assignments ──
    // Delete assignments pointing to plans that no longer exist in DB.
    sqlx::query(
        r#"DELETE FROM boom_key_plan_assignment
           WHERE plan_name NOT IN (SELECT name FROM boom_rate_limit_plan)"#,
    )
    .execute(pool)
    .await?;

    Ok(())
}

// ═══════════════════════════════════════════════════════════
// DB-only loading (source='db' records on top of YAML stores)
// ═══════════════════════════════════════════════════════════

/// Row from boom_model_deployment.
#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DeploymentRow {
    id: uuid::Uuid,
    model_name: String,
    litellm_model: String,
    api_key: Option<String>,
    api_key_env: Option<bool>,
    api_base: Option<String>,
    api_version: Option<String>,
    aws_region_name: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    rpm: Option<i64>,
    tpm: Option<i64>,
    timeout: i64,
    headers: serde_json::Value,
    temperature: Option<f64>,
    max_tokens: Option<i32>,
    enabled: Option<bool>,
    source: Option<String>,
    deployment_id: Option<String>,
}

/// Load source='db' model deployments from DB and add providers to DeploymentStore.
/// Uses add_deployment (not set_deployments) so YAML providers for the same model are preserved.
async fn load_db_only_deployments(pool: &PgPool, deployment_store: &Arc<DeploymentStore>) {
    let rows: Vec<DeploymentRow> = match sqlx::query_as::<_, DeploymentRow>(
        r#"SELECT id, model_name, litellm_model, api_key, api_key_env, api_base, api_version,
                  aws_region_name, aws_access_key_id, aws_secret_access_key,
                  rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source, deployment_id
           FROM boom_model_deployment
           WHERE source = 'db' AND enabled IS NOT FALSE
           ORDER BY model_name, created_at"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to load DB-only deployments: {}", e);
            return;
        }
    };

    let mut deployment_count = 0;

    for row in &rows {
        let mut extra = std::collections::HashMap::new();
        if let Some(obj) = row.headers.as_object() {
            for (k, v) in obj {
                if let Some(s) = v.as_str() {
                    extra.insert(k.clone(), s.to_string());
                }
            }
        }
        if let Some(ref v) = row.api_version {
            extra.insert("api_version".to_string(), v.clone());
        }
        if let Some(ref r) = row.aws_region_name {
            extra.insert("aws_region_name".to_string(), r.clone());
        }

        let api_key = row.api_key.as_ref().map(|k| {
            if row.api_key_env.unwrap_or(false) {
                boom_config::resolve_env_value(k)
            } else {
                k.clone()
            }
        });

        match boom_provider::create_provider(
            &row.litellm_model,
            api_key,
            row.api_base.clone(),
            row.timeout as u64,
            &extra,
            row.deployment_id.clone(),
        ) {
            Ok(provider) => {
                deployment_store.add_deployment(&row.model_name, provider);
                deployment_count += 1;
            }
            Err(e) => {
                tracing::error!(
                    "Failed to create provider for model '{}': {}",
                    row.model_name,
                    e
                );
            }
        }
    }

    tracing::info!("Loaded {} DB-only deployment(s)", deployment_count);
}

/// Row from boom_model_alias.
#[derive(Debug, sqlx::FromRow)]
struct AliasRow {
    alias_name: String,
    target_model: String,
    hidden: Option<bool>,
}

/// Load source='db' model aliases from DB → AliasStore.
async fn load_db_only_aliases(pool: &PgPool, alias_store: &Arc<AliasStore>) {
    let rows: Vec<AliasRow> = match sqlx::query_as::<_, AliasRow>(
        r#"SELECT alias_name, target_model, hidden FROM boom_model_alias WHERE source = 'db'"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to load DB-only aliases: {}", e);
            return;
        }
    };

    for row in &rows {
        alias_store.set_alias(
            row.alias_name.clone(),
            row.target_model.clone(),
            row.hidden.unwrap_or(false),
        );
    }

    tracing::info!("Loaded {} DB-only alias(es)", rows.len());
}

/// Row from boom_rate_limit_plan.
#[derive(Debug, sqlx::FromRow)]
struct PlanRow {
    name: String,
    concurrency_limit: Option<i32>,
    rpm_limit: Option<i64>,
    window_limits: serde_json::Value,
    schedule: serde_json::Value,
    #[allow(dead_code)]
    is_default: Option<bool>,
}

/// Load source='db' plans from DB → PlanStore.
async fn load_db_only_plans(pool: &PgPool, plan_store: &Arc<PlanStore>) {
    let rows: Vec<PlanRow> = match sqlx::query_as::<_, PlanRow>(
        r#"SELECT name, concurrency_limit, rpm_limit, window_limits, schedule, is_default
           FROM boom_rate_limit_plan
           WHERE source = 'db'"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to load DB-only plans: {}", e);
            return;
        }
    };

    for row in &rows {
        let window_limits = parse_window_limits(&row.window_limits);
        let schedule = parse_schedule(&row.schedule);

        let plan = RateLimitPlan {
            name: row.name.clone(),
            concurrency_limit: row.concurrency_limit.map(|v| v as u32),
            rpm_limit: row.rpm_limit.map(|v| v as u64),
            window_limits,
            schedule,
        };

        plan_store.upsert_plan(plan);
    }

    tracing::info!("Loaded {} DB-only plan(s)", rows.len());
}

/// Restore key→plan assignments from DB.
async fn restore_assignments_from_db(pool: &PgPool, plan_store: &Arc<PlanStore>) {
    match sqlx::query_as::<_, (String, String)>(
        r#"SELECT key_hash, plan_name FROM boom_key_plan_assignment"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            let count = rows.len();
            for (key_hash, plan_name) in rows {
                plan_store.restore_assignment(&key_hash, &plan_name);
            }
            tracing::info!("Restored {} key→plan assignment(s) from DB", count);
        }
        Err(e) => {
            tracing::error!("Failed to restore assignments: {}", e);
        }
    }
}

/// Restore rate limit counters from DB.
async fn restore_counters_from_db(pool: &PgPool, limiter: &Arc<SlidingWindowLimiter>) {
    match sqlx::query_as::<_, (String, i64, i64, i64)>(
        r#"SELECT cache_key, count, window_start, window_secs
           FROM boom_rate_limit_state
           WHERE window_start + window_secs > EXTRACT(EPOCH FROM NOW())::BIGINT"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(rows) => {
            let count = rows.len();
            for (cache_key, count_val, window_start, window_secs) in rows {
                limiter.restore_counter(cache_key, count_val as u64, window_start as u64, window_secs as u64);
            }
            tracing::info!("Restored {} rate limit counter(s) from DB", count);
        }
        Err(e) => {
            tracing::error!("Failed to restore rate limit state: {}", e);
        }
    }
}

// ═══════════════════════════════════════════════════════════
// YAML → Memory (no-DB fallback)
// ═══════════════════════════════════════════════════════════

/// Build deployments directly from YAML config into DeploymentStore.
fn build_deployments_from_config(config: &Config, deployment_store: &Arc<DeploymentStore>) {
    deployment_store.clear();

    for entry in &config.model_list {
        let p = &entry.litellm_params;

        let mut extra = p.headers.clone();
        if let Some(ref v) = p.api_version {
            extra.insert("api_version".to_string(), v.clone());
        }
        if let Some(ref r) = p.aws_region_name {
            extra.insert("aws_region_name".to_string(), r.clone());
        }

        let deployment_id = entry.model_info.as_ref().and_then(|mi| mi.id.clone());

        match boom_provider::create_provider(
            &p.model,
            p.api_key.clone(),
            p.api_base.clone(),
            p.timeout,
            &extra,
            deployment_id,
        ) {
            Ok(provider) => {
                deployment_store.add_deployment(&entry.model_name, provider);
            }
            Err(e) => {
                tracing::error!(
                    "Failed to create provider for model '{}': {}",
                    entry.model_name,
                    e
                );
            }
        }
    }

    tracing::info!(
        "Built {} model(s) with {} deployment(s) from YAML",
        deployment_store.len(),
        deployment_store.total_deployments(),
    );
}

/// Build aliases directly from YAML config into AliasStore.
fn build_aliases_from_config(
    config: &Config,
    alias_store: &Arc<AliasStore>,
    deployment_store: &Arc<DeploymentStore>,
) {
    alias_store.clear();

    for (alias, alias_cfg) in &config.router_settings.model_group_alias {
        let target = alias_cfg.target_model();
        if !deployment_store.contains(target) {
            tracing::warn!(
                "Skipping alias '{}' → '{}': target model not found in deployments",
                alias,
                target
            );
            continue;
        }
        tracing::info!("Model alias: '{}' → '{}'", alias, target);
        alias_store.set_alias(alias.clone(), target.to_string(), alias_cfg.is_hidden());
    }

    tracing::info!(
        "Loaded {} alias(es), {} hidden",
        alias_store.len(),
        alias_store.hidden_count(),
    );
}

/// Load plans from YAML config into PlanStore.
fn load_plans_from_config(plan_store: &Arc<PlanStore>, config: &Config) {
    for (name, pc) in &config.plan_settings.plans {
        let window_limits: Vec<(u64, u64)> = pc
            .window_limits
            .iter()
            .filter_map(|w| {
                if w.len() >= 2 {
                    Some((w[0], w[1]))
                } else {
                    None
                }
            })
            .collect();

        let plan = RateLimitPlan {
            name: name.clone(),
            concurrency_limit: pc.concurrency_limit,
            rpm_limit: pc.rpm_limit,
            window_limits,
            schedule: convert_schedule(&pc.schedule),
        };
        plan_store.upsert_plan(plan);
    }

    match &config.plan_settings.default_plan {
        Some(dp) => {
            if plan_store.get_plan(dp).is_some() {
                plan_store.set_default_plan(Some(dp.clone()));
                tracing::info!(default_plan = %dp, "Default plan set");
            } else {
                tracing::warn!(
                    default_plan = %dp,
                    "default_plan '{}' not found in configured plans, ignoring",
                    dp
                );
                plan_store.set_default_plan(None);
            }
        }
        None => {
            plan_store.set_default_plan(None);
            tracing::warn!("没有默认套餐配置，所有用户将无套餐限制。");
        }
    }
}

// ═══════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════

/// Create a scheduling policy from config.
fn create_policy(config: &Config, inflight: &Arc<InFlightTracker>) -> Arc<dyn SchedulePolicy> {
    match config.router_settings.schedule_policy.as_str() {
        "round_robin" | "" => Arc::new(RoundRobinPolicy::new()),
        "key_affinity" => {
            let ctx_threshold = config.router_settings.key_affinity_context_threshold;
            let rebalance_threshold = config.router_settings.key_affinity_rebalance_threshold;
            tracing::info!(
                "Using key_affinity policy: context_threshold={}, rebalance_threshold={}",
                ctx_threshold,
                rebalance_threshold,
            );
            Arc::new(KeyAffinityPolicy::new(
                inflight.clone(),
                ctx_threshold,
                rebalance_threshold,
            ))
        }
        other => {
            tracing::warn!(
                "Unknown schedule_policy '{}', falling back to round_robin",
                other
            );
            Arc::new(RoundRobinPolicy::new())
        }
    }
}

/// Convert config schedule slots into limiter schedule slots.
fn convert_schedule(slots: &[boom_config::ScheduleSlotConfig]) -> Vec<ScheduleSlot> {
    slots
        .iter()
        .map(|s| ScheduleSlot {
            hours: s.hours.clone(),
            concurrency_limit: s.concurrency_limit,
            rpm_limit: s.rpm_limit,
            window_limits: s
                .window_limits
                .iter()
                .filter_map(|w| {
                    if w.len() >= 2 {
                        Some((w[0], w[1]))
                    } else {
                        None
                    }
                })
                .collect(),
        })
        .collect()
}

/// Parse window_limits from JSONB (array of [count, window_secs]).
fn parse_window_limits(value: &serde_json::Value) -> Vec<(u64, u64)> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let a = item.as_array()?;
                    if a.len() >= 2 {
                        Some((a[0].as_u64()?, a[1].as_u64()?))
                    } else {
                        None
                    }
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Parse schedule from JSONB.
fn parse_schedule(value: &serde_json::Value) -> Vec<ScheduleSlot> {
    value
        .as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|item| {
                    let obj = item.as_object()?;
                    Some(ScheduleSlot {
                        hours: obj.get("hours")?.as_str()?.to_string(),
                        concurrency_limit: obj
                            .get("concurrency_limit")
                            .and_then(|v| v.as_u64())
                            .map(|v| v as u32),
                        rpm_limit: obj.get("rpm_limit").and_then(|v| v.as_u64()),
                        window_limits: parse_window_limits(obj.get("window_limits")?),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

// ═══════════════════════════════════════════════════════════
// Config snapshot (DB → YAML)
// ═══════════════════════════════════════════════════════════

/// Row for snapshot: only fields needed for config.yaml export.
#[derive(Debug, sqlx::FromRow)]
struct SnapshotDeploymentRow {
    model_name: String,
    litellm_model: String,
    api_key: Option<String>,
    api_base: Option<String>,
    api_version: Option<String>,
    aws_region_name: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    rpm: Option<i64>,
    tpm: Option<i64>,
    timeout: i64,
    headers: serde_json::Value,
    temperature: Option<f64>,
    max_tokens: Option<i32>,
    deployment_id: Option<String>,
}

/// Row for snapshot: alias.
#[derive(Debug, sqlx::FromRow)]
struct SnapshotAliasRow {
    alias_name: String,
    target_model: String,
}

/// Row for snapshot: plan + is_default.
#[derive(Debug, sqlx::FromRow)]
struct SnapshotPlanRow {
    name: String,
    concurrency_limit: Option<i32>,
    rpm_limit: Option<i64>,
    window_limits: serde_json::Value,
    schedule: serde_json::Value,
    is_default: Option<bool>,
}

/// Build a serde_json::Value representing the current runtime config
/// (model_list, router_settings.model_group_alias, plan_settings).
async fn build_config_snapshot_value(pool: &PgPool) -> Result<serde_json::Value, sqlx::Error> {
    // ── Model list ──
    let model_rows: Vec<SnapshotDeploymentRow> = sqlx::query_as::<_, SnapshotDeploymentRow>(
        r#"SELECT model_name, litellm_model, api_key, api_base, api_version,
                  aws_region_name, aws_access_key_id, aws_secret_access_key,
                  rpm, tpm, timeout, headers, temperature, max_tokens, deployment_id
           FROM boom_model_deployment
           WHERE enabled IS NOT FALSE
           ORDER BY model_name, created_at"#,
    )
    .fetch_all(pool)
    .await?;

    let model_list: Vec<serde_json::Value> = model_rows
        .into_iter()
        .map(|r| {
            let mut litellm_params = serde_json::Map::new();
            litellm_params.insert("model".into(), serde_json::Value::String(r.litellm_model));
            if let Some(k) = r.api_key {
                litellm_params.insert("api_key".into(), serde_json::Value::String(k));
            }
            if let Some(b) = r.api_base {
                litellm_params.insert("api_base".into(), serde_json::Value::String(b));
            }
            if let Some(v) = r.api_version {
                litellm_params.insert("api_version".into(), serde_json::Value::String(v));
            }
            if let Some(r) = r.aws_region_name {
                litellm_params.insert("aws_region_name".into(), serde_json::Value::String(r));
            }
            if let Some(k) = r.aws_access_key_id {
                litellm_params.insert("aws_access_key_id".into(), serde_json::Value::String(k));
            }
            if let Some(k) = r.aws_secret_access_key {
                litellm_params.insert("aws_secret_access_key".into(), serde_json::Value::String(k));
            }
            if let Some(rpm) = r.rpm {
                litellm_params.insert("rpm".into(), serde_json::Value::Number(rpm.into()));
            }
            if let Some(tpm) = r.tpm {
                litellm_params.insert("tpm".into(), serde_json::Value::Number(tpm.into()));
            }
            litellm_params.insert("timeout".into(), serde_json::Value::Number(r.timeout.into()));
            if let Some(t) = r.temperature {
                litellm_params.insert(
                    "temperature".into(),
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(t).unwrap_or(serde_json::Number::from(0)),
                    ),
                );
            }
            if let Some(m) = r.max_tokens {
                litellm_params.insert("max_tokens".into(), serde_json::Value::Number(m.into()));
            }
            // Only include headers if non-empty.
            if let Some(obj) = r.headers.as_object() {
                if !obj.is_empty() {
                    litellm_params.insert("headers".into(), r.headers);
                }
            }

            let mut entry = serde_json::json!({
                "model_name": r.model_name,
                "litellm_params": litellm_params,
            });

            // Include model_info if deployment_id is set.
            if let Some(ref did) = r.deployment_id {
                if !did.is_empty() {
                    entry.as_object_mut().unwrap().insert(
                        "model_info".into(),
                        serde_json::json!({ "id": did }),
                    );
                }
            }

            entry
        })
        .collect();

    // ── Aliases ──
    let alias_rows: Vec<SnapshotAliasRow> = sqlx::query_as::<_, SnapshotAliasRow>(
        r#"SELECT alias_name, target_model FROM boom_model_alias ORDER BY alias_name"#,
    )
    .fetch_all(pool)
    .await?;

    let model_group_alias: serde_json::Map<String, serde_json::Value> = alias_rows
        .into_iter()
        .map(|r| (r.alias_name, serde_json::Value::String(r.target_model)))
        .collect();

    // ── Plans ──
    let plan_rows: Vec<SnapshotPlanRow> = sqlx::query_as::<_, SnapshotPlanRow>(
        r#"SELECT name, concurrency_limit, rpm_limit, window_limits, schedule, is_default
           FROM boom_rate_limit_plan ORDER BY name"#,
    )
    .fetch_all(pool)
    .await?;

    let mut default_plan: Option<String> = None;
    let mut plans_map = serde_json::Map::new();

    for r in &plan_rows {
        if r.is_default.unwrap_or(false) && default_plan.is_none() {
            default_plan = Some(r.name.clone());
        }

        let window_limits: Vec<serde_json::Value> = r
            .window_limits
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        let a = item.as_array()?;
                        if a.len() >= 2 {
                            Some(serde_json::json!([a[0], a[1]]))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        let schedule: Vec<serde_json::Value> = r
            .schedule
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|item| {
                        let obj = item.as_object()?;
                        let mut slot = serde_json::Map::new();
                        if let Some(h) = obj.get("hours").and_then(|v| v.as_str()) {
                            slot.insert("hours".into(), serde_json::Value::String(h.to_string()));
                        }
                        if let Some(v) = obj.get("concurrency_limit").and_then(|v| v.as_u64()) {
                            slot.insert("concurrency_limit".into(), serde_json::Value::Number(v.into()));
                        }
                        if let Some(v) = obj.get("rpm_limit").and_then(|v| v.as_u64()) {
                            slot.insert("rpm_limit".into(), serde_json::Value::Number(v.into()));
                        }
                        if let Some(wl) = obj.get("window_limits") {
                            slot.insert("window_limits".into(), wl.clone());
                        }
                        Some(serde_json::Value::Object(slot))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut plan_obj = serde_json::Map::new();
        if let Some(cl) = r.concurrency_limit {
            plan_obj.insert("concurrency_limit".into(), serde_json::Value::Number(cl.into()));
        }
        if let Some(rpm) = r.rpm_limit {
            plan_obj.insert("rpm_limit".into(), serde_json::Value::Number(rpm.into()));
        }
        if !window_limits.is_empty() {
            plan_obj.insert("window_limits".into(), serde_json::Value::Array(window_limits));
        }
        if !schedule.is_empty() {
            plan_obj.insert("schedule".into(), serde_json::Value::Array(schedule));
        }

        plans_map.insert(r.name.clone(), serde_json::Value::Object(plan_obj));
    }

    // ── Assemble top-level ──
    let mut plan_settings = serde_json::Map::new();
    if let Some(dp) = default_plan {
        plan_settings.insert("default_plan".into(), serde_json::Value::String(dp));
    }
    plan_settings.insert("plans".into(), serde_json::Value::Object(plans_map));

    Ok(serde_json::json!({
        "model_list": model_list,
        "router_settings": {
            "model_group_alias": model_group_alias,
        },
        "plan_settings": plan_settings,
    }))
}
