use arc_swap::ArcSwap;
use boom_auth::DbAuthenticator;
use boom_config::Config;
use boom_core::provider::{Authenticator, Provider};
use boom_limiter::{
    AliasStore, DeploymentStore, PlanStore, RateLimitPlan, ScheduleSlot, SlidingWindowLimiter,
};
use boom_provider;
use sqlx::PgPool;
use std::sync::Arc;

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
}

/// The state that gets swapped on config reload.
/// Only contains config, auth, and health — deployments/aliases live in stores.
pub struct AppStateInner {
    pub config: Config,
    pub auth: Arc<dyn Authenticator>,
    pub health: HealthStatus,
}

impl AppStateInner {
    /// Return all model names that should be visible in the model list.
    /// Includes deployment keys (except "*") plus non-hidden aliases.
    /// Delegates to the stores passed in.
    #[allow(dead_code)]
    pub fn visible_model_names(
        &self,
        deployment_store: &DeploymentStore,
        alias_store: &AliasStore,
    ) -> Vec<String> {
        let mut names: Vec<String> = deployment_store
            .model_names()
            .into_iter()
            .filter(|k| k != "*")
            .collect();

        for alias_name in alias_store.visible_names() {
            if !names.contains(&alias_name) {
                names.push(alias_name);
            }
        }

        names
    }
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
    pub async fn from_config(config: Config, config_path: String) -> anyhow::Result<Self> {
        // 1. Connect to database (optional).
        let db_pool = match &config.general_settings.database_url {
            Some(url) => {
                tracing::info!("Connecting to database...");
                let pool = PgPool::connect(url).await?;
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

        // 5. Run migrations and seed/load from DB.
        let store_model_in_db = config.general_settings.store_model_in_db;

        if let Some(ref pool) = db_pool {
            // Run migrations (all 6 tables).
            if let Err(e) = boom_dashboard::migrations::run_migrations(pool).await {
                tracing::error!("Failed to run migrations: {}", e);
            }

            if store_model_in_db {
                // ─── DB-first mode (store_model_in_db: true) ───
                tracing::info!("store_model_in_db=true — DB is the authority for models/aliases/plans");

                // Seed from YAML if first time, then load from DB.
                let db_seeded: bool = sqlx::query_scalar::<_, bool>(
                    r#"SELECT EXISTS(SELECT 1 FROM boom_config WHERE key = 'db_seeded')"#,
                )
                .fetch_one(pool)
                .await
                .unwrap_or(false);

                if !db_seeded {
                    tracing::info!("First run — seeding DB from YAML config...");
                    if let Err(e) = seed_from_yaml(pool, &config).await {
                        tracing::error!("Failed to seed DB from YAML: {}", e);
                    }
                } else {
                    tracing::info!("DB already seeded — loading config from DB");
                }

                // Load deployments from DB → build providers → DeploymentStore.
                load_deployments_from_db(pool, &deployment_store, &alias_store).await;

                // Load aliases from DB → AliasStore.
                load_aliases_from_db(pool, &alias_store).await;

                // Load plans from DB → PlanStore.
                load_plans_from_db(pool, &plan_store).await;
            } else {
                // ─── YAML-first mode (store_model_in_db: false, default) ───
                tracing::info!("store_model_in_db=false — YAML is the authority, DB only for runtime state");

                // Build deployments and aliases directly from YAML.
                build_deployments_from_config(&config, &deployment_store);
                build_aliases_from_config(&config, &alias_store, &deployment_store);
                load_plans_from_config(&plan_store, &config);
            }

            // These always load from DB regardless of mode.
            // Restore key→plan assignments from DB.
            restore_assignments_from_db(pool, &plan_store).await;

            // Restore rate limit counters from DB.
            restore_counters_from_db(pool, &limiter).await;

            // Override config from boom_config table.
            apply_db_config_overrides(pool, &config).await;
        } else {
            // No DB — build deployments and aliases directly from YAML.
            build_deployments_from_config(&config, &deployment_store);
            build_aliases_from_config(&config, &alias_store, &deployment_store);
            load_plans_from_config(&plan_store, &config);
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
        })
    }

    /// Hot-reload: re-read config file and update state.
    ///
    /// With DB-first mode:
    /// - Only source='yaml' DB records are updated from the new YAML.
    /// - source='db' records are preserved.
    /// - Deployment/Alias stores are rebuilt from DB after YAML merge.
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
                Some(url) => Some(PgPool::connect(url).await?),
                None => None,
            }
        } else {
            self.db_pool.clone()
        };

        let new_reload_count = old_reload_count + 1;

        // 4. Update stores based on store_model_in_db setting.
        let store_model_in_db = new_config.general_settings.store_model_in_db;

        if let Some(ref pool) = db_pool {
            if store_model_in_db {
                // ─── DB-first mode (store_model_in_db: true) ───
                // Re-seed YAML records (only source='yaml' rows), then reload from DB.
                if let Err(e) = reseed_yaml_in_db(pool, &new_config).await {
                    tracing::error!("Failed to re-seed YAML records: {}", e);
                }

                self.deployment_store.clear();
                load_deployments_from_db(pool, &self.deployment_store, &self.alias_store).await;

                self.alias_store.clear();
                load_aliases_from_db(pool, &self.alias_store).await;

                load_plans_from_db(pool, &self.plan_store).await;
            } else {
                // ─── YAML-first mode (store_model_in_db: false, default) ───
                // DB only for runtime state (assignments, counters), not models/aliases/plans.
                self.deployment_store.clear();
                build_deployments_from_config(&new_config, &self.deployment_store);

                self.alias_store.clear();
                build_aliases_from_config(&new_config, &self.alias_store, &self.deployment_store);

                load_plans_from_config(&self.plan_store, &new_config);
            }
        } else {
            // No DB — rebuild from YAML directly.
            self.deployment_store.clear();
            build_deployments_from_config(&new_config, &self.deployment_store);

            self.alias_store.clear();
            build_aliases_from_config(&new_config, &self.alias_store, &self.deployment_store);

            load_plans_from_config(&self.plan_store, &new_config);
        }

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

    /// Select a provider deployment for the given model.
    ///
    /// 1. Try exact match on model name in deployment store.
    /// 2. If not found, try resolving via alias store.
    /// 3. If still not found, fall back to the "*" catch-all deployment.
    ///    Round-robin within the selected deployment group.
    pub fn select_deployment(&self, model: &str) -> Option<Arc<dyn Provider>> {
        // Exact match first.
        if let Some(provider) = self.deployment_store.select(model) {
            return Some(provider);
        }

        // Alias resolution.
        if let Some(target) = self.alias_store.resolve(model) {
            if let Some(provider) = self.deployment_store.select(&target) {
                return Some(provider);
            }
        }

        // Fallback to catch-all "*".
        self.deployment_store.select("*")
    }
}

// ═══════════════════════════════════════════════════════════
// YAML → DB seeding (first run)
// ═══════════════════════════════════════════════════════════

/// Seed all YAML config into DB tables (first run only).
async fn seed_from_yaml(pool: &PgPool, config: &Config) -> Result<(), sqlx::Error> {
    // 1. Seed model deployments.
    // Delete old yaml-sourced rows first (in case of partial seed).
    sqlx::query(r#"DELETE FROM boom_model_deployment WHERE source = 'yaml'"#)
        .execute(pool)
        .await?;

    for entry in &config.model_list {
        let p = &entry.litellm_params;
        let headers_json = serde_json::to_value(&p.headers).unwrap_or(serde_json::json!({}));
        sqlx::query(
            r#"INSERT INTO boom_model_deployment
               (model_name, litellm_model, api_key, api_key_env, api_base, api_version,
                aws_region_name, aws_access_key_id, aws_secret_access_key,
                rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, true, 'yaml')"#,
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
        .execute(pool)
        .await?;
    }
    tracing::info!("Seeded {} model deployment(s) from YAML", config.model_list.len());

    // 2. Seed model aliases.
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
    tracing::info!(
        "Seeded {} alias(es) from YAML",
        config.router_settings.model_group_alias.len()
    );

    // 3. Seed plans.
    sqlx::query(r#"DELETE FROM boom_rate_limit_plan WHERE source = 'yaml'"#)
        .execute(pool)
        .await?;

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
    tracing::info!("Seeded {} plan(s) from YAML", config.plan_settings.plans.len());

    // 4. Seed boom_config (rate_limit/server settings).
    let rl_json = serde_json::to_value(&config.rate_limit).unwrap_or(serde_json::json!({}));
    sqlx::query(
        r#"INSERT INTO boom_config (key, value) VALUES ('rate_limit', $1)
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()"#,
    )
    .bind(&rl_json)
    .execute(pool)
    .await?;

    if let Some(ref dp) = config.plan_settings.default_plan {
        sqlx::query(
            r#"INSERT INTO boom_config (key, value) VALUES ('plan_settings.default_plan', $1)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()"#,
        )
        .bind(serde_json::json!(dp))
        .execute(pool)
        .await?;
    }

    // 5. Mark as seeded.
    sqlx::query(
        r#"INSERT INTO boom_config (key, value) VALUES ('db_seeded', 'true'::JSONB)
           ON CONFLICT (key) DO NOTHING"#,
    )
    .execute(pool)
    .await?;

    tracing::info!("DB seeded from YAML successfully");
    Ok(())
}

/// Re-seed only source='yaml' rows from the current YAML config.
/// source='db' rows are untouched.
async fn reseed_yaml_in_db(pool: &PgPool, config: &Config) -> Result<(), sqlx::Error> {
    // Re-seed deployments (yaml source only).
    sqlx::query(r#"DELETE FROM boom_model_deployment WHERE source = 'yaml'"#)
        .execute(pool)
        .await?;

    for entry in &config.model_list {
        let p = &entry.litellm_params;
        let headers_json = serde_json::to_value(&p.headers).unwrap_or(serde_json::json!({}));
        sqlx::query(
            r#"INSERT INTO boom_model_deployment
               (model_name, litellm_model, api_key, api_key_env, api_base, api_version,
                aws_region_name, aws_access_key_id, aws_secret_access_key,
                rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source)
               VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, true, 'yaml')"#,
        )
        .bind(&entry.model_name)
        .bind(&p.model)
        .bind(&p.api_key)
        .bind(false)
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
        .execute(pool)
        .await?;
    }

    // Re-seed aliases (yaml source only).
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

    // Re-seed plans (yaml source only).
    sqlx::query(r#"DELETE FROM boom_rate_limit_plan WHERE source = 'yaml'"#)
        .execute(pool)
        .await?;

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

    tracing::info!("Re-seeded YAML records in DB");
    Ok(())
}

// ═══════════════════════════════════════════════════════════
// DB → Memory loading
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
}

/// Load model deployments from DB and build providers → DeploymentStore.
async fn load_deployments_from_db(
    pool: &PgPool,
    deployment_store: &Arc<DeploymentStore>,
    _alias_store: &Arc<AliasStore>,
) {
    let rows: Vec<DeploymentRow> = match sqlx::query_as::<_, DeploymentRow>(
        r#"SELECT id, model_name, litellm_model, api_key, api_key_env, api_base, api_version,
                  aws_region_name, aws_access_key_id, aws_secret_access_key,
                  rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source
           FROM boom_model_deployment
           WHERE enabled IS NOT FALSE
           ORDER BY model_name, created_at"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to load deployments from DB: {}", e);
            return;
        }
    };

    let mut model_count = 0;
    let mut deployment_count = 0;

    // Group by model_name and build providers.
    // We need to collect per model_name since each row becomes a provider.
    let mut grouped: std::collections::HashMap<String, Vec<Arc<dyn Provider>>> =
        std::collections::HashMap::new();

    for row in &rows {
        let mut extra = std::collections::HashMap::new();
        // Parse headers from JSONB.
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

        // Resolve api_key (may be env reference).
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
        ) {
            Ok(provider) => {
                grouped
                    .entry(row.model_name.clone())
                    .or_default()
                    .push(provider);
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

    for (model_name, providers) in grouped {
        model_count += 1;
        deployment_store.set_deployments(model_name, providers);
    }

    tracing::info!(
        "Loaded {} model(s) with {} deployment(s) from DB",
        model_count,
        deployment_count,
    );
}

/// Row from boom_model_alias.
#[derive(Debug, sqlx::FromRow)]
struct AliasRow {
    alias_name: String,
    target_model: String,
    hidden: Option<bool>,
}

/// Load model aliases from DB → AliasStore.
async fn load_aliases_from_db(pool: &PgPool, alias_store: &Arc<AliasStore>) {
    let rows: Vec<AliasRow> = match sqlx::query_as::<_, AliasRow>(
        r#"SELECT alias_name, target_model, hidden FROM boom_model_alias"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to load aliases from DB: {}", e);
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

    tracing::info!("Loaded {} alias(es) from DB", rows.len());
}

/// Row from boom_rate_limit_plan.
#[derive(Debug, sqlx::FromRow)]
struct PlanRow {
    name: String,
    concurrency_limit: Option<i32>,
    rpm_limit: Option<i64>,
    window_limits: serde_json::Value,
    schedule: serde_json::Value,
    is_default: Option<bool>,
}

/// Load plans from DB → PlanStore.
async fn load_plans_from_db(pool: &PgPool, plan_store: &Arc<PlanStore>) {
    let rows: Vec<PlanRow> = match sqlx::query_as::<_, PlanRow>(
        r#"SELECT name, concurrency_limit, rpm_limit, window_limits, schedule, is_default
           FROM boom_rate_limit_plan"#,
    )
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to load plans from DB: {}", e);
            return;
        }
    };

    let mut default_plan_name: Option<String> = None;

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

        if row.is_default.unwrap_or(false) {
            default_plan_name = Some(row.name.clone());
        }

        plan_store.upsert_plan(plan);
    }

    if let Some(dp) = default_plan_name {
        plan_store.set_default_plan(Some(dp));
        tracing::info!("Default plan set from DB");
    }

    tracing::info!("Loaded {} plan(s) from DB", rows.len());
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

/// Override config values from boom_config table.
async fn apply_db_config_overrides(pool: &PgPool, _config: &Config) {
    // Currently we read rate_limit and plan settings from DB into stores directly.
    // This is a hook for future overrides (e.g., server.host from DB).
    let _ = pool; // suppress unused warning
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

        match boom_provider::create_provider(
            &p.model,
            p.api_key.clone(),
            p.api_base.clone(),
            p.timeout,
            &extra,
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
        tracing::info!(plan = %name, "Loaded plan from config");
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
