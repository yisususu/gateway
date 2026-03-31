use arc_swap::ArcSwap;
use boom_auth::DbAuthenticator;
use boom_config::Config;
use boom_core::provider::{Authenticator, Provider};
use boom_limiter::{PlanStore, RateLimitPlan, ScheduleSlot, SlidingWindowLimiter};
use boom_provider;
use sqlx::PgPool;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Shared application state.
///
/// `inner` is wrapped in `ArcSwap` for lock-free atomic hot-swap:
///   - New requests immediately see the reloaded config.
///   - In-flight requests keep using the old state until done.
///   - Zero downtime, no races.
///
/// `db_pool` and `limiter` live at this level — they survive reloads
/// so DB connections and rate-limit counters are preserved.
#[derive(Clone)]
pub struct AppState {
    /// Config file path (stored for reload).
    pub config_path: String,
    /// Hot-swappable inner state.
    pub inner: Arc<ArcSwap<AppStateInner>>,
    /// DB pool survives reloads (avoids reconnection).
    pub db_pool: Option<PgPool>,
    /// Limiter survives reloads (preserves in-flight counters).
    pub limiter: Arc<SlidingWindowLimiter>,
    /// Plan store survives reloads (preserves plan definitions and key assignments).
    pub plan_store: Arc<PlanStore>,
}

/// The state that gets swapped on config reload.
pub struct AppStateInner {
    pub config: Config,
    /// model_name → list of provider deployments (for load balancing).
    pub deployments: HashMap<String, Vec<Arc<dyn Provider>>>,
    /// model_name → round-robin index.
    pub rr_counters: std::sync::Mutex<HashMap<String, usize>>,
    pub auth: Arc<dyn Authenticator>,
    pub health: HealthStatus,
    /// alias_name → target_model_name (all aliases, including hidden).
    pub model_aliases: HashMap<String, String>,
    /// Alias names that should NOT appear in model list.
    pub hidden_aliases: HashSet<String>,
}

impl AppStateInner {
    /// Return all model names that should be visible in the model list.
    /// Includes deployment keys (except "*") plus non-hidden aliases.
    pub fn visible_model_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self
            .deployments
            .keys()
            .filter(|k| *k != "*")
            .cloned()
            .collect();

        for alias in self.model_aliases.keys() {
            if !self.hidden_aliases.contains(alias) {
                names.push(alias.clone());
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

        // 3. Build inner state.
        let inner = Self::build_inner(
            config,
            &db_pool,
            &limiter,
            chrono::Utc::now(),
            0,
        )?;

        // 4. Load plans from config into plan store.
        load_plans_from_config(&plan_store, &inner.config);

        // 5. Run migrations and restore persisted state from DB.
        if let Some(ref pool) = db_pool {
            restore_from_db(pool, &plan_store, &limiter).await;
        }

        Ok(Self {
            config_path,
            inner: Arc::new(ArcSwap::from_pointee(inner)),
            db_pool,
            limiter,
            plan_store,
        })
    }

    /// Hot-reload: re-read config file and swap state atomically.
    ///
    /// - Re-reads the YAML config from disk.
    /// - Rebuilds providers (cheap — just HTTP clients).
    /// - Reuses existing DB pool and limiter (preserves connections & counters).
    /// - Atomically swaps inner state via ArcSwap.
    pub async fn reload(&self) -> anyhow::Result<String> {
        tracing::info!("Hot-reloading config from {}...", self.config_path);

        // 1. Re-read config.
        let new_config = boom_config::load_config(&self.config_path)?;

        // 2. Snapshot old state to get counts and started_at.
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
        let model_count = new_config.model_list.len();

        // 4. Build new inner state.
        let new_inner = Self::build_inner(
            new_config,
            &db_pool,
            &self.limiter,
            old_started_at,
            new_reload_count,
        )?;

        // 5. Reload plans from config.
        load_plans_from_config(&self.plan_store, &new_inner.config);

        // 6. Atomic swap — new requests immediately see new state.
        self.inner.store(Arc::new(new_inner));

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
        _limiter: &Arc<SlidingWindowLimiter>,
        started_at: chrono::DateTime<chrono::Utc>,
        reload_count: u64,
    ) -> Result<AppStateInner, anyhow::Error> {
        // Build authenticator.
        let auth: Arc<dyn Authenticator> = Arc::new(DbAuthenticator::new(
            db_pool.clone(),
            config.general_settings.master_key.clone(),
        ));

        // Build provider deployments.
        let mut deployments: HashMap<String, Vec<Arc<dyn Provider>>> = HashMap::new();
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
                    deployments
                        .entry(entry.model_name.clone())
                        .or_default()
                        .push(provider);
                }
                Err(e) => {
                    tracing::error!(
                        "Failed to create provider for model '{}': {}",
                        entry.model_name,
                        e
                    );
                    // Skip this deployment but don't crash — other models can still work.
                }
            }
        }

        tracing::info!(
            "Initialized {} model(s) with {} deployment(s)",
            deployments.len(),
            deployments.values().map(|v| v.len()).sum::<usize>(),
        );

        // Build model alias mappings from config.
        let mut model_aliases: HashMap<String, String> = HashMap::new();
        let mut hidden_aliases: HashSet<String> = HashSet::new();
        for (alias, alias_cfg) in &config.router_settings.model_group_alias {
            let target = alias_cfg.target_model();
            if !deployments.contains_key(target) {
                tracing::warn!(
                    "Skipping alias '{}' → '{}': target model not found in deployments",
                    alias, target
                );
                continue;
            }
            tracing::info!("Model alias: '{}' → '{}'", alias, target);
            model_aliases.insert(alias.clone(), target.to_string());
            if alias_cfg.is_hidden() {
                hidden_aliases.insert(alias.clone());
            }
        }
        tracing::info!(
            "Loaded {} model alias(es), {} hidden",
            model_aliases.len(),
            hidden_aliases.len(),
        );

        let health = HealthStatus {
            started_at,
            last_reload_at: chrono::Utc::now(),
            db_connected: db_pool.is_some(),
            reload_count,
        };

        Ok(AppStateInner {
            config,
            deployments,
            rr_counters: std::sync::Mutex::new(HashMap::new()),
            auth,
            health,
            model_aliases,
            hidden_aliases,
        })
    }

    /// Select a provider deployment for the given model.
    ///
    /// 1. Try exact match on model name.
    /// 2. If not found, try resolving via model alias.
    /// 3. If still not found, fall back to the "*" catch-all deployment (if configured).
    ///    Round-robin within the selected deployment group.
    pub fn select_deployment(&self, model: &str) -> Option<Arc<dyn Provider>> {
        let inner = self.inner.load();

        // Exact match first, then alias resolution, then fallback to catch-all "*".
        let (key, providers) = if let Some(p) = inner.deployments.get(model) {
            (model, p)
        } else if let Some(target) = inner.model_aliases.get(model) {
            if let Some(p) = inner.deployments.get(target.as_str()) {
                (target.as_str(), p)
            } else {
                // Alias target not in deployments — should have been warned during build.
                inner
                    .deployments
                    .get("*")
                    .map(|p| ("*", p))?
            }
        } else {
            inner
                .deployments
                .get("*")
                .map(|p| ("*", p))?
        };

        if providers.is_empty() {
            return None;
        }
        if providers.len() == 1 {
            return Some(providers[0].clone());
        }

        let mut counters = inner.rr_counters.lock().unwrap();
        let idx = counters.entry(key.to_string()).or_insert(0);
        let selected = providers[*idx % providers.len()].clone();
        *idx = (*idx + 1) % providers.len();
        Some(selected)
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

/// Load plans from YAML config into PlanStore.
/// Upserts config-defined plans and sets the default plan.
fn load_plans_from_config(plan_store: &Arc<PlanStore>, config: &Config) {
    // Upsert plans defined in config.
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

    // Set default plan.
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

/// Run migrations and restore persisted rate-limit state & assignments from DB.
async fn restore_from_db(
    pool: &PgPool,
    plan_store: &Arc<PlanStore>,
    limiter: &Arc<SlidingWindowLimiter>,
) {
    // 1. Run migrations (CREATE TABLE IF NOT EXISTS).
    if let Err(e) = boom_dashboard::migrations::run_migrations(pool).await {
        tracing::error!("Failed to run migrations: {}", e);
        return;
    }

    // 2. Restore key→plan assignments.
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

    // 3. Restore rate limit counters (non-expired only).
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
            for (cache_key, count, window_start, window_secs) in rows {
                limiter.restore_counter(
                    cache_key,
                    count as u64,
                    window_start as u64,
                    window_secs as u64,
                );
            }
            tracing::info!("Restored {} rate limit counter(s) from DB", count);
        }
        Err(e) => {
            tracing::error!("Failed to restore rate limit state: {}", e);
        }
    }
}
