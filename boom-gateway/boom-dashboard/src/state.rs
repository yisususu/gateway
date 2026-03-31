use boom_limiter::{AliasStore, DeploymentStore, PlanStore, SlidingWindowLimiter};
use sqlx::PgPool;
use std::sync::Arc;

/// Dashboard-specific state, injected via Extension layer.
/// Independent from boom-gateway's AppState to avoid type coupling.
#[derive(Clone)]
pub struct DashboardState {
    pub db_pool: Option<PgPool>,
    pub plan_store: Arc<PlanStore>,
    pub limiter: Arc<SlidingWindowLimiter>,
    /// Deployment store for model CRUD.
    pub deployment_store: Arc<DeploymentStore>,
    /// Alias store for alias CRUD.
    pub alias_store: Arc<AliasStore>,
    /// JWT signing key (derived from master_key at startup).
    pub jwt_secret: String,
    /// Master key for admin login (constant-time comparison).
    pub master_key: Option<String>,
}

impl DashboardState {
    pub fn new(
        db_pool: Option<PgPool>,
        plan_store: Arc<PlanStore>,
        limiter: Arc<SlidingWindowLimiter>,
        deployment_store: Arc<DeploymentStore>,
        alias_store: Arc<AliasStore>,
        master_key: Option<String>,
    ) -> Self {
        // Derive JWT secret from master_key, or use a random fallback.
        let jwt_secret = master_key
            .as_deref()
            .unwrap_or("boom-dashboard-default-secret")
            .to_string();
        Self {
            db_pool,
            plan_store,
            limiter,
            deployment_store,
            alias_store,
            jwt_secret,
            master_key,
        }
    }
}
