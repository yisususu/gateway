use sqlx::PgPool;

/// Run database migrations for all BooMGateway persistence tables.
///
/// DDL definitions are owned by their respective crates:
/// - boom_audit:      boom_request_log
/// - boom_routing:    boom_model_deployment, boom_model_alias
/// - boom_limiter:    boom_rate_limit_state, boom_key_plan_assignment, boom_rate_limit_plan
/// - boom_dashboard:  boom_config (generic KV store)
///
/// Called once at startup; uses CREATE TABLE IF NOT EXISTS for idempotency.
/// Requires PostgreSQL 13+ (for built-in gen_random_uuid()).
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // 1. Request logs (boom-audit).
    boom_audit::migrations::run_request_log_migration(pool).await?;

    // 2. Model deployments + aliases (boom-routing).
    run_ddl(pool, boom_routing::migrations::deployment_ddl()).await?;
    run_ddl(pool, boom_routing::migrations::alias_ddl()).await?;

    // 3. Rate limit state + assignments + plans (boom-limiter).
    run_ddl(pool, boom_limiter::migrations::rate_limit_state_ddl()).await?;
    run_ddl(pool, boom_limiter::migrations::assignment_ddl()).await?;
    run_ddl(pool, boom_limiter::migrations::plan_ddl()).await?;

    // 4. KV config store (dashboard-owned).
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_config (
            key         TEXT PRIMARY KEY,
            value       JSONB NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;

    tracing::info!("BooMGateway persistence tables ensured (7 tables)");
    Ok(())
}

/// Execute a multi-statement DDL string by splitting on ';'.
async fn run_ddl(pool: &PgPool, ddl: &str) -> Result<(), sqlx::Error> {
    for stmt in ddl.split(';') {
        let trimmed = stmt.trim();
        if !trimmed.is_empty() {
            sqlx::query(trimmed).execute(pool).await?;
        }
    }
    Ok(())
}
