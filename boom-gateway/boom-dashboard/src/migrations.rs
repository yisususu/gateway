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
    tracing::info!("Migration 1/7: request_log...");
    boom_audit::migrations::run_request_log_migration(pool).await?;
    tracing::info!("Migration 1/7: done");

    // 2. Model deployments + aliases (boom-routing).
    tracing::info!("Migration 2/7: deployment...");
    run_ddl(pool, boom_routing::migrations::deployment_ddl()).await?;
    tracing::info!("Migration 2/7: done");
    tracing::info!("Migration 3/7: alias...");
    run_ddl(pool, boom_routing::migrations::alias_ddl()).await?;
    tracing::info!("Migration 3/7: done");

    // 3. Rate limit state + assignments + plans (boom-limiter).
    tracing::info!("Migration 4/7: rate_limit_state...");
    run_ddl(pool, boom_limiter::migrations::rate_limit_state_ddl()).await?;
    tracing::info!("Migration 4/7: done");
    tracing::info!("Migration 5/7: assignment...");
    run_ddl(pool, boom_limiter::migrations::assignment_ddl()).await?;
    tracing::info!("Migration 5/7: done");
    tracing::info!("Migration 6/7: plan...");
    run_ddl(pool, boom_limiter::migrations::plan_ddl()).await?;
    tracing::info!("Migration 6/7: done");

    // 4. KV config store (dashboard-owned).
    tracing::info!("Migration 7/7: boom_config...");
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_config (
            key         TEXT PRIMARY KEY,
            value       JSONB NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;
    tracing::info!("Migration 7/7: done");

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
