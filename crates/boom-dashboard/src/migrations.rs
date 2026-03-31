use sqlx::PgPool;

/// Run database migrations for BooMGateway persistence tables.
/// Called once at startup; uses CREATE TABLE IF NOT EXISTS for idempotency.
pub async fn run_migrations(pool: &PgPool) -> Result<(), sqlx::Error> {
    // 1. Rate limit state checkpoint table.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_rate_limit_state (
            cache_key    TEXT PRIMARY KEY,
            count        BIGINT NOT NULL DEFAULT 0,
            window_start BIGINT NOT NULL,
            window_secs  BIGINT NOT NULL,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;

    // 2. Key→plan assignment persistence table.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_key_plan_assignment (
            key_hash     TEXT PRIMARY KEY,
            plan_name    TEXT NOT NULL,
            assigned_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;

    tracing::info!("BooMGateway persistence tables ensured");
    Ok(())
}
