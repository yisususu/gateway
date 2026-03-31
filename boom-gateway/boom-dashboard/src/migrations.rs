use sqlx::PgPool;

/// Run database migrations for BooMGateway persistence tables.
/// Called once at startup; uses CREATE TABLE IF NOT EXISTS for idempotency.
///
/// Note: requires PostgreSQL 13+ (for built-in gen_random_uuid()).
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

    // 3. KV config store (DB-first settings).
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_config (
            key         TEXT PRIMARY KEY,
            value       JSONB NOT NULL,
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;

    // 4. Model deployment definitions.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_model_deployment (
            id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            model_name        TEXT    NOT NULL,
            litellm_model     TEXT    NOT NULL,
            api_key           TEXT,
            api_key_env       BOOLEAN NOT NULL DEFAULT false,
            api_base          TEXT,
            api_version       TEXT,
            aws_region_name   TEXT,
            aws_access_key_id TEXT,
            aws_secret_access_key TEXT,
            rpm               BIGINT,
            tpm               BIGINT,
            timeout           BIGINT  NOT NULL DEFAULT 120,
            headers           JSONB   NOT NULL DEFAULT '{}',
            temperature       DOUBLE PRECISION,
            max_tokens        INTEGER,
            enabled           BOOLEAN NOT NULL DEFAULT true,
            source            TEXT    NOT NULL DEFAULT 'yaml',
            created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"CREATE INDEX IF NOT EXISTS idx_boom_deployment_model ON boom_model_deployment(model_name)"#,
    )
    .execute(pool)
    .await?;

    // 5. Model aliases.
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_model_alias (
            alias_name    TEXT PRIMARY KEY,
            target_model  TEXT    NOT NULL,
            hidden        BOOLEAN NOT NULL DEFAULT false,
            source        TEXT    NOT NULL DEFAULT 'yaml',
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;

    // 6. Plan definitions (persisted to DB for DB-first config).
    sqlx::query(
        r#"CREATE TABLE IF NOT EXISTS boom_rate_limit_plan (
            name              TEXT PRIMARY KEY,
            concurrency_limit INTEGER,
            rpm_limit         BIGINT,
            window_limits     JSONB  NOT NULL DEFAULT '[]',
            schedule          JSONB  NOT NULL DEFAULT '[]',
            is_default        BOOLEAN NOT NULL DEFAULT false,
            source            TEXT    NOT NULL DEFAULT 'yaml',
            updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )"#,
    )
    .execute(pool)
    .await?;

    sqlx::query(
        r#"CREATE UNIQUE INDEX IF NOT EXISTS idx_boom_plan_default
           ON boom_rate_limit_plan (is_default) WHERE is_default = true"#,
    )
    .execute(pool)
    .await?;

    tracing::info!("BooMGateway persistence tables ensured (6 tables)");
    Ok(())
}
