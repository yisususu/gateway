/// DDL for boom_model_deployment table.
pub fn deployment_ddl() -> &'static str {
    r#"
CREATE TABLE IF NOT EXISTS boom_model_deployment (
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
    timeout           BIGINT  NOT NULL DEFAULT 1200,
    headers           JSONB   NOT NULL DEFAULT '{}',
    temperature       DOUBLE PRECISION,
    max_tokens        INTEGER,
    enabled           BOOLEAN NOT NULL DEFAULT true,
    source            TEXT    NOT NULL DEFAULT 'yaml',
    deployment_id     TEXT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_boom_deployment_model ON boom_model_deployment(model_name);
"#
}

/// DDL for boom_model_alias table.
pub fn alias_ddl() -> &'static str {
    r#"
CREATE TABLE IF NOT EXISTS boom_model_alias (
    alias_name    TEXT PRIMARY KEY,
    target_model  TEXT    NOT NULL,
    hidden        BOOLEAN NOT NULL DEFAULT false,
    source        TEXT    NOT NULL DEFAULT 'yaml',
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"#
}
