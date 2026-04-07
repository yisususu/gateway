use sqlx::PgPool;
use std::time::Instant;
use boom_core::types::AuthIdentity;
use boom_core::GatewayError;
use crate::state::AppState;

/// A single request log record.
pub struct RequestLog {
    pub request_id: Option<String>,
    pub key_hash: String,
    pub key_name: Option<String>,
    pub key_alias: Option<String>,
    pub team_id: Option<String>,
    pub model: String,
    pub api_path: String,
    pub is_stream: bool,
    pub status_code: u16,
    pub error_type: Option<String>,
    pub error_message: Option<String>,
    pub input_tokens: Option<i32>,
    pub output_tokens: Option<i32>,
    pub duration_ms: Option<i32>,
    pub deployment_id: Option<String>,
}

/// Fire-and-forget: spawn a tokio task to INSERT the log record.
/// Does nothing if `pool` is None (no DB configured).
/// Includes a 5s timeout to prevent log writes from starving the connection pool.
pub fn log_request(pool: Option<PgPool>, log: RequestLog) {
    if let Some(pool) = pool {
        tokio::spawn(async move {
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                sqlx::query(
                    r#"INSERT INTO boom_request_log
                       (request_id, key_hash, key_name, key_alias, team_id, model, api_path,
                        is_stream, status_code, error_type, error_message,
                        input_tokens, output_tokens, duration_ms, deployment_id)
                       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)"#,
                )
                .bind(&log.request_id)
                .bind(&log.key_hash)
                .bind(&log.key_name)
                .bind(&log.key_alias)
                .bind(&log.team_id)
                .bind(&log.model)
                .bind(&log.api_path)
                .bind(log.is_stream)
                .bind(log.status_code as i16)
                .bind(&log.error_type)
                .bind(&log.error_message)
                .bind(log.input_tokens)
                .bind(log.output_tokens)
                .bind(log.duration_ms)
                .bind(&log.deployment_id)
                .execute(&pool),
            )
            .await;

            match result {
                Ok(Err(e)) => tracing::debug!("Failed to insert request log: {}", e),
                Err(_) => tracing::warn!("Request log write timed out after 5s, dropping"),
                Ok(Ok(_)) => {}
            }
        });
    }
}

/// Helper to log an error from a route handler. Call this before returning the error.
pub fn log_error(
    state: &AppState,
    identity: &AuthIdentity,
    model: &str,
    api_path: &str,
    is_stream: bool,
    start: Instant,
    error: &GatewayError,
    request_id: Option<String>,
) {
    log_request(
        state.db_pool.clone(),
        RequestLog {
            request_id,
            key_hash: identity.key_hash.clone(),
            key_name: identity.key_name.clone(),
            key_alias: identity.key_alias.clone(),
            team_id: identity.team_id.clone(),
            model: model.to_string(),
            api_path: api_path.to_string(),
            is_stream,
            status_code: error.status_code(),
            error_type: Some(error.error_type().to_string()),
            error_message: Some(error.to_string()),
            input_tokens: None,
            output_tokens: None,
            duration_ms: Some(start.elapsed().as_millis() as i32),
            deployment_id: None,
        },
    );
}
