use crate::extractor::RequiredAuth;
use crate::state::AppState;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use boom_core::provider::RateLimiter;
use boom_core::types::*;
use boom_core::GatewayError;
use futures::StreamExt;
use std::convert::Infallible;

// ============================================================
// Chat Completions
// ============================================================

pub async fn chat_completions(
    State(state): State<AppState>,
    auth: RequiredAuth,
    Json(req): Json<ChatCompletionRequest>,
) -> Result<impl IntoResponse, GatewayErrorReply> {
    let identity = auth.identity();
    let inner = state.inner.load();

    // 1. Model access check.
    inner
        .auth
        .check_model_access(identity, &req.model)
        .map_err(GatewayErrorReply)?;

    // 2. Rate limit check.
    let rl_key = RateLimitKey {
        key_hash: identity.key_hash.clone(),
        model: req.model.clone(),
    };

    let window_limits: Vec<(u64, u64)> = inner
        .config
        .rate_limit
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

    let decision = state
        .limiter
        .check_and_record(&rl_key, identity.rpm_limit, &window_limits)
        .await
        .map_err(GatewayErrorReply)?;

    if !decision.allowed {
        return Err(GatewayErrorReply(GatewayError::RateLimitExceeded {
            retry_after_secs: decision.retry_after_secs,
            message: format!(
                "Rate limit exceeded. Limit: {} per minute.",
                decision.limit
            ),
        }));
    }

    // 3. Select provider deployment.
    let provider = state
        .select_deployment(&req.model)
        .ok_or_else(|| GatewayErrorReply(GatewayError::ModelNotFound(req.model.clone())))?;

    // 4. Route to provider (streaming or non-streaming).
    let is_stream = req.stream.unwrap_or(false);

    if is_stream {
        let stream = provider.chat_stream(req).await.map_err(GatewayErrorReply)?;
        let sse_stream = sse_stream_from_chat_stream(stream);
        let response = Sse::new(sse_stream).keep_alive(KeepAlive::default());
        Ok(response.into_response())
    } else {
        let response = provider.chat(req).await.map_err(GatewayErrorReply)?;
        Ok(Json(response).into_response())
    }
}

// ============================================================
// List Models
// ============================================================

pub async fn list_models(
    State(state): State<AppState>,
    auth: RequiredAuth,
) -> Result<Json<serde_json::Value>, GatewayErrorReply> {
    let _identity = auth.identity();
    let inner = state.inner.load();

    let models: Vec<ModelInfo> = inner
        .deployments
        .keys()
        .map(|name| ModelInfo {
            id: name.clone(),
            object: "model".to_string(),
            created: 0,
            owned_by: "boom-gateway".to_string(),
        })
        .collect();

    Ok(Json(serde_json::json!({
        "object": "list",
        "data": models,
    })))
}

// ============================================================
// Health Check
// ============================================================

#[derive(serde::Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
    pub uptime_secs: u64,
    pub db_connected: bool,
    pub models_count: usize,
    pub reload_count: u64,
    pub last_reload_at: Option<String>,
}

pub async fn health_check(State(state): State<AppState>) -> Json<HealthResponse> {
    let inner = state.inner.load();
    let uptime = chrono::Utc::now()
        .signed_duration_since(inner.health.started_at)
        .num_seconds();

    Json(HealthResponse {
        status: "ok".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        uptime_secs: uptime as u64,
        db_connected: inner.health.db_connected,
        models_count: inner.deployments.len(),
        reload_count: inner.health.reload_count,
        last_reload_at: Some(inner.health.last_reload_at.to_rfc3339()),
    })
}

pub async fn liveness_check() -> &'static str {
    "ok"
}

pub async fn readiness_check(State(state): State<AppState>) -> impl IntoResponse {
    let inner = state.inner.load();
    if inner.health.db_connected || inner.config.general_settings.database_url.is_none() {
        axum::http::StatusCode::OK
    } else {
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    }
}

// ============================================================
// Admin: Config Reload (hot-reload trigger)
// ============================================================

#[derive(serde::Serialize)]
pub struct ReloadResponse {
    pub status: String,
    pub message: String,
}

/// POST /admin/config/reload
///
/// Requires master key. Re-reads config.yaml and atomically swaps
/// the running state. New requests immediately see the new config;
/// in-flight requests complete with the old state untouched.
pub async fn admin_reload_config(
    State(state): State<AppState>,
    auth: RequiredAuth,
) -> Result<Json<ReloadResponse>, GatewayErrorReply> {
    // Only master key can trigger reload.
    let identity = auth.identity();
    if identity.key_hash != "master" {
        return Err(GatewayErrorReply(GatewayError::AuthError(
            "Only master key can trigger config reload".to_string(),
        )));
    }

    match state.reload().await {
        Ok(summary) => Ok(Json(ReloadResponse {
            status: "ok".to_string(),
            message: summary,
        })),
        Err(e) => Err(GatewayErrorReply(GatewayError::ConfigError(format!(
            "Reload failed: {}",
            e
        )))),
    }
}

// ============================================================
// Error Response — wrapper to satisfy Rust's orphan rules.
// ============================================================

pub struct GatewayErrorReply(pub GatewayError);

impl IntoResponse for GatewayErrorReply {
    fn into_response(self) -> axum::response::Response {
        let status = axum::http::StatusCode::from_u16(self.0.status_code())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

        let body = serde_json::json!({
            "error": {
                "message": self.0.to_string(),
                "type": self.0.error_type(),
                "code": self.0.status_code(),
            }
        });

        let mut response = (status, Json(body)).into_response();
        if let GatewayError::RateLimitExceeded {
            retry_after_secs: Some(secs),
            ..
        } = self.0
        {
            response.headers_mut().insert(
                axum::http::header::RETRY_AFTER,
                secs.to_string().parse().unwrap(),
            );
        }
        response
    }
}

impl From<GatewayError> for GatewayErrorReply {
    fn from(e: GatewayError) -> Self {
        GatewayErrorReply(e)
    }
}

// ============================================================
// Helpers
// ============================================================

fn sse_stream_from_chat_stream(
    stream: ChatStream,
) -> impl futures::Stream<Item = Result<Event, Infallible>> {
    stream.map(|result| match result {
        Ok(chunk) => {
            let data = serde_json::to_string(&chunk).unwrap_or_default();
            Ok(Event::default().data(data))
        }
        Err(e) => {
            let error_data =
                serde_json::to_string(&serde_json::json!({"error": e.to_string()}))
                    .unwrap_or_default();
            Ok(Event::default().data(error_data))
        }
    })
}
