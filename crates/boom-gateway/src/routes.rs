use crate::extractor::RequiredAuth;
use crate::state::AppState;
use axum::extract::State;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use boom_core::anthropic::{
    anthropic_request_to_openai, openai_response_to_anthropic, AnthropicStreamTranscoder,
};
use boom_core::provider::RateLimiter;
use boom_core::types::*;
use boom_core::GatewayError;
use futures::StreamExt;
use std::collections::HashMap;
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

    // 1. Model access check (deployment-aware, alias-aware).
    check_model_access(identity, &req.model, &inner.deployments, &inner.model_aliases)
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
    let identity = auth.identity();
    let inner = state.inner.load();

    // Collect all visible model names (deployments + non-hidden aliases, excluding "*").
    let all_names: Vec<String> = inner.visible_model_names();

    let visible: Vec<ModelInfo> = if identity.models.is_empty() {
        // Unrestricted key — show all visible models.
        all_names
            .iter()
            .map(|name| ModelInfo {
                id: name.clone(),
                object: "model".to_string(),
                created: 0,
                owned_by: "boom-gateway".to_string(),
            })
            .collect()
    } else {
        // Restricted key — only show models the key has access to.
        // Check both direct match and alias match (alias ↔ target).
        all_names
            .iter()
            .filter(|name| {
                // Direct match in key's allowed list.
                if identity.models.iter().any(|m| m == *name && m != "*") {
                    return true;
                }
                // If name is an alias, check if key has access to target model.
                if let Some(target) = inner.model_aliases.get(*name) {
                    if identity.models.iter().any(|m| m == target && m != "*") {
                        return true;
                    }
                }
                // If name is a deployment (target), check if key has an alias that maps to it.
                for allowed in &identity.models {
                    if allowed == "*" {
                        continue;
                    }
                    if let Some(target) = inner.model_aliases.get(allowed) {
                        if target == *name {
                            return true;
                        }
                    }
                }
                false
            })
            .map(|name| ModelInfo {
                id: name.clone(),
                object: "model".to_string(),
                created: 0,
                owned_by: "boom-gateway".to_string(),
            })
            .collect()
    };

    Ok(Json(serde_json::json!({
        "object": "list",
        "data": visible,
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
// Model Access Check (deployment-aware)
// ============================================================

/// Check if an identity can access the given model, considering the gateway's
/// configured deployments and model aliases.
///
/// Logic:
/// 1. Unrestricted key (models empty after resolution) → allow
/// 2. Model in key_models → allow (direct match)
/// 3. Alias match: if model is an alias, check if target is in key_models
/// 4. Reverse alias: if key_models contains an alias that targets this model → allow
/// 5. Model NOT in key_models but IS configured in gateway → REJECT
///    (model exists, user has no access)
/// 6. Model NOT in key_models and NOT in gateway, but key has "*" → allow
///    (best-effort: route through catch-all deployment)
/// 7. Otherwise → REJECT
fn check_model_access<V>(
    identity: &AuthIdentity,
    model: &str,
    deployments: &HashMap<String, V>,
    model_aliases: &HashMap<String, String>,
) -> Result<(), GatewayError> {
    // Unrestricted key
    if identity.models.is_empty() {
        tracing::info!(
            "check_model_access: key={:?}, model={}, result=allow (unrestricted)",
            identity.key_name, model
        );
        return Ok(());
    }

    // Direct match
    if identity.models.iter().any(|m| m == model) {
        tracing::info!(
            "check_model_access: key={:?}, model={}, result=allow (direct match)",
            identity.key_name, model
        );
        return Ok(());
    }

    // Alias match: requested model is an alias → check if target is in key_models
    if let Some(target) = model_aliases.get(model) {
        if identity.models.iter().any(|m| m == target) {
            tracing::info!(
                "check_model_access: key={:?}, model={}, result=allow (alias → target={})",
                identity.key_name, model, target
            );
            return Ok(());
        }
    }

    // Reverse alias: key_models has an alias that targets the requested model
    for allowed in &identity.models {
        if let Some(target) = model_aliases.get(allowed) {
            if target == model {
                tracing::info!(
                    "check_model_access: key={:?}, model={}, result=allow (key has alias '{}' → this model)",
                    identity.key_name, model, allowed
                );
                return Ok(());
            }
        }
    }

    // Not in key_models — check if it's a configured model or a wildcard case
    let has_wildcard = identity.models.iter().any(|m| m == "*");
    let model_configured = deployments.contains_key(model);

    if model_configured {
        // Model exists in config but user doesn't have access → REJECT
        tracing::warn!(
            "check_model_access: key={:?}, model={}, result=deny (configured model, not in key_models={:?})",
            identity.key_name, model, identity.models
        );
        return Err(GatewayError::ModelNotAllowed(model.to_string()));
    }

    if has_wildcard {
        // Model not in config, user has "*" → allow (route through catch-all)
        tracing::info!(
            "check_model_access: key={:?}, model={}, result=allow (wildcard fallback)",
            identity.key_name, model
        );
        return Ok(());
    }

    tracing::warn!(
        "check_model_access: key={:?}, model={}, result=deny (no match, no wildcard, key_models={:?})",
        identity.key_name, model, identity.models
    );
    Err(GatewayError::ModelNotAllowed(model.to_string()))
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

// ============================================================
// Anthropic Messages
// ============================================================

pub async fn messages(
    State(state): State<AppState>,
    auth: RequiredAuth,
    Json(req): Json<AnthropicMessagesRequest>,
) -> Result<impl IntoResponse, AnthropicErrorReply> {
    let openai_req = anthropic_request_to_openai(&req);
    let identity = auth.identity();
    let inner = state.inner.load();

    // 1. Model access check (deployment-aware, alias-aware).
    check_model_access(identity, &openai_req.model, &inner.deployments, &inner.model_aliases)
        .map_err(AnthropicErrorReply)?;

    // 2. Rate limit check.
    let rl_key = RateLimitKey {
        key_hash: identity.key_hash.clone(),
        model: openai_req.model.clone(),
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
        .map_err(AnthropicErrorReply)?;

    if !decision.allowed {
        return Err(AnthropicErrorReply(GatewayError::RateLimitExceeded {
            retry_after_secs: decision.retry_after_secs,
            message: format!(
                "Rate limit exceeded. Limit: {} per minute.",
                decision.limit
            ),
        }));
    }

    // 3. Select provider deployment.
    let provider = state
        .select_deployment(&openai_req.model)
        .ok_or_else(|| AnthropicErrorReply(GatewayError::ModelNotFound(openai_req.model.clone())))?;

    // 4. Route to provider.
    let is_stream = openai_req.stream.unwrap_or(false);

    if is_stream {
        let model = openai_req.model.clone();
        let stream = provider.chat_stream(openai_req).await.map_err(AnthropicErrorReply)?;
        let sse_stream = sse_stream_from_anthropic_chat_stream(stream, model);
        let response = Sse::new(sse_stream).keep_alive(KeepAlive::default());
        Ok(response.into_response())
    } else {
        let response = provider.chat(openai_req).await.map_err(AnthropicErrorReply)?;
        let anthropic_resp = openai_response_to_anthropic(&response);
        Ok(Json(anthropic_resp).into_response())
    }
}

/// Convert an OpenAI stream into Anthropic-format SSE events via a transcoder + channel.
fn sse_stream_from_anthropic_chat_stream(
    stream: ChatStream,
    model: String,
) -> impl futures::Stream<Item = Result<Event, Infallible>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(64);

    tokio::spawn(async move {
        let mut transcoder = AnthropicStreamTranscoder::new(model);
        let mut stream = std::pin::pin!(stream);

        while let Some(result) = stream.next().await {
            match result {
                Ok(chunk) => {
                    let events = transcoder.transcode(&chunk);
                    for ev in events {
                        let axum_event = Event::default()
                            .event(&ev.event)
                            .data(ev.data);
                        if tx.send(axum_event).await.is_err() {
                            return;
                        }
                    }
                }
                Err(e) => {
                    let error_data = serde_json::json!({
                        "type": "error",
                        "error": { "type": "api_error", "message": e.to_string() }
                    });
                    let _ = tx
                        .send(
                            Event::default()
                                .event("error")
                                .data(error_data.to_string()),
                        )
                        .await;
                    return;
                }
            }
        }
    });

    tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok)
}

// ============================================================
// Anthropic Error Response
// ============================================================

pub struct AnthropicErrorReply(pub GatewayError);

impl IntoResponse for AnthropicErrorReply {
    fn into_response(self) -> axum::response::Response {
        let status = axum::http::StatusCode::from_u16(self.0.status_code())
            .unwrap_or(axum::http::StatusCode::INTERNAL_SERVER_ERROR);

        let body = serde_json::json!({
            "type": "error",
            "error": {
                "type": self.0.error_type(),
                "message": self.0.to_string(),
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

impl From<GatewayError> for AnthropicErrorReply {
    fn from(e: GatewayError) -> Self {
        AnthropicErrorReply(e)
    }
}
