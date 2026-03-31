use crate::extractor::RequiredAuth;
use crate::state::AppState;
use axum::extract::{Path, State};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::Json;
use boom_core::anthropic::{
    anthropic_request_to_openai, openai_response_to_anthropic, AnthropicStreamTranscoder,
};
use boom_core::provider::RateLimiter;
use boom_core::types::*;
use boom_core::GatewayError;
use boom_limiter::{AliasStore, ConcurrencyGuard, DeploymentStore, GuardedStream, PlanStore, RateLimitPlan};
use futures::StreamExt;
use std::convert::Infallible;
use std::sync::Arc;

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
    check_model_access(identity, &req.model, &state.deployment_store, &state.alias_store)
        .map_err(GatewayErrorReply)?;

    // 2. Plan-based or default rate limiting.
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

    let guard = check_plan_or_default_limits(
        &state.plan_store,
        &state.limiter,
        &identity.key_hash,
        &req.model,
        identity.rpm_limit,
        &window_limits,
    )
    .await
    .map_err(GatewayErrorReply)?;

    // 3. Select provider deployment.
    let provider = state
        .select_deployment(&req.model)
        .ok_or_else(|| GatewayErrorReply(GatewayError::ModelNotFound(req.model.clone())))?;

    // 4. Route to provider (streaming or non-streaming).
    let is_stream = req.stream.unwrap_or(false);

    if is_stream {
        let stream = provider.chat_stream(req).await.map_err(GatewayErrorReply)?;
        let sse_stream = sse_stream_from_chat_stream(stream);
        let guarded = GuardedStream::new(sse_stream, guard);
        let response = Sse::new(guarded).keep_alive(KeepAlive::default());
        Ok(response.into_response())
    } else {
        let response = provider.chat(req).await.map_err(GatewayErrorReply)?;
        // guard dropped here (non-streaming: request processing complete).
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
    let _inner = state.inner.load();

    // Collect all visible model names (deployments + non-hidden aliases, excluding "*").
    let all_names: Vec<String> = state.deployment_store.model_names()
        .into_iter()
        .filter(|k| k != "*")
        .chain(state.alias_store.visible_names())
        .collect();

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
        all_names
            .iter()
            .filter(|name| {
                // Direct match in key's allowed list.
                if identity.models.iter().any(|m| m == *name && m != "*") {
                    return true;
                }
                // If name is an alias, check if key has access to target model.
                if let Some(target) = state.alias_store.resolve(name) {
                    if identity.models.iter().any(|m| m == &target && m != "*") {
                        return true;
                    }
                }
                // If name is a deployment (target), check if key has an alias that maps to it.
                for allowed in &identity.models {
                    if allowed == "*" {
                        continue;
                    }
                    if let Some(target) = state.alias_store.resolve(allowed) {
                        if target == **name {
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
        models_count: state.deployment_store.len(),
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
// Admin: Plan Management
// ============================================================

/// PUT /admin/plans — create or update a rate limit plan.
pub async fn admin_upsert_plan(
    State(state): State<AppState>,
    auth: RequiredAuth,
    Json(plan): Json<RateLimitPlan>,
) -> Result<Json<serde_json::Value>, GatewayErrorReply> {
    require_master(auth.identity())?;
    let name = plan.name.clone();
    state.plan_store.upsert_plan(plan);
    tracing::info!(plan = %name, "Plan upserted");
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Plan '{}' saved", name),
    })))
}

/// GET /admin/plans — list all plans.
pub async fn admin_list_plans(
    State(state): State<AppState>,
    auth: RequiredAuth,
) -> Result<Json<serde_json::Value>, GatewayErrorReply> {
    require_master(auth.identity())?;
    let plans = state.plan_store.list_plans();
    Ok(Json(serde_json::json!({ "plans": plans })))
}

/// DELETE /admin/plans/{name} — delete a plan (clears key assignments).
pub async fn admin_delete_plan(
    State(state): State<AppState>,
    auth: RequiredAuth,
    Path(name): Path<String>,
) -> Result<Json<serde_json::Value>, GatewayErrorReply> {
    require_master(auth.identity())?;
    if state.plan_store.delete_plan(&name) {
        tracing::info!(plan = %name, "Plan deleted");
        Ok(Json(serde_json::json!({
            "status": "ok",
            "message": format!("Plan '{}' deleted", name),
        })))
    } else {
        Err(GatewayErrorReply(GatewayError::ConfigError(format!(
            "Plan '{}' not found",
            name
        ))))
    }
}

/// POST /admin/plans/assign — assign a key to a plan.
#[derive(serde::Deserialize)]
pub(crate) struct AssignRequest {
    key_hash: String,
    plan_name: String,
}

pub async fn admin_assign_key(
    State(state): State<AppState>,
    auth: RequiredAuth,
    Json(body): Json<AssignRequest>,
) -> Result<Json<serde_json::Value>, GatewayErrorReply> {
    require_master(auth.identity())?;
    state
        .plan_store
        .assign_key(&body.key_hash, &body.plan_name)
        .map_err(|e| GatewayErrorReply(GatewayError::ConfigError(e)))?;
    tracing::info!(key_hash = %body.key_hash, plan = %body.plan_name, "Key assigned to plan");
    Ok(Json(serde_json::json!({
        "status": "ok",
        "message": format!("Key '{}' assigned to plan '{}'", body.key_hash, body.plan_name),
    })))
}

/// DELETE /admin/plans/assign/{key_hash} — unassign a key from its plan.
pub async fn admin_unassign_key(
    State(state): State<AppState>,
    auth: RequiredAuth,
    Path(key_hash): Path<String>,
) -> Result<Json<serde_json::Value>, GatewayErrorReply> {
    require_master(auth.identity())?;
    if state.plan_store.unassign_key(&key_hash) {
        tracing::info!(key_hash = %key_hash, "Key unassigned from plan");
        Ok(Json(serde_json::json!({
            "status": "ok",
            "message": format!("Key '{}' unassigned", key_hash),
        })))
    } else {
        Err(GatewayErrorReply(GatewayError::ConfigError(format!(
            "Key '{}' not assigned to any plan",
            key_hash
        ))))
    }
}

/// GET /admin/plans/assignments — list all key-to-plan assignments.
pub async fn admin_list_assignments(
    State(state): State<AppState>,
    auth: RequiredAuth,
) -> Result<Json<serde_json::Value>, GatewayErrorReply> {
    require_master(auth.identity())?;
    let assignments = state.plan_store.list_assignments();
    let data: Vec<_> = assignments
        .into_iter()
        .map(|(key_hash, plan_name)| {
            serde_json::json!({
                "key_hash": key_hash,
                "plan_name": plan_name,
            })
        })
        .collect();
    Ok(Json(serde_json::json!({ "assignments": data })))
}

fn require_master(identity: &AuthIdentity) -> Result<(), GatewayErrorReply> {
    if identity.key_hash != "master" {
        return Err(GatewayErrorReply(GatewayError::AuthError(
            "Only master key can manage plans".to_string(),
        )));
    }
    Ok(())
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
// Model Access Check (deployment-aware, uses stores)
// ============================================================

/// Check if an identity can access the given model, considering the gateway's
/// configured deployments and model aliases.
fn check_model_access(
    identity: &AuthIdentity,
    model: &str,
    deployment_store: &Arc<DeploymentStore>,
    alias_store: &Arc<AliasStore>,
) -> Result<(), GatewayError> {
    // Unrestricted key
    if identity.models.is_empty() {
        tracing::debug!(
            "check_model_access: key={:?}, model={}, result=allow (unrestricted)",
            identity.key_name, model
        );
        return Ok(());
    }

    // Direct match
    if identity.models.iter().any(|m| m == model) {
        tracing::debug!(
            "check_model_access: key={:?}, model={}, result=allow (direct match)",
            identity.key_name, model
        );
        return Ok(());
    }

    // Alias match: requested model is an alias -> check if target is in key_models
    if let Some(target) = alias_store.resolve(model) {
        if identity.models.iter().any(|m| m == &target) {
            tracing::debug!(
                "check_model_access: key={:?}, model={}, result=allow (alias -> target={})",
                identity.key_name, model, target
            );
            return Ok(());
        }
    }

    // Reverse alias: key_models has an alias that targets the requested model
    for allowed in &identity.models {
        if let Some(target) = alias_store.resolve(allowed) {
            if target == model {
                tracing::debug!(
                    "check_model_access: key={:?}, model={}, result=allow (key has alias '{}' -> this model)",
                    identity.key_name, model, allowed
                );
                return Ok(());
            }
        }
    }

    // Not in key_models — check if it's a configured model or a wildcard case
    let has_wildcard = identity.models.iter().any(|m| m == "*");
    let model_configured = deployment_store.contains(model);

    if model_configured {
        tracing::warn!(
            "check_model_access: key={:?}, model={}, result=deny (configured model, not in key_models={:?})",
            identity.key_name, model, identity.models
        );
        return Err(GatewayError::ModelNotAllowed(model.to_string()));
    }

    if has_wildcard {
        tracing::debug!(
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
// Plan-based Rate Limiting Helper
// ============================================================

/// Check plan-based limits if a plan is assigned, otherwise fall back to
/// default per-model rate limits.
async fn check_plan_or_default_limits(
    plan_store: &Arc<PlanStore>,
    limiter: &Arc<boom_limiter::SlidingWindowLimiter>,
    key_hash: &str,
    model: &str,
    rpm_limit: Option<u64>,
    window_limits: &[(u64, u64)],
) -> Result<Option<ConcurrencyGuard>, GatewayError> {
    let plan = plan_store
        .resolve_plan(key_hash)
        .or_else(|| plan_store.get_default_plan());

    match plan {
        Some(plan) => {
            let (concurrency_limit, rpm_limit, window_limits) = plan.effective_limits();
            tracing::debug!(
                key_hash = %key_hash,
                plan = %plan.name,
                "Using plan-based rate limits"
            );

            let guard = if let Some(limit) = concurrency_limit {
                Some(plan_store.try_acquire(key_hash, limit).ok_or_else(|| {
                    GatewayError::ConcurrencyExceeded {
                        limit,
                        message: format!(
                            "Concurrency limit exceeded. Limit: {}",
                            limit
                        ),
                    }
                })?)
            } else {
                None
            };

            let rl_key = RateLimitKey {
                key_hash: key_hash.to_string(),
                model: "__plan__".to_string(),
            };

            let decision = limiter
                .check_and_record(&rl_key, rpm_limit, &window_limits)
                .await?;

            if !decision.allowed {
                drop(guard);
                return Err(GatewayError::RateLimitExceeded {
                    retry_after_secs: decision.retry_after_secs,
                    message: format!(
                        "Rate limit exceeded. Limit: {} per minute.",
                        decision.limit
                    ),
                });
            }

            Ok(guard)
        }
        None => {
            let rl_key = RateLimitKey {
                key_hash: key_hash.to_string(),
                model: model.to_string(),
            };

            let decision = limiter
                .check_and_record(&rl_key, rpm_limit, window_limits)
                .await?;

            if !decision.allowed {
                return Err(GatewayError::RateLimitExceeded {
                    retry_after_secs: decision.retry_after_secs,
                    message: format!(
                        "Rate limit exceeded. Limit: {} per minute.",
                        decision.limit
                    ),
                });
            }

            Ok(None)
        }
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
    check_model_access(identity, &openai_req.model, &state.deployment_store, &state.alias_store)
        .map_err(AnthropicErrorReply)?;

    // 2. Plan-based or default rate limiting.
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

    let guard = check_plan_or_default_limits(
        &state.plan_store,
        &state.limiter,
        &identity.key_hash,
        &openai_req.model,
        identity.rpm_limit,
        &window_limits,
    )
    .await
    .map_err(AnthropicErrorReply)?;

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
        let guarded = GuardedStream::new(sse_stream, guard);
        let response = Sse::new(guarded).keep_alive(KeepAlive::default());
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
