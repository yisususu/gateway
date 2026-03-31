use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use chrono::NaiveDateTime;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::FromRow;
use uuid::Uuid;

use crate::auth::{hash_token, AdminSession};
use crate::state::DashboardState;

// ═══════════════════════════════════════════════════════════
// Plan management (delegated to PlanStore)
// ═══════════════════════════════════════════════════════════

pub async fn list_plans(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Json<Value> {
    let plans = state.plan_store.list_plans();
    Json(json!({"plans": plans}))
}

#[derive(Debug, Deserialize)]
pub struct UpsertPlanRequest {
    pub name: String,
    #[serde(default)]
    pub concurrency_limit: Option<u32>,
    #[serde(default)]
    pub rpm_limit: Option<u64>,
    #[serde(default)]
    pub window_limits: Vec<(u64, u64)>,
    #[serde(default)]
    pub schedule: Vec<boom_limiter::ScheduleSlot>,
}

pub async fn upsert_plan(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(req): Json<UpsertPlanRequest>,
) -> Json<Value> {
    let plan = boom_limiter::RateLimitPlan {
        name: req.name.clone(),
        concurrency_limit: req.concurrency_limit,
        rpm_limit: req.rpm_limit,
        window_limits: req.window_limits,
        schedule: req.schedule,
    };
    state.plan_store.upsert_plan(plan);
    Json(json!({"ok": true, "plan_name": req.name}))
}

pub async fn delete_plan(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(name): Path<String>,
) -> Json<Value> {
    let deleted = state.plan_store.delete_plan(&name);
    Json(json!({"ok": deleted, "plan_name": name}))
}

// ═══════════════════════════════════════════════════════════
// Key management (DB operations)
// ═══════════════════════════════════════════════════════════

/// Row mapper for the keys list query.
/// Types must match boom-auth's VerificationToken to avoid runtime decode errors.
#[derive(Debug, FromRow)]
struct KeyRow {
    token: String,
    key_name: Option<String>,
    key_alias: Option<String>,
    user_id: Option<String>,
    team_id: Option<String>,
    /// litellm stores models as text[] in PostgreSQL.
    models: Vec<String>,
    /// spend has a NOT NULL DEFAULT 0.0 constraint.
    spend: f64,
    blocked: Option<bool>,
    rpm_limit: Option<i64>,
    tpm_limit: Option<i64>,
    max_budget: Option<f64>,
    budget_duration: Option<String>,
    expires: Option<NaiveDateTime>,
    metadata: Option<serde_json::Value>,
    created_at: Option<NaiveDateTime>,
}

#[derive(Debug, Deserialize)]
pub struct ListKeysQuery {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
}

fn default_page() -> i64 {
    1
}
fn default_per_page() -> i64 {
    50
}

pub async fn list_keys(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Query(query): Query<ListKeysQuery>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Database not available",
            )
                .into_response();
        }
    };

    let offset = (query.page - 1).max(0) * query.per_page;

    let rows: Vec<KeyRow> = match sqlx::query_as(
        r#"SELECT token, key_name, key_alias, user_id, team_id, models,
                  spend, blocked, rpm_limit, tpm_limit, max_budget,
                  budget_duration, expires, metadata, created_at
           FROM "LiteLLM_VerificationToken"
           ORDER BY created_at DESC NULLS LAST
           LIMIT $1 OFFSET $2"#,
    )
    .bind(query.per_page)
    .bind(offset)
    .fetch_all(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Dashboard list_keys query failed: {}", e);
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("DB error: {}", e),
            )
                .into_response();
        }
    };

    let keys: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            let token_prefix = format!("{}...", &r.token[..8.min(r.token.len())]);
            json!({
                "token_prefix": token_prefix,
                "token_hash": r.token,
                "key_name": r.key_name,
                "key_alias": r.key_alias,
                "user_id": r.user_id,
                "team_id": r.team_id,
                "models": r.models,
                "spend": r.spend,
                "blocked": r.blocked.unwrap_or(false),
                "rpm_limit": r.rpm_limit,
                "tpm_limit": r.tpm_limit,
                "max_budget": r.max_budget,
                "budget_duration": r.budget_duration,
                "expires": r.expires.map(|d| d.to_string()),
                "metadata": r.metadata,
                "created_at": r.created_at.map(|d| d.to_string()),
            })
        })
        .collect();

    // Get total count.
    let total: (i64,) = sqlx::query_as(r#"SELECT COUNT(*) FROM "LiteLLM_VerificationToken""#)
        .fetch_one(db_pool)
        .await
        .unwrap_or((0,));

    Json(json!({
        "keys": keys,
        "page": query.page,
        "per_page": query.per_page,
        "total": total.0,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    pub key_name: Option<String>,
    pub user_id: Option<String>,
    pub team_id: Option<String>,
    pub models: Option<Vec<String>>,
    pub max_budget: Option<f64>,
    pub budget_duration: Option<String>,
    pub rpm_limit: Option<i64>,
    pub tpm_limit: Option<i64>,
    pub expires: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub plan_name: Option<String>,
}

pub async fn create_key(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(req): Json<CreateKeyRequest>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Database not available",
            )
                .into_response();
        }
    };

    // 1. Generate raw key: sk- + 32 bytes random hex.
    let raw_key = format!("sk-{}", hex::encode(Uuid::new_v4().as_bytes()));
    let token_hash = hash_token(&raw_key);

    // 2. Parse optional expires.
    let expires: Option<NaiveDateTime> = req
        .expires
        .as_deref()
        .and_then(|s| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok());

    let models_json = req
        .models
        .map(|m| serde_json::to_value(m).unwrap_or(json!([])));

    // 3. INSERT into DB.
    let result = sqlx::query(
        r#"INSERT INTO "LiteLLM_VerificationToken"
           (token, key_name, user_id, team_id, models, spend, blocked,
            rpm_limit, tpm_limit, max_budget, budget_duration, expires,
            metadata, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, 0.0, false, $6, $7, $8, $9, $10, $11, NOW(), NOW())"#,
    )
    .bind(&token_hash)
    .bind(&req.key_name)
    .bind(&req.user_id)
    .bind(&req.team_id)
    .bind(&models_json)
    .bind(req.rpm_limit)
    .bind(req.tpm_limit)
    .bind(req.max_budget)
    .bind(&req.budget_duration)
    .bind(expires)
    .bind(&req.metadata)
    .execute(db_pool)
    .await;

    if let Err(e) = result {
        tracing::error!("Dashboard create_key insert failed: {}", e);
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create key: {}", e),
        )
            .into_response();
    }

    // 4. Optionally assign to plan.
    if let Some(ref plan_name) = req.plan_name {
        if let Err(e) = state.plan_store.assign_key(&token_hash, plan_name) {
            tracing::warn!("Key created but plan assignment failed: {}", e);
        }
    }

    // 5. Return the raw key (only shown once).
    Json(json!({
        "key": raw_key,
        "token_hash": token_hash,
        "key_name": req.key_name,
    }))
    .into_response()
}

#[derive(Debug, Deserialize)]
pub struct UpdateKeyRequest {
    pub key_name: Option<String>,
    pub models: Option<Vec<String>>,
    pub max_budget: Option<f64>,
    pub budget_duration: Option<String>,
    pub rpm_limit: Option<i64>,
    pub tpm_limit: Option<i64>,
    pub expires: Option<String>,
    pub metadata: Option<serde_json::Value>,
}

pub async fn update_key(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(token_hash): Path<String>,
    Json(req): Json<UpdateKeyRequest>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Database not available",
            )
                .into_response();
        }
    };

    let models_json = req
        .models
        .map(|m| serde_json::to_value(m).unwrap_or(json!([])));

    let expires: Option<NaiveDateTime> = req
        .expires
        .as_deref()
        .and_then(|s| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok());

    let result = sqlx::query(
        r#"UPDATE "LiteLLM_VerificationToken"
           SET key_name = COALESCE($2, key_name),
               models = COALESCE($3, models),
               max_budget = COALESCE($4, max_budget),
               budget_duration = COALESCE($5, budget_duration),
               rpm_limit = COALESCE($6, rpm_limit),
               tpm_limit = COALESCE($7, tpm_limit),
               expires = COALESCE($8, expires),
               metadata = COALESCE($9, metadata),
               updated_at = NOW()
           WHERE token = $1"#,
    )
    .bind(&token_hash)
    .bind(&req.key_name)
    .bind(&models_json)
    .bind(req.max_budget)
    .bind(&req.budget_duration)
    .bind(req.rpm_limit)
    .bind(req.tpm_limit)
    .bind(expires)
    .bind(&req.metadata)
    .execute(db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => Json(json!({"ok": true})).into_response(),
        Ok(_) => (
            axum::http::StatusCode::NOT_FOUND,
            "Key not found",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Dashboard update_key failed: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("DB error: {}", e),
            )
                .into_response()
        }
    }
}

pub async fn block_key(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(token_hash): Path<String>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Database not available",
            )
                .into_response();
        }
    };

    let result = sqlx::query(
        r#"UPDATE "LiteLLM_VerificationToken" SET blocked = true, updated_at = NOW() WHERE token = $1"#,
    )
    .bind(&token_hash)
    .execute(db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => Json(json!({"ok": true})).into_response(),
        Ok(_) => (
            axum::http::StatusCode::NOT_FOUND,
            "Key not found",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Dashboard block_key failed: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("DB error: {}", e),
            )
                .into_response()
        }
    }
}

pub async fn unblock_key(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(token_hash): Path<String>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return (
                axum::http::StatusCode::SERVICE_UNAVAILABLE,
                "Database not available",
            )
                .into_response();
        }
    };

    let result = sqlx::query(
        r#"UPDATE "LiteLLM_VerificationToken" SET blocked = false, updated_at = NOW() WHERE token = $1"#,
    )
    .bind(&token_hash)
    .execute(db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => Json(json!({"ok": true})).into_response(),
        Ok(_) => (
            axum::http::StatusCode::NOT_FOUND,
            "Key not found",
        )
            .into_response(),
        Err(e) => {
            tracing::error!("Dashboard unblock_key failed: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                format!("DB error: {}", e),
            )
                .into_response()
        }
    }
}

// ═══════════════════════════════════════════════════════════
// Assignment management
// ═══════════════════════════════════════════════════════════

pub async fn list_assignments(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Json<Value> {
    let assignments = state
        .plan_store
        .list_assignments()
        .into_iter()
        .map(|(key_hash, plan_name)| {
            json!({
                "key_hash": key_hash,
                "plan_name": plan_name,
            })
        })
        .collect::<Vec<_>>();

    Json(json!({"assignments": assignments}))
}

#[derive(Debug, Deserialize)]
pub struct AssignRequest {
    pub key_hash: String,
    pub plan_name: String,
}

pub async fn assign_key(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(req): Json<AssignRequest>,
) -> Response {
    match state.plan_store.assign_key(&req.key_hash, &req.plan_name) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e).into_response(),
    }
}

pub async fn unassign_key(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(key_hash): Path<String>,
) -> Json<Value> {
    let removed = state.plan_store.unassign_key(&key_hash);
    Json(json!({"ok": removed}))
}

// ═══════════════════════════════════════════════════════════
// Usage query
// ═══════════════════════════════════════════════════════════

pub async fn get_key_usage(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(key_hash): Path<String>,
) -> Json<Value> {
    let windows: Vec<Value> = state
        .limiter
        .get_usage_for_key(&key_hash)
        .into_iter()
        .map(|w| {
            json!({
                "cache_key": w.cache_key,
                "count": w.count,
                "window_secs": w.window_secs,
                "elapsed_secs": w.elapsed_secs,
            })
        })
        .collect();

    let concurrency = state.plan_store.get_concurrency(&key_hash);

    Json(json!({
        "key_hash": key_hash,
        "concurrency": concurrency,
        "windows": windows,
    }))
}
