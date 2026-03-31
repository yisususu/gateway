use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use chrono::NaiveDateTime;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{FromRow, Row};
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
        schedule: req.schedule.clone(),
    };

    // Persist to DB.
    if let Some(ref pool) = state.db_pool {
        let window_limits_json =
            serde_json::to_value(&plan.window_limits).unwrap_or(json!([]));
        let schedule_json = serde_json::to_value(
            plan.schedule
                .iter()
                .map(|s| {
                    json!({
                        "hours": s.hours,
                        "concurrency_limit": s.concurrency_limit,
                        "rpm_limit": s.rpm_limit,
                        "window_limits": s.window_limits,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .unwrap_or(json!([]));

        if let Err(e) = sqlx::query(
            r#"INSERT INTO boom_rate_limit_plan
               (name, concurrency_limit, rpm_limit, window_limits, schedule, is_default, source)
               VALUES ($1, $2, $3, $4, $5, false, 'db')
               ON CONFLICT (name) DO UPDATE
               SET concurrency_limit = EXCLUDED.concurrency_limit,
                   rpm_limit = EXCLUDED.rpm_limit,
                   window_limits = EXCLUDED.window_limits,
                   schedule = EXCLUDED.schedule,
                   source = 'db',
                   updated_at = NOW()"#,
        )
        .bind(&req.name)
        .bind(req.concurrency_limit.map(|v| v as i32))
        .bind(req.rpm_limit.map(|v| v as i64))
        .bind(&window_limits_json)
        .bind(&schedule_json)
        .execute(pool)
        .await
        {
            tracing::error!("Failed to persist plan to DB: {}", e);
        }
    }

    state.plan_store.upsert_plan(plan);
    Json(json!({"ok": true, "plan_name": req.name}))
}

pub async fn delete_plan(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(name): Path<String>,
) -> Json<Value> {
    let deleted = state.plan_store.delete_plan(&name);

    // Delete from DB.
    if deleted {
        if let Some(ref pool) = state.db_pool {
            if let Err(e) = sqlx::query(
                r#"DELETE FROM boom_rate_limit_plan WHERE name = $1"#,
            )
            .bind(&name)
            .execute(pool)
            .await
            {
                tracing::error!("Failed to delete plan from DB: {}", e);
            }
        }
    }

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
            return Json(json!({"error": "Database not available"})).into_response();
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
    pub key_alias: Option<String>,
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
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    // 1. Generate raw key: sk- + 32 bytes random hex.
    let raw_key = format!("sk-{}", hex::encode(Uuid::new_v4().as_bytes()));
    let token_hash = hash_token(&raw_key);

    // 1b. Check key_alias dedup (if provided).
    if let Some(ref alias) = req.key_alias {
        let exists: bool = sqlx::query_scalar(
            r#"SELECT EXISTS(SELECT 1 FROM "LiteLLM_VerificationToken" WHERE key_alias = $1)"#,
        )
        .bind(alias)
        .fetch_one(db_pool)
        .await
        .unwrap_or(false);

        if exists {
            return (
                axum::http::StatusCode::CONFLICT,
                format!("key_alias '{}' already exists", alias),
            )
                .into_response();
        }
    }

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
           (token, key_name, key_alias, user_id, team_id, models, spend, blocked,
            rpm_limit, tpm_limit, max_budget, budget_duration, expires,
            metadata, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, 0.0, false, $7, $8, $9, $10, $11, $12, NOW(), NOW())"#,
    )
    .bind(&token_hash)
    .bind(&req.key_name)
    .bind(&req.key_alias)
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
            return Json(json!({"error": "Database not available"})).into_response();
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
            return Json(json!({"error": "Database not available"})).into_response();
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
            return Json(json!({"error": "Database not available"})).into_response();
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
        Ok(()) => {
            // Persist assignment to DB.
            if let Some(ref pool) = state.db_pool {
                if let Err(e) = sqlx::query(
                    r#"INSERT INTO boom_key_plan_assignment (key_hash, plan_name, assigned_at)
                       VALUES ($1, $2, NOW())
                       ON CONFLICT (key_hash) DO UPDATE
                       SET plan_name = EXCLUDED.plan_name"#,
                )
                .bind(&req.key_hash)
                .bind(&req.plan_name)
                .execute(pool)
                .await
                {
                    tracing::error!("Failed to persist assignment to DB: {}", e);
                }
            }
            Json(json!({"ok": true})).into_response()
        }
        Err(e) => (axum::http::StatusCode::BAD_REQUEST, e).into_response(),
    }
}

pub async fn unassign_key(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(key_hash): Path<String>,
) -> Json<Value> {
    let removed = state.plan_store.unassign_key(&key_hash);

    // Remove from DB.
    if removed {
        if let Some(ref pool) = state.db_pool {
            if let Err(e) = sqlx::query(
                r#"DELETE FROM boom_key_plan_assignment WHERE key_hash = $1"#,
            )
            .bind(&key_hash)
            .execute(pool)
            .await
            {
                tracing::error!("Failed to delete assignment from DB: {}", e);
            }
        }
    }

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

// ═══════════════════════════════════════════════════════════
// Batch key creation
// ═══════════════════════════════════════════════════════════

pub async fn batch_create_keys(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(reqs): Json<Vec<CreateKeyRequest>>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let mut created = Vec::new();
    let mut skipped = Vec::new();

    for req in reqs {
        // Dedup check on key_alias.
        if let Some(ref alias) = req.key_alias {
            let exists: bool = sqlx::query_scalar(
                r#"SELECT EXISTS(SELECT 1 FROM "LiteLLM_VerificationToken" WHERE key_alias = $1)"#,
            )
            .bind(alias)
            .fetch_one(db_pool)
            .await
            .unwrap_or(false);

            if exists {
                skipped.push(json!({
                    "key_alias": alias,
                    "reason": "duplicate",
                }));
                continue;
            }
        }

        let raw_key = format!("sk-{}", hex::encode(Uuid::new_v4().as_bytes()));
        let token_hash = hash_token(&raw_key);

        let expires: Option<NaiveDateTime> = req
            .expires
            .as_deref()
            .and_then(|s| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok());

        let models_json = req
            .models
            .map(|m| serde_json::to_value(m).unwrap_or(json!([])));

        let result = sqlx::query(
            r#"INSERT INTO "LiteLLM_VerificationToken"
               (token, key_name, key_alias, user_id, team_id, models, spend, blocked,
                rpm_limit, tpm_limit, max_budget, budget_duration, expires,
                metadata, created_at, updated_at)
               VALUES ($1, $2, $3, $4, $5, $6, 0.0, false, $7, $8, $9, $10, $11, $12, NOW(), NOW())"#,
        )
        .bind(&token_hash)
        .bind(&req.key_name)
        .bind(&req.key_alias)
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

        match result {
            Ok(_) => {
                // Optionally assign to plan.
                if let Some(ref plan_name) = req.plan_name {
                    if let Err(e) = state.plan_store.assign_key(&token_hash, plan_name) {
                        tracing::warn!("Batch: key created but plan assignment failed: {}", e);
                    }
                }
                created.push(json!({
                    "key": raw_key,
                    "token_hash": token_hash,
                    "key_alias": req.key_alias,
                }));
            }
            Err(e) => {
                tracing::error!("Dashboard batch_create_keys insert failed: {}", e);
                skipped.push(json!({
                    "key_alias": req.key_alias,
                    "reason": format!("db_error: {}", e),
                }));
            }
        }
    }

    Json(json!({
        "created": created,
        "skipped": skipped,
        "created_count": created.len(),
        "skipped_count": skipped.len(),
    }))
    .into_response()
}

// ═══════════════════════════════════════════════════════════
// Model deployment management (DB + memory)
// ═══════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
pub struct CreateDeploymentRequest {
    pub model_name: String,
    pub litellm_model: String,
    pub api_key: Option<String>,
    pub api_key_env: Option<bool>,
    pub api_base: Option<String>,
    pub api_version: Option<String>,
    pub aws_region_name: Option<String>,
    pub aws_access_key_id: Option<String>,
    pub aws_secret_access_key: Option<String>,
    pub rpm: Option<i64>,
    pub tpm: Option<i64>,
    #[serde(default = "default_timeout")]
    pub timeout: i64,
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    pub temperature: Option<f64>,
    pub max_tokens: Option<i32>,
    #[serde(default = "default_true_val")]
    pub enabled: bool,
}

fn default_timeout() -> i64 {
    120
}
fn default_true_val() -> bool {
    true
}

/// Row from boom_model_deployment (for list queries).
#[derive(Debug, FromRow)]
#[allow(dead_code)]
struct DeploymentRow {
    id: Uuid,
    model_name: String,
    litellm_model: String,
    api_key: Option<String>,
    api_key_env: Option<bool>,
    api_base: Option<String>,
    api_version: Option<String>,
    aws_region_name: Option<String>,
    aws_access_key_id: Option<String>,
    aws_secret_access_key: Option<String>,
    rpm: Option<i64>,
    tpm: Option<i64>,
    timeout: i64,
    headers: serde_json::Value,
    temperature: Option<f64>,
    max_tokens: Option<i32>,
    enabled: Option<bool>,
    source: Option<String>,
    created_at: Option<chrono::NaiveDateTime>,
    updated_at: Option<chrono::NaiveDateTime>,
}

pub async fn list_models(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let rows: Vec<DeploymentRow> = match sqlx::query_as(
        r#"SELECT id, model_name, litellm_model, api_key, api_key_env, api_base, api_version,
                  aws_region_name, aws_access_key_id, aws_secret_access_key,
                  rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source,
                  created_at, updated_at
           FROM boom_model_deployment
           ORDER BY model_name, created_at"#,
    )
    .fetch_all(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Dashboard list_models query failed: {}", e);
            return Json(json!({"error": format!("DB error: {}", e)})).into_response();
        }
    };

    let models: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "id": r.id,
                "model_name": r.model_name,
                "litellm_model": r.litellm_model,
                "api_key_env": r.api_key_env.unwrap_or(false),
                "api_base": r.api_base,
                "api_version": r.api_version,
                "aws_region_name": r.aws_region_name,
                "rpm": r.rpm,
                "tpm": r.tpm,
                "timeout": r.timeout,
                "temperature": r.temperature,
                "max_tokens": r.max_tokens,
                "enabled": r.enabled.unwrap_or(true),
                "source": r.source,
                "created_at": r.created_at.map(|d| d.to_string()),
                "updated_at": r.updated_at.map(|d| d.to_string()),
            })
        })
        .collect();

    Json(json!({"models": models})).into_response()
}

pub async fn create_model(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(req): Json<CreateDeploymentRequest>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let headers_json = serde_json::to_value(&req.headers).unwrap_or(json!({}));

    // Insert into DB.
    let result = sqlx::query(
        r#"INSERT INTO boom_model_deployment
           (model_name, litellm_model, api_key, api_key_env, api_base, api_version,
            aws_region_name, aws_access_key_id, aws_secret_access_key,
            rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 'db')
           RETURNING id"#,
    )
    .bind(&req.model_name)
    .bind(&req.litellm_model)
    .bind(&req.api_key)
    .bind(req.api_key_env.unwrap_or(false))
    .bind(&req.api_base)
    .bind(&req.api_version)
    .bind(&req.aws_region_name)
    .bind(&req.aws_access_key_id)
    .bind(&req.aws_secret_access_key)
    .bind(req.rpm)
    .bind(req.tpm)
    .bind(req.timeout)
    .bind(&headers_json)
    .bind(req.temperature)
    .bind(req.max_tokens)
    .bind(req.enabled)
    .fetch_one(db_pool)
    .await;

    let id: Uuid = match result {
        Ok(row) => row.get("id"),
        Err(e) => {
            tracing::error!("Dashboard create_model insert failed: {}", e);
            return Json(json!({"error": format!("Failed to create model: {}", e)})).into_response();
        }
    };

    // Build provider and add to memory (if enabled).
    if req.enabled {
        if let Some(provider) = build_provider_from_request(&req) {
            state.deployment_store.add_deployment(&req.model_name, provider);
            tracing::info!(model = %req.model_name, "Model deployment created and loaded");
        }
    }

    Json(json!({"ok": true, "id": id, "model_name": req.model_name})).into_response()
}

pub async fn update_model(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<CreateDeploymentRequest>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let headers_json = serde_json::to_value(&req.headers).unwrap_or(json!({}));

    let result = sqlx::query(
        r#"UPDATE boom_model_deployment
           SET model_name = $2, litellm_model = $3, api_key = $4, api_key_env = $5,
               api_base = $6, api_version = $7, aws_region_name = $8,
               aws_access_key_id = $9, aws_secret_access_key = $10,
               rpm = $11, tpm = $12, timeout = $13, headers = $14,
               temperature = $15, max_tokens = $16, enabled = $17, updated_at = NOW()
           WHERE id = $1"#,
    )
    .bind(id)
    .bind(&req.model_name)
    .bind(&req.litellm_model)
    .bind(&req.api_key)
    .bind(req.api_key_env.unwrap_or(false))
    .bind(&req.api_base)
    .bind(&req.api_version)
    .bind(&req.aws_region_name)
    .bind(&req.aws_access_key_id)
    .bind(&req.aws_secret_access_key)
    .bind(req.rpm)
    .bind(req.tpm)
    .bind(req.timeout)
    .bind(&headers_json)
    .bind(req.temperature)
    .bind(req.max_tokens)
    .bind(req.enabled)
    .execute(db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => {
            // Rebuild provider for this model.
            // For simplicity, reload all deployments for this model_name from DB.
            // A more targeted approach would track which deployment changed.
            if req.enabled {
                if let Some(provider) = build_provider_from_request(&req) {
                    state.deployment_store.add_deployment(&req.model_name, provider);
                }
            }
            Json(json!({"ok": true})).into_response()
        }
        Ok(_) => Json(json!({"error": "Model deployment not found"})).into_response(),
        Err(e) => {
            tracing::error!("Dashboard update_model failed: {}", e);
            Json(json!({"error": format!("DB error: {}", e)})).into_response()
        }
    }
}

pub async fn delete_model(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(id): Path<Uuid>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    // Get model_name before deleting (to potentially clean up deployment store).
    let model_name: Option<String> = sqlx::query_scalar(
        r#"SELECT model_name FROM boom_model_deployment WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(db_pool)
    .await
    .ok()
    .flatten();

    let result = sqlx::query(
        r#"DELETE FROM boom_model_deployment WHERE id = $1"#,
    )
    .bind(id)
    .execute(db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => {
            let name = model_name.unwrap_or_default();
            tracing::info!(model = %name, "Model deployment deleted");
            Json(json!({"ok": true, "model_name": name})).into_response()
        }
        Ok(_) => Json(json!({"error": "Model deployment not found"})).into_response(),
        Err(e) => {
            tracing::error!("Dashboard delete_model failed: {}", e);
            Json(json!({"error": format!("DB error: {}", e)})).into_response()
        }
    }
}

/// Build a Provider from a CreateDeploymentRequest.
fn build_provider_from_request(req: &CreateDeploymentRequest) -> Option<std::sync::Arc<dyn boom_core::provider::Provider>> {
    let mut extra = req.headers.clone();
    if let Some(ref v) = req.api_version {
        extra.insert("api_version".to_string(), v.clone());
    }
    if let Some(ref r) = req.aws_region_name {
        extra.insert("aws_region_name".to_string(), r.clone());
    }

    // Resolve api_key (may be env reference).
    let api_key = req.api_key.as_ref().map(|k| {
        if req.api_key_env.unwrap_or(false) {
            boom_config::resolve_env_value(k)
        } else {
            k.clone()
        }
    });

    match boom_provider::create_provider(
        &req.litellm_model,
        api_key,
        req.api_base.clone(),
        req.timeout as u64,
        &extra,
    ) {
        Ok(provider) => Some(provider),
        Err(e) => {
            tracing::error!("Failed to build provider for '{}': {}", req.model_name, e);
            None
        }
    }
}

// ═══════════════════════════════════════════════════════════
// Model alias management (DB + memory)
// ═══════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
pub struct CreateAliasRequest {
    pub alias_name: String,
    pub target_model: String,
    #[serde(default)]
    pub hidden: bool,
}

pub async fn list_aliases(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    #[derive(Debug, FromRow)]
    struct AliasRow {
        alias_name: String,
        target_model: String,
        hidden: Option<bool>,
        source: Option<String>,
        updated_at: Option<chrono::NaiveDateTime>,
    }

    let rows: Vec<AliasRow> = match sqlx::query_as(
        r#"SELECT alias_name, target_model, hidden, source, updated_at
           FROM boom_model_alias
           ORDER BY alias_name"#,
    )
    .fetch_all(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Dashboard list_aliases query failed: {}", e);
            return Json(json!({"error": format!("DB error: {}", e)})).into_response();
        }
    };

    let aliases: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "alias_name": r.alias_name,
                "target_model": r.target_model,
                "hidden": r.hidden.unwrap_or(false),
                "source": r.source,
                "updated_at": r.updated_at.map(|d| d.to_string()),
            })
        })
        .collect();

    Json(json!({"aliases": aliases})).into_response()
}

pub async fn create_alias(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(req): Json<CreateAliasRequest>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let result = sqlx::query(
        r#"INSERT INTO boom_model_alias (alias_name, target_model, hidden, source)
           VALUES ($1, $2, $3, 'db')
           ON CONFLICT (alias_name) DO UPDATE
           SET target_model = EXCLUDED.target_model,
               hidden = EXCLUDED.hidden,
               source = 'db',
               updated_at = NOW()"#,
    )
    .bind(&req.alias_name)
    .bind(&req.target_model)
    .bind(req.hidden)
    .execute(db_pool)
    .await;

    if let Err(e) = result {
        tracing::error!("Dashboard create_alias failed: {}", e);
        return Json(json!({"error": format!("DB error: {}", e)})).into_response();
    }

    // Update in-memory alias store.
    state.alias_store.set_alias(
        req.alias_name.clone(),
        req.target_model.clone(),
        req.hidden,
    );

    tracing::info!(alias = %req.alias_name, target = %req.target_model, "Alias created");
    Json(json!({"ok": true, "alias_name": req.alias_name})).into_response()
}

pub async fn update_alias(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(alias_name): Path<String>,
    Json(req): Json<CreateAliasRequest>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let result = sqlx::query(
        r#"UPDATE boom_model_alias
           SET target_model = $2, hidden = $3, updated_at = NOW()
           WHERE alias_name = $1"#,
    )
    .bind(&alias_name)
    .bind(&req.target_model)
    .bind(req.hidden)
    .execute(db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => {
            // Remove old alias and set new one.
            state.alias_store.remove_alias(&alias_name);
            state.alias_store.set_alias(
                alias_name.clone(),
                req.target_model.clone(),
                req.hidden,
            );
            Json(json!({"ok": true})).into_response()
        }
        Ok(_) => Json(json!({"error": "Alias not found"})).into_response(),
        Err(e) => {
            tracing::error!("Dashboard update_alias failed: {}", e);
            Json(json!({"error": format!("DB error: {}", e)})).into_response()
        }
    }
}

pub async fn delete_alias(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(alias_name): Path<String>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let result = sqlx::query(
        r#"DELETE FROM boom_model_alias WHERE alias_name = $1"#,
    )
    .bind(&alias_name)
    .execute(db_pool)
    .await;

    match result {
        Ok(r) if r.rows_affected() > 0 => {
            state.alias_store.remove_alias(&alias_name);
            tracing::info!(alias = %alias_name, "Alias deleted");
            Json(json!({"ok": true, "alias_name": alias_name})).into_response()
        }
        Ok(_) => Json(json!({"error": "Alias not found"})).into_response(),
        Err(e) => {
            tracing::error!("Dashboard delete_alias failed: {}", e);
            Json(json!({"error": format!("DB error: {}", e)})).into_response()
        }
    }
}

// ═══════════════════════════════════════════════════════════
// Config management (boom_config KV store)
// ═══════════════════════════════════════════════════════════

pub async fn get_config(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    #[derive(Debug, FromRow)]
    #[allow(dead_code)]
    struct ConfigRow {
        key: String,
        value: serde_json::Value,
        updated_at: Option<chrono::NaiveDateTime>,
    }

    let rows: Vec<ConfigRow> = match sqlx::query_as(
        r#"SELECT key, value, updated_at FROM boom_config ORDER BY key"#,
    )
    .fetch_all(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Dashboard get_config query failed: {}", e);
            return Json(json!({"error": format!("DB error: {}", e)})).into_response();
        }
    };

    let config: std::collections::HashMap<String, Value> = rows
        .into_iter()
        .map(|r| (r.key, r.value))
        .collect();

    Json(json!({"config": config})).into_response()
}

#[derive(Debug, Deserialize)]
pub struct PatchConfigRequest {
    pub key: String,
    pub value: serde_json::Value,
}

pub async fn patch_config(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(req): Json<PatchConfigRequest>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let result = sqlx::query(
        r#"INSERT INTO boom_config (key, value) VALUES ($1, $2)
           ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()"#,
    )
    .bind(&req.key)
    .bind(&req.value)
    .execute(db_pool)
    .await;

    if let Err(e) = result {
        tracing::error!("Dashboard patch_config failed: {}", e);
        return Json(json!({"error": format!("DB error: {}", e)})).into_response();
    }

    tracing::info!(key = %req.key, "Config updated");
    Json(json!({"ok": true, "key": req.key})).into_response()
}
