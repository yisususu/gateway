use axum::extract::{Path, Query};
use axum::response::{IntoResponse, Response};
use axum::Extension;
use axum::Json;
use chrono::NaiveDateTime;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::FromRow;
use std::sync::Arc;
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
    let _ = state.admin_tx.send(crate::state::AdminCommand::ConfigChanged).await;
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
        let _ = state.admin_tx.send(crate::state::AdminCommand::ConfigChanged).await;
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
    pub search: Option<String>,
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

    let search_pattern = query
        .search
        .as_deref()
        .map(|s| format!("%{}%", s.replace('%', "\\%").replace('_', "\\_")));

    // Fetch ALL keys from DB (no LIMIT/OFFSET) for global usage sorting.
    let rows: Vec<KeyRow> = if let Some(ref pattern) = search_pattern {
        match sqlx::query_as(
            r#"SELECT token, key_name, key_alias, user_id, team_id, models,
                      spend, blocked, rpm_limit, tpm_limit, max_budget,
                      budget_duration, expires, metadata, created_at
               FROM "boom_verification_token"
               WHERE (key_name ILIKE $1 OR key_alias ILIKE $1 OR user_id ILIKE $1 OR token ILIKE $1)"#,
        )
        .bind(pattern)
        .fetch_all(db_pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Dashboard list_keys query failed: {}", e);
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
                    .into_response();
            }
        }
    } else {
        match sqlx::query_as(
            r#"SELECT token, key_name, key_alias, user_id, team_id, models,
                      spend, blocked, rpm_limit, tpm_limit, max_budget,
                      budget_duration, expires, metadata, created_at
               FROM "boom_verification_token""#,
        )
        .fetch_all(db_pool)
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Dashboard list_keys query failed: {}", e);
                return (
                    axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error",
                )
                    .into_response();
            }
        }
    };

    let total = rows.len() as i64;

    // Single-pass limiter scan: aggregate usage for all keys at once.
    let all_usage = state.limiter.get_all_key_usage();

    let mut keys: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            let token_prefix = format!("{}...", &r.token[..8.min(r.token.len())]);
            let (usage_count, usage_reset_secs) = all_usage.get(&r.token).copied().unwrap_or((0, 0));

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
                "usage_count": usage_count,
                "usage_reset_secs": usage_reset_secs,
            })
        })
        .collect();

    // Sort globally by usage_count descending.
    keys.sort_by(|a, b| {
        let ca = a.get("usage_count").and_then(|v| v.as_u64()).unwrap_or(0);
        let cb = b.get("usage_count").and_then(|v| v.as_u64()).unwrap_or(0);
        cb.cmp(&ca)
    });

    // In-memory pagination.
    let offset = ((query.page - 1).max(0) * query.per_page) as usize;
    let per_page = query.per_page as usize;
    let page_keys: Vec<Value> = keys.into_iter().skip(offset).take(per_page).collect();

    Json(json!({
        "keys": page_keys,
        "page": query.page,
        "per_page": query.per_page,
        "total": total,
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
            r#"SELECT EXISTS(SELECT 1 FROM "boom_verification_token" WHERE key_alias = $1)"#,
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

    let models_list: Vec<String> = req.models.unwrap_or_default();

    // 3. INSERT into DB.
    let result = sqlx::query(
        r#"INSERT INTO "boom_verification_token"
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
    .bind(&models_list)
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
            "Internal error",
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
    pub key_alias: Option<String>,
    pub user_id: Option<String>,
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

    // Check key_alias uniqueness if provided.
    if let Some(ref alias) = req.key_alias {
        if !alias.is_empty() {
            let exists: bool = sqlx::query_scalar(
                r#"SELECT EXISTS(SELECT 1 FROM "boom_verification_token" WHERE key_alias = $1 AND token != $2)"#,
            )
            .bind(alias)
            .bind(&token_hash)
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
    }

    let models_list: Option<Vec<String>> = req.models.clone();

    let expires: Option<NaiveDateTime> = req
        .expires
        .as_deref()
        .and_then(|s| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S").ok());

    let result = sqlx::query(
        r#"UPDATE "boom_verification_token"
           SET key_name = COALESCE($2, key_name),
               key_alias = COALESCE($3, key_alias),
               user_id = COALESCE($4, user_id),
               models = COALESCE($5, models),
               max_budget = COALESCE($6, max_budget),
               budget_duration = COALESCE($7, budget_duration),
               rpm_limit = COALESCE($8, rpm_limit),
               tpm_limit = COALESCE($9, tpm_limit),
               expires = COALESCE($10, expires),
               metadata = COALESCE($11, metadata),
               updated_at = NOW()
           WHERE token = $1"#,
    )
    .bind(&token_hash)
    .bind(&req.key_name)
    .bind(&req.key_alias)
    .bind(&req.user_id)
    .bind(&models_list)
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
                "Internal error",
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
        r#"UPDATE "boom_verification_token" SET blocked = true, updated_at = NOW() WHERE token = $1"#,
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
                "Internal error",
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
        r#"UPDATE "boom_verification_token" SET blocked = false, updated_at = NOW() WHERE token = $1"#,
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
                "Internal error",
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
            let _ = state.admin_tx.send(crate::state::AdminCommand::ConfigChanged).await;
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
        let _ = state.admin_tx.send(crate::state::AdminCommand::ConfigChanged).await;
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
                r#"SELECT EXISTS(SELECT 1 FROM "boom_verification_token" WHERE key_alias = $1)"#,
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

        let models_list: Vec<String> = req.models.clone().unwrap_or_default();

        let result = sqlx::query(
            r#"INSERT INTO "boom_verification_token"
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
        .bind(&models_list)
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
                    "reason": "db_error",
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
    #[serde(default)]
    pub deployment_id: Option<String>,
    /// Quota count multiplier (default 1).
    #[serde(default)]
    pub quota_count_ratio: Option<i64>,
    /// Max concurrent in-flight requests (flow control, 0 = no limit).
    #[serde(default)]
    pub max_inflight_queue_len: Option<i32>,
    /// Max total input context chars across in-flight requests (flow control, 0 = no limit).
    #[serde(default)]
    pub max_context_len: Option<i64>,
}

fn default_timeout() -> i64 {
    1200
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
    auto_disabled: Option<bool>,
    source: Option<String>,
    deployment_id: Option<String>,
    quota_count_ratio: Option<i64>,
    max_inflight_queue_len: Option<i32>,
    max_context_len: Option<i64>,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    updated_at: Option<chrono::DateTime<chrono::Utc>>,
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
                  rpm, tpm, timeout, headers, temperature, max_tokens, enabled, auto_disabled,
                  source, deployment_id, quota_count_ratio,
                  max_inflight_queue_len, max_context_len,
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
            return Json(json!({"error": "Internal error"})).into_response();
        }
    };

    let models: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "id": r.id,
                "model_name": r.model_name,
                "litellm_model": r.litellm_model,
                "api_key": r.api_key,
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
                "auto_disabled": r.auto_disabled.unwrap_or(false),
                "source": r.source,
                "deployment_id": r.deployment_id,
                "quota_count_ratio": r.quota_count_ratio.unwrap_or(1),
                "max_inflight_queue_len": r.max_inflight_queue_len,
                "max_context_len": r.max_context_len,
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
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

    if state.admin_tx.send(crate::state::AdminCommand::CreateModel { req, reply: reply_tx }).await.is_err() {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Admin command handler unavailable",
        )
            .into_response();
    }

    match reply_rx.await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(msg)) => Json(json!({"error": msg})).into_response(),
        Err(_) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Admin command handler dropped reply",
        )
            .into_response(),
    }
}

pub async fn update_model(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(id): Path<Uuid>,
    Json(req): Json<CreateDeploymentRequest>,
) -> Response {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

    if state.admin_tx.send(crate::state::AdminCommand::UpdateModel { id, req, reply: reply_tx }).await.is_err() {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Admin command handler unavailable",
        )
            .into_response();
    }

    match reply_rx.await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(msg)) => Json(json!({"error": msg})).into_response(),
        Err(_) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Admin command handler dropped reply",
        )
            .into_response(),
    }
}

pub async fn delete_model(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Path(id): Path<Uuid>,
) -> Response {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();

    if state.admin_tx.send(crate::state::AdminCommand::DeleteModel { id, reply: reply_tx }).await.is_err() {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Admin command handler unavailable",
        )
            .into_response();
    }

    match reply_rx.await {
        Ok(Ok(value)) => Json(value).into_response(),
        Ok(Err(msg)) => Json(json!({"error": msg})).into_response(),
        Err(_) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Admin command handler dropped reply",
        )
            .into_response(),
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
        updated_at: Option<chrono::DateTime<chrono::Utc>>,
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
            return Json(json!({"error": "Internal error"})).into_response();
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
        return Json(json!({"error": "Internal error"})).into_response();
    }

    // Update in-memory alias store.
    state.alias_store.set_alias(
        req.alias_name.clone(),
        req.target_model.clone(),
        req.hidden,
    );

    tracing::info!(alias = %req.alias_name, target = %req.target_model, "Alias created");
    let _ = state.admin_tx.send(crate::state::AdminCommand::ConfigChanged).await;
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
            let _ = state.admin_tx.send(crate::state::AdminCommand::ConfigChanged).await;
            Json(json!({"ok": true})).into_response()
        }
        Ok(_) => Json(json!({"error": "Alias not found"})).into_response(),
        Err(e) => {
            tracing::error!("Dashboard update_alias failed: {}", e);
            Json(json!({"error": "Internal error"})).into_response()
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
            let _ = state.admin_tx.send(crate::state::AdminCommand::ConfigChanged).await;
            Json(json!({"ok": true, "alias_name": alias_name})).into_response()
        }
        Ok(_) => Json(json!({"error": "Alias not found"})).into_response(),
        Err(e) => {
            tracing::error!("Dashboard delete_alias failed: {}", e);
            Json(json!({"error": "Internal error"})).into_response()
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
        updated_at: Option<chrono::DateTime<chrono::Utc>>,
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
            return Json(json!({"error": "Internal error"})).into_response();
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
        return Json(json!({"error": "Internal error"})).into_response();
    }

    tracing::info!(key = %req.key, "Config updated");
    Json(json!({"ok": true, "key": req.key})).into_response()
}

// ═══════════════════════════════════════════════════════════
// Request Logs
// ═══════════════════════════════════════════════════════════

#[derive(Debug, Deserialize)]
pub struct ListLogsQuery {
    #[serde(default = "default_page")]
    pub page: i64,
    #[serde(default = "default_per_page")]
    pub per_page: i64,
    pub key_hash: Option<String>,
    pub model: Option<String>,
    pub status: Option<String>,
    // Column-level filters (partial match via ILIKE where applicable)
    pub request_id: Option<String>,
    pub key_alias: Option<String>,
    pub api_path: Option<String>,
    pub status_code: Option<i16>,
    pub stream: Option<String>,
    pub error: Option<String>,
    pub team_alias: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct LogRow {
    request_id: Option<String>,
    key_hash: String,
    key_name: Option<String>,
    key_alias: Option<String>,
    team_id: Option<String>,
    team_alias: Option<String>,
    model: String,
    api_path: String,
    is_stream: bool,
    status_code: i16,
    error_type: Option<String>,
    error_message: Option<String>,
    input_tokens: Option<i32>,
    output_tokens: Option<i32>,
    duration_ms: Option<i32>,
    created_at: Option<chrono::DateTime<chrono::Utc>>,
    deployment_id: Option<String>,
}

pub async fn list_logs(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Query(query): Query<ListLogsQuery>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let offset = (query.page - 1).max(0) * query.per_page;

    // Build WHERE clause dynamically.
    let mut where_clauses = Vec::new();
    let mut param_idx = 1u32;

    // Helper: register a param slot, return its index.
    macro_rules! slot {
        ($field:expr) => {
            if $field.is_some() { let i = param_idx; param_idx += 1; Some(i) } else { None }
        };
    }

    let key_hash_param   = slot!(query.key_hash);
    let model_param      = slot!(query.model);
    let status_param     = if query.status.as_deref() == Some("error") { let i = param_idx; param_idx += 1; Some(i) } else { None };
    let request_id_param = slot!(query.request_id);
    let key_alias_param  = slot!(query.key_alias);
    let api_path_param   = slot!(query.api_path);
    let status_code_param= slot!(query.status_code);
    // stream is handled as a static WHERE clause (no param slot needed).
    let error_param      = slot!(query.error);
    let team_alias_param = slot!(query.team_alias);

    if query.key_hash.is_some() {
        where_clauses.push(format!("rl.key_hash = ${}", key_hash_param.unwrap()));
    }
    if query.model.is_some() {
        where_clauses.push(format!("rl.model ILIKE ${}", model_param.unwrap()));
    }
    if query.status.as_deref() == Some("error") {
        where_clauses.push(format!("rl.status_code != ${}", status_param.unwrap()));
    }
    if query.request_id.is_some() {
        where_clauses.push(format!("rl.request_id ILIKE ${}", request_id_param.unwrap()));
    }
    if query.key_alias.is_some() {
        where_clauses.push(format!("(rl.key_alias ILIKE ${0} OR rl.key_name ILIKE ${0})", key_alias_param.unwrap()));
    }
    if query.api_path.is_some() {
        where_clauses.push(format!("rl.api_path ILIKE ${}", api_path_param.unwrap()));
    }
    if query.status_code.is_some() {
        where_clauses.push(format!("rl.status_code = ${}", status_code_param.unwrap()));
    }
    if query.stream.is_some() {
        let s = query.stream.as_deref().unwrap().to_lowercase();
        if s == "yes" || s == "true" || s == "1" {
            where_clauses.push("rl.is_stream = true".to_string());
        } else if s == "no" || s == "false" || s == "0" {
            where_clauses.push("rl.is_stream = false".to_string());
        }
    }
    if query.error.is_some() {
        where_clauses.push(format!("rl.error_message ILIKE ${}", error_param.unwrap()));
    }
    if query.team_alias.is_some() {
        where_clauses.push(format!("bt.team_alias ILIKE ${}", team_alias_param.unwrap()));
    }

    let where_sql = if where_clauses.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", where_clauses.join(" AND "))
    };

    let limit_idx = param_idx;
    param_idx += 1;
    let offset_idx = param_idx;

    let sql = format!(
        r#"SELECT rl.request_id, rl.key_hash, rl.key_name, rl.key_alias, rl.team_id,
                  bt.team_alias,
                  rl.model, rl.api_path,
                  rl.is_stream, rl.status_code, rl.error_type, rl.error_message,
                  rl.input_tokens, rl.output_tokens, rl.duration_ms, rl.created_at,
                  rl.deployment_id
           FROM boom_request_log rl
           LEFT JOIN boom_team_table bt ON rl.team_id = bt.team_id
           {where_sql}
           ORDER BY rl.created_at DESC
           LIMIT ${limit_idx} OFFSET ${offset_idx}"#,
    );

    let count_sql = format!(
        r#"SELECT COUNT(*) FROM boom_request_log rl
           LEFT JOIN boom_team_table bt ON rl.team_id = bt.team_id
           {where_sql}"#,
    );

    let mut q = sqlx::query_as::<_, LogRow>(&sql);
    let mut cq = sqlx::query_scalar::<_, i64>(&count_sql);

    // Pre-build LIKE patterns so they outlive the bind chain.
    let model_pattern      = query.model.as_ref().map(|v| format!("%{}%", v));
    let request_id_pattern = query.request_id.as_ref().map(|v| format!("%{}%", v));
    let key_alias_pattern  = query.key_alias.as_ref().map(|v| format!("%{}%", v));
    let api_path_pattern   = query.api_path.as_ref().map(|v| format!("%{}%", v));
    let error_pattern      = query.error.as_ref().map(|v| format!("%{}%", v));
    let team_alias_pattern = query.team_alias.as_ref().map(|v| format!("%{}%", v));

    // Bind parameters (order must match slot allocation above).
    if let Some(ref v) = query.key_hash {
        q = q.bind(v.clone());
        cq = cq.bind(v.clone());
    }
    if let Some(ref p) = model_pattern {
        q = q.bind(p.clone());
        cq = cq.bind(p.clone());
    }
    if query.status.as_deref() == Some("error") {
        q = q.bind(200i16);
        cq = cq.bind(200i16);
    }
    if let Some(ref p) = request_id_pattern {
        q = q.bind(p.clone());
        cq = cq.bind(p.clone());
    }
    if let Some(ref p) = key_alias_pattern {
        q = q.bind(p.clone());
        cq = cq.bind(p.clone());
    }
    if let Some(ref p) = api_path_pattern {
        q = q.bind(p.clone());
        cq = cq.bind(p.clone());
    }
    if let Some(v) = query.status_code {
        q = q.bind(v);
        cq = cq.bind(v);
    }
    // stream is handled as a static WHERE clause (no bind needed).
    if let Some(ref p) = error_pattern {
        q = q.bind(p.clone());
        cq = cq.bind(p.clone());
    }
    if let Some(ref p) = team_alias_pattern {
        q = q.bind(p.clone());
        cq = cq.bind(p.clone());
    }

    q = q.bind(query.per_page).bind(offset);

    let rows: Vec<LogRow> = match q.fetch_all(db_pool).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Dashboard list_logs query failed: {}", e);
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error",
            )
                .into_response();
        }
    };

    let total: i64 = cq.fetch_one(db_pool).await.unwrap_or(0);

    let logs: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            let display_model = match &r.deployment_id {
                Some(did) if !did.is_empty() => format!("{}:{}", r.model, did),
                _ => r.model.clone(),
            };
            json!({
                "request_id": r.request_id,
                "key_hash": r.key_hash,
                "key_name": r.key_name,
                "key_alias": r.key_alias,
                "team_id": r.team_id,
                "team_alias": r.team_alias,
                "model": display_model,
                "api_path": r.api_path,
                "is_stream": r.is_stream,
                "status_code": r.status_code,
                "error_type": r.error_type,
                "error_message": r.error_message,
                "input_tokens": r.input_tokens,
                "output_tokens": r.output_tokens,
                "duration_ms": r.duration_ms,
                "created_at": r.created_at.map(|d| d.to_rfc3339()),
            })
        })
        .collect();

    Json(json!({
        "logs": logs,
        "page": query.page,
        "per_page": query.per_page,
        "total": total,
    }))
    .into_response()
}

// ═══════════════════════════════════════════════════════════
// Teams
// ═══════════════════════════════════════════════════════════

#[derive(Debug, sqlx::FromRow)]
struct TeamUsageRow {
    team_id: String,
    team_alias: Option<String>,
    key_count: i64,
    total_input_tokens: Option<i64>,
    total_output_tokens: Option<i64>,
    request_count: i64,
}

pub async fn list_teams(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Response {
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return Json(json!({"error": "Database not available"})).into_response();
        }
    };

    let sql = r#"
        SELECT bt.team_id,
               bt.team_alias,
               COALESCE(kc.cnt, 0) AS key_count,
               COALESCE(rl.total_input, 0) AS total_input_tokens,
               COALESCE(rl.total_output, 0) AS total_output_tokens,
               COALESCE(rl.cnt, 0) AS request_count
        FROM boom_team_table bt
        LEFT JOIN (
            SELECT team_id, COUNT(*) AS cnt FROM boom_verification_token GROUP BY team_id
        ) kc ON bt.team_id = kc.team_id
        LEFT JOIN (
            SELECT team_id,
                   SUM(input_tokens)  AS total_input,
                   SUM(output_tokens) AS total_output,
                   COUNT(*)           AS cnt
            FROM boom_request_log GROUP BY team_id
        ) rl ON bt.team_id = rl.team_id
        ORDER BY (COALESCE(rl.total_input, 0) + COALESCE(rl.total_output, 0)) DESC
    "#;

    let rows: Vec<TeamUsageRow> = match sqlx::query_as(sql).fetch_all(db_pool).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Dashboard list_teams query failed: {}", e);
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error",
            )
                .into_response();
        }
    };

    let teams: Vec<Value> = rows
        .into_iter()
        .map(|r| {
            json!({
                "team_id": r.team_id,
                "team_alias": r.team_alias,
                "key_count": r.key_count,
                "total_input_tokens": r.total_input_tokens,
                "total_output_tokens": r.total_output_tokens,
                "request_count": r.request_count,
            })
        })
        .collect();

    Json(json!({ "teams": teams })).into_response()
}

// ═══════════════════════════════════════════════════════════
// Model Statistics
// ═══════════════════════════════════════════════════════════

#[derive(Debug, serde::Serialize, sqlx::FromRow)]
struct ModelStatsRow {
    model: String,
    total_requests: i64,
    success_count: i64,
    error_count: i64,
    total_input_tokens: i64,
    total_output_tokens: i64,
    avg_duration_ms: i32,
    last_request_at: Option<chrono::DateTime<chrono::Utc>>,
}

pub async fn get_model_stats(
    _session: AdminSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Response {
    let pool = match &state.db_pool {
        Some(p) => p,
        None => return Json(json!({"error": "Database not available"})).into_response(),
    };

    let stats = sqlx::query_as::<_, ModelStatsRow>(
        r#"SELECT model,
                  COUNT(*) as total_requests,
                  COUNT(*) FILTER (WHERE status_code = 200) as success_count,
                  COUNT(*) FILTER (WHERE status_code != 200) as error_count,
                  COALESCE(SUM(input_tokens), 0) as total_input_tokens,
                  COALESCE(SUM(output_tokens), 0) as total_output_tokens,
                  COALESCE(AVG(duration_ms), 0)::int as avg_duration_ms,
                  MAX(created_at) as last_request_at
           FROM boom_request_log
           GROUP BY model
           ORDER BY total_requests DESC"#,
    )
    .fetch_all(pool)
    .await;

    match stats {
        Ok(rows) => Json(json!({"models": rows})).into_response(),
        Err(e) => {
            tracing::error!("Failed to query model stats: {}", e);
            Json(json!({"error": e.to_string()})).into_response()
        }
    }
}

// ═══════════════════════════════════════════════════════════
// In-Flight Request Stats (real-time)
// ═══════════════════════════════════════════════════════════

pub async fn get_inflight_stats(
    _session: AdminSession,
    Extension(state): Extension<Arc<DashboardState>>,
) -> Response {
    use std::collections::HashMap;

    let inflight_models = state.inflight.get_stats();
    let inflight_deployments = state.inflight.get_stats_by_deployment();
    let flowcontrol_stats = state.flow_controller.get_stats();

    // Collect model names that already appear in deployment-level stats
    // to avoid double-counting in the model-level fallback.
    let mut models_covered: std::collections::HashSet<String> = std::collections::HashSet::new();

    // Merge by deployment_id (full outer join).
    let mut rows: HashMap<String, serde_json::Value> = HashMap::new();

    // 1. Inflight deployment data — has model + deployment_id + inflight metrics.
    for d in &inflight_deployments {
        models_covered.insert(d.model.clone());
        rows.insert(d.deployment_id.clone(), json!({
            "model": d.model,
            "deployment_id": d.deployment_id,
            "fc_reqs": 0,
            "fc_context": 0,
            "in_reqs": d.inflight_requests,
            "in_context": d.inflight_input_chars,
        }));
    }

    // 2. FlowControl data — has deployment_id + fc metrics.
    for fc in &flowcontrol_stats {
        let did = &fc.deployment_id;
        // FC REQS = waiters (queued requests), FC CONTEXT = current/max usage string.
        let fc_reqs = fc.waiters;
        let fc_context_display = if fc.max_context > 0 {
            format!("{}/{}", fc.current_context, fc.max_context)
        } else if fc.current_context > 0 {
            fc.current_context.to_string()
        } else {
            "-".to_string()
        };
        if let Some(row) = rows.get_mut(did) {
            row["fc_reqs"] = json!(fc_reqs);
            row["fc_context"] = json!(fc_context_display);
        } else {
            let model = state.deployment_store.find_model_by_deployment_id(did)
                .unwrap_or_else(|| "-".to_string());
            rows.insert(did.clone(), json!({
                "model": model,
                "deployment_id": did,
                "fc_reqs": fc_reqs,
                "fc_context": fc_context_display,
                "in_reqs": 0,
                "in_context": 0,
            }));
        }
    }

    // 3. Model-level fallback — deployments without deployment_id.
    for m in &inflight_models {
        if models_covered.contains(&m.model) {
            continue; // Already covered by deployment-level data.
        }
        rows.insert(format!("__model__{}", m.model), json!({
            "model": m.model,
            "deployment_id": "",
            "fc_reqs": 0,
            "fc_context": 0,
            "in_reqs": m.inflight_requests,
            "in_context": m.inflight_input_chars,
        }));
    }

    // Sort by model then deployment_id for stable display.
    let mut result: Vec<_> = rows.into_values().collect();
    result.sort_by(|a, b| {
        let am = a["model"].as_str().unwrap_or("");
        let bm = b["model"].as_str().unwrap_or("");
        am.cmp(bm).then_with(|| {
            a["deployment_id"].as_str().unwrap_or("").cmp(b["deployment_id"].as_str().unwrap_or(""))
        })
    });

    Json(json!({ "deployments": result })).into_response()
}

// ═══════════════════════════════════════════════════════════
// Rate Limit Window Reset
// ═══════════════════════════════════════════════════════════

/// POST /admin/limits/reset/{key_hash} — clear all rate limit windows for one key.
pub async fn reset_limits_for_key(
    _session: AdminSession,
    Extension(state): Extension<Arc<DashboardState>>,
    Path(key_hash): Path<String>,
) -> Json<Value> {
    tracing::info!(key_hash = %key_hash, "Admin resetting rate limit windows for key");
    let removed = state.limiter.clear_for_key(&key_hash);
    Json(json!({
        "ok": true,
        "cleared": removed,
        "message": format!("Cleared {} window counter(s) for key '{}'", removed, key_hash)
    }))
}

/// POST /admin/limits/reset — clear all rate limit windows for all keys.
pub async fn reset_limits_all(
    _session: AdminSession,
    Extension(state): Extension<Arc<DashboardState>>,
) -> Json<Value> {
    tracing::info!("Admin resetting ALL rate limit windows");
    let removed = state.limiter.clear_all();
    Json(json!({
        "ok": true,
        "cleared": removed,
        "message": format!("Cleared all {} window counter(s)", removed)
    }))
}
