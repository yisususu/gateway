use axum::Extension;
use axum::Json;
use serde_json::{json, Value};

use crate::auth::DashboardSession;
use crate::state::DashboardState;

// ── GET /dashboard/api/user/plan ───────────────────────────

pub async fn get_plan(
    session: DashboardSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Json<Value> {
    let key_hash = &session.claims.key_hash;

    // Resolve plan: explicit assignment → default plan → null.
    let plan = state
        .plan_store
        .resolve_plan(key_hash)
        .or_else(|| state.plan_store.get_default_plan());

    match plan {
        Some(p) => {
            let (concurrency_limit, rpm_limit, window_limits) = p.effective_limits();
            Json(json!({
                "plan_name": p.name,
                "concurrency_limit": concurrency_limit,
                "rpm_limit": rpm_limit,
                "window_limits": window_limits,
                "schedule": p.schedule,
            }))
        }
        None => Json(json!({
            "plan_name": null,
            "concurrency_limit": null,
            "rpm_limit": null,
            "window_limits": [],
            "schedule": [],
        })),
    }
}

// ── GET /dashboard/api/user/usage ──────────────────────────

pub async fn get_usage(
    session: DashboardSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Json<Value> {
    let key_hash = &session.claims.key_hash;

    let windows: Vec<Value> = state
        .limiter
        .get_usage_for_key(key_hash)
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

    let concurrency = state.plan_store.get_concurrency(key_hash);

    Json(json!({
        "concurrency": concurrency,
        "windows": windows,
    }))
}

// ── GET /dashboard/api/user/key-info ───────────────────────

pub async fn get_key_info(
    session: DashboardSession,
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
) -> Json<Value> {
    let key_hash = &session.claims.key_hash;

    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => return Json(json!({"error": "Database not available"})),
    };

    let row: Option<(
        Option<String>,
        Option<String>,
        f64,
        Option<chrono::NaiveDateTime>,
        Option<bool>,
        Option<i64>,
        Option<i64>,
        Option<f64>,
        Option<String>,
        Option<serde_json::Value>,
        Option<chrono::NaiveDateTime>,
    )> = sqlx::query_as(
        r#"SELECT key_name, key_alias, spend, expires, blocked,
                  rpm_limit, tpm_limit, max_budget, budget_duration,
                  metadata, created_at
           FROM "LiteLLM_VerificationToken" WHERE token = $1"#,
    )
    .bind(key_hash)
    .fetch_optional(db_pool)
    .await
    .unwrap_or(None);

    match row {
        Some((key_name, key_alias, spend, expires, blocked, rpm_limit, tpm_limit, max_budget, budget_duration, metadata, created_at)) => {
            Json(json!({
                "key_name": key_name,
                "key_alias": key_alias,
                "spend": spend,
                "expires": expires.map(|d| d.to_string()),
                "blocked": blocked.unwrap_or(false),
                "rpm_limit": rpm_limit,
                "tpm_limit": tpm_limit,
                "max_budget": max_budget,
                "budget_duration": budget_duration,
                "metadata": metadata,
                "created_at": created_at.map(|d| d.to_string()),
                "token_prefix": format!("{}...", &key_hash[..8.min(key_hash.len())]),
            }))
        }
        None => Json(json!({"error": "Key not found"})),
    }
}
