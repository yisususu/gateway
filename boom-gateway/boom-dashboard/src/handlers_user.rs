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
           FROM "boom_verification_token" WHERE token = $1"#,
    )
    .bind(key_hash)
    .fetch_optional(db_pool)
    .await
    .unwrap_or(None);

    match row {
        Some((key_name, key_alias, spend, expires, blocked, rpm_limit, tpm_limit, max_budget, budget_duration, metadata, created_at)) => {
            // Query token usage from LiteLLM_SpendLogs (may not exist in all deployments).
            let (input_tokens, output_tokens) = query_token_usage(db_pool, key_hash).await;

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
                "total_input_tokens": input_tokens,
                "total_output_tokens": output_tokens,
            }))
        }
        None => Json(json!({"error": "Key not found"})),
    }
}

/// Query aggregated token usage from litellm's SpendLogs table.
/// Returns (input, output) token counts. Returns (None, None) if the table
/// doesn't exist or the query fails.
async fn query_token_usage(
    pool: &sqlx::PgPool,
    key_hash: &str,
) -> (Option<i64>, Option<i64>) {
    // SUM always produces a row; COALESCE handles the no-matches case.
    // ::BIGINT ensures sqlx can decode into i64 regardless of source column type.
    let row: Option<(i64, i64)> = sqlx::query_as(
        r#"SELECT COALESCE(SUM(prompt_tokens), 0)::BIGINT,
                  COALESCE(SUM(completion_tokens), 0)::BIGINT
           FROM "LiteLLM_SpendLogs" WHERE api_key = $1"#,
    )
    .bind(key_hash)
    .fetch_one(pool)
    .await
    .ok();

    match row {
        Some((input, output)) => {
            // If both are 0 the table exists but has no data for this key.
            // Still return the values so the frontend can show "0".
            (Some(input), Some(output))
        }
        None => (None, None),
    }
}
