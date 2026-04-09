use boom_core::provider::Provider;
use boom_dashboard::state::AdminCommand;
use boom_routing::DeploymentStore;
use serde_json::{json, Value};
use sqlx::Row;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

use crate::state::AppState;

/// Background task: receives AdminCommand from dashboard and executes writes.
/// Has access to AppState (db_pool, deployment_store, boom-provider, boom-config).
pub async fn admin_command_handler(mut rx: tokio::sync::mpsc::Receiver<AdminCommand>, state: AppState) {
    tracing::info!("Admin command handler started");
    while let Some(cmd) = rx.recv().await {
        match cmd {
            AdminCommand::CreateModel { req, reply } => {
                let result = handle_create_model(&state, req).await;
                let _ = reply.send(result);
                state.dump_config_snapshot().await;
            }
            AdminCommand::UpdateModel { id, req, reply } => {
                let result = handle_update_model(&state, id, req).await;
                let _ = reply.send(result);
                state.dump_config_snapshot().await;
            }
            AdminCommand::DeleteModel { id, reply } => {
                let result = handle_delete_model(&state, id).await;
                let _ = reply.send(result);
                state.dump_config_snapshot().await;
            }
            AdminCommand::ConfigChanged => {
                state.dump_config_snapshot().await;
            }
        }
    }
    tracing::warn!("Admin command handler stopped (channel closed)");
}

async fn handle_create_model(
    state: &AppState,
    req: boom_dashboard::handlers_admin::CreateDeploymentRequest,
) -> Result<Value, String> {
    let db_pool = state.db_pool.as_ref().ok_or("Database not available")?;
    let headers_json = serde_json::to_value(&req.headers).unwrap_or(json!({}));

    // Insert into DB.
    let result = sqlx::query(
        r#"INSERT INTO boom_model_deployment
           (model_name, litellm_model, api_key, api_key_env, api_base, api_version,
            aws_region_name, aws_access_key_id, aws_secret_access_key,
            rpm, tpm, timeout, headers, temperature, max_tokens, enabled, source, deployment_id, quota_count_ratio)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, 'db', $17, $18)
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
    .bind(&req.deployment_id)
    .bind(req.quota_count_ratio.unwrap_or(1))
    .fetch_one(db_pool)
    .await
    .map_err(|e| format!("DB insert failed: {}", e))?;

    let id: Uuid = result.get("id");

    // Build provider and add to memory (if enabled).
    if req.enabled {
        if let Some(provider) = build_provider(&req) {
            state.deployment_store.add_deployment(&req.model_name, provider);
            let ratio = req.quota_count_ratio.unwrap_or(1) as u64;
            state.deployment_store.set_quota_ratio(&req.model_name, ratio);
            tracing::info!(model = %req.model_name, "Model deployment created and loaded");
        }
    }

    Ok(json!({"ok": true, "id": id, "model_name": req.model_name}))
}

async fn handle_update_model(
    state: &AppState,
    id: Uuid,
    req: boom_dashboard::handlers_admin::CreateDeploymentRequest,
) -> Result<Value, String> {
    let db_pool = state.db_pool.as_ref().ok_or("Database not available")?;
    let headers_json = serde_json::to_value(&req.headers).unwrap_or(json!({}));

    let result = sqlx::query(
        r#"UPDATE boom_model_deployment
           SET model_name = $2, litellm_model = $3, api_key = $4, api_key_env = $5,
               api_base = $6, api_version = $7, aws_region_name = $8,
               aws_access_key_id = $9, aws_secret_access_key = $10,
               rpm = $11, tpm = $12, timeout = $13, headers = $14,
               temperature = $15, max_tokens = $16, enabled = $17,
               deployment_id = $18, quota_count_ratio = $19, updated_at = NOW()
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
    .bind(&req.deployment_id)
    .bind(req.quota_count_ratio.unwrap_or(1))
    .execute(db_pool)
    .await
    .map_err(|e| format!("DB update failed: {}", e))?;

    if result.rows_affected() == 0 {
        return Err("Model deployment not found".to_string());
    }

    // Rebuild provider list for this model from DB (handles enable/disable/rename).
    reload_model_deployments(db_pool, &state.deployment_store, &req.model_name).await;

    if req.enabled {
        let ratio = req.quota_count_ratio.unwrap_or(1) as u64;
        state.deployment_store.set_quota_ratio(&req.model_name, ratio);
    }

    Ok(json!({"ok": true}))
}

async fn handle_delete_model(
    state: &AppState,
    id: Uuid,
) -> Result<Value, String> {
    let db_pool = state.db_pool.as_ref().ok_or("Database not available")?;

    // Get model_name before deleting.
    let model_name: Option<String> = sqlx::query_scalar(
        r#"SELECT model_name FROM boom_model_deployment WHERE id = $1"#,
    )
    .bind(id)
    .fetch_optional(db_pool)
    .await
    .map_err(|e| format!("DB lookup failed: {}", e))?
    .flatten();

    let result = sqlx::query(
        r#"DELETE FROM boom_model_deployment WHERE id = $1"#,
    )
    .bind(id)
    .execute(db_pool)
    .await
    .map_err(|e| format!("DB delete failed: {}", e))?;

    if result.rows_affected() == 0 {
        return Err("Model deployment not found".to_string());
    }

    let name = model_name.unwrap_or_default();

    // Reload deployments for this model_name from DB to keep memory in sync.
    // This handles the case where multiple deployments exist for the same model.
    reload_model_deployments(db_pool, &state.deployment_store, &name).await;

    tracing::info!(model = %name, "Model deployment deleted");
    Ok(json!({"ok": true, "model_name": name}))
}

/// Reload all deployments for a specific model_name from DB into the deployment store.
async fn reload_model_deployments(
    pool: &sqlx::PgPool,
    deployment_store: &Arc<DeploymentStore>,
    model_name: &str,
) {
    let rows: Vec<DeploymentRow> = match sqlx::query_as::<_, DeploymentRow>(
        r#"SELECT model_name, litellm_model, api_key, api_key_env, api_base, api_version,
                  aws_region_name, timeout, headers, deployment_id
           FROM boom_model_deployment
           WHERE model_name = $1 AND enabled IS NOT FALSE
           ORDER BY created_at"#,
    )
    .bind(model_name)
    .fetch_all(pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("Failed to reload deployments for '{}': {}", model_name, e);
            return;
        }
    };

    let mut providers: Vec<Arc<dyn Provider>> = Vec::new();
    for row in &rows {
        if let Some(p) = build_provider_from_row(row) {
            providers.push(p);
        }
    }

    if providers.is_empty() {
        deployment_store.remove_deployments(model_name);
    } else {
        deployment_store.set_deployments(model_name.to_string(), providers);
    }
}

#[derive(Debug, sqlx::FromRow)]
struct DeploymentRow {
    model_name: String,
    litellm_model: String,
    api_key: Option<String>,
    api_key_env: Option<bool>,
    api_base: Option<String>,
    api_version: Option<String>,
    aws_region_name: Option<String>,
    timeout: i64,
    headers: serde_json::Value,
    deployment_id: Option<String>,
}

/// Build a Provider from a DB deployment row.
fn build_provider_from_row(row: &DeploymentRow) -> Option<Arc<dyn Provider>> {
    let mut extra = HashMap::new();
    if let Some(obj) = row.headers.as_object() {
        for (k, v) in obj {
            if let Some(s) = v.as_str() {
                extra.insert(k.clone(), s.to_string());
            }
        }
    }
    if let Some(ref v) = row.api_version {
        extra.insert("api_version".to_string(), v.clone());
    }
    if let Some(ref r) = row.aws_region_name {
        extra.insert("aws_region_name".to_string(), r.clone());
    }

    let api_key = row.api_key.as_ref().map(|k| {
        if row.api_key_env.unwrap_or(false) {
            boom_config::resolve_env_value(k)
        } else {
            k.clone()
        }
    });

    match boom_provider::create_provider(
        &row.litellm_model,
        api_key,
        row.api_base.clone(),
        row.timeout as u64,
        &extra,
        row.deployment_id.clone(),
    ) {
        Ok(provider) => Some(provider),
        Err(e) => {
            tracing::error!("Failed to build provider for '{}': {}", row.model_name, e);
            None
        }
    }
}

/// Build a Provider from a CreateDeploymentRequest (dashboard API).
fn build_provider(req: &boom_dashboard::handlers_admin::CreateDeploymentRequest) -> Option<Arc<dyn Provider>> {
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
        req.deployment_id.clone(),
    ) {
        Ok(provider) => Some(provider),
        Err(e) => {
            tracing::error!("Failed to build provider for '{}': {}", req.model_name, e);
            None
        }
    }
}
