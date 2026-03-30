use crate::models::VerificationToken;
use boom_core::provider::Authenticator;
use boom_core::types::AuthIdentity;
use boom_core::GatewayError;
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use sqlx::PgPool;
use std::time::Duration;
use tracing;

/// Database-backed authenticator compatible with litellm's schema.
///
/// Flow:
/// 1. Extract raw API key from request
/// 2. SHA-256 hash it (for sk- prefixed keys)
/// 3. Check in-memory cache first (moka)
/// 4. Fall back to PostgreSQL query on `LiteLLM_VerificationToken`
/// 5. Validate: not blocked, not expired, budget OK
pub struct DbAuthenticator {
    db: Option<PgPool>,
    /// Master key for admin access (plain text, compared with constant-time comparison).
    master_key: Option<String>,
    /// Cache: hashed_token → VerificationToken.
    cache: moka::future::Cache<String, VerificationToken>,
}

impl DbAuthenticator {
    pub fn new(db: Option<PgPool>, master_key: Option<String>) -> Self {
        Self {
            db,
            master_key,
            cache: moka::future::Cache::builder()
                .max_capacity(10_000)
                .time_to_idle(Duration::from_secs(300)) // 5 min TTL
                .build(),
        }
    }

    /// SHA-256 hash a raw API key, matching litellm's hash_token function.
    pub fn hash_token(token: &str) -> String {
        let mut hasher = Sha256::new();
        hasher.update(token.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Check if a raw key matches the master key using constant-time comparison.
    fn is_master_key(&self, raw_key: &str) -> bool {
        match &self.master_key {
            Some(master) => {
                // Constant-time comparison to prevent timing attacks.
                let equal = raw_key.as_bytes().len() == master.as_bytes().len()
                    && raw_key
                        .as_bytes()
                        .iter()
                        .zip(master.as_bytes().iter())
                        .fold(0, |acc, (a, b)| acc | (a ^ b))
                        == 0;
                equal
            }
            None => false,
        }
    }

    /// Look up a token by its hash, checking cache first then DB.
    async fn lookup_token(&self, hashed: &str) -> Result<Option<VerificationToken>, GatewayError> {
        // 1. Check cache
        if let Some(cached) = self.cache.get(hashed).await {
            tracing::debug!("Token cache hit: {}", &hashed[..8]);
            return Ok(Some(cached));
        }

        // 2. No database configured — can only use master key.
        let db = match &self.db {
            Some(pool) => pool,
            None => return Ok(None),
        };

        // 3. Query database
        tracing::debug!("Token cache miss, querying DB: {}", &hashed[..8]);
        let result = sqlx::query_as::<_, VerificationToken>(
            r#"SELECT token, key_name, key_alias, spend, expires, models,
                      aliases, config, user_id, team_id, permissions,
                      max_parallel_requests, metadata, blocked,
                      tpm_limit, rpm_limit, max_budget, budget_duration,
                      budget_reset_at, allowed_cache_controls, allowed_routes,
                      model_spend, model_max_budget, budget_id, organization_id,
                      created_at, created_by, updated_at
               FROM "LiteLLM_VerificationToken"
               WHERE token = $1"#,
        )
        .bind(hashed)
        .fetch_optional(db)
        .await
        .map_err(|e| {
            tracing::error!("DB query failed for token lookup: {}", e);
            GatewayError::InternalError(format!("Database error: {}", e))
        })?;

        // 3. Cache the result
        if let Some(ref token) = result {
            self.cache.insert(hashed.to_string(), token.clone()).await;
        }

        Ok(result)
    }

    /// Convert a DB token row into an AuthIdentity.
    fn token_to_identity(&self, token: VerificationToken) -> AuthIdentity {
        AuthIdentity {
            key_hash: token.token.clone(),
            key_name: token.key_name,
            user_id: token.user_id,
            team_id: token.team_id,
            models: token.models,
            rpm_limit: token.rpm_limit.map(|v| v as u64),
            tpm_limit: token.tpm_limit.map(|v| v as u64),
            max_budget: token.max_budget,
            spend: token.spend,
            blocked: token.blocked.unwrap_or(false),
            expires_at: token.expires,
            metadata: token.metadata.unwrap_or(serde_json::Value::Null),
        }
    }
}

#[async_trait]
impl Authenticator for DbAuthenticator {
    async fn authenticate(&self, raw_key: &str) -> Result<AuthIdentity, GatewayError> {
        // 1. Check master key
        if self.is_master_key(raw_key) {
            tracing::debug!("Master key authenticated");
            return Ok(AuthIdentity {
                key_hash: "master".to_string(),
                key_name: Some("master".to_string()),
                user_id: None,
                team_id: None,
                models: vec![], // master can access all models
                rpm_limit: None,
                tpm_limit: None,
                max_budget: None,
                spend: 0.0,
                blocked: false,
                expires_at: None,
                metadata: serde_json::Value::Null,
            });
        }

        // 2. Hash the key (litellm hashes all sk- prefixed keys)
        let hashed = if raw_key.starts_with("sk-") {
            Self::hash_token(raw_key)
        } else {
            raw_key.to_string()
        };

        // 3. Look up in cache / DB
        let token = self
            .lookup_token(&hashed)
            .await?
            .ok_or_else(|| GatewayError::AuthError("Invalid API key".to_string()))?;

        // 4. Validate the token
        let identity = self.token_to_identity(token);

        if identity.blocked {
            return Err(GatewayError::KeyBlocked);
        }

        if identity.is_expired() {
            return Err(GatewayError::KeyExpired);
        }

        if identity.is_budget_exceeded() {
            return Err(GatewayError::BudgetExceeded);
        }

        tracing::debug!(
            "Key authenticated: {:?}, team={:?}",
            identity.key_name,
            identity.team_id
        );

        Ok(identity)
    }

    fn check_model_access(&self, identity: &AuthIdentity, model: &str) -> Result<(), GatewayError> {
        tracing::debug!(
            "check_model_access: key={:?}, requested_model={}, allowed_models={:?}",
            identity.key_name, model, identity.models
        );
        if identity.can_call_model(model) {
            Ok(())
        } else {
            tracing::warn!(
                "Model not allowed: key={:?}, requested={}, allowed={:?}",
                identity.key_name, model, identity.models
            );
            Err(GatewayError::ModelNotAllowed(model.to_string()))
        }
    }
}
