use axum::extract::FromRequestParts;
use axum::http::header::{COOKIE, SET_COOKIE};
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::Utc;
use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::state::DashboardState;

// ── JWT Claims ──────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardClaims {
    /// "admin" or user_id.
    pub sub: String,
    /// "admin" | "user".
    pub role: String,
    /// User's token hash (empty for admin).
    pub key_hash: String,
    pub exp: i64,
    pub iat: i64,
}

const SESSION_COOKIE_NAME: &str = "boom_session";
const SESSION_DURATION_SECS: i64 = 7200; // 2 hours

// ── Session Extractor ──────────────────────────────────────

/// Extractor that reads the session cookie and verifies the JWT.
#[derive(Debug, Clone)]
pub struct DashboardSession {
    pub claims: DashboardClaims,
}

impl<S: Send + Sync> FromRequestParts<S> for DashboardSession {
    type Rejection = Response;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let result = extract_session(parts);
        std::future::ready(result)
    }
}

fn extract_session(parts: &mut Parts) -> Result<DashboardSession, Response> {
    let state = parts
        .extensions
        .get::<std::sync::Arc<DashboardState>>()
        .cloned()
        .ok_or_else(|| {
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "DashboardState not found",
            )
                .into_response()
        })?;

    let cookie_header = parts
        .headers
        .get_all(COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .find_map(|cookie| {
            let cookie = cookie.trim();
            let (name, value) = cookie.split_once('=')?;
            if name.trim() == SESSION_COOKIE_NAME {
                Some(value.trim().to_string())
            } else {
                None
            }
        })
        .ok_or_else(|| {
            (axum::http::StatusCode::UNAUTHORIZED, "No session cookie").into_response()
        })?;

    let token_data = decode::<DashboardClaims>(
        &cookie_header,
        &DecodingKey::from_secret(state.jwt_secret.as_bytes()),
        &Validation::default(),
    )
    .map_err(|_| {
        (axum::http::StatusCode::UNAUTHORIZED, "Invalid session").into_response()
    })?;

    Ok(DashboardSession {
        claims: token_data.claims,
    })
}

// ── Admin Session Extractor ────────────────────────────────

/// Extractor that requires admin role.
pub struct AdminSession {
    pub claims: DashboardClaims,
}

impl<S: Send + Sync> FromRequestParts<S> for AdminSession {
    type Rejection = Response;

    fn from_request_parts(
        parts: &mut Parts,
        _state: &S,
    ) -> impl std::future::Future<Output = Result<Self, Self::Rejection>> + Send {
        let result = extract_session(parts).and_then(|session| {
            if session.claims.role == "admin" {
                Ok(AdminSession {
                    claims: session.claims,
                })
            } else {
                Err(
                    (axum::http::StatusCode::FORBIDDEN, "Admin access required")
                        .into_response(),
                )
            }
        });
        std::future::ready(result)
    }
}

// ── Login Request ──────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct LoginRequest {
    pub user_id: String,
    pub api_key: String,
}

#[derive(Debug, Serialize)]
pub struct LoginResponse {
    pub role: String,
    pub user_id: String,
}

#[derive(Debug, Serialize)]
pub struct MeResponse {
    pub user_id: String,
    pub role: String,
}

// ── Login Handler ──────────────────────────────────────────

pub async fn login(
    Extension(state): Extension<std::sync::Arc<DashboardState>>,
    Json(req): Json<LoginRequest>,
) -> Response {
    // Admin login: user_id == "admin" + constant-time comparison with master_key.
    if req.user_id == "admin" {
        let master_key = match &state.master_key {
            Some(k) => k,
            None => {
                return (axum::http::StatusCode::FORBIDDEN, "Admin login disabled").into_response();
            }
        };

        // Constant-time comparison.
        let equal = constant_time_eq(req.api_key.as_bytes(), master_key.as_bytes());
        if !equal {
            return (axum::http::StatusCode::UNAUTHORIZED, "Invalid credentials")
                .into_response();
        }

        return sign_and_respond(&state, "admin".to_string(), "admin".to_string(), String::new());
    }

    // User login: SHA-256(api_key) → lookup in DB.
    // user_id field is ignored for non-admin login (can be "user", empty, or anything).
    let db_pool = match &state.db_pool {
        Some(pool) => pool,
        None => {
            return (axum::http::StatusCode::SERVICE_UNAVAILABLE, "Database not available")
                .into_response();
        }
    };

    let token_hash = hash_token(&req.api_key);

    let row: Option<(Option<String>, Option<String>, Option<bool>)> = sqlx::query_as(
        r#"SELECT user_id, key_alias, blocked FROM "LiteLLM_VerificationToken" WHERE token = $1"#,
    )
    .bind(&token_hash)
    .fetch_optional(db_pool)
    .await
    .unwrap_or(None);

    let (user_id, key_alias, blocked) = match row {
        Some((uid, alias, blk)) => (uid, alias, blk),
        None => {
            return (axum::http::StatusCode::UNAUTHORIZED, "Invalid API key")
                .into_response();
        }
    };

    // Check blocked.
    if blocked.unwrap_or(false) {
        return (axum::http::StatusCode::FORBIDDEN, "Key is blocked").into_response();
    }

    // Use key_alias as display name, fallback to user_id or "user".
    let display_name = key_alias
        .or(user_id)
        .unwrap_or_else(|| "user".to_string());

    sign_and_respond(&state, display_name, "user".to_string(), token_hash)
}

fn sign_and_respond(
    state: &DashboardState,
    user_id: String,
    role: String,
    key_hash: String,
) -> Response {
    let now = Utc::now().timestamp();
    let claims = DashboardClaims {
        sub: user_id.clone(),
        role: role.clone(),
        key_hash,
        exp: now + SESSION_DURATION_SECS,
        iat: now,
    };

    let token = match encode(
        &Header::default(),
        &claims,
        &EncodingKey::from_secret(state.jwt_secret.as_bytes()),
    ) {
        Ok(t) => t,
        Err(_) => {
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to create session",
            )
                .into_response();
        }
    };

    let cookie = format!(
        "{}={}; HttpOnly; SameSite=Lax; Path=/dashboard; Max-Age={}",
        SESSION_COOKIE_NAME, token, SESSION_DURATION_SECS
    );

    let body = serde_json::to_string(&LoginResponse { role, user_id }).unwrap();

    ([(SET_COOKIE, cookie)], body).into_response()
}

// ── Logout Handler ─────────────────────────────────────────

pub async fn logout() -> Response {
    let cookie = format!(
        "{}=; HttpOnly; SameSite=Lax; Path=/dashboard; Max-Age=0",
        SESSION_COOKIE_NAME
    );
    ([(SET_COOKIE, cookie)], axum::http::StatusCode::NO_CONTENT).into_response()
}

// ── Me Handler ─────────────────────────────────────────────

pub async fn me(session: DashboardSession) -> Json<MeResponse> {
    Json(MeResponse {
        user_id: session.claims.sub,
        role: session.claims.role,
    })
}

// ── Helpers ────────────────────────────────────────────────

pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut result = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        result |= x ^ y;
    }
    result == 0
}
