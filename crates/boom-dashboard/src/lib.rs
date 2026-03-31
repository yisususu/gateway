pub mod auth;
pub mod handlers_admin;
pub mod handlers_static;
pub mod handlers_user;
pub mod state;

use axum::routing::{delete, get, post, put};
use axum::Router;
use std::sync::Arc;

pub use state::DashboardState;

/// Build the dashboard router.
///
/// Returns `Router<S>` — state is injected via `Extension<Arc<DashboardState>>`,
/// so S can be any state type (including the gateway's AppState).
/// No handlers extract `State<S>`, so this works with any S.
pub fn build_router<S: Clone + Send + Sync + 'static>(state: DashboardState) -> Router<S> {
    let state_arc = Arc::new(state);

    Router::new()
        // Root redirect → /dashboard
        .route("/", get(handlers_static::redirect_root))
        // Static files (SPA).
        // Note: use "/dashboard" (no trailing slash) so both /dashboard and /dashboard/ work.
        .route("/dashboard", get(handlers_static::index))
        .route("/dashboard/", get(handlers_static::index))
        .route("/dashboard/style.css", get(handlers_static::style_css))
        .route("/dashboard/app.js", get(handlers_static::app_js))
        // Auth endpoints.
        .route("/dashboard/api/auth/login", post(auth::login))
        .route("/dashboard/api/auth/logout", post(auth::logout))
        .route("/dashboard/api/auth/me", get(auth::me))
        // User endpoints.
        .route("/dashboard/api/user/plan", get(handlers_user::get_plan))
        .route("/dashboard/api/user/usage", get(handlers_user::get_usage))
        .route(
            "/dashboard/api/user/key-info",
            get(handlers_user::get_key_info),
        )
        // Admin — Plan management.
        .route(
            "/dashboard/api/admin/plans",
            get(handlers_admin::list_plans).put(handlers_admin::upsert_plan),
        )
        .route(
            "/dashboard/api/admin/plans/{name}",
            delete(handlers_admin::delete_plan),
        )
        // Admin — Key management.
        .route(
            "/dashboard/api/admin/keys",
            get(handlers_admin::list_keys).post(handlers_admin::create_key),
        )
        .route(
            "/dashboard/api/admin/keys/{token_hash}",
            put(handlers_admin::update_key),
        )
        .route(
            "/dashboard/api/admin/keys/{token_hash}/block",
            post(handlers_admin::block_key),
        )
        .route(
            "/dashboard/api/admin/keys/{token_hash}/unblock",
            post(handlers_admin::unblock_key),
        )
        // Admin — Assignment management.
        .route(
            "/dashboard/api/admin/assignments",
            get(handlers_admin::list_assignments).post(handlers_admin::assign_key),
        )
        .route(
            "/dashboard/api/admin/assignments/{key_hash}",
            delete(handlers_admin::unassign_key),
        )
        // Admin — Usage query.
        .route(
            "/dashboard/api/admin/usage/{key_hash}",
            get(handlers_admin::get_key_usage),
        )
        // SPA fallback — must be last.
        .route("/dashboard/{*path}", get(handlers_static::spa_fallback))
        // Inject state via Extension layer.
        .layer(axum::Extension(state_arc))
}
