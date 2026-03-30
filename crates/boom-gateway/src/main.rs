mod extractor;
mod routes;
mod state;

use axum::routing::{delete, get, post, put};
use axum::Router;
use clap::Parser;
use state::AppState;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

#[derive(Parser, Debug)]
#[command(name = "boom-gateway", about = "BooMGateway — High-performance LLM API Gateway")]
struct Args {
    /// Path to config YAML file.
    #[arg(short, long, default_value = "config.yaml")]
    config: String,

    /// Bind host (overrides config file).
    #[arg(long)]
    host: Option<String>,

    /// Bind port (overrides config file).
    #[arg(long)]
    port: Option<u16>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    init_tracing();
    tracing::info!("BooMGateway starting...");

    // Load config.
    let config = boom_config::load_config(&args.config)?;

    // CLI overrides.
    let host = args.host.unwrap_or(config.server.host.clone());
    let port = args.port.unwrap_or(config.server.port);

    // Build state (connects DB, initializes providers).
    let state = AppState::from_config(config, args.config.clone()).await?;

    // Spawn SIGHUP reload listener.
    spawn_sighup_listener(state.clone());

    // Build router.
    let app = build_router(state);

    // Start server.
    let addr = format!("{}:{}", host, port);
    tracing::info!("BooMGateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    tracing::info!("BooMGateway shutdown complete");
    Ok(())
}

fn build_router(state: AppState) -> Router {
    // Public OpenAI-compatible API routes.
    let api_routes = Router::new()
        .route("/v1/chat/completions", post(routes::chat_completions))
        .route("/v1/messages", post(routes::messages))
        .route("/v1/models", get(routes::list_models));

    // Health check routes (no auth required).
    let health_routes = Router::new()
        .route("/health", get(routes::health_check))
        .route("/health/live", get(routes::liveness_check))
        .route("/health/ready", get(routes::readiness_check));

    // Admin routes (require master key).
    let admin_routes = Router::new()
        .route("/admin/config/reload", post(routes::admin_reload_config))
        // Plan management.
        .route(
            "/admin/plans",
            put(routes::admin_upsert_plan).get(routes::admin_list_plans),
        )
        .route("/admin/plans/{name}", delete(routes::admin_delete_plan))
        .route("/admin/plans/assign", post(routes::admin_assign_key))
        .route(
            "/admin/plans/assign/{key_hash}",
            delete(routes::admin_unassign_key),
        )
        .route(
            "/admin/plans/assignments",
            get(routes::admin_list_assignments),
        );

    Router::new()
        .merge(api_routes)
        .merge(health_routes)
        .merge(admin_routes)
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

/// Listen for SIGHUP and trigger hot-reload.
fn spawn_sighup_listener(state: AppState) {
    #[cfg(unix)]
    {
        tokio::spawn(async move {
            let mut stream = match tokio::signal::unix::signal(
                tokio::signal::unix::SignalKind::hangup(),
            ) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("Failed to install SIGHUP handler: {}", e);
                    return;
                }
            };

            loop {
                stream.recv().await;
                tracing::info!("Received SIGHUP — triggering hot-reload...");
                match state.reload().await {
                    Ok(summary) => tracing::info!("SIGHUP reload: {}", summary),
                    Err(e) => tracing::error!("SIGHUP reload failed: {}", e),
                }
            }
        });
        tracing::info!("SIGHUP hot-reload listener installed");
    }

    #[cfg(not(unix))]
    {
        tracing::info!("SIGHUP not supported on this platform — use POST /admin/config/reload");
        let _ = state; // suppress unused warning
    }
}

fn init_tracing() {
    use tracing_subscriber::EnvFilter;
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .json()
        .with_target(false)
        .init();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            tracing::info!("Received Ctrl+C, shutting down...");
        },
        _ = terminate => {
            tracing::info!("Received SIGTERM, shutting down...");
        },
    }
}
