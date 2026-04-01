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

    // Shutdown broadcast channel: send once to cancel all background tasks.
    let (shutdown_tx, _) = tokio::sync::broadcast::channel::<()>(1);

    // Spawn SIGHUP reload listener.
    spawn_sighup_listener(state.clone(), shutdown_tx.subscribe());

    // Spawn background sync task (persist rate limit state + cleanup memory).
    spawn_sync_task(state.clone(), shutdown_tx.subscribe());

    // Build router.
    let app = build_router(state);

    // Start server.
    let addr = format!("{}:{}", host, port);
    tracing::info!("BooMGateway listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(&addr).await?;

    // Graceful shutdown with a hard deadline:
    //   1. Ctrl+C triggers graceful shutdown (stop accepting new connections,
    //      let in-flight requests finish).
    //   2. Wait at most 3s for in-flight requests to complete.
    //   3. Force exit — don't get stuck on idle browser keep-alive connections
    //      kept alive by the dashboard's 5s polling timer.
    let server = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal());

    tokio::select! {
        result = server => {
            if let Err(e) = result {
                tracing::error!("Server error: {}", e);
            }
        }
        _ = tokio::time::sleep(std::time::Duration::from_secs(3)) => {
            tracing::warn!("Graceful shutdown timed out, forcing exit");
            std::process::exit(0);
        }
    }

    // Signal all background tasks to stop.
    tracing::info!("Shutting down background tasks...");
    let _ = shutdown_tx.send(());

    // Give tasks a moment to finish, then exit.
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;

    tracing::info!("BooMGateway shutdown complete");
    Ok(())
}

fn build_router(state: AppState) -> Router {
    // Choose API routes based on pass-through mode.
    let pass_through_enabled = state
        .inner
        .load()
        .config
        .pass_through
        .as_ref()
        .map(|pt| pt.enabled)
        .unwrap_or(false);

    let api_routes = if pass_through_enabled {
        tracing::info!("Pass-through mode enabled — forwarding to upstream gateway");
        Router::new()
            .route("/v1/chat/completions", post(routes::pt_chat_completions))
            .route("/v1/messages", post(routes::pt_messages))
            .route("/v1/models", get(routes::list_models))
    } else {
        Router::new()
            .route("/v1/chat/completions", post(routes::chat_completions))
            .route("/v1/messages", post(routes::messages))
            .route("/v1/models", get(routes::list_models))
    };

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

    // Dashboard router (Web UI + dashboard API).
    // Returns Router<()> — state injected via Extension<Arc<DashboardState>>.
    let master_key = state.inner.load().config.general_settings.master_key.clone();
    let dashboard_state = boom_dashboard::DashboardState::new(
        state.db_pool.clone(),
        state.plan_store.clone(),
        state.limiter.clone(),
        state.deployment_store.clone(),
        state.alias_store.clone(),
        master_key,
    );
    let dashboard_router = boom_dashboard::build_router(dashboard_state);

    Router::new()
        .merge(api_routes)
        .merge(health_routes)
        .merge(admin_routes)
        .merge(dashboard_router)
        .with_state(state)
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
}

/// Listen for SIGHUP and trigger hot-reload.
fn spawn_sighup_listener(state: AppState, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
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
                tokio::select! {
                    _ = stream.recv() => {
                        tracing::info!("Received SIGHUP — triggering hot-reload...");
                        match state.reload().await {
                            Ok(summary) => tracing::info!("SIGHUP reload: {}", summary),
                            Err(e) => tracing::error!("SIGHUP reload failed: {}", e),
                        }
                    }
                    _ = shutdown.recv() => {
                        tracing::debug!("SIGHUP listener shutting down");
                        return;
                    }
                }
            }
        });
        tracing::info!("SIGHUP hot-reload listener installed");
    }

    #[cfg(not(unix))]
    {
        tracing::info!("SIGHUP not supported on this platform — use POST /admin/config/reload");
        let _ = state; // suppress unused warning
        let _ = shutdown;
    }
}

/// Background task: every 10 minutes, snapshot in-memory state to DB and cleanup.
fn spawn_sync_task(state: AppState, mut shutdown: tokio::sync::broadcast::Receiver<()>) {
    let db_pool = state.db_pool.clone();
    let limiter = state.limiter.clone();
    let plan_store = state.plan_store.clone();

    tokio::spawn(async move {
        let pool = match db_pool {
            Some(p) => p,
            None => return, // No DB — nothing to persist.
        };

        // Initial delay to let startup traffic settle.
        tokio::select! {
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {}
            _ = shutdown.recv() => {
                tracing::debug!("Sync task shutting down during initial delay");
                return;
            }
        }

        loop {
            // Wait for next sync cycle or shutdown signal.
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_secs(600)) => {}
                _ = shutdown.recv() => {
                    tracing::debug!("Sync task shutting down");
                    return;
                }
            }

            // 1. Snapshot rate limit counters → upsert into DB.
            let entries = limiter.snapshot();
            if !entries.is_empty() {
                for (cache_key, count, window_start, window_secs) in &entries {
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO boom_rate_limit_state (cache_key, count, window_start, window_secs, updated_at)
                           VALUES ($1, $2, $3, $4, NOW())
                           ON CONFLICT (cache_key) DO UPDATE
                           SET count = EXCLUDED.count,
                               window_start = EXCLUDED.window_start,
                               window_secs = EXCLUDED.window_secs,
                               updated_at = NOW()"#,
                    )
                    .bind(cache_key)
                    .bind(*count as i64)
                    .bind(*window_start as i64)
                    .bind(*window_secs as i64)
                    .execute(&pool)
                    .await
                    {
                        tracing::error!("Failed to upsert rate limit state: {}", e);
                    }
                }
                tracing::debug!("Synced {} rate limit counter(s) to DB", entries.len());
            }

            // 2. Snapshot assignments → upsert into DB.
            let assignments = plan_store.snapshot_assignments();
            if !assignments.is_empty() {
                for (key_hash, plan_name) in &assignments {
                    if let Err(e) = sqlx::query(
                        r#"INSERT INTO boom_key_plan_assignment (key_hash, plan_name, assigned_at)
                           VALUES ($1, $2, NOW())
                           ON CONFLICT (key_hash) DO UPDATE
                           SET plan_name = EXCLUDED.plan_name"#,
                    )
                    .bind(key_hash)
                    .bind(plan_name)
                    .execute(&pool)
                    .await
                    {
                        tracing::error!("Failed to upsert assignment: {}", e);
                    }
                }
            }

            // 3. Cleanup stale in-memory entries.
            let expired = limiter.cleanup_expired();
            let concurrency_freed = plan_store.cleanup_concurrency();
            if expired > 0 || concurrency_freed > 0 {
                tracing::info!(
                    "Cleanup: removed {} expired window(s), {} idle concurrency counter(s)",
                    expired,
                    concurrency_freed
                );
            }
        }
    });
    tracing::info!("Background sync task spawned (every 10 min)");
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
