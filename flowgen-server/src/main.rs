//! Flowgen Server - Git-to-Cache synchronization service.
//!
//! Syncs flow configurations and resources from a Git repository to a
//! cache backend (NATS KV), enabling workers to load flows from cache
//! with hot reload support.

mod config;
mod discover;
mod git;
mod sync;
mod validate;

use config::ServerAppConfig;
use std::env;
use std::process;
use std::sync::Arc;
use tokio::sync::oneshot;
use tracing::{error, info};

/// Logging format for structured output.
enum LogFormat {
    /// Human-readable compact format for development.
    Compact,
    /// Structured JSON format for production.
    Json,
}

/// Determines the appropriate log format based on LOG_FORMAT variable and TTY detection.
///
/// Priority order:
/// 1. LOG_FORMAT environment variable (explicit override).
/// 2. TTY detection (terminal = compact, no TTY = json).
fn determine_log_format() -> LogFormat {
    if let Ok(format) = env::var("LOG_FORMAT") {
        return match format.to_lowercase().as_str() {
            "compact" => LogFormat::Compact,
            _ => LogFormat::Json,
        };
    }

    match atty::is(atty::Stream::Stdout) {
        true => LogFormat::Compact,
        false => LogFormat::Json,
    }
}

/// Initializes the tracing subscriber with the determined log format.
fn init_tracing() {
    let format = determine_log_format();

    match format {
        LogFormat::Compact => {
            tracing_subscriber::fmt()
                .compact()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        }
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .init();
        }
    }
}

/// Default cache database name, shared with flowgen-worker.
const DEFAULT_CACHE_DB_NAME: &str = "flowgen_cache";

/// Main entry point for the flowgen server.
///
/// Initializes tracing, loads configuration, connects to cache,
/// and runs the Git-to-cache sync loop on a configurable interval.
#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing();

    let config_path = match env::var("CONFIG_PATH") {
        Ok(path) => path,
        Err(e) => {
            error!("Environment variable CONFIG_PATH should be set: {}", e);
            process::exit(1);
        }
    };

    let config = match ::config::Config::builder()
        .add_source(::config::File::with_name(&config_path))
        .add_source(::config::Environment::with_prefix("APP"))
        .build()
    {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to build config: {}", e);
            process::exit(1);
        }
    };

    let app_config = match config.try_deserialize::<ServerAppConfig>() {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to deserialize server config: {}", e);
            process::exit(1);
        }
    };

    // Initialize NATS cache connection.
    let db_name = app_config
        .cache
        .db_name
        .as_deref()
        .unwrap_or(DEFAULT_CACHE_DB_NAME);

    let cache: Arc<dyn flowgen_core::cache::Cache> = match flowgen_nats::cache::CacheBuilder::new()
        .credentials_path(app_config.cache.credentials_path.clone())
        .url(app_config.cache.url.clone())
        .build()
        .and_then(|builder| futures::executor::block_on(async { builder.init(db_name).await }))
    {
        Ok(cache) => Arc::new(cache),
        Err(e) => {
            error!("Failed to initialize cache: {}", e);
            process::exit(1);
        }
    };

    info!(
        repository = %app_config.flows.git.repository_url,
        branch = %app_config.flows.git.branch,
        sync_interval = ?app_config.flows.git.sync_interval,
        "Starting flowgen-server"
    );

    // Setup graceful shutdown signal handler.
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel();

    tokio::spawn(async move {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};

            let mut sigterm = match signal(SignalKind::terminate()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to setup SIGTERM handler: {}", e);
                    return;
                }
            };

            let mut sigint = match signal(SignalKind::interrupt()) {
                Ok(s) => s,
                Err(e) => {
                    error!("Failed to setup SIGINT handler: {}", e);
                    return;
                }
            };

            tokio::select! {
                _ = sigterm.recv() => {
                    info!("Received SIGTERM, initiating graceful shutdown...");
                }
                _ = sigint.recv() => {
                    info!("Received SIGINT, initiating graceful shutdown...");
                }
            }
        }

        #[cfg(not(unix))]
        {
            match tokio::signal::ctrl_c().await {
                Ok(()) => {
                    info!("Received shutdown signal, initiating graceful shutdown...");
                }
                Err(err) => {
                    error!("Failed to listen for shutdown signal: {}", err);
                    return;
                }
            }
        }

        let _ = shutdown_tx.send(());
    });

    // Main sync loop.
    let sync_interval = app_config.flows.git.sync_interval;

    loop {
        // Run a sync cycle.
        match sync::run_sync(&app_config, cache.as_ref()).await {
            Ok(stats) => {
                info!(
                    added = stats.flows_added,
                    updated = stats.flows_updated,
                    deleted = stats.flows_deleted,
                    unchanged = stats.flows_unchanged,
                    resources_synced = stats.resources_synced,
                    commit = %stats.commit,
                    "Sync cycle completed"
                );
            }
            Err(e) => {
                error!(error = %e, "Sync cycle failed");
            }
        }

        // Wait for the next sync interval or shutdown signal.
        tokio::select! {
            _ = tokio::time::sleep(sync_interval) => {}
            _ = &mut shutdown_rx => {
                info!("Shutting down flowgen-server.");
                break;
            }
        }
    }
}
