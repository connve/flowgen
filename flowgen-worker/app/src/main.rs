use config::Config;
use flowgen_worker::app::App;
use flowgen_worker::config::AppConfig;
use std::env;
use std::process;
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
/// 1. LOG_FORMAT environment variable (explicit override)
/// 2. TTY detection (terminal → compact, no TTY → json)
fn determine_log_format() -> LogFormat {
    // Check explicit LOG_FORMAT override.
    if let Ok(format) = env::var("LOG_FORMAT") {
        return match format.to_lowercase().as_str() {
            "compact" => LogFormat::Compact,
            _ => LogFormat::Json,
        };
    }

    // Auto-detect based on TTY (terminal = compact, pipe/Docker = json).
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

/// Main entry point for the flowgen application.
///
/// Initializes tracing, loads configuration from environment variables and files,
/// creates the application instance, and runs it. Exits with code 1 on any error.
#[tokio::main]
async fn main() {
    // Install rustls crypto provider (ring) as process-level default.
    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing();

    let config_path = match env::var("CONFIG_PATH") {
        Ok(path) => path,
        Err(e) => {
            error!("Environment variable CONFIG_PATH should be set: {}", e);
            process::exit(1);
        }
    };

    let config = match Config::builder()
        .add_source(config::File::with_name(&config_path))
        .add_source(config::Environment::with_prefix("APP"))
        .build()
    {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to build config: {}", e);
            process::exit(1);
        }
    };

    let app_config = match config.try_deserialize::<AppConfig>() {
        Ok(config) => config,
        Err(e) => {
            error!("Failed to deserialize app config: {}", e);
            process::exit(1);
        }
    };

    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    // Listen for Unix SIGTERM signal (sent by Kubernetes on pod termination)
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

    let app = App { config: app_config };
    if let Err(e) = app.start(shutdown_rx).await {
        error!("Application failed to run: {}", e);
        process::exit(1);
    }
}
