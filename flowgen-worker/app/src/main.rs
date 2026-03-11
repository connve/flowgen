use config::Config;
use flowgen_worker::app::App;
use flowgen_worker::config::AppConfig;
use std::env;
use std::process;
use tokio::sync::oneshot;
use tracing::{error, info};

/// Main entry point for the flowgen application.
///
/// Initializes tracing, loads configuration from environment variables and files,
/// creates the application instance, and runs it. Exits with code 1 on any error.
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

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
