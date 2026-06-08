use clap::Parser;
use config::Config;
use flowgen::app::App;
use flowgen::config::AppConfig;
use std::env;
use std::process;
use tokio::sync::oneshot;
use tracing::{error, info, warn};

#[derive(Parser)]
#[command(name = "flowgen", version, about = "Data activation with a blast 💥")]
struct Cli {
    /// Path to configuration file.
    #[arg(short, long, env = "CONFIG_PATH")]
    config: String,
}

enum LogFormat {
    Compact,
    Json,
}

/// Determines the log format from LOG_FORMAT env var or TTY detection.
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

fn init_tracing() {
    let format = determine_log_format();

    let env_filter = match tracing_subscriber::EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => {
            tracing_subscriber::EnvFilter::new("info,opentelemetry=warn,opentelemetry_sdk=warn")
        }
    };

    match format {
        LogFormat::Compact => {
            tracing_subscriber::fmt()
                .compact()
                .with_env_filter(env_filter)
                .init();
        }
        LogFormat::Json => {
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .init();
        }
    }
}

#[tokio::main]
async fn main() {
    let _ = rustls::crypto::ring::default_provider().install_default();

    init_tracing();

    let cli = Cli::parse();

    let config = match Config::builder()
        .add_source(config::File::with_name(&cli.config))
        .add_source(config::Environment::with_prefix("APP"))
        .build()
    {
        Ok(config) => config,
        Err(e) => {
            let cwd = env::current_dir()
                .map(|d| d.display().to_string())
                .unwrap_or_default();
            let msg = e.to_string();
            let msg = msg
                .chars()
                .next()
                .map(|c| c.to_uppercase().to_string() + &msg[c.len_utf8()..])
                .unwrap_or(msg);
            error!("{msg} (working directory: {cwd})");
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

            let _ = shutdown_tx.send(());

            tokio::select! {
                _ = sigterm.recv() => {}
                _ = sigint.recv() => {}
            }
            warn!("Received second signal, forcing shutdown");
            process::exit(1);
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

            let _ = shutdown_tx.send(());

            if tokio::signal::ctrl_c().await.is_ok() {
                warn!("Received second signal, forcing shutdown");
                process::exit(1);
            }
        }
    });

    let app = App { config: app_config };
    if let Err(e) = app.start(shutdown_rx).await {
        error!("Application failed to run: {}", e);
        process::exit(1);
    }
}
