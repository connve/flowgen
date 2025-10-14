use config::Config;
use flowgen_worker::app::App;
use flowgen_worker::config::AppConfig;
use std::env;
use std::process;
use tracing::error;

/// Main entry point for the flowgen application.
///
/// Initializes tracing, loads configuration from environment variables and files,
/// creates the application instance, and runs it. Exits with code 1 on any error.
#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

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
    let app = App { config: app_config };
    if let Err(e) = app.start().await {
        error!("Application failed to run: {}", e);
        process::exit(1);
    }
}
