use config::Config;
use flowgen::app::App;
use flowgen::config::AppConfig;
use flowgen_core::task::runner::Runner;
use std::env;
use std::process;
use tracing::event;
use tracing::Level;

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
            event!(
                Level::ERROR,
                "Environment variable CONFIG_PATH should be set: {}",
                e
            );
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
            event!(Level::ERROR, "Failed to build config: {}", e);
            process::exit(1);
        }
    };

    let app_config = match config.try_deserialize::<AppConfig>() {
        Ok(config) => config,
        Err(e) => {
            event!(Level::ERROR, "Failed to deserialize app config: {}", e);
            process::exit(1);
        }
    };
    let app = App { config: app_config };
    if let Err(e) = app.run().await {
        event!(Level::ERROR, "Application failed to run: {}", e);
        process::exit(1);
    }
}
