use std::env;
use std::process;
use tracing::error;

#[tokio::main]
async fn main() {
    // Install global log collector.
    tracing_subscriber::fmt::init();

    // Setup environment variables
    let config_path = env::var("CONFIG_PATH").expect("env variable CONFIG_PATH should be set");

    // Run salesforce event relay service with a provided config.
    flowgen::service::Builder::new(config_path.into())
        .build()
        .unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        })
        .run()
        .await
        .unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        });
}
