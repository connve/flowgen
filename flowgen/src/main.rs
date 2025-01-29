use glob::glob;
use std::env;
use std::process;
use tracing::error;
use tracing::event;
use tracing::Level;
pub const DEFAULT_TOPIC_NAME: &str = "/data/ChangeEvents";

#[tokio::main]
async fn main() {
    // Install global log collector.
    tracing_subscriber::fmt::init();

    // Setup environment variables
    let config_dir = env::var("CONFIG_DIR").expect("env variable CONFIG_DIR should be set");

    if let Ok(configs) = glob(&config_dir) {
        let num_configs = configs.count();
        if num_configs == 0 {
            event!(
                Level::WARN,
                "{} flow configurations found at path: {}",
                num_configs,
                config_dir
            );
        }
    }

    let mut all_handle_list = Vec::new();
    for config in glob(&config_dir).unwrap_or_else(|err| {
        error!("{:?}", err);
        process::exit(1);
    }) {
        let config_path = config.unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        });

        let f = flowgen::flow::Builder::new(config_path)
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

        if let Some(handle_list) = f.handle_list {
            for handle in handle_list {
                all_handle_list.push(handle);
            }
        }
    }
    let result = futures::future::join_all(all_handle_list).await;
    for r in result {
        let _ = r.unwrap();
    }
}
