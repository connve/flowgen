use glob::glob;
use std::env;
use std::process;
use std::result;
use tracing::error;
use tracing::event;
use tracing::Level;
pub const DEFAULT_TOPIC_NAME: &str = "/data/ChangeEvents";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

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

    for handle in all_handle_list {
        let result = handle.await;
        match result {
            Ok(result) => {
                if let Err(e) = result {
                    error!("{:?}", e);
                };
            }
            Err(e) => {
                error!("{:?}", e);
            }
        }
    }
}
