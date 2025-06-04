use glob::glob;
use std::env;
use std::process;
use tracing::error;
use tracing::event;
use tracing::Level;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let config_dir = env::var("CONFIG_DIR").expect("env variable CONFIG_DIR should be set");
    let cache_credentials_path = env::var("CACHE_CREDENTIALS_PATH")
        .expect("env variable CACHE_CREDENTIALS_PATH should be set");

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

    for config in glob(&config_dir).unwrap_or_else(|err| {
        error!("{:?}", err);
        process::exit(1);
    }) {
        let config_path = config.unwrap_or_else(|err| {
            error!("{:?}", err);
            process::exit(1);
        });

        let f = flowgen::flow::FlowBuilder::new()
            .config_path(config_path)
            .cache_credentials_path(cache_credentials_path.clone().into())
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

        if let Some(tasks) = f.task_list {
            futures_util::future::join_all(tasks).await;
        }
    }
}
