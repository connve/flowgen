use crate::config::{AppConfig, FlowConfig};
use config::{Config, File};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{event, Level};

/// Errors that can occur during application execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// File system error occurred while globbing flow configuration files.
    #[error(transparent)]
    Glob(#[from] glob::GlobError),
    /// Invalid glob pattern provided for flow discovery.
    #[error(transparent)]
    Pattern(#[from] glob::PatternError),
    /// Configuration parsing or deserialization error.
    #[error(transparent)]
    Config(#[from] config::ConfigError),
    /// Flow directory path is invalid or cannot be converted to string.
    #[error("Invalid path")]
    InvalidPath,
}
/// Main application that loads and runs flows concurrently.
pub struct App {
    /// Global application configuration.
    pub config: AppConfig,
}

impl flowgen_core::task::runner::Runner for App {
    /// Loads flow configurations from disk, builds flows, starts HTTP server, and runs all tasks concurrently.
    ///
    /// This method discovers flow configuration files using the glob pattern specified in the app config,
    /// parses each configuration file, builds flow instances, registers HTTP routes, starts the HTTP server,
    /// and finally runs all flow tasks concurrently along with the server.
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let app_config = Arc::new(self.config);

        let glob_pattern = app_config
            .flows
            .dir
            .as_ref()
            .and_then(|path| path.to_str())
            .ok_or(Error::InvalidPath)?;

        let flow_configs: Vec<FlowConfig> = glob::glob(glob_pattern)?
            .map(|path| -> Result<FlowConfig, Error> {
                let path = path?;
                event!(Level::INFO, "Loading flow: {:?}", path);
                let config = Config::builder().add_source(File::from(path)).build()?;
                Ok(config.try_deserialize::<FlowConfig>()?)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create shared HTTP Server.
        let http_server = Arc::new(flowgen_http::server::HttpServer::new());

        // Initialize flows and register routes
        let mut flow_tasks = Vec::new();
        for config in flow_configs {
            let app_config = Arc::clone(&app_config);
            let http_server = Arc::clone(&http_server);

            let mut flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
                .http_server(http_server);

            if let Some(cache) = &app_config.cache {
                if cache.enabled {
                    flow_builder = flow_builder.cache_credentials_path(&cache.credentials_path);
                }
            }

            // Register routes and collect tasks
            let flow = match flow_builder.build() {
                Ok(flow) => flow,
                Err(e) => {
                    event!(Level::ERROR, "Flow build failed: {}", e);
                    continue;
                }
            };

            let flow = match flow.run().await {
                Ok(flow) => flow,
                Err(e) => {
                    event!(Level::ERROR, "{}", e);
                    continue;
                }
            };

            // Collect tasks for concurrent execution
            if let Some(tasks) = flow.task_list {
                flow_tasks.extend(tasks);
            }
        }

        // Start server with registered routes
        let server_handle = tokio::spawn(async move {
            if let Err(e) = http_server.start_server().await {
                event!(Level::ERROR, "Failed to start HTTP Server: {}", e);
            }
        });

        // Run tasks concurrently with server
        let flow_handle = tokio::spawn(async move {
            handle_task_results(flow_tasks).await;
        });

        // Wait for server and tasks
        let (_, _) = tokio::join!(server_handle, flow_handle);

        Ok(())
    }
}

/// Processes task results and logs errors.
async fn handle_task_results(tasks: Vec<JoinHandle<Result<(), super::flow::Error>>>) {
    let task_results = futures_util::future::join_all(tasks).await;
    for result in task_results {
        log_task_error(result);
    }
}

/// Logs task execution and logic errors.
fn log_task_error(result: Result<Result<(), super::flow::Error>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            event!(Level::ERROR, "{}", error);
        }
        Err(error) => {
            event!(Level::ERROR, "{}", error);
        }
    }
}
