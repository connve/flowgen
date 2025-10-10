use crate::config::{AppConfig, FlowConfig};
use config::Config;
use flowgen_core::client::Client;
use std::sync::Arc;
use tokio::task::JoinHandle;
use tracing::{error, info, warn, Instrument};

/// Errors that can occur during application execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Input/output operation failed.
    #[error("IO operation failed on path {path}: {source}")]
    IO {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// File system error occurred while globbing flow configuration files.
    #[error("Failed to glob flow configuration files: {source}")]
    Glob {
        #[source]
        source: glob::GlobError,
    },
    /// Invalid glob pattern provided for flow discovery.
    #[error("Invalid glob pattern: {source}")]
    Pattern {
        #[source]
        source: glob::PatternError,
    },
    /// Configuration parsing or deserialization error.
    #[error("Failed to parse configuration: {source}")]
    Config {
        #[source]
        source: config::ConfigError,
    },
    /// Flow directory path is invalid or cannot be converted to string.
    #[error("Invalid path")]
    InvalidPath,
    /// Kubernetes host creation error.
    #[error("Failed to create Kubernetes host: {source}")]
    Kube {
        #[source]
        source: kube::Error,
    },
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    /// Environment variable error.
    #[error("Failed to read environment variable: {source}")]
    Env {
        #[source]
        source: std::env::VarError,
    },
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
    #[tracing::instrument(skip(self), name = "app")]
    async fn run(self) -> Result<(), Error> {
        let app_config = Arc::new(self.config);

        let glob_pattern = app_config
            .flows
            .dir
            .as_ref()
            .and_then(|path| path.to_str())
            .ok_or(Error::InvalidPath)?;

        let flow_configs: Vec<FlowConfig> = glob::glob(glob_pattern)
            .map_err(|e| Error::Pattern { source: e })?
            .map(|path| -> Result<FlowConfig, Error> {
                let path = path.map_err(|e| Error::Glob { source: e })?;
                info!("Loading flow: {:?}", path);
                let contents = std::fs::read_to_string(&path).map_err(|e| Error::IO {
                    path: path.clone(),
                    source: e,
                })?;

                // Determine file format from extension.
                let file_format = match path.extension().and_then(|s| s.to_str()) {
                    Some("yaml") | Some("yml") => config::FileFormat::Yaml,
                    Some("json") => config::FileFormat::Json,
                    _ => config::FileFormat::Json,
                };

                let config = Config::builder()
                    .add_source(config::File::from_str(&contents, file_format))
                    .build()
                    .map_err(|e| Error::Config { source: e })?;
                config
                    .try_deserialize::<FlowConfig>()
                    .map_err(|e| Error::Config { source: e })
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Create shared HTTP Server.
        let http_server = Arc::new(flowgen_http::server::HttpServer::new());

        // Create host client if configured.
        let host_client = if let Some(host_options) = &app_config.host {
            match &host_options.host_type {
                crate::config::HostType::K8s => {
                    // Get holder identity from environment variable.
                    let holder_identity = std::env::var("HOSTNAME")
                        .or_else(|_| std::env::var("POD_NAME"))
                        .map_err(|e| Error::Env { source: e })?;

                    let host_builder = flowgen_core::host::k8s::K8sHostBuilder::new()
                        .holder_identity(holder_identity);

                    match host_builder
                        .build()
                        .map_err(|e| Error::Host(Box::new(e)))?
                        .connect()
                        .await
                    {
                        Ok(connected_host) => {
                            Some(Arc::new(flowgen_core::task::context::HostClient {
                                client: std::sync::Arc::new(connected_host),
                            }))
                        }
                        Err(e) => {
                            warn!("{}. Continuing without host coordination.", e);
                            None
                        }
                    }
                }
            }
        } else {
            None
        };

        // Initialize flows and register routes
        let mut flow_tasks = Vec::new();
        for config in flow_configs {
            let app_config = Arc::clone(&app_config);
            let http_server = Arc::clone(&http_server);
            let host_client = host_client.as_ref().map(Arc::clone);

            let mut flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
                .http_server(http_server)
                .host(host_client);

            if let Some(cache) = &app_config.cache {
                if cache.enabled {
                    flow_builder = flow_builder.cache_credentials_path(&cache.credentials_path);
                }
            }

            // Register routes and collect tasks
            let flow = match flow_builder.build() {
                Ok(flow) => flow,
                Err(e) => {
                    error!("Flow build failed: {}", e);
                    continue;
                }
            };

            let flow = match flow.run().await {
                Ok(flow) => flow,
                Err(e) => {
                    error!("{}", e);
                    continue;
                }
            };

            // Collect tasks for concurrent execution
            if let Some(tasks) = flow.task_list {
                flow_tasks.extend(tasks);
            }
        }

        // Start server with registered routes
        let configured_port = app_config.http.as_ref().and_then(|http| http.port);
        let span = tracing::Span::current();
        let server_handle = tokio::spawn(
            async move {
                if let Err(e) = http_server.start_server(configured_port).await {
                    error!("Failed to start HTTP Server: {}", e);
                }
            }
            .instrument(span),
        );

        // Run tasks concurrently with server
        let flow_handle = tokio::spawn(async move {
            handle_task_results(flow_tasks).await;
        });

        // Wait for server and tasks
        let (_, _) = tokio::join!(server_handle, flow_handle);

        Ok(())
    }
}

/// Processes task results and logs errors as they complete.
async fn handle_task_results(mut tasks: Vec<JoinHandle<Result<(), super::flow::Error>>>) {
    while !tasks.is_empty() {
        let (result, _index, remaining) = futures_util::future::select_all(tasks).await;
        log_task_error(result);
        tasks = remaining;
    }
}

/// Logs task execution and logic errors.
fn log_task_error(result: Result<Result<(), super::flow::Error>, tokio::task::JoinError>) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            error!("{}", error);
        }
        Err(error) => {
            error!("{}", error);
        }
    }
}
