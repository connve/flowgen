use crate::config::{AppConfig, FlowConfig};
use config::Config;
use flowgen_core::client::Client;
use std::sync::Arc;
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

impl App {
    /// Loads flow configurations from disk, builds flows, starts HTTP server, and runs all tasks concurrently.
    ///
    /// This method discovers flow configuration files using the glob pattern specified in the app config,
    /// parses each configuration file, builds flow instances, registers HTTP routes, starts the HTTP server,
    /// and finally runs all flow tasks concurrently along with the server.
    #[tracing::instrument(skip(self), name = "app")]
    pub async fn start(self) -> Result<(), Error> {
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
        let mut http_server_builder = flowgen_http::server::HttpServerBuilder::new();
        if let Some(http_config) = &app_config.http_server {
            if let Some(ref prefix) = http_config.routes_prefix {
                http_server_builder = http_server_builder.routes_prefix(prefix.clone());
            }
        }
        let http_server = Arc::new(http_server_builder.build());

        // Create shared cache if configured.
        let cache: Option<Arc<flowgen_nats::cache::Cache>> =
            if let Some(cache_config) = &app_config.cache {
                if cache_config.enabled {
                    let db_name = cache_config
                        .db_name
                        .as_deref()
                        .unwrap_or(crate::config::DEFAULT_CACHE_DB_NAME);

                    flowgen_nats::cache::CacheBuilder::new()
                        .credentials_path(cache_config.credentials_path.clone())
                        .build()
                        .map_err(|e| {
                            warn!("Failed to build cache: {}. Continuing without cache.", e);
                            e
                        })
                        .ok()
                        .and_then(|builder| {
                            futures::executor::block_on(async {
                                builder
                                    .init(db_name)
                                    .await
                                    .map_err(|e| {
                                        warn!(
                                        "Failed to initialize cache: {}. Continuing without cache.",
                                        e
                                    );
                                        e
                                    })
                                    .ok()
                            })
                        })
                        .map(Arc::new)
                } else {
                    None
                }
            } else {
                None
            };

        // Create host client if configured.
        let host_client = if let Some(host) = &app_config.host {
            if host.enabled {
                match &host.host_type {
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
                            Ok(connected_host) => Some(std::sync::Arc::new(connected_host)
                                as std::sync::Arc<dyn flowgen_core::host::Host>),
                            Err(e) => {
                                warn!("{}. Continuing without host coordination.", e);
                                None
                            }
                        }
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // Initialize flows and spawn each flow's execution.
        // Separate flows that need to complete setup before server starts.
        let mut blocking_flows = Vec::new();
        let mut background_flows = Vec::new();

        for config in flow_configs {
            let http_server = Arc::clone(&http_server);
            let host = host_client.as_ref().map(Arc::clone);
            let cache = cache
                .as_ref()
                .map(|c| Arc::clone(c) as Arc<dyn flowgen_core::cache::Cache>);

            let flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
                .http_server(http_server)
                .host(host)
                .cache(cache);

            // Build flow and spawn its execution
            let flow = match flow_builder.build() {
                Ok(flow) => flow,
                Err(e) => {
                    error!("Flow build failed: {}", e);
                    continue;
                }
            };

            // Check if this flow should complete setup before server starts
            let wait_for_ready = flow.should_wait_for_ready();

            let span = tracing::Span::current();
            let flow_handle = tokio::spawn(
                async move {
                    match flow.run().await {
                        Ok(flow) => {
                            // Flow setup complete, monitor spawned tasks
                            if let Some(task_list) = flow.task_list {
                                let results = futures_util::future::join_all(task_list).await;
                                for (idx, result) in results.into_iter().enumerate() {
                                    if let Err(e) = result {
                                        error!("Task {} panicked: {}", idx, e);
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            error!("Flow execution failed: {}", e);
                        }
                    }
                }
                .instrument(span),
            );

            if wait_for_ready {
                blocking_flows.push(flow_handle);
            } else {
                background_flows.push(flow_handle);
            }
        }

        // Wait for blocking flows to complete setup (e.g., registering HTTP routes)
        if !blocking_flows.is_empty() {
            info!(
                "Waiting for {} flow(s) to complete initial setup",
                blocking_flows.len()
            );
            futures_util::future::join_all(blocking_flows).await;
        }

        // Start HTTP server now that setup is complete
        let configured_port = app_config.http_server.as_ref().and_then(|http| http.port);
        let span = tracing::Span::current();
        let server_handle = tokio::spawn(
            async move {
                if let Err(e) = http_server.start_server(configured_port).await {
                    error!("Failed to start HTTP Server: {}", e);
                }
            }
            .instrument(span),
        );
        background_flows.push(server_handle);

        // Wait for all background flows and server
        let results = futures_util::future::join_all(background_flows).await;
        for result in results {
            if let Err(e) = result {
                error!("Background task panicked: {}", e);
            }
        }

        Ok(())
    }
}
