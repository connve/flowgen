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
    /// Flow path is missing or invalid.
    #[error("Flow path is not configured or invalid. Please set 'flows.path' in your configuration (e.g., flows.path: \"/etc/app/flows/*.yaml\")")]
    InvalidFlowsPath,
    /// Kubernetes host creation error.
    #[error("Failed to create Kubernetes host: {source}")]
    Kube {
        #[source]
        source: kube::Error,
    },
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    /// Missing HOSTNAME or POD_NAME environment variable for K8s host coordination.
    #[error("HOSTNAME or POD_NAME must be set for K8s host coordination")]
    MissingK8EnvVariables(#[source] std::env::VarError),
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
            .path
            .as_ref()
            .and_then(|path| path.to_str())
            .ok_or(Error::InvalidFlowsPath)?;

        let flow_configs: Vec<FlowConfig> = glob::glob(glob_pattern)
            .map_err(|e| Error::Pattern { source: e })?
            .filter_map(|path| {
                match path {
                    Ok(path) => {
                        info!("Loading flow: {:?}", path);
                        let contents = match std::fs::read_to_string(&path) {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Failed to read flow file {:?}: {}. Skipping this flow.", path, e);
                                return None;
                            }
                        };

                        // Determine file format from extension.
                        let file_format = match path.extension().and_then(|s| s.to_str()) {
                            Some("yaml") | Some("yml") => config::FileFormat::Yaml,
                            Some("json") => config::FileFormat::Json,
                            _ => config::FileFormat::Json,
                        };

                        let config = match Config::builder()
                            .add_source(config::File::from_str(&contents, file_format))
                            .build()
                        {
                            Ok(c) => c,
                            Err(e) => {
                                error!("Failed to parse flow config {:?}: {}. Skipping this flow.", path, e);
                                return None;
                            }
                        };

                        match config.try_deserialize::<FlowConfig>() {
                            Ok(flow_config) => Some(flow_config),
                            Err(e) => {
                                error!("Failed to deserialize flow config {:?}: {}. Skipping this flow.", path, e);
                                None
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to read flow path: {}. Skipping.", e);
                        None
                    }
                }
            })
            .collect();

        // Create shared HTTP Server if enabled.
        let http_server: Option<Arc<dyn flowgen_core::http_server::HttpServer>> =
            match &app_config.http_server {
                Some(http_config) if http_config.enabled => {
                    let mut http_server_builder = flowgen_http::server::HttpServerBuilder::new();
                    if let Some(ref prefix) = http_config.routes_prefix {
                        http_server_builder = http_server_builder.routes_prefix(prefix.clone());
                    }
                    Some(Arc::new(http_server_builder.build()))
                }
                _ => None,
            };

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
                        .url(cache_config.url.clone())
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
                            .map_err(Error::MissingK8EnvVariables)?;

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
                                warn!("Continuing without host coordination due to error: {}", e);
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

        // Build all flows from configuration files.
        let mut flows: Vec<super::flow::Flow> = Vec::new();
        for config in flow_configs {
            let http_server = http_server.as_ref().map(Arc::clone);
            let host = host_client.as_ref().map(Arc::clone);
            let cache = cache
                .as_ref()
                .map(|c| Arc::clone(c) as Arc<dyn flowgen_core::cache::Cache>);

            let mut flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
                .host(host)
                .cache(cache);

            if let Some(server) = http_server {
                flow_builder = flow_builder.http_server(server);
            }

            if let Some(buffer_size) = app_config.event_buffer_size {
                flow_builder = flow_builder.event_buffer_size(buffer_size);
            }

            if let Some(retry_config) = &app_config.retry {
                flow_builder = flow_builder.retry(retry_config.clone());
            }

            match flow_builder.build() {
                Ok(flow) => flows.push(flow),
                Err(e) => {
                    error!("Flow build failed: {}", e);
                    continue;
                }
            };
        }

        // Initialize flow setup.
        for flow in &mut flows {
            if let Err(e) = flow.init().await {
                error!("Flow initialization failed for {}: {}", flow.name(), e);
            }
        }

        // Run HTTP handlers and wait for them to register (only if HTTP server is enabled).
        if app_config
            .http_server
            .as_ref()
            .is_some_and(|http| http.enabled)
        {
            let mut http_handler_tasks = Vec::new();
            for flow in &flows {
                match flow.run_http_handlers().await {
                    Ok(handles) => http_handler_tasks.extend(handles),
                    Err(e) => {
                        error!("Failed to run http handlers for {}: {}", flow.name(), e);
                    }
                }
            }

            if !http_handler_tasks.is_empty() {
                info!(
                    "Waiting for {} HTTP handler(s) to complete setup...",
                    http_handler_tasks.len()
                );
                let results = futures_util::future::join_all(http_handler_tasks).await;
                for result in results {
                    if let Err(e) = result {
                        error!("HTTP handler setup task panicked: {}", e);
                    }
                }
            }
        }

        // Start the main HTTP server.
        let mut background_handles = Vec::new();
        if let Some(http_server) = http_server {
            let configured_port = app_config.http_server.as_ref().and_then(|http| http.port);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    // Downcast to concrete HttpServer type to access start_server method
                    if let Some(server) = http_server
                        .as_any()
                        .downcast_ref::<flowgen_http::server::HttpServer>()
                    {
                        if let Err(e) = server.start_server(configured_port).await {
                            error!("Failed to start HTTP Server: {}", e);
                        }
                    } else {
                        error!("Failed to downcast HTTP Server to concrete type");
                    }
                }
                .instrument(span),
            );
            background_handles.push(server_handle);
        }

        // Start all background flow tasks.
        for flow in flows {
            background_handles.push(flow.run());
        }

        // Wait for all background flows and the server to complete.
        let results = futures_util::future::join_all(background_handles).await;
        for result in results {
            if let Err(e) = result {
                error!("Background task panicked: {}", e);
            }
        }

        Ok(())
    }
}
