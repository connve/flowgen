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
    #[error("IO error on path {path}: {source}")]
    IO {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// File system error occurred while globbing flow configuration files.
    #[error("Error globbing flow configuration files: {source}")]
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
    #[error("Error parsing configuration: {source}")]
    Config {
        #[source]
        source: config::ConfigError,
    },
    /// Flow path is missing or invalid.
    #[error("Flow path is not configured or invalid. Please set 'flows.path' in your configuration (e.g., flows.path: \"/etc/app/flows/*.yaml\")")]
    InvalidFlowsPath,
    /// Kubernetes host creation error.
    #[error("Error creating Kubernetes host: {source}")]
    Kube {
        #[source]
        source: kube::Error,
    },
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    /// Flow build error.
    #[error("Flow build failed: {source}")]
    FlowBuild {
        #[source]
        source: crate::flow::Error,
    },
    /// Flow initialization error.
    #[error("Flow initialization failed for {flow_name}: {source}")]
    FlowInit {
        flow_name: String,
        #[source]
        source: crate::flow::Error,
    },
    /// HTTP handler startup error.
    #[error("Failed to run HTTP handlers for {flow_name}: {source}")]
    HttpHandlerStartup {
        flow_name: String,
        #[source]
        source: crate::flow::Error,
    },
    /// HTTP handler setup completion error.
    #[error("Failed to complete HTTP handler setup: {source}")]
    HttpHandlerSetup {
        #[source]
        source: tokio::task::JoinError,
    },
    /// HTTP server startup error.
    #[error("Failed to start HTTP server: {source}")]
    HttpServerStart {
        #[source]
        source: flowgen_http::server::Error,
    },
    /// HTTP server downcast error.
    #[error("Failed to downcast HTTP server to concrete type")]
    HttpServerDowncast,
    /// Background task panic error.
    #[error("Background task panicked: {source}")]
    BackgroundTaskPanic {
        #[source]
        source: tokio::task::JoinError,
    },
    /// Flow file read error.
    #[error("Failed to read flow file {path:?}: {source}")]
    FlowFileRead {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Flow config parse error.
    #[error("Failed to parse flow config {path:?}: {source}")]
    FlowConfigParse {
        path: std::path::PathBuf,
        #[source]
        source: config::ConfigError,
    },
    /// Flow config deserialization error.
    #[error("Failed to deserialize flow config {path:?}: {source}")]
    FlowConfigDeserialize {
        path: std::path::PathBuf,
        #[source]
        source: config::ConfigError,
    },
    /// Flow file path canonicalization error.
    #[error("Failed to resolve canonical path for {path:?}: {source}")]
    FlowFileCanonicalize {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
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

        let flows_path = app_config
            .flows
            .path
            .as_ref()
            .ok_or(Error::InvalidFlowsPath)?;

        let flows_path_str = flows_path.to_str().ok_or(Error::InvalidFlowsPath)?;

        // Check if path contains wildcards (backward compatibility).
        let glob_patterns: Vec<String> = if flows_path_str.contains('*') {
            // Use the path directly as a glob pattern (old behavior).
            vec![flows_path_str.to_string()]
        } else {
            // Treat as base path and construct recursive glob patterns (new behavior).
            crate::config::FLOW_CONFIG_EXTENSIONS
                .iter()
                .map(|ext| format!("{}/**/*.{}", flows_path_str.trim_end_matches('/'), ext))
                .collect()
        };

        let mut flow_configs: Vec<FlowConfig> = Vec::new();
        let mut seen_paths = std::collections::HashSet::new();

        for glob_pattern in glob_patterns {
            let matched_flows: Vec<FlowConfig> = glob::glob(&glob_pattern)
                .map_err(|e| Error::Pattern { source: e })?
                .filter_map(|path| {
                    match path {
                        Ok(path) => {
                            let canonical_path = match std::fs::canonicalize(&path) {
                                Ok(p) => p,
                                Err(source) => {
                                    let err = Error::FlowFileCanonicalize {
                                        path: path.clone(),
                                        source,
                                    };
                                    error!("{}. Skipping this flow.", err);
                                    return None;
                                }
                            };

                            if seen_paths.contains(&canonical_path) {
                                return None;
                            }
                            seen_paths.insert(canonical_path.clone());

                            let contents = match std::fs::read_to_string(&path) {
                                Ok(c) => c,
                                Err(source) => {
                                    let err = Error::FlowFileRead {
                                        path: path.clone(),
                                        source,
                                    };
                                    error!("{}. Skipping this flow.", err);
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
                                Err(source) => {
                                    let err = Error::FlowConfigParse {
                                        path: path.clone(),
                                        source,
                                    };
                                    error!("{}. Skipping this flow.", err);
                                    return None;
                                }
                            };

                            match config.try_deserialize::<FlowConfig>() {
                                Ok(flow_config) => {
                                    info!(flow = %flow_config.flow.name, "Loaded flow");
                                    Some(flow_config)
                                }
                                Err(source) => {
                                    let err = Error::FlowConfigDeserialize {
                                        path: path.clone(),
                                        source,
                                    };
                                    error!("{}. Skipping this flow.", err);
                                    None
                                }
                            }
                        }
                        Err(source) => {
                            let err = Error::Glob { source };
                            error!("{}. Skipping.", err);
                            None
                        }
                    }
                })
                .collect();

            flow_configs.extend(matched_flows);
        }

        // Create shared HTTP Server if enabled.
        let http_server: Option<Arc<dyn flowgen_core::http_server::HttpServer>> = match app_config
            .worker
            .as_ref()
            .and_then(|w| w.http_server.as_ref())
        {
            Some(http_config) if http_config.enabled => {
                let mut http_server_builder = flowgen_http::server::HttpServerBuilder::new();
                if let Some(ref prefix) = http_config.routes_prefix {
                    http_server_builder = http_server_builder.routes_prefix(prefix.clone());
                }
                Some(Arc::new(http_server_builder.build()))
            }
            _ => None,
        };

        let cache: Arc<dyn flowgen_core::cache::Cache> =
            if let Some(cache_config) = &app_config.cache {
                if cache_config.enabled {
                    let db_name = cache_config
                        .db_name
                        .as_deref()
                        .unwrap_or(crate::config::DEFAULT_CACHE_DB_NAME);

                    match flowgen_nats::cache::CacheBuilder::new()
                        .credentials_path(cache_config.credentials_path.clone())
                        .url(cache_config.url.clone())
                        .build()
                        .and_then(|builder| {
                            futures::executor::block_on(async { builder.init(db_name).await })
                        }) {
                        Ok(nats_cache) => {
                            info!("Using NATS distributed cache");
                            Arc::new(nats_cache) as Arc<dyn flowgen_core::cache::Cache>
                        }
                        Err(e) => {
                            warn!(
                            "Failed to initialize NATS cache: {}, falling back to in-memory cache",
                            e
                        );
                            Arc::new(flowgen_core::cache::memory::MemoryCache::new())
                                as Arc<dyn flowgen_core::cache::Cache>
                        }
                    }
                } else {
                    info!("Cache disabled in config, using in-memory cache");
                    Arc::new(flowgen_core::cache::memory::MemoryCache::new())
                        as Arc<dyn flowgen_core::cache::Cache>
                }
            } else {
                info!("No cache configured, using in-memory cache");
                Arc::new(flowgen_core::cache::memory::MemoryCache::new())
                    as Arc<dyn flowgen_core::cache::Cache>
            };

        // Helper to check if running in Kubernetes
        let is_in_k8s =
            || std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists();

        let host_client = match app_config.worker.as_ref().and_then(|w| w.host.as_ref()) {
            Some(host) if host.enabled => {
                info!("K8s host coordination enabled in config");
                create_k8s_host().await
            }
            Some(host) if !host.enabled => {
                info!("K8s host coordination disabled in config");
                None
            }
            None if is_in_k8s() => {
                info!("Auto-detected Kubernetes environment, enabling K8s host coordination");
                create_k8s_host().await
            }
            _ => None,
        };

        async fn create_k8s_host() -> Option<std::sync::Arc<dyn flowgen_core::host::Host>> {
            let host_builder = flowgen_core::host::k8s::K8sHostBuilder::new();

            match host_builder.build() {
                Ok(host) => match host.connect().await {
                    Ok(connected_host) => Some(std::sync::Arc::new(connected_host)
                        as std::sync::Arc<dyn flowgen_core::host::Host>),
                    Err(e) => {
                        warn!("K8s host coordinator failed to connect: {}", e);
                        None
                    }
                },
                Err(e) => {
                    warn!("Failed to build K8s host coordinator: {}", e);
                    None
                }
            }
        }

        // Create resource loader from app config if configured.
        let resource_loader = app_config.resources.as_ref().map(|resource_options| {
            flowgen_core::resource::ResourceLoader::new(Some(resource_options.path.clone()))
        });

        // Build all flows from configuration files.
        let mut flows: Vec<super::flow::Flow> = Vec::new();
        for config in flow_configs {
            let http_server = http_server.as_ref().map(Arc::clone);
            let host = host_client.as_ref().map(Arc::clone);

            let mut flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
                .host(host)
                .cache(Arc::clone(&cache));

            if let Some(server) = http_server {
                flow_builder = flow_builder.http_server(server);
            }

            if let Some(retry_config) = app_config.worker.as_ref().and_then(|w| w.retry.as_ref()) {
                flow_builder = flow_builder.retry(retry_config.clone());
            }

            if let Some(buffer_size) = app_config.worker.as_ref().and_then(|w| w.event_buffer_size)
            {
                flow_builder = flow_builder.event_buffer_size(buffer_size);
            }

            if let Some(ref loader) = resource_loader {
                flow_builder = flow_builder.resource_loader(loader.clone());
            }

            match flow_builder.build() {
                Ok(flow) => flows.push(flow),
                Err(source) => {
                    let err = Error::FlowBuild { source };
                    error!("{}", err);
                    continue;
                }
            };
        }

        // Initialize flow setup.
        for flow in &mut flows {
            if let Err(source) = flow.init().await {
                let err = Error::FlowInit {
                    flow_name: flow.name().to_string(),
                    source,
                };
                error!("{}", err);
            }
        }

        let mut http_handler_tasks = Vec::new();
        for flow in &flows {
            match flow.run_http_handlers().await {
                Ok(handles) => http_handler_tasks.extend(handles),
                Err(source) => {
                    let err = Error::HttpHandlerStartup {
                        flow_name: flow.name().to_string(),
                        source,
                    };
                    error!("{}", err);
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
                if let Err(source) = result {
                    let err = Error::HttpHandlerSetup { source };
                    error!("{}", err);
                }
            }
        }

        // Start the main HTTP server.
        let mut background_handles = Vec::new();
        if let Some(http_server) = http_server {
            let configured_port = app_config
                .worker
                .as_ref()
                .and_then(|w| w.http_server.as_ref())
                .and_then(|http| http.port);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    // Downcast to concrete HttpServer type to access start_server method
                    if let Some(server) = http_server
                        .as_any()
                        .downcast_ref::<flowgen_http::server::HttpServer>()
                    {
                        if let Err(source) = server.start_server(configured_port).await {
                            let err = Error::HttpServerStart { source };
                            error!("{}", err);
                        }
                    } else {
                        error!("{}", Error::HttpServerDowncast);
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
            if let Err(source) = result {
                let err = Error::BackgroundTaskPanic { source };
                error!("{}", err);
            }
        }

        Ok(())
    }
}
