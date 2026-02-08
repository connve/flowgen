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

        for glob_pattern in glob_patterns {
            let matched_flows: Vec<FlowConfig> = glob::glob(&glob_pattern)
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
                            warn!("Failed to build cache: {}", e);
                            e
                        })
                        .ok()
                        .and_then(|builder| {
                            futures::executor::block_on(async {
                                builder
                                    .init(db_name)
                                    .await
                                    .map_err(|e| {
                                        warn!("Failed to initialize cache: {}", e);
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

        // Helper to check if running in Kubernetes
        let is_in_k8s =
            || std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists();

        // Create host client with auto-detection
        let host_client = match app_config.worker.as_ref().and_then(|w| w.host.as_ref()) {
            Some(host) if host.enabled => {
                // User enabled host coordination in config
                info!("K8s host coordination enabled in config");
                create_k8s_host().await
            }
            Some(host) if !host.enabled => {
                // User disabled in config
                info!("K8s host coordination disabled in config");
                None
            }
            None if is_in_k8s() => {
                // Auto-detect: running in K8s but no config
                info!("Auto-detected Kubernetes environment, enabling K8s host coordination");
                create_k8s_host().await
            }
            _ => {
                // Not in K8s, no config needed
                None
            }
        };

        async fn create_k8s_host() -> Option<std::sync::Arc<dyn flowgen_core::host::Host>> {
            // Builder auto-detects holder_identity from POD_NAME/HOSTNAME
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
            .worker
            .as_ref()
            .and_then(|w| w.http_server.as_ref())
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
