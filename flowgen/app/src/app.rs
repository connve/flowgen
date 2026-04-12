use crate::config::{AppConfig, FlowConfig};
use config::Config;
use std::sync::Arc;
use tracing::{debug, error, info, warn, Instrument};

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
    /// Flow build error.
    #[error("Flow build failed: {source}")]
    FlowBuild {
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// Flow initialization error.
    #[error("Flow initialization failed for {flow_name}: {source}")]
    FlowInit {
        flow_name: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// HTTP handler startup error.
    #[error("Failed to run HTTP handlers for {flow_name}: {source}")]
    HttpHandlerStartup {
        flow_name: String,
        #[source]
        source: Box<crate::flow::Error>,
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
    /// Cache-based flow loading requires a configured cache backend.
    #[error("Cache-based flow loading is enabled but no cache is configured")]
    CacheFlowsWithoutCache,
    /// Failed to initialize the metadata cache for flow/resource loading.
    #[error("Failed to initialize metadata cache: {source}")]
    MetadataCacheInit {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// Failed to list flow keys from cache.
    #[error("Failed to list flow keys from cache: {source}")]
    CacheListFlows {
        #[source]
        source: flowgen_core::cache::CacheError,
    },
    /// Failed to get a flow from cache.
    #[error("Failed to get flow '{key}' from cache: {source}")]
    CacheGetFlow {
        key: String,
        #[source]
        source: flowgen_core::cache::CacheError,
    },
    /// Flow content in cache is not valid UTF-8.
    #[error("Flow '{key}' in cache is not valid UTF-8: {source}")]
    CacheFlowUtf8 {
        key: String,
        #[source]
        source: std::string::FromUtf8Error,
    },
    /// Failed to parse YAML flow content from cache.
    #[error("Failed to parse flow '{key}' from cache: {source}")]
    CacheFlowParse {
        key: String,
        #[source]
        source: serde_yaml::Error,
    },
}
/// Main application that loads and runs flows concurrently.
pub struct App {
    /// Global application configuration.
    pub config: AppConfig,
}

impl App {
    /// Initializes a cache connection for the metadata bucket.
    /// Separate from the runtime cache to avoid key collisions.
    fn init_metadata_cache(
        cache_config: &crate::config::CacheOptions,
        db_name: &str,
    ) -> Result<Arc<dyn flowgen_core::cache::Cache>, Error> {
        let metadata_cache = flowgen_nats::cache::CacheBuilder::new()
            .credentials_path(cache_config.credentials_path.clone())
            .url(cache_config.url.clone())
            .build()
            .and_then(|builder| {
                futures::executor::block_on(async { builder.init(db_name).await })
            })
            .map_err(|e| Error::MetadataCacheInit {
                source: Box::new(e),
            })?;
        Ok(Arc::new(metadata_cache))
    }

    /// Loads flow configurations from the local filesystem using the path-based glob pattern.
    ///
    /// Supports both direct glob patterns (e.g., "/flows/*.yaml") and base directory paths
    /// (e.g., "/flows") which are expanded to recursive globs across all supported extensions.
    fn load_flows_from_filesystem(app_config: &AppConfig) -> Result<Vec<FlowConfig>, Error> {
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

        Ok(flow_configs)
    }

    /// Loads flow configurations from the cache. Malformed flows are logged and skipped.
    async fn load_flows_from_cache(
        cache: &dyn flowgen_core::cache::Cache,
        prefix: &str,
    ) -> Result<Vec<FlowConfig>, Error> {
        let keys = cache
            .list_keys(prefix)
            .await
            .map_err(|source| Error::CacheListFlows { source })?;

        let mut flow_configs: Vec<FlowConfig> = Vec::new();

        for key in keys {
            let bytes = match cache
                .get(&key)
                .await
                .map_err(|source| Error::CacheGetFlow {
                    key: key.clone(),
                    source,
                })? {
                Some(bytes) => bytes,
                None => {
                    warn!(key = %key, "Flow key disappeared from cache during load, skipping");
                    continue;
                }
            };

            let content = match String::from_utf8(bytes.to_vec()) {
                Ok(c) => c,
                Err(source) => {
                    let err = Error::CacheFlowUtf8 {
                        key: key.clone(),
                        source,
                    };
                    error!("{}. Skipping this flow.", err);
                    continue;
                }
            };

            match serde_yaml::from_str::<FlowConfig>(&content) {
                Ok(mut flow_config) => {
                    // Replace flow.name with the path-based key to preserve folder
                    // context in spans, logs, and runtime cache keys.
                    if let Some(relative_path) = key.strip_prefix(&format!("{prefix}.")) {
                        if flow_config.flow.name != relative_path {
                            flow_config.flow.name = relative_path.to_string();
                        }
                    }

                    info!(flow = %flow_config.flow.name, key = %key, "Loaded flow from cache");
                    flow_configs.push(flow_config);
                }
                Err(source) => {
                    let err = Error::CacheFlowParse {
                        key: key.clone(),
                        source,
                    };
                    error!("{}. Skipping this flow.", err);
                }
            }
        }

        Ok(flow_configs)
    }

    /// Loads flow configurations from disk, builds flows, starts HTTP server, and runs all tasks concurrently.
    ///
    /// This method discovers flow configuration files using the glob pattern specified in the app config,
    /// parses each configuration file, builds flow instances, registers HTTP routes, starts the HTTP server,
    /// and finally runs all flow tasks concurrently along with the server.
    ///
    /// The shutdown_rx parameter allows graceful shutdown by releasing all leases when a shutdown signal is received.
    pub async fn start(self, shutdown_rx: tokio::sync::oneshot::Receiver<()>) -> Result<(), Error> {
        let app_config = Arc::new(self.config);

        // Initialize OpenTelemetry if configured.
        let _telemetry_guard = if let Some(telemetry_config) = &app_config.telemetry {
            if telemetry_config.enabled {
                let config = flowgen_core::telemetry::TelemetryConfig {
                    otlp_endpoint: telemetry_config.otlp_endpoint.clone(),
                    service_name: telemetry_config.service_name.clone(),
                    service_version: env!("CARGO_PKG_VERSION").to_string(),
                    metrics_export_interval_secs: telemetry_config
                        .metrics_export_interval
                        .as_secs(),
                };
                match flowgen_core::telemetry::init_telemetry(config) {
                    Ok(guard) => {
                        debug!("OpenTelemetry initialized successfully");
                        Some(guard)
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to initialize OpenTelemetry, continuing without metrics");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // Initialize cache first — flow loading from cache depends on it.
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

        // Load flow configurations from filesystem and/or cache.
        // Filesystem flows (bootstrap, e.g., git_sync) are loaded first.
        // Cache flows are loaded second and merged. On name collision, filesystem wins
        // so bootstrap flows cannot be overridden by cache content.
        let mut flow_configs: Vec<FlowConfig> = Vec::new();
        let mut seen_names: std::collections::HashSet<String> = std::collections::HashSet::new();

        // Load from filesystem if configured.
        if app_config.flows.path.is_some() {
            let file_flows = Self::load_flows_from_filesystem(&app_config)?;
            for flow in file_flows {
                seen_names.insert(flow.flow.name.clone());
                flow_configs.push(flow);
            }
        }

        // Load from cache if configured.
        if let Some(flow_cache) = &app_config.flows.cache {
            if flow_cache.enabled {
                let cache_config = app_config
                    .cache
                    .as_ref()
                    .filter(|c| c.enabled)
                    .ok_or(Error::CacheFlowsWithoutCache)?;

                let metadata_cache =
                    Self::init_metadata_cache(cache_config, &flow_cache.db_name)?;

                info!(prefix = %flow_cache.prefix, db = %flow_cache.db_name, "Loading flows from cache");
                let cache_flows =
                    Self::load_flows_from_cache(metadata_cache.as_ref(), &flow_cache.prefix)
                        .await?;

                for flow in cache_flows {
                    if !seen_names.contains(&flow.flow.name) {
                        seen_names.insert(flow.flow.name.clone());
                        flow_configs.push(flow);
                    }
                }
            }
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

        // Create resource loader from app config if configured.
        // Cache-based loading always takes precedence when enabled, even if `path` is also set.
        // When loading from cache, connects to the metadata bucket (separate from the runtime cache).
        let resource_loader = app_config
            .flow_resources
            .as_ref()
            .map(|resource_options| {
                if let Some(cache_opts) = &resource_options.cache {
                    if cache_opts.enabled {
                        // Connect to the metadata bucket for resource loading.
                        if let Some(cache_config) =
                            app_config.cache.as_ref().filter(|c| c.enabled)
                        {
                            match Self::init_metadata_cache(cache_config, &cache_opts.db_name) {
                                Ok(metadata_cache) => {
                                    info!(prefix = %cache_opts.prefix, db = %cache_opts.db_name, "Loading resources from cache");
                                    return flowgen_core::resource::ResourceLoader::from_cache(
                                        metadata_cache,
                                        cache_opts.prefix.clone(),
                                    );
                                }
                                Err(e) => {
                                    warn!("Failed to initialize metadata cache for resources: {}, falling back to filesystem", e);
                                }
                            }
                        }
                    }
                }
                flowgen_core::resource::ResourceLoader::new(Some(resource_options.path.clone()))
            });

        // Build all flows from configuration files.
        let mut flows: Vec<super::flow::Flow> = Vec::new();
        for config in flow_configs {
            let http_server = http_server.as_ref().map(Arc::clone);

            let mut flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
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
                    let err = Error::FlowBuild {
                        source: Box::new(source),
                    };
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
                    source: Box::new(source),
                };
                error!("{}", err);
            }
        }

        let mut http_handler_tasks = Vec::new();
        for flow in &flows {
            match flow.start_tasks().await {
                Ok(handles) => http_handler_tasks.extend(handles),
                Err(source) => {
                    let err = Error::HttpHandlerStartup {
                        flow_name: flow.name().to_string(),
                        source: Box::new(source),
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
                    // Downcast to concrete HttpServer type to access start_server method.
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

        // Collect task managers for shutdown cleanup.
        let task_managers: Vec<Arc<flowgen_core::task::manager::TaskManager>> =
            flows.iter().filter_map(|f| f.task_manager()).collect();

        // Start all background flow tasks.
        for flow in flows {
            background_handles.push(flow.run());
        }

        // Wait for shutdown signal. In production, flows run indefinitely until shutdown.
        shutdown_rx.await.ok();

        info!("Shutdown signal received, stopping all flows...");

        // Signal all flow tasks to stop by marking them for cancellation.
        // Each task will be cancelled at its next await point, which happens frequently
        // since flows continuously process events from async channels and streams.
        for handle in &background_handles {
            handle.abort();
        }

        // Wait for all flow tasks to complete their shutdown.
        // This blocks until every task has been fully cancelled or completed. We must wait
        // for all flows to stop before deleting leases to prevent duplicate event processing.
        // If we delete leases while flows are still running, new pods will immediately acquire
        // the leases while old pods continue processing events, causing duplicate work.
        let _ = futures::future::join_all(background_handles).await;

        // All flows have now fully stopped. Clean up leases to allow new pods to acquire leadership.
        // At this point it is safe to delete leases because no flows are processing events.
        for task_manager in task_managers {
            if let Err(e) = task_manager.shutdown().await {
                warn!("Failed to shutdown task manager: {}", e);
            }
        }

        info!("Shutdown complete, all flows stopped and leases released");
        Ok(())
    }
}
