use crate::config::{AppConfig, FlowConfig};
use config::Config;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tracing::{debug, error, info, warn, Instrument};

/// Tracks a running flow.
///
/// Held in the flow registry so the reconciler knows how to stop a flow
/// cleanly: cancel its tasks, await the join handle, and bulk-deregister any
/// webhook / MCP tool / AI gateway entries the flow owns from the shared
/// servers (via each server's own `deregister_flow(flow_name)`).
pub struct FlowHandle {
    /// Token used to signal the flow's tasks to stop gracefully.
    pub cancellation_token: tokio_util::sync::CancellationToken,
    /// Join handle for the flow's background monitor task spawned by `run()`.
    pub join_handle: tokio::task::JoinHandle<()>,
    /// True when the flow was loaded from the filesystem at startup.
    /// Cache-sourced reload events must not overwrite filesystem flows.
    pub from_filesystem: bool,
}

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
        source: flowgen_core::http_server::Error,
    },
    /// MCP server startup error.
    #[error("Failed to start MCP server: {source}")]
    McpServerStart {
        #[source]
        source: flowgen_core::http_server::Error,
    },
    /// AI gateway server startup error.
    #[error("Failed to start AI gateway server: {source}")]
    AiGatewayServerStart {
        #[source]
        source: flowgen_core::http_server::Error,
    },
    /// Auth provider initialization failed.
    #[error("Failed to build auth provider: {source}")]
    AuthProviderInit {
        #[source]
        source: flowgen_core::auth::AuthError,
    },
    /// Failed to read MCP credentials file.
    #[error("Failed to read MCP credentials from {path}: {source}")]
    McpCredentialsRead {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to parse MCP credentials file.
    #[error("Failed to parse MCP credentials from {path}: {source}")]
    McpCredentialsParse {
        path: std::path::PathBuf,
        #[source]
        source: serde_json::Error,
    },
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
    /// Failed to initialize the system cache for flow/resource loading.
    #[error("Failed to initialize system cache: {source}")]
    SystemCacheInit {
        #[source]
        source: flowgen_nats::cache::Error,
    },
    /// Failed to list keys from the system cache.
    #[error("Failed to list flow keys from cache: {source}")]
    CacheListKeys {
        #[source]
        source: flowgen_core::cache::Error,
    },
    /// Failed to read a flow from the system cache.
    #[error("Failed to read flow {key:?} from cache: {source}")]
    CacheFlowRead {
        key: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    /// Failed to parse flow content from cache as UTF-8.
    #[error("Flow {key:?} from cache is not valid UTF-8: {source}")]
    CacheFlowUtf8 {
        key: String,
        #[source]
        source: std::string::FromUtf8Error,
    },
    /// Failed to parse flow config from cache.
    #[error("Failed to parse flow config from cache key {key:?}: {source}")]
    CacheFlowConfigParse {
        key: String,
        #[source]
        source: config::ConfigError,
    },
}
/// Main application that loads and runs flows concurrently.
pub struct App {
    /// Global application configuration.
    pub config: AppConfig,
}

impl App {
    /// Initializes a system cache connection for flow/resource loading.
    /// Separate from the runtime cache to avoid key collisions during list operations.
    fn init_system_cache(
        app_config: &AppConfig,
        db_name: &str,
    ) -> Result<Arc<dyn flowgen_core::cache::Cache>, Error> {
        let cache_config = app_config.cache.as_ref().ok_or(Error::InvalidFlowsPath)?;

        let nats_cache = flowgen_nats::cache::CacheBuilder::new()
            .credentials_path(cache_config.credentials_path.clone())
            .url(cache_config.url.clone())
            .build()
            .and_then(|builder| futures::executor::block_on(async { builder.init(db_name).await }))
            .map_err(|source| Error::SystemCacheInit { source })?;

        Ok(Arc::new(nats_cache))
    }

    /// Loads flow configurations from a system cache bucket.
    /// Lists all keys under the given prefix, reads each value, and parses as FlowConfig.
    async fn load_flows_from_cache(
        cache: &dyn flowgen_core::cache::Cache,
        prefix: &str,
    ) -> Result<Vec<FlowConfig>, Error> {
        let keys = cache
            .list_keys(prefix)
            .await
            .map_err(|source| Error::CacheListKeys { source })?;

        let mut flow_configs = Vec::new();
        for key in keys {
            let bytes = match cache
                .get(&key)
                .await
                .map_err(|source| Error::CacheFlowRead {
                    key: key.clone(),
                    source,
                })? {
                Some(b) => b,
                None => {
                    warn!(key = %key, "Flow key listed but not found, skipping");
                    continue;
                }
            };

            let content =
                String::from_utf8(bytes.to_vec()).map_err(|source| Error::CacheFlowUtf8 {
                    key: key.clone(),
                    source,
                })?;

            // Determine format from key extension, default to YAML.
            let file_format = if key.ends_with(".json") {
                config::FileFormat::Json
            } else {
                config::FileFormat::Yaml
            };

            let config = Config::builder()
                .add_source(config::File::from_str(&content, file_format))
                .build()
                .map_err(|source| Error::CacheFlowConfigParse {
                    key: key.clone(),
                    source,
                })?;

            match config.try_deserialize::<FlowConfig>() {
                Ok(flow_config) => {
                    if let Err(reason) = flow_config.validate() {
                        error!(
                            key = %key,
                            error = %reason,
                            "Flow config from cache failed name validation. Skipping."
                        );
                        continue;
                    }
                    info!(flow = %flow_config.flow.name, key = %key, "Loaded flow from cache");
                    flow_configs.push(flow_config);
                }
                Err(source) => {
                    error!(
                        key = %key,
                        error = %source,
                        "Failed to deserialize flow config from cache. Skipping."
                    );
                }
            }
        }

        Ok(flow_configs)
    }

    /// Loads flow configurations from the filesystem using the configured path.
    ///
    /// Returns an empty vector when `flows.path` is not configured. This allows
    /// the worker to run cache-only (every flow loaded from the system cache)
    /// or to mix sources (a few local flows mounted from disk, the rest pulled
    /// from the system cache by a sync flow).
    fn load_flows_from_filesystem(app_config: &AppConfig) -> Result<Vec<FlowConfig>, Error> {
        let flows_path = match app_config.flows.path.as_ref() {
            Some(path) => path,
            None => return Ok(Vec::new()),
        };

        let flows_path_str = flows_path.to_str().ok_or(Error::InvalidFlowsPath)?;

        // Check if path contains wildcards (backward compatibility).
        let glob_patterns: Vec<String> = if flows_path_str.contains('*') {
            vec![flows_path_str.to_string()]
        } else {
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
                .filter_map(|path| match path {
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
                                if let Err(reason) = flow_config.validate() {
                                    error!(
                                        path = %path.display(),
                                        error = %reason,
                                        "Flow config failed name validation. Skipping this flow."
                                    );
                                    return None;
                                }
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
                })
                .collect();

            flow_configs.extend(matched_flows);
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

        // Load flows from filesystem and (optionally) from the distributed cache.
        // The two sources are merged so a worker can run any combination of:
        //   - filesystem only (no cache section, classic mode);
        //   - cache only (no `flows.path` set, every flow comes from the cache);
        //   - hybrid (a small set of bootstrap flows mounted from disk while the
        //     bulk of user flows lives in the cache, populated by a sync flow).
        // On name collisions the filesystem entry wins, which keeps locally
        // mounted bootstrap flows from being silently overridden by a stale
        // cache entry with the same name.
        let filesystem_flows = Self::load_flows_from_filesystem(&app_config)?;
        info!("Loaded {} flows from filesystem.", filesystem_flows.len());

        let (cache_flows, system_cache) = match app_config.flows.cache.as_ref() {
            Some(cache_opts) if cache_opts.enabled => {
                match Self::init_system_cache(&app_config, &cache_opts.db_name) {
                    Ok(cache) => {
                        info!(
                            "Initialized system cache for flow loading on bucket '{}'.",
                            cache_opts.db_name
                        );
                        let configs =
                            Self::load_flows_from_cache(cache.as_ref(), &cache_opts.prefix).await?;
                        info!("Loaded {} flows from cache.", configs.len());
                        (configs, Some((cache, cache_opts.clone())))
                    }
                    Err(e) => {
                        error!(
                            error = %e,
                            "Failed to initialize system cache. Continuing with filesystem flows only."
                        );
                        (Vec::new(), None)
                    }
                }
            }
            _ => (Vec::new(), None),
        };

        let mut flow_configs = filesystem_flows;
        // Record filesystem flow names before merging so the reconciler can enforce
        // the invariant that cache reload events never overwrite filesystem flows.
        let filesystem_flow_names: HashSet<String> =
            flow_configs.iter().map(|f| f.flow.name.clone()).collect();
        let mut seen_names = filesystem_flow_names.clone();
        for cache_flow in cache_flows {
            if seen_names.insert(cache_flow.flow.name.clone()) {
                flow_configs.push(cache_flow);
            } else {
                warn!(
                    flow = %cache_flow.flow.name,
                    "Cache flow shadowed by a filesystem flow with the same name. Skipping cache copy."
                );
            }
        }

        // Create shared webhook HTTP server if enabled.
        let http_server: Option<Arc<flowgen_http::server::WebhookServer>> = match app_config
            .worker
            .as_ref()
            .and_then(|w| w.http_server.as_ref())
        {
            Some(http_config) if http_config.enabled => {
                let path = http_config.path.clone();
                let auth_provider = match http_config.auth.clone() {
                    Some(auth_config) => Some(
                        auth_config
                            .build()
                            .await
                            .map_err(|source| Error::AuthProviderInit { source })?,
                    ),
                    None => None,
                };
                let server = flowgen_core::http_server::HttpServer::<
                    flowgen_http::server::WebhookDispatcher,
                >::new(path)
                .with_credentials_path(http_config.credentials_path.clone())
                .with_auth_provider(auth_provider);
                Some(Arc::new(server))
            }
            _ => None,
        };

        // Create MCP server if enabled by admin, HTTP server is available, and flows contain mcp_tool tasks.
        let mcp_enabled = app_config
            .worker
            .as_ref()
            .and_then(|w| w.mcp_server.as_ref())
            .map(|mcp| mcp.enabled)
            .unwrap_or(false);

        let has_mcp_tools = flow_configs.iter().any(|fc| {
            fc.flow
                .tasks
                .iter()
                .any(|t| matches!(t, crate::config::TaskType::mcp_tool(_)))
        });

        if has_mcp_tools && !mcp_enabled {
            warn!("Flows contain mcp_tool tasks but mcp_server is not enabled in config. MCP tools will not be registered.");
        }

        let mcp_server: Option<Arc<flowgen_mcp::server::McpServer>> = if mcp_enabled
            && has_mcp_tools
        {
            let mcp_config = app_config
                .worker
                .as_ref()
                .and_then(|w| w.mcp_server.as_ref());
            // Load MCP credentials if configured.
            let credentials = mcp_config
                .and_then(|mcp_config| mcp_config.credentials_path.as_ref())
                .and_then(|path| match std::fs::read_to_string(path) {
                    Ok(content) => {
                        match serde_json::from_str::<flowgen_mcp::config::Credentials>(&content) {
                            Ok(creds) => Some(creds),
                            Err(source) => {
                                let err = Error::McpCredentialsParse {
                                    path: path.clone(),
                                    source,
                                };
                                error!("{err}");
                                None
                            }
                        }
                    }
                    Err(source) => {
                        let err = Error::McpCredentialsRead {
                            path: path.clone(),
                            source,
                        };
                        error!("{err}");
                        None
                    }
                });
            let path = mcp_config
                .map(|c| c.path.clone())
                .unwrap_or_else(|| flowgen_mcp::server::DEFAULT_MCP_PATH.to_string());
            let auth_provider = match mcp_config.and_then(|c| c.auth.clone()) {
                Some(auth_config) => Some(
                    auth_config
                        .build()
                        .await
                        .map_err(|source| Error::AuthProviderInit { source })?,
                ),
                None => None,
            };
            Some(Arc::new(flowgen_mcp::server::new_mcp_server(
                path,
                credentials,
                auth_provider,
            )))
        } else {
            None
        };

        // Create AI gateway server if enabled and any flow contains llm_proxy tasks.
        let has_ai_gateway_tasks = flow_configs.iter().any(|fc| {
            fc.flow
                .tasks
                .iter()
                .any(|t| matches!(t, crate::config::TaskType::llm_proxy(_)))
        });
        let ai_gateway_enabled = app_config
            .worker
            .as_ref()
            .and_then(|w| w.ai_gateway.as_ref())
            .map(|g| g.enabled)
            .unwrap_or(false);

        if has_ai_gateway_tasks && !ai_gateway_enabled {
            warn!("Flows contain llm_proxy tasks but worker.ai_gateway is not enabled. LLM proxy endpoints will not be registered.");
        }

        let ai_gateway_server: Option<Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>> =
            if ai_gateway_enabled && has_ai_gateway_tasks {
                let ai_config = app_config
                    .worker
                    .as_ref()
                    .and_then(|w| w.ai_gateway.as_ref());
                let path = ai_config.map(|c| c.path.clone()).unwrap_or_else(|| {
                    flowgen_ai_agent::ai_gateway::server::DEFAULT_AI_GATEWAY_PATH.to_string()
                });
                let credentials_path = ai_config.and_then(|c| c.credentials_path.clone());
                let auth_provider = match ai_config.and_then(|c| c.auth.clone()) {
                    Some(auth_config) => Some(
                        auth_config
                            .build()
                            .await
                            .map_err(|source| Error::AuthProviderInit { source })?,
                    ),
                    None => None,
                };
                let server = flowgen_core::http_server::HttpServer::<
                    flowgen_ai_agent::ai_gateway::server::AiGatewayDispatcher,
                >::new(path)
                .with_credentials_path(credentials_path)
                .with_auth_provider(auth_provider);
                Some(Arc::new(server))
            } else {
                None
            };

        let cache: Arc<dyn flowgen_core::cache::Cache> =
            if let Some(cache_config) = &app_config.cache {
                if cache_config.enabled {
                    let db_name = cache_config
                        .db_name
                        .as_deref()
                        .unwrap_or(crate::config::DEFAULT_CACHE_DB_NAME);

                    let mut cache_builder = flowgen_nats::cache::CacheBuilder::new()
                        .credentials_path(cache_config.credentials_path.clone())
                        .url(cache_config.url.clone());
                    if let Some(history) = cache_config.history {
                        cache_builder = cache_builder.history(history);
                    }
                    if let Some(ttl) = cache_config.tombstone_ttl {
                        cache_builder = cache_builder.tombstone_ttl(ttl);
                    }
                    match cache_builder.build().and_then(|builder| {
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

        // Build the resource loader. Filesystem and cache sources are
        // independent: configure either, both, or neither. When both are
        // active, the loader tries the filesystem first and falls back to
        // the cache on a miss, matching the gradual-migration semantics used
        // for flow loading.
        let resource_loader = match app_config.resources.as_ref() {
            Some(resource_options) => {
                let base_path = resource_options.path.clone();

                let cache_source = match resource_options.cache.as_ref() {
                    Some(rc) if rc.enabled => {
                        // Reuse the system cache initialised for flow loading
                        // when the bucket matches; otherwise spin up a fresh one.
                        let cache = if system_cache.as_ref().map(|(_, opts)| opts.db_name.as_str())
                            == Some(&rc.db_name)
                        {
                            system_cache.as_ref().map(|(c, _)| c.clone())
                        } else {
                            match Self::init_system_cache(&app_config, &rc.db_name) {
                                Ok(cache) => {
                                    info!(
                                        "Initialized system cache for resource loading on bucket '{}'.",
                                        rc.db_name
                                    );
                                    Some(cache)
                                }
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        "Failed to init resource system cache. Continuing with filesystem only."
                                    );
                                    None
                                }
                            }
                        };
                        cache.map(|c| (c, rc.prefix.clone()))
                    }
                    _ => None,
                };

                match (base_path, cache_source) {
                    (Some(path), Some((cache, prefix))) => Some(
                        flowgen_core::resource::ResourceLoader::new(Some(path))
                            .with_cache(cache, prefix),
                    ),
                    (Some(path), None) => {
                        Some(flowgen_core::resource::ResourceLoader::new(Some(path)))
                    }
                    (None, Some((cache, prefix))) => Some(
                        flowgen_core::resource::ResourceLoader::from_cache(cache, prefix),
                    ),
                    (None, None) => None,
                }
            }
            None => None,
        };

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

            if let Some(ref server) = mcp_server {
                flow_builder = flow_builder.mcp_server(Arc::clone(server));
            }

            if let Some(ref server) = ai_gateway_server {
                flow_builder = flow_builder.ai_gateway_server(Arc::clone(server));
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

        // Start the webhook HTTP server.
        let mut background_handles = Vec::new();
        if let Some(ref http_server) = http_server {
            let configured_port = app_config
                .worker
                .as_ref()
                .and_then(|w| w.http_server.as_ref())
                .map(|http| http.port)
                .unwrap_or(flowgen_http::server::DEFAULT_WEBHOOK_PORT);
            let http_server = Arc::clone(http_server);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    if let Err(source) = http_server.start_server(configured_port).await {
                        let err = Error::HttpServerStart { source };
                        error!("{}", err);
                    }
                }
                .instrument(span),
            );
            background_handles.push(server_handle);
        }

        // Start the MCP server on its own port.
        if let Some(ref mcp_server) = mcp_server {
            let configured_port = app_config
                .worker
                .as_ref()
                .and_then(|w| w.mcp_server.as_ref())
                .map(|c| c.port)
                .unwrap_or(flowgen_mcp::server::DEFAULT_MCP_PORT);
            let server = Arc::clone(mcp_server);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    if let Err(source) = server.start_server(configured_port).await {
                        error!("{}", Error::McpServerStart { source });
                    }
                }
                .instrument(span),
            );
            background_handles.push(server_handle);
        }

        // Start the AI gateway server on its own port.
        if let Some(ref ai_gateway_server) = ai_gateway_server {
            let configured_port = app_config
                .worker
                .as_ref()
                .and_then(|w| w.ai_gateway.as_ref())
                .map(|c| c.port)
                .unwrap_or(flowgen_ai_agent::ai_gateway::server::DEFAULT_AI_GATEWAY_PORT);
            let server = Arc::clone(ai_gateway_server);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    if let Err(source) = server.start_server(configured_port).await {
                        error!("{}", Error::AiGatewayServerStart { source });
                    }
                }
                .instrument(span),
            );
            background_handles.push(server_handle);
        }

        // Collect task managers for shutdown cleanup.
        let task_managers: Vec<Arc<flowgen_core::task::manager::TaskManager>> =
            flows.iter().filter_map(|f| f.task_manager()).collect();

        // Build the flow registry keyed by flow name. The watcher reconciler uses
        // this to stop and deregister flows on hot-reload events.
        let flow_registry: Arc<RwLock<HashMap<String, FlowHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Start all background flow tasks and populate the registry.
        for flow in flows {
            let flow_name = flow.name().to_string();
            let from_filesystem = filesystem_flow_names.contains(&flow_name);

            let cancellation_token = flow
                .cancellation_token()
                .unwrap_or_else(tokio_util::sync::CancellationToken::new);

            let join_handle = flow.run();

            if let Ok(mut registry) = flow_registry.write() {
                registry.insert(
                    flow_name,
                    FlowHandle {
                        cancellation_token,
                        join_handle,
                        from_filesystem,
                    },
                );
            }
        }

        // Spawn the hot-reload watcher and reconciler if the system cache supports watching.
        // The watcher subscribes to flow key changes and the reconciler applies them.
        let watcher_shutdown = tokio_util::sync::CancellationToken::new();
        if let Some((cache, cache_opts)) = &system_cache {
            let prefix = cache_opts.prefix.clone();
            let (watch_tx, watch_rx) =
                tokio::sync::mpsc::channel::<flowgen_core::cache::WatchEvent>(256);

            let watcher_handle = crate::watcher::spawn(
                Arc::clone(cache) as Arc<dyn flowgen_core::cache::Cache>,
                prefix.clone(),
                watch_tx,
                watcher_shutdown.clone(),
            );
            background_handles.push(watcher_handle);

            let reconciler_ctx = crate::reconciler::ReconcilerContext {
                cache: Arc::clone(cache) as Arc<dyn flowgen_core::cache::Cache>,
                app_config: Arc::clone(&app_config),
                resource_loader: resource_loader.clone(),
                http_server: http_server.clone(),
                mcp_server: mcp_server.clone(),
                ai_gateway_server: ai_gateway_server.clone(),
                filesystem_flow_names: Arc::new(filesystem_flow_names.clone()),
                flow_registry: Arc::clone(&flow_registry),
            };
            let reconciler_shutdown = watcher_shutdown.clone();
            let reconciler_handle = tokio::spawn(async move {
                crate::reconciler::run(watch_rx, reconciler_ctx, reconciler_shutdown).await;
            });
            background_handles.push(reconciler_handle);

            info!(prefix = %prefix, "Hot-reload watcher and reconciler started.");
        }

        // Wait for shutdown signal. In production, flows run indefinitely until shutdown.
        shutdown_rx.await.ok();

        info!("Shutdown signal received, stopping all flows...");

        // Stop the watcher and reconciler first so no new reload events arrive
        // while flows are being shut down.
        watcher_shutdown.cancel();

        // Cancel all running flows via their cancellation tokens, then abort server
        // handles (HTTP, MCP) which do not use tokens.
        if let Ok(registry) = flow_registry.read() {
            for handle in registry.values() {
                handle.cancellation_token.cancel();
            }
        }
        for handle in &background_handles {
            handle.abort();
        }

        // Await all server background tasks and all flow join handles.
        let _ = futures::future::join_all(background_handles).await;
        let flow_join_handles: Vec<tokio::task::JoinHandle<()>> = match flow_registry.write() {
            Ok(mut registry) => registry
                .drain()
                .map(|(_, handle)| handle.join_handle)
                .collect(),
            Err(_) => Vec::new(),
        };
        let _ = futures::future::join_all(flow_join_handles).await;

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
