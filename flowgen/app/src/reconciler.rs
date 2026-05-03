//! Hot-reload reconciler for cache-sourced flows.
//!
//! Receives batched `WatchEvent`s from the watcher, debounces rapid sequences of
//! writes (e.g. a git push syncing many flows at once), and reconciles the running
//! flow registry: starting new flows, stopping removed ones, and swapping changed ones
//! without interrupting flows that were not affected.

use crate::app::FlowHandle;
use crate::config::FlowConfig;
use flowgen_core::cache::{Cache, WatchEvent};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Coalescing window: events that arrive within this window are batched together.
const DEBOUNCE_WINDOW: Duration = Duration::from_secs(2);

/// Ceiling: a batch is always flushed after this duration regardless of ongoing events.
const DEBOUNCE_CEILING: Duration = Duration::from_secs(10);

/// Errors that can occur in the reconciler.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to parse a flow configuration received from the cache.
    #[error("Failed to parse flow config for key '{key}': {source}")]
    FlowConfigParse {
        key: String,
        #[source]
        source: config::ConfigError,
    },
    /// Failed to read the flow registry.
    #[error("Failed to read flow registry")]
    RegistryReadFailed,
    /// Failed to write the flow registry.
    #[error("Failed to write flow registry")]
    RegistryWriteFailed,
    /// Failed to build a new flow during hot-reload.
    #[error("Failed to build flow '{flow}': {source}")]
    FlowBuild {
        flow: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// Failed to initialise a new flow during hot-reload.
    #[error("Failed to initialise flow '{flow}': {source}")]
    FlowInit {
        flow: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// Failed to start tasks for a new flow during hot-reload.
    #[error("Failed to start tasks for flow '{flow}': {source}")]
    FlowStartTasks {
        flow: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
}

/// All dependencies the reconciler needs to build and start replacement flows.
pub struct ReconcilerContext {
    pub cache: Arc<dyn Cache>,
    pub app_config: Arc<crate::config::AppConfig>,
    pub resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    pub http_server: Option<Arc<flowgen_http::server::WebhookServer>>,
    pub mcp_server: Option<Arc<flowgen_mcp::server::McpServer>>,
    pub ai_gateway_server: Option<Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>>,
    pub filesystem_flow_names: Arc<HashSet<String>>,
    pub flow_registry: Arc<RwLock<HashMap<String, FlowHandle>>>,
}

/// Runs the reconciler loop until `shutdown` is cancelled.
///
/// Drains `WatchEvent`s from the watcher channel, coalesces rapid sequences into
/// batches using a 2-second debounce window with a 10-second ceiling, then calls
/// `reconcile_batch` for each batch.
pub async fn run(
    mut rx: tokio::sync::mpsc::Receiver<WatchEvent>,
    ctx: ReconcilerContext,
    shutdown: CancellationToken,
) {
    loop {
        let batch = match collect_batch(&mut rx, &shutdown).await {
            Some(b) => b,
            None => break,
        };
        reconcile_batch(batch, &ctx).await;
    }
    info!("Hot-reload reconciler stopped.");
}

/// Collects a debounced batch of watch events.
///
/// Waits for the first event, then accumulates additional events that arrive within
/// `DEBOUNCE_WINDOW`. Forces a flush after `DEBOUNCE_CEILING` regardless of ongoing
/// activity. Returns `None` on shutdown.
async fn collect_batch(
    rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
    shutdown: &CancellationToken,
) -> Option<Vec<WatchEvent>> {
    let first = tokio::select! {
        _ = shutdown.cancelled() => return None,
        ev = rx.recv() => ev?,
    };

    let mut batch = vec![first];
    let deadline = Instant::now() + DEBOUNCE_CEILING;
    let mut window = Box::pin(tokio::time::sleep(DEBOUNCE_WINDOW));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = &mut window => break,
            _ = tokio::time::sleep_until(deadline) => break,
            ev = rx.recv() => match ev {
                None => break,
                Some(e) => {
                    batch.push(e);
                    window.as_mut().reset(Instant::now() + DEBOUNCE_WINDOW);
                }
            },
        }
    }
    Some(batch)
}

/// Applies a batch of watch events to the running flow registry.
///
/// Deduplicates events by key (last write wins within a batch), then processes
/// each key: replacing existing flows or starting new ones for `Put` events,
/// and stopping flows for `Delete` events. Filesystem flows are never overwritten.
async fn reconcile_batch(batch: Vec<WatchEvent>, ctx: &ReconcilerContext) {
    // Deduplicate: last event per key wins.
    let mut by_key: HashMap<String, WatchEvent> = HashMap::new();
    for event in batch {
        let key = match &event {
            WatchEvent::Put { key, .. } | WatchEvent::Delete { key } => key.clone(),
        };
        by_key.insert(key, event);
    }

    for (key, event) in by_key {
        match event {
            WatchEvent::Put { value, .. } => reconcile_put(&key, value, ctx).await,
            WatchEvent::Delete { .. } => reconcile_delete(&key, ctx).await,
        }
    }
}

/// Handles a `Put` event: parses the config, builds and starts the new flow,
/// then stops and deregisters the old one if it was running.
///
/// If any step of building or starting the new flow fails, the old flow is left
/// running and the error is logged. The new flow is fully ready before the old
/// one is touched.
async fn reconcile_put(key: &str, value: bytes::Bytes, ctx: &ReconcilerContext) {
    let flow_name = match derive_flow_name(key, ctx) {
        Some(name) => name,
        None => return,
    };

    if ctx.filesystem_flow_names.contains(&flow_name) {
        warn!(
            flow = %flow_name,
            key = %key,
            "Ignoring cache update: this flow is loaded from the filesystem and takes precedence."
        );
        return;
    }

    // Parse the YAML config from the cache value.
    let config = match parse_flow_config(key, &value) {
        Ok(c) => c,
        Err(e) => {
            error!(error = %e);
            return;
        }
    };

    // Build and start the replacement flow before touching the running one.
    let mut new_flow = match build_flow(config, ctx) {
        Ok(f) => f,
        Err(e) => {
            error!(error = %e);
            return;
        }
    };

    if let Err(e) = new_flow.init().await {
        error!(
            error = %Error::FlowInit {
                flow: flow_name.clone(),
                source: Box::new(e),
            }
        );
        return;
    }

    let blocking_handles = match new_flow.start_tasks().await {
        Ok(h) => h,
        Err(e) => {
            error!(
                error = %Error::FlowStartTasks {
                    flow: flow_name.clone(),
                    source: Box::new(e),
                }
            );
            return;
        }
    };

    // Await blocking handles (webhook/AI gateway registration) before swapping.
    let results = futures::future::join_all(blocking_handles).await;
    for result in results {
        if let Err(e) = result {
            error!(flow = %flow_name, "Blocking task failed during hot-reload: {e}");
        }
    }

    let cancellation_token = new_flow
        .cancellation_token()
        .unwrap_or_else(tokio_util::sync::CancellationToken::new);

    let join_handle = new_flow.run();

    // Stop the old flow and bulk-deregister its server entries.
    stop_and_deregister(&flow_name, ctx).await;

    // Insert the new handle into the registry.
    match ctx.flow_registry.write() {
        Ok(mut registry) => {
            registry.insert(
                flow_name.clone(),
                FlowHandle {
                    cancellation_token,
                    join_handle,
                    from_filesystem: false,
                },
            );
            info!(flow = %flow_name, key = %key, "Flow hot-reloaded.");
        }
        Err(_) => {
            error!(error = %Error::RegistryWriteFailed);
        }
    }
}

/// Handles a `Delete` event: stops the flow and deregisters its routes/tools.
async fn reconcile_delete(key: &str, ctx: &ReconcilerContext) {
    let flow_name = match derive_flow_name(key, ctx) {
        Some(name) => name,
        None => return,
    };

    if ctx.filesystem_flow_names.contains(&flow_name) {
        warn!(
            flow = %flow_name,
            key = %key,
            "Ignoring cache delete: this flow is loaded from the filesystem."
        );
        return;
    }

    stop_and_deregister(&flow_name, ctx).await;
    info!(flow = %flow_name, key = %key, "Flow removed via hot-reload.");
}

/// Cancels a running flow, awaits its shutdown, and deregisters its HTTP routes
/// and MCP tools from the shared servers.
async fn stop_and_deregister(flow_name: &str, ctx: &ReconcilerContext) {
    let old_handle = match ctx.flow_registry.write() {
        Ok(mut registry) => registry.remove(flow_name),
        Err(_) => {
            error!(error = %Error::RegistryWriteFailed);
            return;
        }
    };

    let old = match old_handle {
        Some(h) => h,
        None => return,
    };

    old.cancellation_token.cancel();
    // Await with a timeout so a stuck flow does not block the reconciler indefinitely.
    let timeout = Duration::from_secs(30);
    if tokio::time::timeout(timeout, old.join_handle)
        .await
        .is_err()
    {
        warn!(
            flow = %flow_name,
            "Flow did not stop within 30 seconds; proceeding with deregistration."
        );
    }

    // Deregister every webhook, MCP tool, and AI gateway entry owned by this
    // flow in one pass via the generic server's bulk deregister.
    if let Some(http_server) = &ctx.http_server {
        http_server.deregister_flow(flow_name);
    }
    if let Some(mcp_server) = &ctx.mcp_server {
        mcp_server.deregister_flow(flow_name);
    }
    if let Some(ai_gateway_server) = &ctx.ai_gateway_server {
        ai_gateway_server.deregister_flow(flow_name);
    }
}

/// Derives a flow name from a cache key by stripping the configured flow prefix.
///
/// Returns `None` and logs a warning if the key does not match the expected prefix format.
fn derive_flow_name(key: &str, ctx: &ReconcilerContext) -> Option<String> {
    let prefix = ctx
        .app_config
        .flows
        .cache
        .as_ref()
        .map(|c| c.prefix.as_str())
        .unwrap_or("flowgen.flows");

    key.strip_prefix(&format!("{prefix}."))
        .map(|s| s.replace('.', "-"))
        .or_else(|| {
            warn!(key = %key, "Watch event key does not match flow prefix '{prefix}'; ignoring.");
            None
        })
}

/// Parses a raw cache value as a `FlowConfig`.
fn parse_flow_config(key: &str, value: &bytes::Bytes) -> Result<FlowConfig, Error> {
    let content = String::from_utf8_lossy(value);
    let format = if key.ends_with(".json") {
        config::FileFormat::Json
    } else {
        config::FileFormat::Yaml
    };

    config::Config::builder()
        .add_source(config::File::from_str(&content, format))
        .build()
        .and_then(|c| c.try_deserialize::<FlowConfig>())
        .map_err(|source| Error::FlowConfigParse {
            key: key.to_string(),
            source,
        })
}

/// Builds a `Flow` from a parsed config using the current app context.
fn build_flow(
    flow_config: FlowConfig,
    ctx: &ReconcilerContext,
) -> Result<crate::flow::Flow, Error> {
    let flow_name = flow_config.flow.name.clone();
    let mut builder = crate::flow::FlowBuilder::new()
        .config(Arc::new(flow_config))
        .cache(Arc::clone(&ctx.cache));

    if let Some(server) = &ctx.http_server {
        builder = builder.http_server(Arc::clone(server));
    }
    if let Some(server) = &ctx.mcp_server {
        builder = builder.mcp_server(Arc::clone(server));
    }
    if let Some(retry) = ctx
        .app_config
        .worker
        .as_ref()
        .and_then(|w| w.retry.as_ref())
    {
        builder = builder.retry(retry.clone());
    }
    if let Some(buf) = ctx
        .app_config
        .worker
        .as_ref()
        .and_then(|w| w.event_buffer_size)
    {
        builder = builder.event_buffer_size(buf);
    }
    if let Some(loader) = &ctx.resource_loader {
        builder = builder.resource_loader(loader.clone());
    }

    builder.build().map_err(|source| Error::FlowBuild {
        flow: flow_name,
        source: Box::new(source),
    })
}
