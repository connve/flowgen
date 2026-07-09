//! Embedded web admin interface for flowgen.
//!
//! Serves the static SvelteKit UI and a small read-only API that exposes the
//! currently loaded flows. The static assets are compiled into the binary with
//! `rust-embed`, so the single `flowgen` binary remains self-contained.

use axum::{
    extract::{Path as AxumPath, State},
    http::{HeaderMap, StatusCode, Uri},
    response::sse::{Event as SseEvent, KeepAlive, Sse},
    response::{IntoResponse, Redirect},
    routing::get,
    Json, Router,
};
use futures::stream::Stream;
use futures_util::StreamExt;
use rust_embed::RustEmbed;
use serde::Serialize;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{info, warn};

/// Default port for the admin web server.
pub const DEFAULT_WEB_PORT: u16 = 8080;

/// Default path prefix for the admin web UI.
pub const DEFAULT_WEB_PATH: &str = "/";

/// Summary of a loaded flow returned by the admin API. Combines static
/// registration data (name, description, tags…) with live counters
/// pulled from the FlowRegistry the tracing layer maintains.
#[derive(Debug, Clone, Serialize)]
pub struct FlowSummary {
    /// Unique flow name.
    pub name: String,
    /// Human-readable name taken from `labels.display_name`. UI falls back to
    /// `name` when absent.
    pub display_name: Option<String>,
    /// Optional description taken from the `description` label.
    pub description: Option<String>,
    /// Tags taken from the `tags` label array (empty when none).
    pub tags: Vec<String>,
    /// Whether the flow requires leader election.
    pub require_leader_election: bool,
    /// Number of tasks in the flow.
    pub task_count: usize,
    /// Source of the flow configuration: "filesystem" or "cache".
    pub source: String,
    /// RFC 3339 timestamp of when the flow supervisor was spawned.
    pub started_at: Option<String>,
    /// RFC 3339 timestamp of the most recent info!() inside a task.handle
    /// scope; `None` when the flow has never processed an event yet.
    pub last_event_at: Option<String>,
    /// Same as above but for warn!().
    pub last_warning_at: Option<String>,
    /// Same as above but for error!().
    pub last_error_at: Option<String>,
    /// Total successful events since process start.
    pub events_total: u64,
    /// Total warn events since process start.
    pub warnings_total: u64,
    /// Total error events since process start.
    pub errors_total: u64,
    /// Coarse status derived from the three "last_*_at" timestamps.
    pub status: flowgen_core::flow::activity::FlowStatus,
}

/// Full flow detail returned by `GET /api/flows/{name}` for the inspector modal.
#[derive(Debug, Clone, Serialize)]
pub struct FlowDetail {
    /// Flow name (echoed back for the client).
    pub name: String,
    /// Human-readable name taken from `labels.display_name`.
    pub display_name: Option<String>,
    /// YAML source of the flow config as loaded from disk / cache.
    pub yaml: String,
}

/// Errors that can occur while running the admin web server.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to bind the TCP listener.
    #[error("Error binding admin web listener on port {port}: {source}")]
    BindListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    /// Failed to serve HTTP requests.
    #[error("Error serving admin web requests: {source}")]
    ServeHttp {
        #[source]
        source: std::io::Error,
    },
}

/// Embedded static assets produced by the SvelteKit build.
#[derive(RustEmbed)]
#[folder = "../../web/build"]
struct WebAssets;

/// State shared with the admin API handlers.
pub struct WebState {
    /// Registry of currently running flows.
    pub flow_registry: Arc<RwLock<std::collections::HashMap<String, crate::app::FlowHandle>>>,
    /// Path prefix the UI is mounted at (e.g. "" or "/flowgen"), used to
    /// strip the prefix from asset lookups. Always without a trailing slash.
    pub prefix: String,
    /// Optional resource loader used by the admin resources endpoints to
    /// list and fetch templates, prompts, SQL files, etc.
    pub resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    /// Shared activity registry populated by the tracing layer. Used by
    /// the flow list, the flow detail, and the SSE stream.
    pub flow_activity: Arc<flowgen_core::flow::activity::FlowRegistry>,
    /// Cache the SSE stream subscribes to for cross-replica activity.
    pub cache: Arc<dyn flowgen_core::cache::Cache>,
}

/// Summary of a resource returned by the admin API.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceSummary {
    /// Resource key relative to the loader's base path (e.g. `"gcp/create_demo_tables.sql"`).
    pub key: String,
    /// Filename extension (`sql`, `md`, `yaml`, …) — used by the UI to pick
    /// a syntax hint and a viewer.
    pub extension: Option<String>,
    /// File size in bytes when known, `None` for cache-backed entries.
    pub size: Option<u64>,
}

/// Full-content resource response.
#[derive(Debug, Clone, Serialize)]
pub struct ResourceContent {
    /// Resource key echoed back for the client.
    pub key: String,
    /// Filename extension (`sql`, `md`, `yaml`, …).
    pub extension: Option<String>,
    /// UTF-8 file contents.
    pub content: String,
}

/// Starts the admin web server on the given port.
///
/// The server mounts the embedded UI at `path` and exposes `GET /api/flows`
/// alongside it. All other requests fall back to `index.html` so the SvelteKit
/// client-side router can handle them.
pub async fn start_web_server(port: u16, path: &str, mut state: WebState) -> Result<(), Error> {
    let prefix = path.trim_end_matches('/').to_string();
    let api_prefix = if prefix.is_empty() {
        "/api".to_string()
    } else {
        format!("{prefix}/api")
    };
    state.prefix = prefix.clone();

    let app = Router::new()
        .route(&format!("{api_prefix}/flows"), get(list_flows))
        .route(&format!("{api_prefix}/flows/stream"), get(stream_flows))
        .route(&format!("{api_prefix}/flows/{{name}}"), get(get_flow))
        .route(&format!("{api_prefix}/version"), get(get_version))
        .route(&format!("{api_prefix}/resources"), get(list_resources))
        .route(
            &format!("{api_prefix}/resources/{{*key}}"),
            get(get_resource),
        )
        .fallback(serve_embedded)
        .with_state(Arc::new(state));

    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .map_err(|source| Error::BindListener { port, source })?;

    info!(port, path = %path, "Starting admin web server");

    axum::serve(listener, app)
        .await
        .map_err(|source| Error::ServeHttp { source })
}

/// Returns a list of currently loaded flows.
async fn list_flows(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    let flows = match state.flow_registry.read() {
        Ok(registry) => registry
            .values()
            .map(|handle| build_summary(handle, &state.flow_activity))
            .collect::<Vec<_>>(),
        Err(_) => {
            warn!("Flow registry is poisoned, returning empty flow list");
            Vec::new()
        }
    };

    Json(flows)
}

/// Merges the registered flow handle (static config-time data) with
/// whatever live metrics the tracing layer has collected so far.
fn build_summary(
    handle: &crate::app::FlowHandle,
    activity: &flowgen_core::flow::activity::FlowRegistry,
) -> FlowSummary {
    let started_at = system_time_to_rfc3339(handle.started_at());
    let source = match handle.from_filesystem {
        true => "filesystem".to_string(),
        false => "cache".to_string(),
    };
    let snapshot = activity.snapshot(handle.flow_name());
    let (
        last_event_at,
        last_warning_at,
        last_error_at,
        events_total,
        warnings_total,
        errors_total,
        status,
    ) = match snapshot {
        Some(s) => (
            s.last_event_at_ms.and_then(ms_to_rfc3339),
            s.last_warning_at_ms.and_then(ms_to_rfc3339),
            s.last_error_at_ms.and_then(ms_to_rfc3339),
            s.events_total,
            s.warnings_total,
            s.errors_total,
            s.status,
        ),
        None => (
            None,
            None,
            None,
            0,
            0,
            0,
            flowgen_core::flow::activity::FlowStatus::Idle,
        ),
    };
    FlowSummary {
        name: handle.flow_name().to_string(),
        display_name: handle.display_name().map(ToString::to_string),
        description: handle.description().map(ToString::to_string),
        tags: handle.tags().to_vec(),
        require_leader_election: handle.require_leader_election(),
        task_count: handle.task_count(),
        source,
        started_at,
        last_event_at,
        last_warning_at,
        last_error_at,
        events_total,
        warnings_total,
        errors_total,
        status,
    }
}

fn system_time_to_rfc3339(t: std::time::SystemTime) -> Option<String> {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => {
            let secs = d.as_secs() as i64;
            let nsecs = d.subsec_nanos();
            chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs).map(|dt| dt.to_rfc3339())
        }
        Err(_) => None,
    }
}

fn ms_to_rfc3339(ms: u64) -> Option<String> {
    let secs = (ms / 1000) as i64;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs).map(|dt| dt.to_rfc3339())
}

/// Returns the YAML source of a single flow so operators can inspect the
/// loaded flow from the admin UI.
async fn get_flow(
    State(state): State<Arc<WebState>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<FlowDetail>, (StatusCode, String)> {
    let Ok(registry) = state.flow_registry.read() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Flow registry is poisoned".into(),
        ));
    };
    match registry.get(&name) {
        Some(handle) => Ok(Json(FlowDetail {
            name: handle.flow_name().to_string(),
            display_name: handle.display_name().map(ToString::to_string),
            yaml: handle.flow_yaml().to_string(),
        })),
        None => Err((StatusCode::NOT_FOUND, format!("Flow '{name}' not found"))),
    }
}

/// Streams flow activity to the admin UI over Server-Sent Events.
///
/// The stream subscribes to the shared cache under [`ACTIVITY_PREFIX`]
/// with `include_history=true`, so a UI opening the modal receives the
/// retained per-key history first (letting it replay recent flashes for
/// the DAG) before switching to live updates. Multi-node deployments
/// backed by NATS get cross-replica visibility for free — every publish
/// lands in the same KV bucket regardless of which node runs the flow.
async fn stream_flows(
    State(state): State<Arc<WebState>>,
) -> Sse<impl Stream<Item = Result<SseEvent, axum::Error>>> {
    use flowgen_core::cache::WatchEvent;
    use flowgen_core::flow::activity::ACTIVITY_PREFIX;

    let snapshot = state.flow_activity.snapshot_all();
    let snapshot_frame = SseEvent::default()
        .event("snapshot")
        .json_data(&snapshot)
        .unwrap_or_else(|_| SseEvent::default().data("[]"));

    let watch = state.cache.watch(ACTIVITY_PREFIX, true).await;
    let live = match watch {
        Ok(w) => w
            .filter_map(|res| async move {
                let ev = match res {
                    Ok(ev) => ev,
                    Err(source) => {
                        warn!(error = %source, "Cache watch error on activity stream");
                        return None;
                    }
                };
                let bytes = match ev {
                    WatchEvent::Put { value, .. } => value,
                    WatchEvent::Delete { .. } => return None,
                };
                match serde_json::from_slice::<serde_json::Value>(&bytes) {
                    Ok(activity) => match SseEvent::default().event("activity").json_data(activity)
                    {
                        Ok(ev) => Some(Ok(ev)),
                        Err(source) => {
                            warn!(error = %source, "Failed to encode SSE activity frame");
                            None
                        }
                    },
                    Err(source) => {
                        warn!(error = %source, "Failed to decode cached activity payload");
                        None
                    }
                }
            })
            .boxed(),
        Err(source) => {
            warn!(error = %source, "Cache does not support watch; live SSE frames disabled");
            futures_util::stream::empty().boxed()
        }
    };

    let stream = tokio_stream::once(Ok(snapshot_frame)).chain(live);
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

/// Returns the list of resources discoverable from the filesystem loader.
/// Cache-backed loaders are not walked today (no listing API on the cache
/// abstraction); those installations get an empty list until we add one.
async fn list_resources(State(state): State<Arc<WebState>>) -> Json<Vec<ResourceSummary>> {
    let Some(loader) = &state.resource_loader else {
        return Json(Vec::new());
    };
    let Some(base) = loader.base_path() else {
        return Json(Vec::new());
    };

    let mut entries: Vec<ResourceSummary> = walkdir::WalkDir::new(base)
        .follow_links(false)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|e| e.file_type().is_file())
        .filter_map(|e| {
            let rel = e.path().strip_prefix(base).ok()?;
            let key = rel.to_string_lossy().replace('\\', "/");
            let extension = e
                .path()
                .extension()
                .and_then(|s| s.to_str())
                .map(str::to_string);
            let size = e.metadata().ok().map(|m| m.len());
            Some(ResourceSummary {
                key,
                extension,
                size,
            })
        })
        .collect();
    entries.sort_by(|a, b| a.key.cmp(&b.key));
    Json(entries)
}

/// Returns the content of a single resource by key.
async fn get_resource(
    State(state): State<Arc<WebState>>,
    AxumPath(key): AxumPath<String>,
) -> Result<Json<ResourceContent>, (StatusCode, String)> {
    let Some(loader) = &state.resource_loader else {
        return Err((
            StatusCode::NOT_FOUND,
            "Resource loader not configured".into(),
        ));
    };
    // Guard path traversal — the loader itself would resolve `..` against
    // its base, so a hostile key could escape the resources directory.
    if key.split('/').any(|seg| seg == "..") {
        return Err((StatusCode::BAD_REQUEST, "Invalid resource key".into()));
    }
    match loader.load(&key).await {
        Ok(content) => {
            let extension = std::path::Path::new(&key)
                .extension()
                .and_then(|s| s.to_str())
                .map(str::to_string);
            Ok(Json(ResourceContent {
                key,
                extension,
                content,
            }))
        }
        Err(source) => Err((StatusCode::NOT_FOUND, source.to_string())),
    }
}

/// Returns the running flowgen version so the UI can render it in the sidebar.
async fn get_version() -> impl IntoResponse {
    #[derive(serde::Serialize)]
    struct Version {
        version: &'static str,
    }
    Json(Version {
        version: env!("CARGO_PKG_VERSION"),
    })
}

/// Serves a file from the embedded asset folder.
async fn serve_embedded(State(state): State<Arc<WebState>>, uri: Uri) -> axum::response::Response {
    let raw = uri.path();
    // When a non-empty prefix is configured, requests outside that prefix
    // must not surface the UI — the client would then baked-in a wrong
    // base path and every subsequent API call would 404 into the HTML
    // fallback. Redirect to the mount point so both UI and API share the
    // same prefix.
    let stripped = if state.prefix.is_empty() {
        raw
    } else {
        match raw.strip_prefix(&state.prefix) {
            Some(rest) if rest.is_empty() || rest.starts_with('/') => rest,
            _ => return Redirect::temporary(&format!("{}/", state.prefix)).into_response(),
        }
    };
    let path = match stripped.trim_start_matches('/') {
        "" => "index.html".to_string(),
        rest => rest.to_string(),
    };

    match WebAssets::get(&path) {
        Some(content) => {
            let content_type = mime_guess::from_path(&path).first_or_octet_stream();
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                content_type.to_string().parse().unwrap(),
            );
            (StatusCode::OK, headers, content.data).into_response()
        }
        None => match WebAssets::get("index.html") {
            Some(content) => {
                let mut headers = HeaderMap::new();
                headers.insert(
                    axum::http::header::CONTENT_TYPE,
                    "text/html".parse().unwrap(),
                );
                (StatusCode::OK, headers, content.data).into_response()
            }
            None => (StatusCode::NOT_FOUND, HeaderMap::new(), Vec::new()).into_response(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_WEB_PORT, 8080);
        assert_eq!(DEFAULT_WEB_PATH, "/");
    }

    #[test]
    fn test_web_state_allows_empty_registry() {
        let cache: Arc<dyn flowgen_core::cache::Cache> =
            Arc::new(flowgen_core::cache::memory::MemoryCache::new());
        let state = WebState {
            flow_registry: Arc::new(RwLock::new(HashMap::new())),
            prefix: String::new(),
            resource_loader: None,
            flow_activity: flowgen_core::flow::activity::FlowRegistry::builder()
                .cache(Arc::clone(&cache))
                .build(),
            cache,
        };
        let registry = state.flow_registry.read().unwrap();
        assert!(registry.is_empty());
    }
}
