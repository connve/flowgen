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
use flowgen_client::types as api;
use futures::stream::Stream;
use futures_util::StreamExt;
use rust_embed::RustEmbed;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tracing::{info, warn};

/// Default port for the admin web server.
pub const DEFAULT_WEB_PORT: u16 = 8080;

/// Default path prefix for the admin web UI.
pub const DEFAULT_WEB_PATH: &str = "/";

/// Base path the SvelteKit bundle was compiled with (`PUBLIC_BASE`
/// in `web/svelte.config.js`). `serve_embedded` rewrites this to
/// whatever `web.path` was configured.
const BUILT_BASE_PATH: &str = "/flowgen";

// Response types come from `flowgen_client::types` — see openapi.yaml.

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
    /// Backend-agnostic log query used by the SSE stream and the
    /// history endpoint.
    pub logs_query: Option<Arc<dyn flowgen_core::telemetry::query::LogsQuery>>,
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
        .route(&format!("{api_prefix}/openapi.yaml"), get(get_openapi))
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
) -> api::FlowSummary {
    let source = match handle.from_filesystem {
        true => api::FlowSummarySource::Filesystem,
        false => api::FlowSummarySource::Cache,
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
            s.last_event_at_ms.and_then(ms_to_datetime),
            s.last_warning_at_ms.and_then(ms_to_datetime),
            s.last_error_at_ms.and_then(ms_to_datetime),
            s.events_total,
            s.warnings_total,
            s.errors_total,
            core_status_to_api(s.status),
        ),
        None => (None, None, None, 0, 0, 0, api::FlowStatus::Idle),
    };
    api::FlowSummary {
        name: handle.flow_name().to_string(),
        display_name: handle.display_name().map(ToString::to_string),
        description: handle.description().map(ToString::to_string),
        tags: handle.tags().to_vec(),
        require_leader_election: handle.require_leader_election(),
        task_count: handle.task_count() as u64,
        source,
        started_at: system_time_to_datetime(handle.started_at()),
        last_event_at,
        last_warning_at,
        last_error_at,
        events_total: events_total as i64,
        warnings_total: warnings_total as i64,
        errors_total: errors_total as i64,
        status,
    }
}

fn core_status_to_api(s: flowgen_core::flow::activity::FlowStatus) -> api::FlowStatus {
    use flowgen_core::flow::activity::FlowStatus as Core;
    match s {
        Core::Idle => api::FlowStatus::Idle,
        Core::Running => api::FlowStatus::Running,
        Core::Warning => api::FlowStatus::Warning,
        Core::Error => api::FlowStatus::Error,
    }
}

fn system_time_to_datetime(t: std::time::SystemTime) -> Option<chrono::DateTime<chrono::Utc>> {
    let d = t.duration_since(std::time::UNIX_EPOCH).ok()?;
    chrono::DateTime::<chrono::Utc>::from_timestamp(d.as_secs() as i64, d.subsec_nanos())
}

fn ms_to_datetime(ms: u64) -> Option<chrono::DateTime<chrono::Utc>> {
    let secs = (ms / 1000) as i64;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nsecs)
}

/// Returns the YAML source of a single flow so operators can inspect the
/// loaded flow from the admin UI.
async fn get_flow(
    State(state): State<Arc<WebState>>,
    AxumPath(name): AxumPath<String>,
) -> Result<Json<api::FlowDetail>, (StatusCode, String)> {
    let Ok(registry) = state.flow_registry.read() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Flow registry is poisoned".into(),
        ));
    };
    match registry.get(&name) {
        Some(handle) => Ok(Json(api::FlowDetail {
            name: handle.flow_name().to_string(),
            display_name: handle.display_name().map(ToString::to_string),
            yaml: handle.flow_yaml().to_string(),
        })),
        None => Err((StatusCode::NOT_FOUND, format!("Flow '{name}' not found"))),
    }
}

/// Streams flow activity to the admin UI over Server-Sent Events.
///
/// Emits a `snapshot` frame with per-flow metrics, then streams
/// `activity` frames pulled from the configured `LogsQuery` backend.
async fn stream_flows(
    State(state): State<Arc<WebState>>,
) -> Sse<impl Stream<Item = Result<SseEvent, axum::Error>>> {
    use flowgen_core::telemetry::query::LogFilter;

    let snapshot = state.flow_activity.snapshot_all();
    let snapshot_frame = SseEvent::default()
        .event("snapshot")
        .json_data(&snapshot)
        .unwrap_or_else(|_| SseEvent::default().data("[]"));

    let live = match state.logs_query.as_ref() {
        Some(query) => {
            let filter = LogFilter::default();
            let history = match query.query(filter.clone(), usize::MAX).await {
                Ok(records) => records,
                Err(source) => {
                    warn!(error = %source, "Log query history read failed");
                    Vec::new()
                }
            };
            let tail = match query.tail(filter).await {
                Ok(stream) => stream,
                Err(source) => {
                    warn!(error = %source, "Log query tail subscription failed");
                    futures_util::stream::empty().boxed()
                }
            };
            let flow_activity = Arc::clone(&state.flow_activity);
            let history_stream = futures_util::stream::iter(history);
            history_stream
                .chain(tail)
                .filter_map(move |record| {
                    let flow_activity = Arc::clone(&flow_activity);
                    async move { activity_from_stored(&record, &flow_activity) }
                })
                .filter_map(|activity| async move {
                    match SseEvent::default().event("activity").json_data(&activity) {
                        Ok(ev) => Some(Ok(ev)),
                        Err(source) => {
                            warn!(error = %source, "Failed to encode SSE activity frame");
                            None
                        }
                    }
                })
                .boxed()
        }
        None => {
            warn!("No logs query backend configured; live SSE frames disabled");
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

/// Turns a `StoredLog` from the telemetry backend into a `FlowActivity`
/// frame the admin UI understands. Returns `None` for records that do
/// not belong to a flow (background logs, framework messages).
fn activity_from_stored(
    record: &flowgen_core::telemetry::StoredLog,
    flow_activity: &flowgen_core::flow::activity::FlowRegistry,
) -> Option<flowgen_core::flow::activity::FlowActivity> {
    use flowgen_core::flow::activity::ActivityLevel;
    let mut flow: Option<&str> = None;
    let mut task: Option<&str> = None;
    let mut task_type: Option<&str> = None;
    let mut level = ActivityLevel::Info;
    let mut ts_ms: u64 = 0;
    let mut duration_ms: Option<u64> = None;
    let mut event_id: Option<String> = None;
    let mut extra: Vec<(String, String)> = Vec::new();
    let mut is_activity = false;
    for (k, v) in &record.attributes {
        match k.as_str() {
            "flow" => flow = Some(v),
            "task" => task = Some(v),
            "task_type" => task_type = Some(v),
            "task_id" => {}
            "activity" => is_activity = v == "true",
            "level" => {
                level = match v.as_str() {
                    "warning" | "warn" => ActivityLevel::Warning,
                    "error" => ActivityLevel::Error,
                    _ => ActivityLevel::Info,
                };
            }
            "timestamp" => {
                // RFC3339 from tracing_subscriber::fmt::json().
                if let Ok(dt) = chrono::DateTime::parse_from_rfc3339(v) {
                    ts_ms = dt.timestamp_millis().max(0) as u64;
                }
            }
            "duration_ms" => {
                if let Ok(parsed) = v.parse::<u64>() {
                    duration_ms = Some(parsed);
                }
            }
            "event_id" | "event.id" => event_id = Some(v.clone()),
            "target" | "ts_ms" | "event.subject" => {}
            "context" => {
                // `EventLogger::context()` serializes fields as a JSON
                // object; split them back into individual entries so the
                // admin UI renders each as its own attribute row.
                match serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(v) {
                    Ok(map) => {
                        for (ck, cv) in map {
                            let cv_str = match cv {
                                serde_json::Value::String(s) => s,
                                other => other.to_string(),
                            };
                            extra.push((ck, cv_str));
                        }
                    }
                    Err(_) => extra.push((k.clone(), v.clone())),
                }
            }
            _ => extra.push((k.clone(), v.clone())),
        }
    }
    let flow = flow?.to_string();
    // Drop framework logs that never entered a task scope — the admin
    // UI shows per-task activity, so a row without task_type is noise.
    let task_type = task_type?.to_string();
    // Drop lifecycle logs emitted outside a `task.handle` scope
    // (registration, startup, shutdown). Every `task.handle` span
    // declares `activity = true`; its absence means the log came from
    // task lifecycle and does not belong in the per-event feed.
    if !is_activity {
        return None;
    }
    let metrics = flow_activity
        .snapshot(&flow)
        .unwrap_or_else(|| flowgen_core::flow::activity::FlowMetricsSnapshot::empty(&flow));
    Some(flowgen_core::flow::activity::FlowActivity {
        flow,
        task: task.map(str::to_string),
        task_type: Some(task_type),
        level,
        ts_ms,
        message: record.body.clone(),
        duration_ms,
        event_id,
        extra,
        metrics,
    })
}

/// Returns the list of resources discoverable from the filesystem loader.
/// Cache-backed loaders are not walked today (no listing API on the cache
/// abstraction); those installations get an empty list until we add one.
async fn list_resources(State(state): State<Arc<WebState>>) -> Json<Vec<api::ResourceSummary>> {
    let Some(loader) = &state.resource_loader else {
        return Json(Vec::new());
    };
    let Some(base) = loader.base_path() else {
        return Json(Vec::new());
    };

    let mut entries: Vec<api::ResourceSummary> = walkdir::WalkDir::new(base)
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
            let size = e.metadata().ok().map(|m| m.len() as i64);
            Some(api::ResourceSummary {
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
) -> Result<Json<api::ResourceContent>, (StatusCode, String)> {
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
            Ok(Json(api::ResourceContent {
                key,
                extension,
                content,
            }))
        }
        Err(source) => Err((StatusCode::NOT_FOUND, source.to_string())),
    }
}

/// Returns the running flowgen version so the UI can render it in the sidebar.
async fn get_version() -> Json<api::VersionInfo> {
    Json(api::VersionInfo {
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Returns the bundled OpenAPI spec.
async fn get_openapi() -> impl IntoResponse {
    let mut headers = HeaderMap::new();
    headers.insert(
        axum::http::header::CONTENT_TYPE,
        axum::http::HeaderValue::from_static("application/yaml"),
    );
    (StatusCode::OK, headers, flowgen_client::OPENAPI_YAML)
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

    let (asset_path, content) = match WebAssets::get(&path) {
        Some(content) => (path, content),
        None => match WebAssets::get("index.html") {
            Some(content) => ("index.html".to_string(), content),
            None => return (StatusCode::NOT_FOUND, HeaderMap::new(), Vec::new()).into_response(),
        },
    };

    let content_type = mime_guess::from_path(&asset_path).first_or_octet_stream();
    let content_type_header = match axum::http::HeaderValue::from_str(content_type.as_ref()) {
        Ok(v) => v,
        Err(_) => axum::http::HeaderValue::from_static("application/octet-stream"),
    };
    let mut headers = HeaderMap::new();
    headers.insert(axum::http::header::CONTENT_TYPE, content_type_header);

    let body = match rewrite_base_path(&asset_path, &content.data, &state.prefix) {
        Some(rewritten) => rewritten,
        None => content.data.into_owned(),
    };

    (StatusCode::OK, headers, body).into_response()
}

/// Rewrites `BUILT_BASE_PATH` occurrences in text assets to `prefix`.
/// Returns `None` for binary assets or when `prefix == BUILT_BASE_PATH`.
fn rewrite_base_path(asset_path: &str, bytes: &[u8], prefix: &str) -> Option<Vec<u8>> {
    if prefix == BUILT_BASE_PATH {
        return None;
    }
    let ext = asset_path.rsplit('.').next()?;
    match ext {
        "html" | "js" | "css" | "json" | "map" | "webmanifest" => {}
        _ => return None,
    }
    let text = std::str::from_utf8(bytes).ok()?;
    // Slashed form first so the bare replace does not overwrite
    // asset URLs that share the `/flowgen` prefix.
    let slashed_replacement = match prefix {
        "" => "/".to_string(),
        other => format!("{other}/"),
    };
    let slashed_needle = format!("{BUILT_BASE_PATH}/");
    let rewritten = text
        .replace(&slashed_needle, &slashed_replacement)
        .replace(BUILT_BASE_PATH, prefix);
    Some(rewritten.into_bytes())
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
        let state = WebState {
            flow_registry: Arc::new(RwLock::new(HashMap::new())),
            prefix: String::new(),
            resource_loader: None,
            flow_activity: flowgen_core::flow::activity::FlowRegistry::builder().build(),
            logs_query: None,
        };
        let registry = state.flow_registry.read().unwrap();
        assert!(registry.is_empty());
    }

    #[test]
    fn rewrite_base_path_is_noop_when_prefix_matches_build() {
        let html = br#"<script src="/flowgen/_app/foo.js"></script>"#;
        let out = rewrite_base_path("index.html", html, BUILT_BASE_PATH);
        assert!(out.is_none(), "no rewrite needed when prefix == built base");
    }

    #[test]
    fn rewrite_base_path_replaces_prefix_in_html() {
        let html = br#"<script src="/flowgen/_app/foo.js"></script>"#;
        let out = rewrite_base_path("index.html", html, "/ortofan").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(text, r#"<script src="/ortofan/_app/foo.js"></script>"#);
    }

    #[test]
    fn rewrite_base_path_replaces_prefix_in_js() {
        let js = br#"const base = "/flowgen"; fetch("/flowgen/api/flows");"#;
        let out = rewrite_base_path("app.js", js, "/nested/path").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert!(text.contains(r#"fetch("/nested/path/api/flows")"#));
        assert!(text.contains(r#"const base = "/nested/path""#));
    }

    #[test]
    fn rewrite_base_path_replaces_both_bare_and_slashed_forms() {
        let html = br#"<script>base="/flowgen"</script><link href="/flowgen/style.css">"#;
        let out = rewrite_base_path("index.html", html, "/test").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(
            text,
            r#"<script>base="/test"</script><link href="/test/style.css">"#
        );
    }

    #[test]
    fn rewrite_base_path_maps_empty_prefix_to_root() {
        let html = br#"<link href="/flowgen/style.css">"#;
        let out = rewrite_base_path("index.html", html, "").expect("rewrite");
        let text = std::str::from_utf8(&out).unwrap();
        assert_eq!(text, r#"<link href="/style.css">"#);
    }

    #[test]
    fn rewrite_base_path_skips_binary_assets() {
        let png = &[0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a];
        let out = rewrite_base_path("logo.png", png, "/anything");
        assert!(out.is_none(), "binary assets must not be rewritten");
    }

    #[test]
    fn rewrite_base_path_returns_input_when_no_hits() {
        let css = br#"body { color: red; }"#;
        let out = rewrite_base_path("style.css", css, "/other").expect("rewrite");
        assert_eq!(out, css);
    }
}
