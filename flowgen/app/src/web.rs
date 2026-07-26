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
    routing::{get, post},
    Json, Router,
};
use flowgen_client::types as api;
use futures::stream::Stream;
use futures_util::StreamExt;
use rust_embed::RustEmbed;
use std::collections::HashMap;
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

/// SSE event name carrying `FlowMetricsSnapshot` payloads on `/api/flows/stream`.
const SSE_EVENT_SNAPSHOT: &str = "snapshot";

/// SSE event name carrying `LogRecord` payloads on `/api/logs/stream`.
const SSE_EVENT_LOG: &str = "log";

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
    /// Shared metrics store populated by the tracing layer. Used by
    /// the flow list, the flow detail, and the SSE stream.
    pub metrics_store: Arc<dyn flowgen_core::flow::activity::MetricsStore>,
    /// Backend-agnostic log query used by the SSE stream and the
    /// history endpoint.
    pub logs_store: Option<Arc<dyn flowgen_core::telemetry::query::LogsStore>>,
    /// Running application configuration, surfaced read-only by the admin
    /// config viewer. Secrets serialize as `"***"` (see `JwtConfig`).
    pub app_config: Arc<crate::config::AppConfig>,
    /// Cache backing the built-in Agents conversation history — the store our
    /// UI reads and writes; a persistence flow can later copy it into a
    /// database. This is the **system** cache (`flowgen_system`), which is out
    /// of flow-script reach, so chats are not exposed to `ctx.cache`. Proxy
    /// traffic stays stateless: conversations are our UI's domain, not the
    /// gateway's.
    pub conversation_cache: Arc<dyn flowgen_core::cache::Cache>,
    /// Whether a dedicated system bucket actually backs `conversation_cache`.
    /// False in single-binary/in-memory mode, where it falls back to the
    /// runtime cache that flow scripts can reach — `start_web_server` warns
    /// once at startup so operators know.
    pub system_bucket_present: bool,
    /// TTL applied to each conversation write, refreshed on every save. `None`
    /// persists indefinitely. From `web.agents.conversation_history_ttl`.
    pub conversation_history_ttl: Option<Duration>,
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

    // Without a dedicated system cache bucket, conversation history shares the
    // runtime cache that flow scripts can read and write via `ctx.cache`. Warn
    // once at startup so operators know to configure a system bucket when that
    // access matters.
    let system_bucket_present = state.system_bucket_present;

    let app = Router::new()
        .route(&format!("{api_prefix}/flows"), get(list_flows))
        .route(&format!("{api_prefix}/flows/stream"), get(stream_flows))
        .route(&format!("{api_prefix}/flows/{{*path}}"), get(get_flow))
        .route(&format!("{api_prefix}/logs"), get(list_logs))
        .route(&format!("{api_prefix}/logs/stream"), get(stream_logs))
        .route(&format!("{api_prefix}/version"), get(get_version))
        .route(&format!("{api_prefix}/config"), get(get_config))
        .route(&format!("{api_prefix}/agents/chat"), post(proxy_chat))
        .route(&format!("{api_prefix}/agents/models"), get(proxy_models))
        .route(
            &format!("{api_prefix}/agents/conversations"),
            get(list_conversations),
        )
        .route(
            &format!("{api_prefix}/agents/conversations/{{id}}"),
            get(get_conversation)
                .put(put_conversation)
                .delete(delete_conversation),
        )
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

    if !system_bucket_present {
        warn!(
            "Agents conversation history is stored in the runtime cache (no system cache bucket \
             configured), which flow scripts can read and write via ctx.cache"
        );
    }

    axum::serve(listener, app)
        .await
        .map_err(|source| Error::ServeHttp { source })
}

/// Returns a list of currently loaded flows.
async fn list_flows(State(state): State<Arc<WebState>>) -> impl IntoResponse {
    // Fetch all metrics up front (no lock held across the await), then do
    // the usual synchronous pass over the flow registry using a lookup.
    let metrics: HashMap<String, flowgen_core::flow::activity::FlowMetricsSnapshot> = state
        .metrics_store
        .snapshot_all()
        .await
        .unwrap_or_default()
        .into_iter()
        .map(|s| (s.flow.clone(), s))
        .collect();

    let flows = match state.flow_registry.read() {
        Ok(registry) => registry
            .values()
            .map(|handle| build_summary(handle, &metrics))
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
    metrics: &HashMap<String, flowgen_core::flow::activity::FlowMetricsSnapshot>,
) -> api::FlowSummary {
    let source = match handle.from_filesystem {
        true => api::FlowSummarySource::Filesystem,
        false => api::FlowSummarySource::Cache,
    };
    let snapshot = metrics.get(handle.identity());
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
        path: handle.identity().to_string(),
        name: handle.identity().to_string(),
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
        Core::Ok => api::FlowStatus::Ok,
        Core::Warn => api::FlowStatus::Warn,
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

/// Current wall-clock time in epoch milliseconds, for stamping conversation
/// writes. Saturates to 0 before the epoch, which never happens in practice.
fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Returns the YAML source of a single flow so operators can inspect the
/// loaded flow from the admin UI.
async fn get_flow(
    State(state): State<Arc<WebState>>,
    AxumPath(path): AxumPath<String>,
) -> Result<Json<api::FlowDetail>, (StatusCode, String)> {
    let Ok(registry) = state.flow_registry.read() else {
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Flow registry is poisoned".into(),
        ));
    };
    match registry.get(&path) {
        Some(handle) => Ok(Json(api::FlowDetail {
            path: handle.identity().to_string(),
            name: handle.identity().to_string(),
            display_name: handle.display_name().map(ToString::to_string),
            yaml: handle.flow_yaml().to_string(),
        })),
        None => Err((StatusCode::NOT_FOUND, format!("Flow '{path}' not found"))),
    }
}

/// Streams live per-flow metrics to the admin UI over Server-Sent Events.
///
/// Emits one `snapshot` frame with every flow's current metrics on
/// connect, then a `snapshot` frame carrying a single-element array
/// whenever any flow's counters change — the frontend already merges
/// `snapshot` payloads by `flow`, so a partial array updates just that
/// flow. Event/log history and live tail for a flow come from
/// `/api/logs` and `/api/logs/stream` (with `flow` set) — the same
/// source `/logs` uses — not from this endpoint.
async fn stream_flows(
    State(state): State<Arc<WebState>>,
) -> Sse<impl Stream<Item = Result<SseEvent, axum::Error>>> {
    let initial = state.metrics_store.snapshot_all().await.unwrap_or_default();
    let initial_frame = match SseEvent::default()
        .event(SSE_EVENT_SNAPSHOT)
        .json_data(&initial)
    {
        Ok(ev) => ev,
        Err(source) => {
            warn!(error = %source, "Failed to encode SSE snapshot frame");
            SseEvent::default().data("[]")
        }
    };

    let live = match state.metrics_store.watch_all().await {
        Ok(stream) => stream
            .filter_map(|snapshot| async move {
                match SseEvent::default()
                    .event(SSE_EVENT_SNAPSHOT)
                    .json_data(&[snapshot])
                {
                    Ok(ev) => Some(Ok(ev)),
                    Err(source) => {
                        warn!(error = %source, "Failed to encode SSE snapshot frame");
                        None
                    }
                }
            })
            .boxed(),
        Err(source) => {
            warn!(error = %source, "Metrics store watch subscription failed");
            futures_util::stream::empty().boxed()
        }
    };

    let stream = tokio_stream::once(Ok(initial_frame)).chain(live);
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

/// Default and maximum `?limit` for `/api/logs` snapshots.
const LOGS_SNAPSHOT_DEFAULT_LIMIT: usize = 500;
const LOGS_SNAPSHOT_MAX_LIMIT: usize = 10_000;

/// Returns retained log records — framework, lifecycle, and per-task
/// activity in one place. The per-flow Activity panel calls this with
/// `flow` set to backfill its history from the same source the global
/// `/logs` viewer uses (unscoped).
async fn list_logs(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<LogsQuery>,
) -> Json<Vec<api::LogRecord>> {
    let query = match state.logs_store.as_ref() {
        Some(q) => q,
        None => return Json(Vec::new()),
    };
    let limit = match params.limit {
        Some(n) => n.min(LOGS_SNAPSHOT_MAX_LIMIT),
        None => LOGS_SNAPSHOT_DEFAULT_LIMIT,
    };
    let filter = flowgen_core::telemetry::query::LogFilter {
        flow: params.flow,
        ..Default::default()
    };
    let records = match query.query(filter, limit).await {
        Ok(r) => r,
        Err(source) => {
            warn!(error = %source, "Log query history read failed");
            return Json(Vec::new());
        }
    };
    let wire: Vec<api::LogRecord> = records.into_iter().map(stored_to_wire).collect();
    Json(wire)
}

/// Streams log records as they arrive. Same scope as `list_logs`:
/// unscoped by default (the global `/logs` UI groups and filters by
/// level / task / flow / free text client-side); the per-flow Activity
/// panel passes `flow` so it only receives that flow's live records.
async fn stream_logs(
    State(state): State<Arc<WebState>>,
    axum::extract::Query(params): axum::extract::Query<LogsQuery>,
) -> Sse<impl Stream<Item = Result<SseEvent, axum::Error>>> {
    // Tail-only: `/api/logs` returns the initial snapshot, this endpoint
    // streams new records as they arrive. Sending history here too would
    // duplicate every retained record for a UI that already loaded them.
    let live = match state.logs_store.as_ref() {
        Some(query) => {
            let filter = flowgen_core::telemetry::query::LogFilter {
                flow: params.flow,
                ..Default::default()
            };
            let tail = match query.tail(filter).await {
                Ok(stream) => stream,
                Err(source) => {
                    warn!(error = %source, "Log query tail subscription failed");
                    futures_util::stream::empty().boxed()
                }
            };
            tail.filter_map(|record| async move {
                let wire = stored_to_wire(record);
                match SseEvent::default().event(SSE_EVENT_LOG).json_data(&wire) {
                    Ok(ev) => Some(Ok(ev)),
                    Err(source) => {
                        warn!(error = %source, "Failed to encode SSE log frame");
                        None
                    }
                }
            })
            .boxed()
        }
        None => {
            warn!("No logs query backend configured; /api/logs/stream is empty");
            futures_util::stream::empty().boxed()
        }
    };
    Sse::new(live).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}

#[derive(serde::Deserialize)]
struct LogsQuery {
    limit: Option<usize>,
    /// Restrict to one flow's records. Used by the per-flow Activity panel;
    /// omitted by the global `/logs` viewer, which shows every flow.
    flow: Option<String>,
}

/// Converts an internal `StoredLog` to the OpenAPI wire shape.
fn stored_to_wire(record: flowgen_core::telemetry::StoredLog) -> api::LogRecord {
    let spans = record
        .spans
        .into_iter()
        .map(|s| api::LogSpan {
            name: s.name,
            fields: s.fields.into_iter().map(kv_to_wire).collect(),
        })
        .collect();
    let timestamp = match record.timestamp.as_deref() {
        None => None,
        Some(ts) => match chrono::DateTime::parse_from_rfc3339(ts) {
            Ok(dt) => Some(dt.with_timezone(&chrono::Utc)),
            Err(_) => None,
        },
    };
    let level = match record.level.as_str() {
        "warn" | "warning" => api::LogRecordLevel::Warn,
        "error" => api::LogRecordLevel::Error,
        "debug" => api::LogRecordLevel::Debug,
        "trace" => api::LogRecordLevel::Trace,
        _ => api::LogRecordLevel::Info,
    };
    api::LogRecord {
        body: record.body,
        level,
        timestamp,
        target: record.target,
        spans,
        fields: record.fields.into_iter().map(kv_to_wire).collect(),
    }
}

fn kv_to_wire((k, v): (String, String)) -> api::KeyValue {
    api::KeyValue { key: k, value: v }
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

/// Returns the running application configuration as YAML for the admin
/// config viewer. Secrets are redacted at serialization time (see
/// `JwtConfig`), so no additional masking is needed here.
async fn get_config(State(state): State<Arc<WebState>>) -> Json<api::ConfigInfo> {
    let yaml = match serde_yaml::to_string(&*state.app_config) {
        Ok(yaml) => yaml,
        Err(source) => {
            warn!(error = %source, "Failed to serialize app config to YAML");
            String::new()
        }
    };
    Json(api::ConfigInfo { yaml })
}

/// Header and value identifying the built-in Agents chat to the AI gateway.
/// Resolves the base URL the built-in Agents chat proxies to. Prefers the
/// explicit `web.ai_gateway_url`; otherwise targets the same-process AI
/// gateway on loopback. Returns `None` when no gateway is configured.
fn gateway_base_url(state: &WebState) -> Option<String> {
    if let Some(url) = state
        .app_config
        .web
        .as_ref()
        .and_then(|w| w.ai_gateway_url.as_ref())
    {
        return Some(url.trim_end_matches('/').to_string());
    }
    let gateway = state.app_config.ai_gateway.as_ref()?;
    let path = gateway.path.trim_end_matches('/');
    Some(format!("http://127.0.0.1:{}{path}", gateway.port))
}

/// Builds the outbound headers sent with every proxied request to the AI
/// gateway, from `web.headers`. Used to identify this admin server to
/// `llm_proxy`/`mcp_tool` `headers` scoping (e.g. `X-Flowgen-Client:
/// flowgen-ui`). Entries that aren't valid header names/values are skipped.
fn outbound_gateway_headers(state: &WebState) -> reqwest::header::HeaderMap {
    let mut headers = reqwest::header::HeaderMap::new();
    let Some(web) = state.app_config.web.as_ref() else {
        return headers;
    };
    for (name, value) in &web.headers {
        let Ok(header_name) = reqwest::header::HeaderName::try_from(name.as_str()) else {
            continue;
        };
        let Ok(header_value) = reqwest::header::HeaderValue::from_str(value) else {
            continue;
        };
        headers.insert(header_name, header_value);
    }
    headers
}

/// Proxies a chat-completion request to the AI gateway, streaming the
/// response body straight back. The browser stays same-origin with the admin
/// server, so no gateway-side CORS is required and the gateway need not be
/// publicly reachable.
async fn proxy_chat(
    State(state): State<Arc<WebState>>,
    body: axum::body::Bytes,
) -> axum::response::Response {
    let Some(base) = gateway_base_url(&state) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "AI gateway is not configured",
        )
            .into_response();
    };
    let upstream = reqwest::Client::new()
        .post(format!("{base}/chat/completions"))
        .header(axum::http::header::CONTENT_TYPE, "application/json")
        .headers(outbound_gateway_headers(&state))
        .body(body)
        .send()
        .await;
    match upstream {
        Ok(resp) => {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/json")
                .to_string();
            let mut headers = HeaderMap::new();
            if let Ok(value) = axum::http::HeaderValue::from_str(&content_type) {
                headers.insert(axum::http::header::CONTENT_TYPE, value);
            }
            let stream = resp.bytes_stream();
            let body = axum::body::Body::from_stream(stream);
            (
                StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY),
                headers,
                body,
            )
                .into_response()
        }
        Err(source) => {
            warn!(error = %source, "Failed to reach AI gateway from Agents chat proxy");
            (StatusCode::BAD_GATEWAY, "Failed to reach AI gateway").into_response()
        }
    }
}

/// Proxies the gateway model list so the Agents chat can populate its model
/// selector without knowing the gateway URL.
async fn proxy_models(State(state): State<Arc<WebState>>) -> axum::response::Response {
    let Some(base) = gateway_base_url(&state) else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "AI gateway is not configured",
        )
            .into_response();
    };
    match reqwest::Client::new()
        .get(format!("{base}/models"))
        .headers(outbound_gateway_headers(&state))
        .send()
        .await
    {
        Ok(resp) => {
            let status =
                StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let text = resp.text().await.unwrap_or_default();
            let mut headers = HeaderMap::new();
            headers.insert(
                axum::http::header::CONTENT_TYPE,
                axum::http::HeaderValue::from_static("application/json"),
            );
            (status, headers, text).into_response()
        }
        Err(source) => {
            warn!(error = %source, "Failed to list AI gateway models from Agents chat proxy");
            (StatusCode::BAD_GATEWAY, "Failed to reach AI gateway").into_response()
        }
    }
}

// --- Built-in Agents conversation history -------------------------------
//
// Persistence for the admin UI's Agents chat. The gateway proxy stays
// stateless; conversation memory is our UI's domain and lives in the
// configured system cache (see `WebState::conversation_cache`), out of
// user-script reach. A persistence flow can later copy these into a database.
// Types (`api::Conversation`, etc.) are generated from openapi.yaml.

/// Key prefix for conversations in the system cache. The bucket name already
/// carries "flowgen", so keys stay unprefixed (matching `lease.`/`peers.`).
const CONVERSATION_KEY_PREFIX: &str = "agents.conversations.";

/// Validates a client-supplied conversation id: `[A-Za-z0-9_-]+`, non-empty.
/// Anything else is rejected rather than silently sanitized — the id is the
/// client's own handle, and `.` would break the dotted KV key namespace.
fn valid_conversation_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

fn conversation_key(id: &str) -> String {
    format!("{CONVERSATION_KEY_PREFIX}{id}")
}

/// Lists stored conversations (summaries only), newest first.
async fn list_conversations(State(state): State<Arc<WebState>>) -> axum::response::Response {
    let keys = match state
        .conversation_cache
        .list_keys(CONVERSATION_KEY_PREFIX)
        .await
    {
        Ok(keys) => keys,
        Err(source) => {
            warn!(error = %source, "Failed to list conversations from cache");
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response();
        }
    };

    let mut summaries = Vec::with_capacity(keys.len());
    for key in keys {
        match state.conversation_cache.get(&key).await {
            Ok(Some(bytes)) => match serde_json::from_slice::<api::Conversation>(&bytes) {
                Ok(c) => summaries.push(api::ConversationSummary {
                    id: c.id,
                    title: c.title,
                    updated_at: c.updated_at,
                    message_count: c.messages.len() as i64,
                }),
                // A single corrupt entry shouldn't sink the whole list.
                Err(source) => {
                    warn!(key = %key, error = %source, "Skipping unparseable conversation")
                }
            },
            Ok(None) => {}
            Err(source) => warn!(key = %key, error = %source, "Failed to read conversation"),
        }
    }
    summaries.sort_by_key(|s| std::cmp::Reverse(s.updated_at));

    Json(serde_json::json!({ "conversations": summaries })).into_response()
}

/// Returns a single conversation with its full message history.
async fn get_conversation(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    match state.conversation_cache.get(&conversation_key(&id)).await {
        Ok(Some(bytes)) => match serde_json::from_slice::<api::Conversation>(&bytes) {
            Ok(c) => Json(c).into_response(),
            Err(source) => {
                warn!(id = %id, error = %source, "Stored conversation is unparseable");
                (StatusCode::INTERNAL_SERVER_ERROR, "Corrupt conversation").into_response()
            }
        },
        Ok(None) => (StatusCode::NOT_FOUND, "Conversation not found").into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to read conversation from cache");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
}

/// Creates or overwrites a conversation. The path id is authoritative and the
/// `updated_at` is server-stamped; the TTL is refreshed on every write, so the
/// expiry window counts from the last activity.
async fn put_conversation(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
    Json(body): Json<api::ConversationUpsert>,
) -> axum::response::Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }

    let conversation = api::Conversation {
        id: id.clone(),
        title: body.title,
        messages: body.messages,
        updated_at: now_millis(),
    };
    let bytes = match serde_json::to_vec(&conversation) {
        Ok(bytes) => bytes,
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to serialize conversation");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store conversation",
            )
                .into_response();
        }
    };

    let ttl_secs = state.conversation_history_ttl.and_then(|d| {
        let secs = d.as_secs();
        (secs > 0).then_some(secs)
    });
    match state
        .conversation_cache
        .put(&conversation_key(&id), bytes.into(), ttl_secs)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to write conversation to cache");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
}

/// Deletes a conversation. Idempotent — deleting a missing id still succeeds.
async fn delete_conversation(
    State(state): State<Arc<WebState>>,
    AxumPath(id): AxumPath<String>,
) -> axum::response::Response {
    if !valid_conversation_id(&id) {
        return (StatusCode::BAD_REQUEST, "Invalid conversation id").into_response();
    }
    match state
        .conversation_cache
        .delete(&conversation_key(&id))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(source) => {
            warn!(id = %id, error = %source, "Failed to delete conversation from cache");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "Conversation store unavailable",
            )
                .into_response()
        }
    }
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
        let app_config = Arc::new(crate::config::AppConfig {
            cache: None,
            flows: crate::config::FlowOptions {
                path: None,
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        });
        let state = WebState {
            flow_registry: Arc::new(RwLock::new(HashMap::new())),
            prefix: String::new(),
            resource_loader: None,
            metrics_store: flowgen_core::flow::activity::OtlpMetricsStore::builder().build(),
            logs_store: None,
            app_config,
            conversation_cache: Arc::new(flowgen_core::cache::memory::MemoryCache::new()),
            system_bucket_present: false,
            conversation_history_ttl: None,
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
