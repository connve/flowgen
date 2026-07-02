//! MCP role for the generic HTTP server.
//!
//! Implements the MCP Streamable HTTP transport: a single POST endpoint that
//! accepts JSON-RPC 2.0 messages and responds with either JSON or SSE streams.
//! The server lifecycle, dispatch table, and hot-reload semantics live in
//! `flowgen_core::http_server`; this module only owns the MCP-specific URL
//! layout and JSON-RPC handling logic.
//!
//! Roles, ports and listener instantiation are configured in `flowgen_app`.

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::post,
    Router,
};
use flowgen_core::http_server::{DispatchState, Dispatcher, HasFlowName, HttpServer};
use flowgen_core::registry::{Content, ProgressEvent, ResponseRegistry, ToolResult};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::{error, info, Instrument};

/// Default MCP server port.
pub const DEFAULT_MCP_PORT: u16 = 3001;

/// Default MCP endpoint path.
pub const DEFAULT_MCP_PATH: &str = "/mcp/v1";

/// MCP protocol version supported by this server.
const MCP_PROTOCOL_VERSION: &str = "2025-03-26";

/// Convenience type alias for the MCP server.
pub type McpServer = HttpServer<McpDispatcher>;

/// Errors that can occur during MCP server operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to serialize JSON-RPC response: {source}")]
    JsonSerialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("Invalid tool call parameters: {source}")]
    InvalidParams {
        #[source]
        source: serde_json::Error,
    },
    #[error("Authentication failed.")]
    Unauthorized,
    #[error("Unknown tool: {name}")]
    UnknownTool { name: String },
    #[error("Failed to build event: {source}")]
    EventBuild {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to send event to pipeline: {0}")]
    PipelineSend(String),
    #[error(
        "MCP tool name collision: '{name}' is already registered. Use distinct names across flows."
    )]
    ToolNameCollision { name: String },
    #[error(
        "MCP resource URI collision: '{uri}' is already registered. Use distinct URIs across flows."
    )]
    ResourceUriCollision { uri: String },
    #[error(
        "MCP prompt name collision: '{name}' is already registered. Use distinct names across flows."
    )]
    PromptNameCollision { name: String },
    #[error("Unsupported JSON-RPC version: {version}")]
    UnsupportedJsonRpcVersion { version: String },
    #[error("Method not found: {method}")]
    MethodNotFound { method: String },
    #[error("Pipeline did not produce a result.")]
    PipelineNoResult,
    #[error("Tool execution timed out after {duration}.")]
    ToolTimeout { duration: String },
    #[error("Failed to build SSE response body: {source}")]
    SseBody {
        #[source]
        source: axum::http::Error,
    },
    #[error("Failed to build SSE response for session stream: {source}")]
    SseResponseBuild {
        #[source]
        source: axum::http::Error,
    },
    #[error("Failed to encode Mcp-Session-Id header: {source}")]
    SessionIdEncoding {
        #[source]
        source: axum::http::header::InvalidHeaderValue,
    },
    #[error("Failed to render templated resource: {source}")]
    ResourceTemplateRender {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Failed to render prompt message: {source}")]
    PromptMessageRender {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Unknown resource URI: {uri}")]
    UnknownResourceUri { uri: String },
    #[error("Unknown prompt: {name}")]
    UnknownPrompt { name: String },
    #[error("Missing required prompt argument: {name}")]
    MissingPromptArgument { name: String },
    #[error("Registered concrete resource carries a template body")]
    ConcreteBodyIsTemplate,
    #[error("Registered resource template carries a concrete body")]
    TemplateBodyIsConcrete,
}

/// Dispatch-table entry describing one registered MCP tool.
///
/// Stored in the MCP server's dispatch table keyed by tool name. `flow_name`
/// lets the server bulk-deregister every tool owned by a flow when the flow
/// is stopped or hot-reloaded.
#[derive(Clone, Debug)]
pub struct ToolRegistration {
    /// Name of the flow that registered this tool.
    pub flow_name: String,
    /// Tool description for LLMs.
    pub description: String,
    /// JSON Schema for tool input parameters.
    pub input_schema: serde_json::Value,
    /// Channel to send events into the flow pipeline.
    pub tx: mpsc::Sender<flowgen_core::event::Event>,
    /// Optional credentials for this specific tool.
    pub credentials: Option<super::config::Credentials>,
    /// Optional timeout for waiting on flow completion.
    pub ack_timeout: Option<std::time::Duration>,
    /// Whether user-level auth is required for this tool.
    pub auth_required: bool,
    /// Number of leaf tasks reachable from the mcp_tool source. Used to size
    /// the per-request completion channel so the response is delivered only
    /// after every leaf has signalled.
    pub leaf_count: usize,
}

impl HasFlowName for ToolRegistration {
    fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

/// Body of a registered MCP resource.
///
/// `Concrete` resolves at task init; `Template` defers to `resources/read`
/// so URI-binding params can drive rendering.
#[derive(Clone, Debug)]
pub enum ResourceBody {
    Concrete(String),
    Template(flowgen_core::resource::Source),
}

/// URI-template parameter descriptor with resolved completion values.
#[derive(Clone, Debug)]
pub struct TemplateParameter {
    pub name: String,
    /// Static list of candidate values served by `completion/complete`.
    /// `None` disables completion for the parameter.
    pub completion_values: Option<Vec<String>>,
}

/// Dispatch-table entry for an MCP resource or resource template.
#[derive(Clone, Debug)]
pub struct ResourceRegistration {
    pub flow_name: String,
    pub uri: String,
    pub name: String,
    pub description: String,
    pub mime_type: String,
    pub body: ResourceBody,
    /// URI-template parameter descriptors; empty for concrete resources.
    pub parameters: Vec<TemplateParameter>,
    /// Loader used to resolve template `Source::Resource` content at read
    /// time. Concrete resources ignore this; templates need it.
    pub resource_loader: Option<flowgen_core::resource::ResourceLoader>,
}

impl HasFlowName for ResourceRegistration {
    fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

/// Argument definition attached to an MCP prompt registration.
#[derive(Clone, Debug)]
pub struct PromptArgument {
    pub name: String,
    pub description: String,
    pub required: bool,
    pub default: Option<String>,
    /// Static list of candidate values served by `completion/complete`.
    /// `None` disables completion for the argument.
    pub completion_values: Option<Vec<String>>,
}

/// Message role for a prompt registration.
#[derive(Clone, Copy, Debug)]
pub enum PromptRole {
    User,
    Assistant,
}

impl PromptRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            PromptRole::User => "user",
            PromptRole::Assistant => "assistant",
        }
    }
}

/// One message in a prompt template.
#[derive(Clone, Debug)]
pub struct PromptMessage {
    pub role: PromptRole,
    pub content: flowgen_core::resource::Source,
}

/// Dispatch-table entry for an MCP prompt.
#[derive(Clone, Debug)]
pub struct PromptRegistration {
    pub flow_name: String,
    pub name: String,
    pub description: String,
    pub arguments: Vec<PromptArgument>,
    pub messages: Vec<PromptMessage>,
    pub resource_loader: Option<flowgen_core::resource::ResourceLoader>,
}

impl HasFlowName for PromptRegistration {
    fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

/// MCP-specific shared state passed to the dispatcher via `DispatchState::extras`.
#[derive(Clone)]
pub struct McpExtras {
    /// Registry for correlating requests with pipeline results / progress.
    pub response_registry: Arc<ResponseRegistry>,
    /// Optional global credentials shared across tools.
    pub credentials: Arc<Option<super::config::Credentials>>,
    /// URI scheme for auto-generated `mcp_resource` URIs of the form
    /// `<scheme>://<flow_name>/<name>`. White-label deployments set this
    /// so LLM-visible identifiers carry the deployment's brand.
    pub resource_uri_scheme: Arc<str>,
    /// Concrete resources keyed by URI.
    pub resources: Arc<dashmap::DashMap<String, ResourceRegistration>>,
    /// Resource templates keyed by RFC 6570 URI template pattern.
    pub resource_templates: Arc<dashmap::DashMap<String, ResourceRegistration>>,
    /// Prompts keyed by name.
    pub prompts: Arc<dashmap::DashMap<String, PromptRegistration>>,
    /// Long-lived SSE sessions keyed by `Mcp-Session-Id`. Populated by
    /// the GET endpoint; drained by lazy cleanup when a broadcast fails.
    pub sessions: Arc<dashmap::DashMap<String, tokio::sync::mpsc::Sender<String>>>,
}

/// Dispatcher for MCP traffic.
///
/// Wires a single `POST <path>` route that parses incoming JSON-RPC requests
/// and dispatches them by method (`initialize`, `tools/list`, `tools/call`,
/// `notifications/initialized`).
pub struct McpDispatcher;

impl Dispatcher for McpDispatcher {
    type Registration = ToolRegistration;
    type Extras = McpExtras;

    fn build_router(state: DispatchState<Self::Registration, Self::Extras>) -> Router {
        let route = state.path.clone();
        Router::new()
            .route(&route, post(handle_mcp).get(handle_mcp_sse))
            .with_state(state)
    }
}

/// Constructs an MCP server with default extras (empty response registry,
/// optional global credentials, optional auth provider).
pub fn new_mcp_server(
    path: String,
    credentials: Option<super::config::Credentials>,
    auth_provider: Option<Arc<dyn flowgen_core::auth::AuthProvider>>,
    resource_uri_scheme: String,
) -> McpServer {
    let extras = McpExtras {
        response_registry: Arc::new(ResponseRegistry::new()),
        credentials: Arc::new(credentials),
        resource_uri_scheme: Arc::from(resource_uri_scheme),
        resources: Arc::new(dashmap::DashMap::new()),
        resource_templates: Arc::new(dashmap::DashMap::new()),
        prompts: Arc::new(dashmap::DashMap::new()),
        sessions: Arc::new(dashmap::DashMap::new()),
    };
    McpServer::new_with_extras(path, extras).with_auth_provider(auth_provider)
}

/// Registers a tool. Returns an error if the tool name collides with an
/// already-registered tool in the same server.
pub fn register_tool(
    server: &McpServer,
    name: String,
    registration: ToolRegistration,
) -> Result<(), Error> {
    server
        .try_register(name.clone(), registration)
        .map_err(|_| Error::ToolNameCollision { name })
}

/// Registers a concrete resource keyed by URI. Broadcasts
/// `notifications/resources/list_changed` on success.
pub fn register_resource(
    server: &McpServer,
    uri: String,
    registration: ResourceRegistration,
) -> Result<(), Error> {
    let extras = server.extras();
    // Insert-and-release the shard lock before broadcasting so notification
    // fan-out never runs while the entry lock is held.
    let inserted = match extras.resources.entry(uri.clone()) {
        dashmap::mapref::entry::Entry::Occupied(_) => false,
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(registration);
            true
        }
    };
    match inserted {
        true => {
            broadcast_notification(&extras.sessions, "notifications/resources/list_changed");
            Ok(())
        }
        false => Err(Error::ResourceUriCollision { uri }),
    }
}

/// Registers a resource template keyed by URI-template pattern.
/// Broadcasts `notifications/resources/list_changed` on success.
pub fn register_resource_template(
    server: &McpServer,
    template: String,
    registration: ResourceRegistration,
) -> Result<(), Error> {
    let extras = server.extras();
    let inserted = match extras.resource_templates.entry(template.clone()) {
        dashmap::mapref::entry::Entry::Occupied(_) => false,
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(registration);
            true
        }
    };
    match inserted {
        true => {
            broadcast_notification(&extras.sessions, "notifications/resources/list_changed");
            Ok(())
        }
        false => Err(Error::ResourceUriCollision { uri: template }),
    }
}

/// Registers a prompt keyed by name. Broadcasts
/// `notifications/prompts/list_changed` on success.
pub fn register_prompt(
    server: &McpServer,
    name: String,
    registration: PromptRegistration,
) -> Result<(), Error> {
    let extras = server.extras();
    let inserted = match extras.prompts.entry(name.clone()) {
        dashmap::mapref::entry::Entry::Occupied(_) => false,
        dashmap::mapref::entry::Entry::Vacant(entry) => {
            entry.insert(registration);
            true
        }
    };
    match inserted {
        true => {
            broadcast_notification(&extras.sessions, "notifications/prompts/list_changed");
            Ok(())
        }
        false => Err(Error::PromptNameCollision { name }),
    }
}

/// Removes every tool, resource, resource template, and prompt owned by
/// the named flow. Broadcasts a `list_changed` notification for each
/// table that lost at least one entry so subscribed MCP clients refresh
/// their cached lists.
pub fn deregister_flow_all(server: &McpServer, flow_name: &str) {
    server.deregister_flow(flow_name);
    let extras = server.extras();

    let resources_removed = retain_and_count(&extras.resources, |reg| reg.flow_name != flow_name);
    let templates_removed =
        retain_and_count(&extras.resource_templates, |reg| reg.flow_name != flow_name);
    let prompts_removed = retain_and_count(&extras.prompts, |reg| reg.flow_name != flow_name);

    if resources_removed + templates_removed > 0 {
        broadcast_notification(&extras.sessions, "notifications/resources/list_changed");
    }
    if prompts_removed > 0 {
        broadcast_notification(&extras.sessions, "notifications/prompts/list_changed");
    }
}

/// Runs `DashMap::retain` and returns the number of dropped entries.
/// Counting inside the retain closure removes a race window where a
/// concurrent insert between `len()` calls could hide a real removal.
fn retain_and_count<K, V, F>(map: &dashmap::DashMap<K, V>, mut keep: F) -> usize
where
    K: Eq + std::hash::Hash,
    F: FnMut(&V) -> bool,
{
    use std::sync::atomic::{AtomicUsize, Ordering};
    let removed = AtomicUsize::new(0);
    map.retain(|_, v| {
        let should_keep = keep(v);
        if !should_keep {
            removed.fetch_add(1, Ordering::Relaxed);
        }
        should_keep
    });
    removed.into_inner()
}

/// Returns the response registry that pipeline tasks deliver results through.
pub fn response_registry(server: &McpServer) -> &Arc<ResponseRegistry> {
    &server.extras().response_registry
}

// ---------------------------------------------------------------------------
// JSON-RPC protocol
// ---------------------------------------------------------------------------

/// JSON-RPC 2.0 request message.
#[derive(Debug, Deserialize)]
struct JsonRpcRequest {
    /// JSON-RPC version, must be "2.0".
    #[serde(default)]
    jsonrpc: Option<String>,
    /// Request identifier for response correlation.
    id: Option<serde_json::Value>,
    /// Method name (e.g., "tools/list", "tools/call", "initialize").
    method: String,
    /// Method parameters.
    #[serde(default)]
    params: serde_json::Value,
}

/// JSON-RPC 2.0 response message.
#[derive(Debug, Serialize)]
struct JsonRpcResponse {
    jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Serialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

/// Parameters for the tools/call method.
#[derive(Debug, Deserialize)]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: serde_json::Value,
}

// --- Wire result envelopes -------------------------------------------------

#[derive(Debug, Serialize)]
struct InitializeResult {
    #[serde(rename = "protocolVersion")]
    protocol_version: &'static str,
    capabilities: ServerCapabilities,
    #[serde(rename = "serverInfo")]
    server_info: ServerInfo,
}

#[derive(Debug, Serialize)]
struct ServerCapabilities {
    tools: ToolsCapability,
    resources: ResourcesCapability,
    prompts: PromptsCapability,
    completions: CompletionsCapability,
}

/// Empty object per spec — presence signals server support.
#[derive(Debug, Serialize)]
struct CompletionsCapability {}

#[derive(Debug, Serialize)]
struct ToolsCapability {
    #[serde(rename = "listChanged")]
    list_changed: bool,
}

#[derive(Debug, Serialize)]
struct ResourcesCapability {
    subscribe: bool,
    #[serde(rename = "listChanged")]
    list_changed: bool,
}

#[derive(Debug, Serialize)]
struct PromptsCapability {
    #[serde(rename = "listChanged")]
    list_changed: bool,
}

#[derive(Debug, Serialize)]
struct ServerInfo {
    name: &'static str,
    version: &'static str,
}

#[derive(Debug, Serialize)]
struct ToolListEntry<'a> {
    name: &'a str,
    description: &'a str,
    #[serde(rename = "inputSchema")]
    input_schema: &'a serde_json::Value,
}

#[derive(Debug, Serialize)]
struct ToolsListResult<'a> {
    tools: Vec<ToolListEntry<'a>>,
}

#[derive(Debug, Serialize)]
struct ResourceListEntry<'a> {
    uri: &'a str,
    name: &'a str,
    description: &'a str,
    #[serde(rename = "mimeType")]
    mime_type: &'a str,
}

#[derive(Debug, Serialize)]
struct ResourcesListResult<'a> {
    resources: Vec<ResourceListEntry<'a>>,
}

#[derive(Debug, Serialize)]
struct ResourceTemplateListEntry<'a> {
    #[serde(rename = "uriTemplate")]
    uri_template: &'a str,
    name: &'a str,
    description: &'a str,
    #[serde(rename = "mimeType")]
    mime_type: &'a str,
}

#[derive(Debug, Serialize)]
struct ResourceTemplatesListResult<'a> {
    #[serde(rename = "resourceTemplates")]
    resource_templates: Vec<ResourceTemplateListEntry<'a>>,
}

#[derive(Debug, Serialize)]
struct ResourceContent<'a> {
    uri: &'a str,
    #[serde(rename = "mimeType")]
    mime_type: &'a str,
    text: String,
}

#[derive(Debug, Serialize)]
struct ResourcesReadResult<'a> {
    contents: Vec<ResourceContent<'a>>,
}

#[derive(Debug, Serialize)]
struct PromptArgumentEntry<'a> {
    name: &'a str,
    description: &'a str,
    required: bool,
}

#[derive(Debug, Serialize)]
struct PromptListEntry<'a> {
    name: &'a str,
    description: &'a str,
    arguments: Vec<PromptArgumentEntry<'a>>,
}

#[derive(Debug, Serialize)]
struct PromptsListResult<'a> {
    prompts: Vec<PromptListEntry<'a>>,
}

#[derive(Debug, Serialize)]
struct PromptTextContent {
    #[serde(rename = "type")]
    kind: &'static str,
    text: String,
}

impl PromptTextContent {
    fn text(text: String) -> Self {
        Self { kind: "text", text }
    }
}

#[derive(Debug, Serialize)]
struct PromptMessageWire {
    role: &'static str,
    content: PromptTextContent,
}

#[derive(Debug, Serialize)]
struct PromptsGetResult<'a> {
    description: &'a str,
    messages: Vec<PromptMessageWire>,
}

#[derive(Debug, Deserialize)]
struct PromptsGetParams {
    name: String,
    #[serde(default)]
    arguments: std::collections::HashMap<String, String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum CompletionRef {
    #[serde(rename = "ref/prompt")]
    Prompt { name: String },
    #[serde(rename = "ref/resource")]
    Resource { uri: String },
}

#[derive(Debug, Deserialize)]
struct CompletionArgument {
    name: String,
    #[serde(default)]
    value: String,
}

#[derive(Debug, Deserialize)]
struct CompleteParams {
    #[serde(rename = "ref")]
    reference: CompletionRef,
    argument: CompletionArgument,
}

#[derive(Debug, Serialize)]
struct CompletionValues {
    values: Vec<String>,
    total: usize,
    #[serde(rename = "hasMore")]
    has_more: bool,
}

#[derive(Debug, Serialize)]
struct CompleteResult {
    completion: CompletionValues,
}

// ---------------------------------------------------------------------------
// Dispatcher entry point
// ---------------------------------------------------------------------------

/// HTTP header name for MCP session correlation.
const MCP_SESSION_ID_HEADER: &str = "Mcp-Session-Id";

/// JSON-RPC notification envelope (no `id`, no result).
#[derive(Debug, Serialize)]
struct JsonRpcNotification<'a> {
    jsonrpc: &'static str,
    method: &'a str,
}

/// Broadcasts a `list_changed`-style notification to every open SSE
/// session. Uses non-blocking `try_send`; sessions whose receiver has
/// been dropped are evicted, while slow clients (queue Full) are
/// preserved so a single missed tick does not close their stream.
fn broadcast_notification(
    sessions: &dashmap::DashMap<String, tokio::sync::mpsc::Sender<String>>,
    method: &str,
) {
    let notification = JsonRpcNotification {
        jsonrpc: "2.0",
        method,
    };
    let payload = match serde_json::to_string(&notification) {
        Ok(s) => s,
        Err(e) => {
            error!("Failed to serialize MCP notification: {e}");
            return;
        }
    };
    let sse_event = format!("data: {payload}\n\n");

    sessions.retain(|_id, sender| {
        matches!(
            sender.try_send(sse_event.clone()),
            Ok(()) | Err(tokio::sync::mpsc::error::TrySendError::Full(_))
        )
    });
}

/// Channel capacity for a single session's outbound SSE queue. Undersized
/// enough that a stalled client trips `try_send` and gets cleaned up
/// rather than pinning memory forever.
const SESSION_CHANNEL_CAPACITY: usize = 32;

/// Opens a long-lived SSE stream on GET so the server can push
/// `notifications/*/list_changed` between client requests. The session
/// ID is returned in the `Mcp-Session-Id` header per MCP spec.
async fn handle_mcp_sse(
    State(state): State<DispatchState<ToolRegistration, McpExtras>>,
) -> Response {
    let session_id = uuid::Uuid::now_v7().to_string();
    // UUID v7 is ASCII-only, so this conversion cannot fail. If it ever
    // does, the client has no way to route subsequent POSTs to this
    // session, so we surface the error rather than emitting a stream
    // the client cannot use.
    let session_header = match axum::http::HeaderValue::from_str(&session_id) {
        Ok(value) => value,
        Err(source) => {
            error!(error = %source, "Failed to build Mcp-Session-Id header");
            return json_rpc_error_response(
                None,
                -32603,
                Error::SessionIdEncoding { source }.to_string(),
            );
        }
    };

    let (tx, rx) = tokio::sync::mpsc::channel::<String>(SESSION_CHANNEL_CAPACITY);
    state.extras.sessions.insert(session_id, tx);

    let stream = ReceiverStream::new(rx).map(Ok::<_, std::convert::Infallible>);
    let body = Body::from_stream(stream);

    match Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .header(MCP_SESSION_ID_HEADER, session_header)
        .body(body)
    {
        Ok(response) => response,
        Err(source) => {
            error!(error = %source, "Failed to build SSE response");
            json_rpc_error_response(None, -32603, Error::SseResponseBuild { source }.to_string())
        }
    }
}

/// Main MCP endpoint handler. Dispatches based on the JSON-RPC method field.
#[tracing::instrument(skip_all, name = "mcp.request", fields(method = %request.method))]
async fn handle_mcp(
    State(state): State<DispatchState<ToolRegistration, McpExtras>>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<JsonRpcRequest>,
) -> Response {
    if let Some(ref version) = request.jsonrpc {
        if version != "2.0" {
            let err = Error::UnsupportedJsonRpcVersion {
                version: version.clone(),
            };
            return json_rpc_error_response(request.id, -32600, err.to_string());
        }
    }

    match request.method.as_str() {
        "initialize" => handle_initialize(request),
        "tools/list" => handle_tools_list(&state, &headers, request),
        "tools/call" => handle_tools_call(&state, &headers, request).await,
        "resources/list" => handle_resources_list(&state, &headers, request),
        "resources/templates/list" => handle_resource_templates_list(&state, &headers, request),
        "resources/read" => handle_resources_read(&state, &headers, request).await,
        "prompts/list" => handle_prompts_list(&state, &headers, request),
        "prompts/get" => handle_prompts_get(&state, &headers, request).await,
        "completion/complete" => handle_completion_complete(&state, &headers, request),
        "notifications/initialized" => StatusCode::ACCEPTED.into_response(),
        _ => {
            let err = Error::MethodNotFound {
                method: request.method.clone(),
            };
            json_rpc_error_response(request.id, -32601, err.to_string())
        }
    }
}

fn handle_initialize(request: JsonRpcRequest) -> Response {
    let result = InitializeResult {
        protocol_version: MCP_PROTOCOL_VERSION,
        capabilities: ServerCapabilities {
            tools: ToolsCapability {
                list_changed: false,
            },
            resources: ResourcesCapability {
                subscribe: false,
                list_changed: true,
            },
            prompts: PromptsCapability { list_changed: true },
            completions: CompletionsCapability {},
        },
        server_info: ServerInfo {
            name: "flowgen",
            version: env!("CARGO_PKG_VERSION"),
        },
    };
    json_rpc_response(request.id, result)
}

fn handle_tools_list(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let entries: Vec<_> = state
        .table
        .iter()
        .map(|entry| {
            let reg = entry.value();
            (
                entry.key().clone(),
                reg.description.clone(),
                reg.input_schema.clone(),
            )
        })
        .collect();
    let tools = entries
        .iter()
        .map(|(name, description, input_schema)| ToolListEntry {
            name,
            description,
            input_schema,
        })
        .collect();
    json_rpc_response(request.id, ToolsListResult { tools })
}

/// Parameters for `resources/read`.
#[derive(Debug, Deserialize)]
struct ResourceReadParams {
    uri: String,
}

fn handle_resources_list(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let entries: Vec<_> = state
        .extras
        .resources
        .iter()
        .map(|entry| {
            let reg = entry.value();
            (
                reg.uri.clone(),
                reg.name.clone(),
                reg.description.clone(),
                reg.mime_type.clone(),
            )
        })
        .collect();
    let resources = entries
        .iter()
        .map(|(uri, name, description, mime_type)| ResourceListEntry {
            uri,
            name,
            description,
            mime_type,
        })
        .collect();
    json_rpc_response(request.id, ResourcesListResult { resources })
}

fn handle_resource_templates_list(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let entries: Vec<_> = state
        .extras
        .resource_templates
        .iter()
        .map(|entry| {
            let reg = entry.value();
            (
                entry.key().clone(),
                reg.name.clone(),
                reg.description.clone(),
                reg.mime_type.clone(),
            )
        })
        .collect();
    let resource_templates = entries
        .iter()
        .map(
            |(uri_template, name, description, mime_type)| ResourceTemplateListEntry {
                uri_template,
                name,
                description,
                mime_type,
            },
        )
        .collect();
    json_rpc_response(
        request.id,
        ResourceTemplatesListResult { resource_templates },
    )
}

async fn handle_resources_read(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let params: ResourceReadParams = match serde_json::from_value(request.params.clone()) {
        Ok(p) => p,
        Err(source) => {
            let err = Error::InvalidParams { source };
            return json_rpc_error_response(request.id, -32602, err.to_string());
        }
    };

    // Fast path: exact URI match against the concrete-resource table.
    if let Some(entry) = state.extras.resources.get(&params.uri) {
        let reg = entry.value();
        let text = match &reg.body {
            ResourceBody::Concrete(content) => content.clone(),
            // Concrete registrations must not carry a Template body.
            ResourceBody::Template(_) => {
                return json_rpc_error_response(
                    request.id,
                    -32603,
                    Error::ConcreteBodyIsTemplate.to_string(),
                );
            }
        };
        return resource_read_response(request.id, &reg.uri, &reg.mime_type, text);
    }

    // Slow path: match against every resource-template pattern.
    let matched = state.extras.resource_templates.iter().find_map(|entry| {
        match_uri_template(entry.key(), &params.uri)
            .map(|bindings| (entry.value().clone(), bindings))
    });

    let (reg, bindings) = match matched {
        Some(pair) => pair,
        None => {
            return json_rpc_error_response(
                request.id,
                -32602,
                Error::UnknownResourceUri { uri: params.uri }.to_string(),
            );
        }
    };

    let source = match reg.body {
        ResourceBody::Template(src) => src,
        ResourceBody::Concrete(_) => {
            return json_rpc_error_response(
                request.id,
                -32603,
                Error::TemplateBodyIsConcrete.to_string(),
            );
        }
    };

    let text = match source.render(reg.resource_loader.as_ref(), &bindings).await {
        Ok(rendered) => rendered,
        Err(source) => {
            return json_rpc_error_response(
                request.id,
                -32603,
                Error::ResourceTemplateRender { source }.to_string(),
            );
        }
    };

    resource_read_response(request.id, &params.uri, &reg.mime_type, text)
}

fn resource_read_response(
    id: Option<serde_json::Value>,
    uri: &str,
    mime_type: &str,
    text: String,
) -> Response {
    let result = ResourcesReadResult {
        contents: vec![ResourceContent {
            uri,
            mime_type,
            text,
        }],
    };
    json_rpc_response(id, result)
}

fn handle_prompts_list(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let entries: Vec<_> = state
        .extras
        .prompts
        .iter()
        .map(|entry| {
            let reg = entry.value();
            (
                reg.name.clone(),
                reg.description.clone(),
                reg.arguments.clone(),
            )
        })
        .collect();
    let prompts = entries
        .iter()
        .map(|(name, description, args)| PromptListEntry {
            name,
            description,
            arguments: args
                .iter()
                .map(|a| PromptArgumentEntry {
                    name: &a.name,
                    description: &a.description,
                    required: a.required,
                })
                .collect(),
        })
        .collect();
    json_rpc_response(request.id, PromptsListResult { prompts })
}

async fn handle_prompts_get(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let params: PromptsGetParams = match serde_json::from_value(request.params.clone()) {
        Ok(p) => p,
        Err(source) => {
            let err = Error::InvalidParams { source };
            return json_rpc_error_response(request.id, -32602, err.to_string());
        }
    };

    let reg = match state.extras.prompts.get(&params.name) {
        Some(entry) => entry.value().clone(),
        None => {
            return json_rpc_error_response(
                request.id,
                -32602,
                Error::UnknownPrompt { name: params.name }.to_string(),
            );
        }
    };

    // Enforce required arguments and fold defaults into the render context.
    let mut context: std::collections::HashMap<String, String> = std::collections::HashMap::new();
    for arg in &reg.arguments {
        match params.arguments.get(&arg.name) {
            Some(value) => {
                context.insert(arg.name.clone(), value.clone());
            }
            None if arg.required => {
                return json_rpc_error_response(
                    request.id,
                    -32602,
                    Error::MissingPromptArgument {
                        name: arg.name.clone(),
                    }
                    .to_string(),
                );
            }
            None => {
                context.insert(arg.name.clone(), arg.default.clone().unwrap_or_default());
            }
        }
    }
    // Client-supplied args that the prompt does not declare pass through
    // unchanged; users may still reference them from the template.
    for (k, v) in &params.arguments {
        context.entry(k.clone()).or_insert_with(|| v.clone());
    }

    #[derive(Serialize)]
    struct RenderContext<'a> {
        arguments: &'a std::collections::HashMap<String, String>,
    }
    let render_data = RenderContext {
        arguments: &context,
    };
    let mut messages = Vec::with_capacity(reg.messages.len());
    for msg in &reg.messages {
        let rendered = match msg
            .content
            .render(reg.resource_loader.as_ref(), &render_data)
            .await
        {
            Ok(text) => text,
            Err(source) => {
                return json_rpc_error_response(
                    request.id,
                    -32603,
                    Error::PromptMessageRender { source }.to_string(),
                );
            }
        };
        messages.push(PromptMessageWire {
            role: msg.role.as_str(),
            content: PromptTextContent::text(rendered),
        });
    }

    json_rpc_response(
        request.id,
        PromptsGetResult {
            description: &reg.description,
            messages,
        },
    )
}

/// Maximum completion values returned per `completion/complete` response
/// (per MCP spec).
const COMPLETION_MAX_VALUES: usize = 100;

fn handle_completion_complete(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let params: CompleteParams = match serde_json::from_value(request.params.clone()) {
        Ok(p) => p,
        Err(source) => {
            let err = Error::InvalidParams { source };
            return json_rpc_error_response(request.id, -32602, err.to_string());
        }
    };

    let candidates: Option<Vec<String>> = match &params.reference {
        CompletionRef::Prompt { name } => state.extras.prompts.get(name).and_then(|entry| {
            entry
                .value()
                .arguments
                .iter()
                .find(|a| a.name == params.argument.name)
                .and_then(|a| a.completion_values.clone())
        }),
        CompletionRef::Resource { uri } => {
            state.extras.resource_templates.get(uri).and_then(|entry| {
                entry
                    .value()
                    .parameters
                    .iter()
                    .find(|p| p.name == params.argument.name)
                    .and_then(|p| p.completion_values.clone())
            })
        }
    };

    let needle = params.argument.value.to_lowercase();
    let filtered: Vec<String> = candidates
        .unwrap_or_default()
        .into_iter()
        .filter(|v| needle.is_empty() || v.to_lowercase().starts_with(&needle))
        .collect();
    let total = filtered.len();
    let has_more = total > COMPLETION_MAX_VALUES;
    let values = match has_more {
        true => filtered.into_iter().take(COMPLETION_MAX_VALUES).collect(),
        false => filtered,
    };

    json_rpc_response(
        request.id,
        CompleteResult {
            completion: CompletionValues {
                values,
                total,
                has_more,
            },
        },
    )
}

/// Matches a concrete URI against an RFC 6570-lite pattern using literal
/// text plus `{param}` placeholders. Returns the bound param map on match.
///
/// Only supports the subset needed today: single-segment `{name}` captures
/// that do not contain `/`.
fn match_uri_template(
    template: &str,
    uri: &str,
) -> Option<std::collections::HashMap<String, String>> {
    let mut bindings = std::collections::HashMap::new();
    let mut t = template;
    let mut u = uri;

    while !t.is_empty() {
        match t.find('{') {
            None => {
                return match t == u {
                    true => Some(bindings),
                    false => None,
                };
            }
            Some(open) => {
                let literal = &t[..open];
                if !u.starts_with(literal) {
                    return None;
                }
                u = &u[literal.len()..];
                let close = t[open..].find('}')? + open;
                let param = &t[open + 1..close];
                if param.is_empty() {
                    return None;
                }
                t = &t[close + 1..];

                // The captured value runs up to the next literal segment,
                // and must not span '/' boundaries per our subset rule.
                let (value_end, remaining_after_value) = match t.find('{') {
                    None => (u.len(), u.len()),
                    Some(next_open) => {
                        let next_literal = &t[..next_open];
                        match u[..].find(next_literal) {
                            Some(idx) => (idx, idx + next_literal.len()),
                            None => return None,
                        }
                    }
                };
                let value = &u[..value_end];
                if value.contains('/') || value.is_empty() {
                    return None;
                }
                bindings.insert(param.to_string(), value.to_string());
                u = &u[remaining_after_value..];
                if let Some(next_open) = t.find('{') {
                    t = &t[next_open..];
                } else {
                    t = "";
                }
            }
        }
    }

    match u.is_empty() {
        true => Some(bindings),
        false => None,
    }
}

async fn handle_tools_call(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    match execute_tool_call(state, headers, &request).await {
        Ok(response) => response,
        Err(e) => {
            error!(error = %e, "Tool call failed");
            let code = match &e {
                Error::InvalidParams { .. } => -32602,
                Error::UnknownTool { .. } => -32602,
                Error::Unauthorized => -32000,
                _ => -32603,
            };
            json_rpc_error_response(request.id, code, e.to_string())
        }
    }
}

async fn execute_tool_call(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: &JsonRpcRequest,
) -> Result<Response, Error> {
    let params: ToolCallParams = serde_json::from_value(request.params.clone())
        .map_err(|source| Error::InvalidParams { source })?;

    validate_auth(state, headers, Some(&params.name)).map_err(|_| Error::Unauthorized)?;

    let (tool_tx, ack_timeout, auth_required, leaf_count) = state
        .table
        .get(&params.name)
        .map(|entry| {
            (
                entry.tx.clone(),
                entry.ack_timeout,
                entry.auth_required,
                entry.leaf_count,
            )
        })
        .ok_or_else(|| Error::UnknownTool {
            name: params.name.clone(),
        })?;

    let user_context = validate_user_auth(state, headers, auth_required)
        .await
        .map_err(|_| Error::Unauthorized)?;

    let correlation_id = uuid::Uuid::now_v7().to_string();

    let (progress_tx, mut progress_rx) = mpsc::channel::<ProgressEvent>(32);

    state
        .extras
        .response_registry
        .insert(
            correlation_id.clone(),
            flowgen_core::registry::ResponseSender {
                progress_tx,
                result_tx: None,
            },
        )
        .await;

    let mut meta = serde_json::Map::new();
    meta.insert(
        flowgen_core::registry::CORRELATION_ID.to_string(),
        serde_json::Value::String(correlation_id.clone()),
    );
    if let Some(ctx) = user_context {
        if let Ok(value) = serde_json::to_value(ctx) {
            meta.insert(flowgen_core::auth::AUTH.to_string(), value);
        }
    }

    let (completion_state, completion_rx) = flowgen_core::event::new_completion_channel(leaf_count);

    let event = flowgen_core::event::EventBuilder::new()
        .data(flowgen_core::event::EventData::Json(params.arguments))
        .subject(params.name.clone())
        .task_id(0)
        .task_type("mcp_tool")
        .meta(meta)
        .completion_tx(completion_state)
        .build()
        .map_err(|source| Error::EventBuild { source })?;

    // The dedicated `MCP tool invoked` log below carries richer fields
    // than the generic event line, so the event itself goes through
    // `send_silent` to avoid two near-duplicate entries per call.
    use flowgen_core::event::EventExt;
    event
        .send_silent(Some(&tool_tx))
        .await
        .map_err(|e| Error::PipelineSend(e.to_string()))?;

    info!(
        tool = %params.name,
        correlation_id = %correlation_id,
        "MCP tool invoked"
    );

    let response_registry = Arc::clone(&state.extras.response_registry);
    let cid_for_cleanup = correlation_id.clone();
    let request_id = request.id.clone();
    let tool_name_for_span = params.name.clone();
    let correlation_id_for_span = correlation_id.clone();
    let (sse_tx, sse_rx) = mpsc::channel::<Result<String, std::convert::Infallible>>(32);

    let span = tracing::info_span!(
        "mcp.tool_execution",
        tool = %tool_name_for_span,
        correlation_id = %correlation_id_for_span,
        outcome = tracing::field::Empty,
    );

    tokio::spawn(
        async move {
            let format_progress = |evt: &ProgressEvent| -> Option<String> {
                serde_json::to_string(evt)
                    .ok()
                    .map(|data| format!("event: progress\ndata: {data}\n\n"))
            };

            tokio::pin!(completion_rx);

            type CompletionResult = Result<
                Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>,
                tokio::sync::oneshot::error::RecvError,
            >;
            let result: Option<CompletionResult> = loop {
                tokio::select! {
                    progress = progress_rx.recv() => {
                        match progress {
                            Some(evt) => {
                                if let Some(msg) = format_progress(&evt) {
                                    if sse_tx.send(Ok(msg)).await.is_err() {
                                        response_registry.remove(&cid_for_cleanup).await;
                                        return;
                                    }
                                }
                            }
                            None => break None,
                        }
                    }
                    result = async {
                        match ack_timeout {
                            Some(timeout) => {
                                match tokio::time::timeout(timeout, &mut completion_rx).await {
                                    Ok(result) => Some(result),
                                    Err(_) => {
                                        let duration = humantime::format_duration(timeout).to_string();
                                        let err = Error::ToolTimeout { duration };
                                        let tool_result = ToolResult {
                                            content: vec![Content::Text { text: err.to_string() }],
                                            is_error: true,
                                        };
                                        response_registry.remove(&cid_for_cleanup).await;
                                        let final_event = build_result_event(request_id.clone(), Some(tool_result), None);
                                        if let Some(event_str) = final_event {
                                            let _ = sse_tx.send(Ok(event_str)).await;
                                        }
                                        None
                                    }
                                }
                            }
                            None => Some((&mut completion_rx).await),
                        }
                    } => {
                        response_registry.remove(&cid_for_cleanup).await;

                        while let Ok(evt) = progress_rx.try_recv() {
                            if let Some(msg) = format_progress(&evt) {
                                let _ = sse_tx.send(Ok(msg)).await;
                            }
                        }

                        match result {
                            Some(completion) => break Some(completion),
                            None => return,
                        }
                    }
                }
            };

            let outcome = match &result {
                Some(Ok(Ok(Some(_)))) => "success",
                Some(Ok(Ok(None))) => "success_no_data",
                Some(Ok(Err(_))) => "error",
                Some(Err(_)) => "no_result",
                None => "timeout",
            };

            tracing::Span::current().record("outcome", outcome);
            info!(outcome = outcome, "MCP tool execution completed");

            if let Some(completion) = result {
                let final_event = completion_to_result_event(request_id, completion);
                if let Some(event_str) = final_event {
                    let _ = sse_tx.send(Ok(event_str)).await;
                }
            }
        }
        .instrument(span),
    );

    let stream = ReceiverStream::new(sse_rx);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .map_err(|source| Error::SseBody { source })
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/// Validates user-level authentication via the auth provider on the server.
async fn validate_user_auth(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    auth_required: bool,
) -> Result<Option<flowgen_core::auth::UserContext>, StatusCode> {
    if !auth_required {
        if let Some(provider) = &state.auth_provider {
            if let Some(auth_header) = headers
                .get(header::AUTHORIZATION)
                .and_then(|h| h.to_str().ok())
            {
                if let Some(token) = flowgen_core::auth::extract_bearer_token(auth_header) {
                    return Ok(provider.validate(token).await.ok());
                }
            }
        }
        return Ok(None);
    }

    let provider = state
        .auth_provider
        .as_ref()
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let token =
        flowgen_core::auth::extract_bearer_token(auth_header).ok_or(StatusCode::UNAUTHORIZED)?;
    provider
        .validate(token)
        .await
        .map(Some)
        .map_err(|_| StatusCode::UNAUTHORIZED)
}

/// Validates endpoint-level API key authentication for an incoming request.
fn validate_auth(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    tool_name: Option<&str>,
) -> Result<(), StatusCode> {
    let tool_creds = tool_name.and_then(|name| {
        state
            .table
            .get(name)
            .and_then(|reg| reg.credentials.clone())
    });

    let credentials = tool_creds
        .as_ref()
        .or(state.extras.credentials.as_ref().as_ref());

    let credentials = match credentials {
        Some(creds) => creds,
        None => return Ok(()),
    };

    if credentials.api_keys.is_empty() {
        return Ok(());
    }

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;

    let token =
        flowgen_core::auth::extract_bearer_token(auth_header).ok_or(StatusCode::UNAUTHORIZED)?;

    if credentials.api_keys.iter().any(|k| k == token) {
        Ok(())
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}

// ---------------------------------------------------------------------------
// Response helpers
// ---------------------------------------------------------------------------

fn json_rpc_response<T: Serialize>(id: Option<serde_json::Value>, result: T) -> Response {
    let value = match serde_json::to_value(result) {
        Ok(v) => v,
        Err(e) => {
            error!("Failed to serialize MCP result: {e}");
            return json_rpc_error_response(
                id,
                -32603,
                Error::JsonSerialize { source: e }.to_string(),
            );
        }
    };
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: Some(value),
        error: None,
    };
    axum::Json(response).into_response()
}

fn build_result_event(
    id: Option<serde_json::Value>,
    tool_result: Option<ToolResult>,
    error: Option<JsonRpcError>,
) -> Option<String> {
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: tool_result.and_then(|r| serde_json::to_value(&r).ok()),
        error,
    };
    match serde_json::to_string(&response) {
        Ok(data) => Some(format!("data: {data}\n\n")),
        Err(e) => {
            error!("Failed to serialize MCP result event: {e}");
            None
        }
    }
}

fn completion_to_result_event(
    id: Option<serde_json::Value>,
    result: Result<
        Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>,
        tokio::sync::oneshot::error::RecvError,
    >,
) -> Option<String> {
    match result {
        Ok(Ok(Some(data))) => {
            let tool_result = ToolResult {
                content: vec![Content::Text {
                    text: match serde_json::to_string_pretty(&data) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Failed to serialize result data: {e}");
                            format!("{data}")
                        }
                    },
                }],
                is_error: false,
            };
            build_result_event(id, Some(tool_result), None)
        }
        Ok(Ok(None)) => {
            let tool_result = ToolResult {
                content: vec![Content::Text {
                    text: "Tool executed successfully.".to_string(),
                }],
                is_error: false,
            };
            build_result_event(id, Some(tool_result), None)
        }
        Ok(Err(e)) => {
            let tool_result = ToolResult {
                content: vec![Content::Text {
                    text: format!("Tool execution failed: {e}"),
                }],
                is_error: true,
            };
            build_result_event(id, Some(tool_result), None)
        }
        Err(_) => {
            let err = Error::PipelineNoResult;
            build_result_event(
                id,
                None,
                Some(JsonRpcError {
                    code: -32603,
                    message: err.to_string(),
                }),
            )
        }
    }
}

fn json_rpc_error_response(id: Option<serde_json::Value>, code: i32, message: String) -> Response {
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(JsonRpcError { code, message }),
    };
    axum::Json(response).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;
    use dashmap::DashMap;
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    /// Creates an MCP server with no global credentials and no auth provider.
    fn test_server() -> McpServer {
        new_mcp_server(
            DEFAULT_MCP_PATH.to_string(),
            None,
            None,
            "flowgen".to_string(),
        )
    }

    /// Creates a ToolRegistration suitable for tests. The mpsc sender is
    /// functional but nothing reads from the receiver.
    fn test_tool_registration(flow: &str, description: &str) -> ToolRegistration {
        let (tx, _rx) = mpsc::channel(1);
        ToolRegistration {
            flow_name: flow.to_string(),
            description: description.to_string(),
            input_schema: serde_json::json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string" }
                }
            }),
            tx,
            credentials: None,
            ack_timeout: None,
            auth_required: false,
            leaf_count: 1,
        }
    }

    /// Build a DispatchState from the public API of the given McpServer, plus
    /// a fresh DashMap that we also register tools into directly.
    fn build_test_state(
        server: &McpServer,
        table: Arc<DashMap<String, ToolRegistration>>,
    ) -> DispatchState<ToolRegistration, McpExtras> {
        DispatchState {
            table,
            credentials_path: server.credentials_path().map(|p| p.to_path_buf()),
            auth_provider: server.auth_provider(),
            path: server.path().to_string(),
            extras: server.extras().clone(),
        }
    }

    /// Helper: build the router from a pre-built DispatchState and send a
    /// JSON-RPC request, returning the parsed response body.
    async fn send_jsonrpc_with_state(
        state: DispatchState<ToolRegistration, McpExtras>,
        body: serde_json::Value,
        auth_header: Option<&str>,
    ) -> serde_json::Value {
        let path = state.path.clone();
        let router = McpDispatcher::build_router(state);

        let mut req_builder = Request::builder()
            .method("POST")
            .uri(&path)
            .header("content-type", "application/json");

        if let Some(token) = auth_header {
            req_builder = req_builder.header("authorization", token);
        }

        let req = req_builder
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();

        let response = router.oneshot(req).await.unwrap();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        serde_json::from_slice(&bytes).unwrap()
    }

    // -----------------------------------------------------------------------
    // Server construction
    // -----------------------------------------------------------------------

    #[test]
    fn new_mcp_server_has_empty_tool_registry() {
        let server = test_server();
        assert_eq!(server.path(), DEFAULT_MCP_PATH);
        // Verify empty: try_register should succeed for any key.
        let reg = test_tool_registration("f", "d");
        assert!(server.try_register("probe".to_string(), reg).is_ok());
        // Clean up the probe entry.
        server.deregister_flow("f");
    }

    // -----------------------------------------------------------------------
    // Tool registration
    // -----------------------------------------------------------------------

    #[test]
    fn register_tool_succeeds_for_new_name() {
        let server = test_server();
        let reg = test_tool_registration("my_flow", "A test tool");
        let result = register_tool(&server, "my_tool".to_string(), reg);
        assert!(result.is_ok());
    }

    #[test]
    fn register_tool_returns_error_on_duplicate() {
        let server = test_server();
        let reg1 = test_tool_registration("flow_a", "First");
        let reg2 = test_tool_registration("flow_b", "Second");
        register_tool(&server, "dup_tool".to_string(), reg1).unwrap();
        let result = register_tool(&server, "dup_tool".to_string(), reg2);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("dup_tool"),
            "Error should mention the colliding tool name"
        );
    }

    #[test]
    fn response_registry_returns_shared_registry() {
        let server = test_server();
        let reg = response_registry(&server);
        // Verify it is the same Arc by cloning and checking pointer equality.
        let reg2 = response_registry(&server);
        assert!(Arc::ptr_eq(reg, reg2));
    }

    // -----------------------------------------------------------------------
    // JSON-RPC: initialize
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn handle_initialize_returns_protocol_version() {
        let server = test_server();
        let table = Arc::new(DashMap::new());
        let state = build_test_state(&server, table);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {}
        });
        let resp = send_jsonrpc_with_state(state, body, None).await;
        assert_eq!(resp["jsonrpc"], "2.0");
        assert_eq!(resp["id"], 1);
        assert_eq!(resp["result"]["protocolVersion"], MCP_PROTOCOL_VERSION);
        assert_eq!(resp["result"]["serverInfo"]["name"], "flowgen");
    }

    // -----------------------------------------------------------------------
    // JSON-RPC: tools/list
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn tools_list_returns_empty_when_no_tools() {
        let server = test_server();
        let table = Arc::new(DashMap::new());
        let state = build_test_state(&server, table);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/list",
            "params": {}
        });
        let resp = send_jsonrpc_with_state(state, body, None).await;
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert!(tools.is_empty());
    }

    #[tokio::test]
    async fn tools_list_returns_registered_tools() {
        let server = test_server();
        let table = Arc::new(DashMap::new());
        let reg = test_tool_registration("my_flow", "Search the web");
        table.insert("web_search".to_string(), reg);

        let state = build_test_state(&server, table);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/list",
            "params": {}
        });
        let resp = send_jsonrpc_with_state(state, body, None).await;
        let tools = resp["result"]["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "web_search");
        assert_eq!(tools[0]["description"], "Search the web");
        assert!(tools[0]["inputSchema"]["properties"]["query"].is_object());
    }

    // -----------------------------------------------------------------------
    // JSON-RPC: method_not_found
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn unknown_method_returns_error() {
        let server = test_server();
        let table = Arc::new(DashMap::new());
        let state = build_test_state(&server, table);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 4,
            "method": "nonexistent/method",
            "params": {}
        });
        let resp = send_jsonrpc_with_state(state, body, None).await;
        assert_eq!(resp["error"]["code"], -32601);
        assert!(resp["error"]["message"]
            .as_str()
            .unwrap()
            .contains("nonexistent/method"));
    }

    // -----------------------------------------------------------------------
    // Auth validation
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn no_credentials_allows_all_requests() {
        // Server with no global credentials — tools/list should succeed.
        let server = test_server();
        let table = Arc::new(DashMap::new());
        let state = build_test_state(&server, table);
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 5,
            "method": "tools/list",
            "params": {}
        });
        let resp = send_jsonrpc_with_state(state, body, None).await;
        assert!(resp["result"].is_object());
        assert!(resp["error"].is_null());
    }

    #[tokio::test]
    async fn correct_bearer_token_passes() {
        let creds = super::super::config::Credentials {
            api_keys: vec!["secret-key-123".to_string()],
        };
        let server = new_mcp_server(
            DEFAULT_MCP_PATH.to_string(),
            Some(creds),
            None,
            "flowgen".to_string(),
        );
        let table = Arc::new(DashMap::new());
        let reg = test_tool_registration("flow_a", "A tool");
        table.insert("tool_a".to_string(), reg);
        let state = build_test_state(&server, table);

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 6,
            "method": "tools/list",
            "params": {}
        });
        let resp = send_jsonrpc_with_state(state, body, Some("Bearer secret-key-123")).await;
        assert!(resp["result"].is_object());
        assert!(resp["error"].is_null());
    }

    #[tokio::test]
    async fn wrong_bearer_token_returns_unauthorized() {
        let creds = super::super::config::Credentials {
            api_keys: vec!["secret-key-123".to_string()],
        };
        let server = new_mcp_server(
            DEFAULT_MCP_PATH.to_string(),
            Some(creds),
            None,
            "flowgen".to_string(),
        );
        let table = Arc::new(DashMap::new());
        let state = build_test_state(&server, table);

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/list",
            "params": {}
        });
        let resp = send_jsonrpc_with_state(state, body, Some("Bearer wrong-key")).await;
        assert!(resp["error"].is_object());
    }

    // -----------------------------------------------------------------------
    // URI-template matcher
    // -----------------------------------------------------------------------

    #[test]
    fn template_matches_single_binding() {
        let bindings =
            match_uri_template("flowgen://account/{id}", "flowgen://account/001Qy0").unwrap();
        assert_eq!(bindings.get("id").map(String::as_str), Some("001Qy0"));
    }

    #[test]
    fn template_matches_two_bindings() {
        let bindings =
            match_uri_template("flowgen://{scope}/item-{id}", "flowgen://sales/item-42").unwrap();
        assert_eq!(bindings.get("scope").map(String::as_str), Some("sales"));
        assert_eq!(bindings.get("id").map(String::as_str), Some("42"));
    }

    #[test]
    fn template_rejects_slash_inside_binding() {
        // `{id}` must not span '/' — otherwise `flowgen://account/foo/bar` would
        // match `flowgen://account/{id}` with id="foo/bar", which is not the
        // intended single-segment capture.
        assert!(
            match_uri_template("flowgen://account/{id}", "flowgen://account/foo/bar").is_none()
        );
    }

    #[test]
    fn template_rejects_empty_binding() {
        assert!(match_uri_template("flowgen://account/{id}", "flowgen://account/").is_none());
    }

    #[test]
    fn template_rejects_mismatched_prefix() {
        assert!(match_uri_template("flowgen://account/{id}", "acme://account/001Qy0").is_none());
    }

    #[test]
    fn template_rejects_trailing_garbage() {
        assert!(
            match_uri_template("flowgen://account/{id}", "flowgen://account/001Qy0/extra")
                .is_none()
        );
    }

    #[test]
    fn template_no_bindings_exact_match() {
        let bindings = match_uri_template("flowgen://static", "flowgen://static").unwrap();
        assert!(bindings.is_empty());
    }

    // -----------------------------------------------------------------------
    // Notification broadcast
    // -----------------------------------------------------------------------

    fn test_prompt_registration(flow: &str) -> PromptRegistration {
        PromptRegistration {
            flow_name: flow.to_string(),
            name: "p".to_string(),
            description: "d".to_string(),
            arguments: vec![],
            messages: vec![PromptMessage {
                role: PromptRole::User,
                content: flowgen_core::resource::Source::Inline("hi".to_string()),
            }],
            resource_loader: None,
        }
    }

    #[tokio::test]
    async fn register_prompt_broadcasts_list_changed() {
        let server = test_server();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        server.extras().sessions.insert("session-1".to_string(), tx);

        register_prompt(&server, "p".to_string(), test_prompt_registration("f")).unwrap();

        let msg = rx.recv().await.expect("expected notification");
        assert!(msg.contains("notifications/prompts/list_changed"));
        assert!(msg.starts_with("data: "));
    }

    #[tokio::test]
    async fn deregister_flow_all_broadcasts_only_when_something_removed() {
        let server = test_server();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        server.extras().sessions.insert("session-1".to_string(), tx);

        // No entries registered under "ghost" flow — no notification.
        deregister_flow_all(&server, "ghost");
        assert!(rx.try_recv().is_err());

        // Register + deregister — a prompt-list-changed notification fires
        // twice: once for register, once for deregister.
        register_prompt(&server, "p".to_string(), test_prompt_registration("f")).unwrap();
        assert!(rx.recv().await.is_some());
        deregister_flow_all(&server, "f");
        assert!(rx.recv().await.is_some());
    }

    #[test]
    fn broadcast_evicts_sessions_whose_receiver_dropped() {
        let server = test_server();
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1);
        server
            .extras()
            .sessions
            .insert("dead-session".to_string(), tx);
        drop(rx);

        broadcast_notification(
            &server.extras().sessions,
            "notifications/prompts/list_changed",
        );
        assert_eq!(server.extras().sessions.len(), 0);
    }
}
