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
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, Instrument};

/// Default MCP server port.
pub const DEFAULT_MCP_PORT: u16 = 3001;

/// Default MCP endpoint path.
pub const DEFAULT_MCP_PATH: &str = "/mcp";

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

/// MCP-specific shared state passed to the dispatcher via `DispatchState::extras`.
#[derive(Clone)]
pub struct McpExtras {
    /// Registry for correlating requests with pipeline results / progress.
    pub response_registry: Arc<ResponseRegistry>,
    /// Optional global credentials shared across tools.
    pub credentials: Arc<Option<super::config::Credentials>>,
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
            .route(&route, post(handle_mcp))
            .with_state(state)
    }
}

/// Constructs an MCP server with default extras (empty response registry,
/// optional global credentials, optional auth provider).
pub fn new_mcp_server(
    path: String,
    credentials: Option<super::config::Credentials>,
    auth_provider: Option<Arc<dyn flowgen_core::auth::AuthProvider>>,
) -> McpServer {
    let extras = McpExtras {
        response_registry: Arc::new(ResponseRegistry::new()),
        credentials: Arc::new(credentials),
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

// ---------------------------------------------------------------------------
// Dispatcher entry point
// ---------------------------------------------------------------------------

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
        "notifications/initialized" => StatusCode::OK.into_response(),
        _ => {
            let err = Error::MethodNotFound {
                method: request.method.clone(),
            };
            json_rpc_error_response(request.id, -32601, err.to_string())
        }
    }
}

fn handle_initialize(request: JsonRpcRequest) -> Response {
    json_rpc_response(
        request.id,
        serde_json::json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": {
                "tools": {
                    "listChanged": false
                }
            },
            "serverInfo": {
                "name": "flowgen",
                "version": env!("CARGO_PKG_VERSION")
            }
        }),
    )
}

fn handle_tools_list(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    if let Err(status) = validate_auth(state, headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let tools: Vec<serde_json::Value> = state
        .table
        .iter()
        .map(|entry| {
            serde_json::json!({
                "name": entry.key(),
                "description": entry.value().description,
                "inputSchema": entry.value().input_schema,
            })
        })
        .collect();

    json_rpc_response(request.id, serde_json::json!({ "tools": tools }))
}

async fn handle_tools_call(
    state: &DispatchState<ToolRegistration, McpExtras>,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    match execute_tool_call(state, headers, &request).await {
        Ok(response) => response,
        Err(e) => {
            error!(error = %e, "Tool call failed.");
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

    tool_tx
        .send(event)
        .await
        .map_err(|e| Error::PipelineSend(e.to_string()))?;

    info!(
        tool = %params.name,
        correlation_id = %correlation_id,
        "MCP tool invoked."
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
            info!(outcome = outcome, "MCP tool execution completed.");

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

fn json_rpc_response(id: Option<serde_json::Value>, result: serde_json::Value) -> Response {
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: Some(result),
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
        Ok(data) => Some(format!("event: message\ndata: {data}\n\n")),
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
