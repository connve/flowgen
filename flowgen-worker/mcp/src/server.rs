//! MCP server implementation with tool registry and Axum handlers.
//!
//! Implements the MCP Streamable HTTP transport: a single POST endpoint that
//! accepts JSON-RPC 2.0 messages and responds with either JSON or SSE streams.

use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use dashmap::DashMap;
use flowgen_core::mcp::registry::{McpContent, McpProgressEvent, McpToolResult, ResponseRegistry};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, Instrument};

/// Errors that can occur during MCP server operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("MCP server bind error on port {port}: {source}")]
    BindListener {
        port: u16,
        #[source]
        source: std::io::Error,
    },
    #[error("MCP server HTTP error: {source}")]
    ServeHttp {
        #[source]
        source: std::io::Error,
    },
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
}

/// Registration entry for an MCP tool.
#[derive(Debug)]
pub struct ToolRegistration {
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
}

/// MCP server with tool registry and response tracking.
#[derive(Clone)]
pub struct McpServer {
    /// Registry of available MCP tools.
    tool_registry: Arc<DashMap<String, ToolRegistration>>,
    /// Registry for correlating requests with pipeline results.
    response_registry: Arc<ResponseRegistry>,
    /// Optional global credentials from worker configuration.
    global_credentials: Arc<Option<super::config::Credentials>>,
}

impl std::fmt::Debug for McpServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("McpServer")
            .field("tool_count", &self.tool_registry.len())
            .finish()
    }
}

impl flowgen_core::mcp_server::McpServer for McpServer {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl McpServer {
    /// Creates a new MCP server with optional global credentials.
    pub fn new(global_credentials: Option<super::config::Credentials>) -> Self {
        Self {
            tool_registry: Arc::new(DashMap::new()),
            response_registry: Arc::new(ResponseRegistry::new()),
            global_credentials: Arc::new(global_credentials),
        }
    }

    /// Registers a tool in the MCP server.
    ///
    /// Returns an error if a tool with the same name is already registered.
    /// Use distinct tool names across flows to avoid collisions.
    pub fn register_tool(&self, name: String, registration: ToolRegistration) -> Result<(), Error> {
        if self.tool_registry.contains_key(&name) {
            return Err(Error::ToolNameCollision { name });
        }
        info!("Registering MCP tool: {}", name);
        self.tool_registry.insert(name, registration);
        Ok(())
    }

    /// Returns the response registry for pipeline tasks to deliver results.
    pub fn response_registry(&self) -> &Arc<ResponseRegistry> {
        &self.response_registry
    }

    /// Default MCP server port.
    const DEFAULT_PORT: u16 = 3001;

    /// Default MCP endpoint path.
    const DEFAULT_PATH: &str = "/mcp";

    /// Starts the MCP server on its own port with the configured endpoint path.
    pub async fn start_server(&self, port: Option<u16>, path: Option<String>) -> Result<(), Error> {
        let server_port = port.unwrap_or(Self::DEFAULT_PORT);
        let mcp_path = path.unwrap_or_else(|| Self::DEFAULT_PATH.to_string());

        let router = axum::Router::new().route(
            &mcp_path,
            axum::routing::post(handle_mcp).with_state(self.clone()),
        );

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{server_port}"))
            .await
            .map_err(|e| Error::BindListener {
                port: server_port,
                source: e,
            })?;

        info!("Starting MCP server on port {server_port} at {mcp_path}.");
        axum::serve(listener, router)
            .await
            .map_err(|e| Error::ServeHttp { source: e })
    }

    /// Validates the API key from the Authorization header.
    ///
    /// Checks tool-specific credentials first, then falls back to global credentials.
    /// If neither is configured, allows all requests.
    fn validate_auth(
        &self,
        headers: &HeaderMap,
        tool_name: Option<&str>,
    ) -> Result<(), StatusCode> {
        // Resolve tool-specific credentials if a tool name is provided.
        let tool_creds = tool_name.and_then(|name| {
            self.tool_registry
                .get(name)
                .and_then(|reg| reg.credentials.clone())
        });

        // Use tool-specific credentials, falling back to global.
        let credentials = tool_creds
            .as_ref()
            .or(self.global_credentials.as_ref().as_ref());

        let credentials = match credentials {
            Some(creds) => creds,
            None => return Ok(()),
        };

        if credentials.api_keys.is_empty() {
            return Ok(());
        }

        let auth_header = headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .ok_or(StatusCode::UNAUTHORIZED)?;

        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or(StatusCode::UNAUTHORIZED)?;

        if credentials.api_keys.iter().any(|k| k == token) {
            Ok(())
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}

// --- MCP JSON-RPC Protocol Types ---

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
    /// JSON-RPC version, always "2.0".
    jsonrpc: &'static str,
    /// Response identifier matching the request.
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<serde_json::Value>,
    /// Successful result payload.
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<serde_json::Value>,
    /// Error payload for failed requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

/// JSON-RPC 2.0 error object.
#[derive(Debug, Serialize)]
struct JsonRpcError {
    /// Error code following JSON-RPC conventions.
    code: i32,
    /// Human-readable error description.
    message: String,
}

/// Parameters for the tools/call method.
#[derive(Debug, Deserialize)]
struct ToolCallParams {
    /// Name of the tool to invoke.
    name: String,
    /// Tool input arguments as a JSON object.
    #[serde(default)]
    arguments: serde_json::Value,
}

/// MCP protocol version supported by this server.
const MCP_PROTOCOL_VERSION: &str = "2025-03-26";

// --- Axum Handler ---

/// Main MCP endpoint handler. Dispatches based on the JSON-RPC method field.
#[tracing::instrument(skip_all, name = "mcp.request", fields(method = %request.method))]
async fn handle_mcp(
    State(server): State<McpServer>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<JsonRpcRequest>,
) -> Response {
    // Validate JSON-RPC version if provided.
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
        "tools/list" => handle_tools_list(&server, &headers, request),
        "tools/call" => handle_tools_call(&server, &headers, request).await,
        "notifications/initialized" => {
            // Client acknowledgment after initialize, no response needed.
            StatusCode::OK.into_response()
        }
        _ => {
            let err = Error::MethodNotFound {
                method: request.method.clone(),
            };
            json_rpc_error_response(request.id, -32601, err.to_string())
        }
    }
}

/// Handles the `initialize` method by returning server capabilities.
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

/// Handles the `tools/list` method by returning all registered tools.
fn handle_tools_list(server: &McpServer, headers: &HeaderMap, request: JsonRpcRequest) -> Response {
    if let Err(status) = server.validate_auth(headers, None) {
        return json_rpc_error_response(request.id, -32000, status.to_string());
    }

    let tools: Vec<serde_json::Value> = server
        .tool_registry
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

/// Handles the `tools/call` method by invoking a tool and streaming results via SSE.
async fn handle_tools_call(
    server: &McpServer,
    headers: &HeaderMap,
    request: JsonRpcRequest,
) -> Response {
    match execute_tool_call(server, headers, &request).await {
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

/// Executes a tool call, returning an SSE streaming response on success.
async fn execute_tool_call(
    server: &McpServer,
    headers: &HeaderMap,
    request: &JsonRpcRequest,
) -> Result<Response, Error> {
    let params: ToolCallParams = serde_json::from_value(request.params.clone())
        .map_err(|source| Error::InvalidParams { source })?;

    server
        .validate_auth(headers, Some(&params.name))
        .map_err(|_| Error::Unauthorized)?;

    // Look up tool in registry and extract sender and timeout.
    let (tool_tx, ack_timeout) = server
        .tool_registry
        .get(&params.name)
        .map(|entry| (entry.tx.clone(), entry.ack_timeout))
        .ok_or_else(|| Error::UnknownTool {
            name: params.name.clone(),
        })?;

    // Generate a unique correlation identifier for this request.
    let correlation_id = uuid::Uuid::new_v4().to_string();

    // Create progress channel for streaming intermediate updates to the client.
    let (progress_tx, mut progress_rx) = mpsc::channel::<McpProgressEvent>(32);

    // Register in the response registry so pipeline tasks can send progress events.
    server
        .response_registry
        .insert(
            correlation_id.clone(),
            flowgen_core::mcp::registry::ResponseSender {
                progress_tx,
                result_tx: None,
            },
        )
        .await;

    // Build event with correlation_id in meta and inject into the pipeline.
    let mut meta = serde_json::Map::new();
    meta.insert(
        "correlation_id".to_string(),
        serde_json::Value::String(correlation_id.clone()),
    );

    // Create completion channel to receive the pipeline result.
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel::<
        Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>,
    >();

    let event = flowgen_core::event::EventBuilder::new()
        .data(flowgen_core::event::EventData::Json(params.arguments))
        .subject(params.name.clone())
        .task_id(0)
        .task_type("mcp_tool")
        .meta(meta)
        .completion_tx(completion_tx)
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

    // Spawn a background task that streams progress and then delivers the final result.
    let response_registry = server.response_registry.clone();
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

    tokio::spawn(async move {
        let start = std::time::Instant::now();
        // Formats a progress event as an SSE string.
        let format_progress = |evt: &McpProgressEvent| -> Option<String> {
            serde_json::to_string(evt)
                .ok()
                .map(|data| format!("event: progress\ndata: {data}\n\n"))
        };

        // Stream progress events concurrently with waiting for pipeline completion.
        tokio::pin!(completion_rx);

        // Wait for pipeline completion, streaming progress concurrently.
        // Result is None if timeout fired (already handled), Some if completion arrived.
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
                                    // Timeout expired.
                                    let duration = humantime::format_duration(timeout).to_string();
                                    let err = Error::ToolTimeout { duration };
                                    let tool_result = McpToolResult {
                                        content: vec![McpContent::Text { text: err.to_string() }],
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
                    // Clean up registry.
                    response_registry.remove(&cid_for_cleanup).await;

                    // Drain remaining progress events.
                    while let Ok(evt) = progress_rx.try_recv() {
                        if let Some(msg) = format_progress(&evt) {
                            let _ = sse_tx.send(Ok(msg)).await;
                        }
                    }

                    match result {
                        Some(completion) => break Some(completion),
                        None => return, // Timeout already handled above.
                    }
                }
            }
        };

        // Send the final result and record outcome.
        let elapsed = start.elapsed();
        let outcome = match &result {
            Some(Ok(Ok(Some(_)))) => "success",
            Some(Ok(Ok(None))) => "success_no_data",
            Some(Ok(Err(_))) => "error",
            Some(Err(_)) => "no_result",
            None => "timeout",
        };

        tracing::Span::current().record("outcome", outcome);
        info!(
            duration_ms = elapsed.as_millis() as u64,
            outcome = outcome,
            "MCP tool execution completed."
        );

        if let Some(completion) = result {
            let final_event = completion_to_result_event(request_id, completion);
            if let Some(event_str) = final_event {
                let _ = sse_tx.send(Ok(event_str)).await;
            }
        }
    }.instrument(span));

    let stream = ReceiverStream::new(sse_rx);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .map_err(|e| {
            error!("Failed to build SSE response: {e}");
            Error::ServeHttp {
                source: std::io::Error::other(e.to_string()),
            }
        })
}

/// Builds a JSON-RPC success response.
fn json_rpc_response(id: Option<serde_json::Value>, result: serde_json::Value) -> Response {
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: Some(result),
        error: None,
    };
    axum::Json(response).into_response()
}

/// Builds an SSE result event string from a tool result or error.
fn build_result_event(
    id: Option<serde_json::Value>,
    tool_result: Option<McpToolResult>,
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

/// Converts a pipeline completion result into an SSE result event string.
fn completion_to_result_event(
    id: Option<serde_json::Value>,
    result: Result<
        Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>,
        tokio::sync::oneshot::error::RecvError,
    >,
) -> Option<String> {
    match result {
        Ok(Ok(Some(data))) => {
            let tool_result = McpToolResult {
                content: vec![McpContent::Text {
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
            let tool_result = McpToolResult {
                content: vec![McpContent::Text {
                    text: "Tool executed successfully.".to_string(),
                }],
                is_error: false,
            };
            build_result_event(id, Some(tool_result), None)
        }
        Ok(Err(e)) => {
            let tool_result = McpToolResult {
                content: vec![McpContent::Text {
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

/// Builds a JSON-RPC error response.
fn json_rpc_error_response(id: Option<serde_json::Value>, code: i32, message: String) -> Response {
    let response = JsonRpcResponse {
        jsonrpc: "2.0",
        id,
        result: None,
        error: Some(JsonRpcError { code, message }),
    };
    axum::Json(response).into_response()
}
