//! LLM proxy processor — OpenAI-compatible chat completions endpoint.
//!
//! Accepts OpenAI-format requests and routes them to any configured AI provider
//! via the Rig framework. Supports streaming and non-streaming responses.

use super::config;
use super::types::*;
use crate::agent::{AgentClient, ClientBuilder, CompletionChunk};
use crate::completion::processor::Credentials;
use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::MethodRouter,
};
use flowgen_core::config::ConfigExt;
use flowgen_core::credentials::HttpCredentials;
use futures_util::StreamExt;
use rig::tool::{rmcp::McpClientHandler, server::ToolServer};
use std::{fs, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, Instrument};

/// Errors that can occur during LLM proxy processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("AI client error: {source}")]
    AgentClient {
        #[source]
        source: crate::agent::Error,
    },
    #[error("Error reading credentials file at {path}: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("No authorization header provided")]
    NoCredentials,
    #[error("Invalid authorization credentials")]
    InvalidCredentials,
    #[error("Malformed authorization header")]
    MalformedCredentials,
    #[error("MCP connection failed: {message}")]
    McpConnection { message: String },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Error::NoCredentials | Error::InvalidCredentials | Error::MalformedCredentials => {
                (StatusCode::UNAUTHORIZED, self.to_string())
            }
            Error::SerdeJson { .. } => (StatusCode::BAD_REQUEST, self.to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, self.to_string()),
        };

        let body = serde_json::json!({
            "error": {
                "message": message,
                "type": "server_error",
            }
        });

        (status, axum::Json(body)).into_response()
    }
}

/// Shared state for the LLM proxy Axum handler.
#[derive(Clone)]
struct ProxyState {
    /// Pre-built AI client (reused across requests).
    client: Arc<AgentClient>,
    /// Default model from config.
    default_model: String,
    /// Default system prompt from config.
    system_prompt: Option<String>,
    /// Pre-loaded auth credentials for validating incoming requests.
    auth_credentials: Option<HttpCredentials>,
}

impl ProxyState {
    /// Validate authentication credentials against incoming request.
    fn validate_auth(&self, headers: &axum::http::HeaderMap) -> Result<(), Error> {
        let credentials = match &self.auth_credentials {
            Some(creds) => creds,
            None => return Ok(()),
        };

        let auth_header = match headers.get("authorization") {
            Some(header) => header,
            None => return Err(Error::NoCredentials),
        };

        let auth_value = match auth_header.to_str() {
            Ok(value) => value,
            Err(_) => return Err(Error::MalformedCredentials),
        };

        if let Some(expected_token) = &credentials.bearer_auth {
            match auth_value.strip_prefix("Bearer ") {
                Some(token) if token == expected_token => return Ok(()),
                Some(_) => return Err(Error::InvalidCredentials),
                None => {}
            }
        }

        Err(Error::InvalidCredentials)
    }
}

/// Handle an OpenAI-compatible chat completion request.
async fn handle_chat_completion(
    State(state): State<ProxyState>,
    headers: axum::http::HeaderMap,
    axum::Json(request): axum::Json<ChatCompletionRequest>,
) -> Result<Response, Error> {
    state.validate_auth(&headers)?;

    // Extract system prompt from messages or use default.
    let system_prompt = request
        .messages
        .iter()
        .find(|m| m.role == "system")
        .map(|m| m.content.clone())
        .or_else(|| state.system_prompt.clone());

    // Build the user prompt from non-system messages.
    let user_messages: Vec<&Message> = request
        .messages
        .iter()
        .filter(|m| m.role != "system")
        .collect();

    let prompt = user_messages
        .iter()
        .map(|m| format!("{}: {}", m.role, m.content))
        .collect::<Vec<_>>()
        .join("\n");

    let model = request
        .model
        .as_deref()
        .unwrap_or(&state.default_model)
        .to_string();

    let request_id = format!("chatcmpl-{}", uuid::Uuid::new_v4());
    let created = chrono::Utc::now().timestamp();

    if request.stream {
        handle_streaming(
            state,
            &prompt,
            system_prompt.as_deref(),
            model,
            request_id,
            created,
        )
        .await
    } else {
        handle_non_streaming(
            state,
            &prompt,
            system_prompt.as_deref(),
            model,
            request_id,
            created,
        )
        .await
    }
}

/// Handle a non-streaming chat completion request.
async fn handle_non_streaming(
    state: ProxyState,
    prompt: &str,
    system_prompt: Option<&str>,
    model: String,
    request_id: String,
    created: i64,
) -> Result<Response, Error> {
    let text = state
        .client
        .complete(prompt, system_prompt)
        .await
        .map_err(|source| Error::AgentClient { source })?;

    let response = ChatCompletionResponse {
        id: request_id,
        object: "chat.completion",
        created,
        model,
        choices: vec![Choice {
            index: 0,
            message: Message {
                role: "assistant".to_string(),
                content: text,
            },
            finish_reason: "stop",
        }],
        usage: None,
    };

    Ok(axum::Json(response).into_response())
}

/// Handle a streaming chat completion request with SSE.
async fn handle_streaming(
    state: ProxyState,
    prompt: &str,
    system_prompt: Option<&str>,
    model: String,
    request_id: String,
    created: i64,
) -> Result<Response, Error> {
    let mut stream = state.client.complete_stream(prompt, system_prompt).await;
    let (sse_tx, sse_rx) = mpsc::channel::<Result<String, std::convert::Infallible>>(32);

    // Send initial role chunk.
    let initial_chunk = ChatCompletionChunk {
        id: request_id.clone(),
        object: "chat.completion.chunk",
        created,
        model: model.clone(),
        choices: vec![StreamChoice {
            index: 0,
            delta: Delta {
                role: Some("assistant".to_string()),
                content: None,
            },
            finish_reason: None,
        }],
    };

    let _ = sse_tx
        .send(Ok(format!(
            "data: {}\n\n",
            serde_json::to_string(&initial_chunk).unwrap_or_default()
        )))
        .await;

    tokio::spawn(async move {
        while let Some(chunk) = stream.next().await {
            let (content, finish) = match chunk {
                CompletionChunk::Text(text) => (Some(text), None),
                CompletionChunk::Final(_) => (None, Some("stop")),
                CompletionChunk::Error(e) => {
                    error!(error = %e, "LLM proxy stream error.");
                    break;
                }
            };

            let sse_chunk = ChatCompletionChunk {
                id: request_id.clone(),
                object: "chat.completion.chunk",
                created,
                model: model.clone(),
                choices: vec![StreamChoice {
                    index: 0,
                    delta: Delta {
                        role: None,
                        content,
                    },
                    finish_reason: finish,
                }],
            };

            let data = format!(
                "data: {}\n\n",
                serde_json::to_string(&sse_chunk).unwrap_or_default()
            );

            if sse_tx.send(Ok(data)).await.is_err() {
                break;
            }

            if finish.is_some() {
                break;
            }
        }

        // Send [DONE] marker per OpenAI spec.
        let _ = sse_tx.send(Ok("data: [DONE]\n\n".to_string())).await;
    });

    let stream = ReceiverStream::new(sse_rx);

    Ok(Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .unwrap())
}

/// LLM proxy processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<config::Processor>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = ();

    async fn init(&self) -> Result<(), Error> {
        Ok(())
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        // Load provider credentials.
        let credentials = match &init_config.credentials_path {
            Some(path) => {
                let content = fs::read_to_string(path).map_err(|e| Error::ReadCredentials {
                    path: path.clone(),
                    source: e,
                })?;
                Some(
                    serde_json::from_str::<Credentials>(&content)
                        .map_err(|source| Error::SerdeJson { source })?,
                )
            }
            None => None,
        };

        // Build the AI client.
        let mut builder =
            ClientBuilder::new(init_config.provider.clone(), init_config.model.clone());

        if let Some(creds) = credentials {
            builder = builder.credentials(creds);
        }

        if let Some(endpoint) = &init_config.endpoint {
            builder = builder.endpoint(endpoint.clone());
        }

        if let Some(temp) = init_config.temperature {
            builder = builder.temperature(temp);
        }

        if let Some(tokens) = init_config.max_tokens {
            builder = builder.max_tokens(tokens);
        }

        if let Some(turns) = init_config.max_turns {
            builder = builder.max_turns(turns);
        }

        builder = builder.name(init_config.name.clone());

        // Connect to MCP servers for tool discovery.
        if !init_config.mcp_servers.is_empty() {
            let tool_server_handle = ToolServer::new().run();
            let client_info = rmcp::model::ClientInfo::new(
                rmcp::model::ClientCapabilities::default(),
                rmcp::model::Implementation::from_build_env(),
            );

            for mcp_config in &init_config.mcp_servers {
                let handler =
                    McpClientHandler::new(client_info.clone(), tool_server_handle.clone());
                let mut transport_config =
                    rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(
                        mcp_config.url.as_str(),
                    );

                if let Some(ref creds_path) = mcp_config.credentials_path {
                    let creds = flowgen_core::credentials::load_http_credentials(creds_path)
                        .await
                        .map_err(|e| Error::McpConnection {
                            message: e.to_string(),
                        })?;
                    if let Some(bearer_token) = creds.bearer_auth {
                        transport_config = transport_config.auth_header(bearer_token);
                    }
                }

                let transport =
                    rmcp::transport::StreamableHttpClientTransport::from_config(transport_config);
                handler
                    .connect(transport)
                    .await
                    .map_err(|e| Error::McpConnection {
                        message: e.to_string(),
                    })?;

                info!(
                    url = %mcp_config.url,
                    "Connected to MCP server for LLM proxy tool discovery."
                );
            }

            builder = builder.tool_server_handle(tool_server_handle);
        }

        let client = Arc::new(
            builder
                .build()
                .map_err(|source| Error::AgentClient { source })?,
        );

        // Load auth credentials for incoming request validation.
        let auth_credentials = match &init_config.auth_credentials_path {
            Some(path) => {
                let content = fs::read_to_string(path).map_err(|e| Error::ReadCredentials {
                    path: path.clone(),
                    source: e,
                })?;
                Some(
                    serde_json::from_str::<HttpCredentials>(&content)
                        .map_err(|source| Error::SerdeJson { source })?,
                )
            }
            None => None,
        };

        let state = ProxyState {
            client,
            default_model: init_config.model.clone(),
            system_prompt: init_config.system_prompt.clone(),
            auth_credentials,
        };

        let span = tracing::Span::current();
        let handler = move |state: State<ProxyState>,
                            headers: axum::http::HeaderMap,
                            body: axum::Json<ChatCompletionRequest>| {
            let span = span.clone();
            async move { handle_chat_completion(state, headers, body).await }.instrument(span)
        };

        let method_router: MethodRouter = MethodRouter::new().post(handler).with_state(state);

        if let Some(http_server) = &self.task_context.http_server {
            http_server
                .register_route(init_config.path.clone(), Box::new(method_router))
                .await;

            info!(
                path = %init_config.path,
                provider = ?init_config.provider,
                model = %init_config.model,
                "LLM proxy endpoint registered."
            );
        }

        Ok(())
    }
}

/// Builder for LLM proxy processor.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<config::Processor>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}
