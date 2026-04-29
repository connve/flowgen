//! AI gateway processor — OpenAI-compatible chat completions endpoint.
//!
//! Thin HTTP layer that accepts OpenAI-format requests, translates them into
//! pipeline events, and formats responses back to OpenAI format. The actual
//! LLM call is handled by a downstream `ai_completion` task in the flow.
//! Registers at the exact path (no prefix nesting) for OpenAI client compatibility.

use super::config::{
    self, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Message, SSE_DONE,
};
use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::MethodRouter,
};
use base64::Engine;
use flowgen_core::auth::{extract_bearer_token, AuthProvider};
use flowgen_core::config::ConfigExt;
use flowgen_core::credentials::HttpCredentials;
use flowgen_core::event::{new_completion_channel, Event, EventBuilder, EventData, EventExt};
use flowgen_core::registry::{ProgressEvent, ResponseRegistry, ResponseSender};
use std::{fs, sync::Arc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info, Instrument};

/// Errors that can occur during AI gateway processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Error reading credentials file at {path}: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("No authorization header provided.")]
    NoCredentials,
    #[error("Invalid authorization credentials.")]
    InvalidCredentials,
    #[error("Malformed authorization header.")]
    MalformedCredentials,
    #[error("User authentication failed: {source}")]
    AuthFailed {
        #[source]
        source: flowgen_core::auth::AuthError,
    },
    #[error("Auth provider not configured but auth.required is true.")]
    AuthProviderMissing,
    #[error("Flow completion failed or timed out.")]
    FlowCompletionFailed,
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, message) = match &self {
            Error::NoCredentials | Error::InvalidCredentials | Error::MalformedCredentials => {
                (StatusCode::UNAUTHORIZED, self.to_string())
            }
            Error::AuthFailed { .. } | Error::AuthProviderMissing => {
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

/// Shared state for the AI gateway Axum handler.
#[derive(Clone)]
struct ProxyState {
    /// Cancellation token for graceful shutdown.
    cancellation_token: tokio_util::sync::CancellationToken,
    /// Processor configuration.
    config: Arc<config::Processor>,
    /// Event sender channel into the pipeline.
    tx: Option<mpsc::Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Task type for event categorization.
    task_type: &'static str,
    /// Pre-loaded endpoint credentials for basic/bearer auth.
    credentials: Option<HttpCredentials>,
    /// Auth provider for user identity resolution (from worker config).
    auth_provider: Option<Arc<dyn AuthProvider>>,
    /// Response registry for streaming responses.
    response_registry: Arc<ResponseRegistry>,
    /// Number of leaf tasks reachable from this gateway. Used to size the
    /// per-request completion channel so the response is delivered only
    /// after every leaf has signalled.
    leaf_count: usize,
}

impl ProxyState {
    /// Validate endpoint-level authentication (bearer/basic).
    fn validate_endpoint_auth(&self, headers: &axum::http::HeaderMap) -> Result<(), Error> {
        let credentials = match &self.credentials {
            Some(creds) => creds,
            None => return Ok(()),
        };

        let auth_header = match headers.get(axum::http::header::AUTHORIZATION) {
            Some(header) => header,
            None => return Err(Error::NoCredentials),
        };

        let auth_value = match auth_header.to_str() {
            Ok(value) => value,
            Err(_) => return Err(Error::MalformedCredentials),
        };

        if let Some(expected_token) = &credentials.bearer_auth {
            match flowgen_core::auth::extract_bearer_token(auth_value) {
                Some(token) if token == expected_token => return Ok(()),
                Some(_) => return Err(Error::InvalidCredentials),
                None => {}
            }
        }

        if let Some(basic_auth) = &credentials.basic_auth {
            if let Some(encoded) = auth_value.strip_prefix("Basic ") {
                match base64::engine::general_purpose::STANDARD.decode(encoded) {
                    Ok(decoded_bytes) => match String::from_utf8(decoded_bytes) {
                        Ok(decoded_str) => {
                            let expected =
                                format!("{}:{}", basic_auth.username, basic_auth.password);
                            return match decoded_str == expected {
                                true => Ok(()),
                                false => Err(Error::InvalidCredentials),
                            };
                        }
                        Err(_) => return Err(Error::MalformedCredentials),
                    },
                    Err(_) => return Err(Error::MalformedCredentials),
                }
            }
        }

        Err(Error::InvalidCredentials)
    }

    /// Validate user-level authentication via the worker auth provider.
    async fn validate_user_auth(
        &self,
        headers: &axum::http::HeaderMap,
    ) -> Result<Option<flowgen_core::auth::UserContext>, Error> {
        match &self.config.auth {
            Some(config) if config.required => {}
            _ => return Ok(None),
        }

        let provider = self
            .auth_provider
            .as_ref()
            .ok_or(Error::AuthProviderMissing)?;

        let auth_header = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|h| h.to_str().ok())
            .ok_or(Error::NoCredentials)?;

        let token = extract_bearer_token(auth_header).ok_or(Error::MalformedCredentials)?;

        let user_context = provider
            .validate(token)
            .await
            .map_err(|source| Error::AuthFailed { source })?;

        Ok(Some(user_context))
    }
}

/// Handle an OpenAI-compatible chat completion request.
async fn handle_chat_completion(
    State(state): State<ProxyState>,
    headers: axum::http::HeaderMap,
    axum::Json(request): axum::Json<ChatCompletionRequest>,
) -> Result<Response, Error> {
    if state.cancellation_token.is_cancelled() {
        return Ok(StatusCode::SERVICE_UNAVAILABLE.into_response());
    }

    state.validate_endpoint_auth(&headers)?;

    // User-level auth (JWT/OIDC/session via worker auth provider).
    let user_context = state.validate_user_auth(&headers).await?;

    // Extract prompt from OpenAI messages.
    let system_prompt = request
        .messages
        .iter()
        .find(|m| m.is_system())
        .map(|m| m.content.clone());

    let user_messages: Vec<&Message> = request.messages.iter().filter(|m| !m.is_system()).collect();

    let prompt = user_messages
        .iter()
        .map(|m| m.content.as_str())
        .collect::<Vec<_>>()
        .join("\n");

    let model = request.model.unwrap_or_default();
    let request_id = format!("chatcmpl-{}", uuid::Uuid::now_v7());
    let created = chrono::Utc::now().timestamp();
    let is_stream = request.stream;

    // Build event data with the parsed request.
    let data = serde_json::json!({
        "prompt": prompt,
        "system_prompt": system_prompt,
        "model": model,
        "temperature": request.temperature,
        "max_tokens": request.max_tokens,
        "stream": is_stream,
    });

    // Inject user context into event meta if authenticated.
    let mut meta = serde_json::Map::new();
    if let Some(ref ctx) = user_context {
        if let Ok(value) = serde_json::to_value(ctx) {
            meta.insert(flowgen_core::auth::AUTH.to_string(), value);
        }
    }

    if is_stream {
        handle_streaming(state, data, meta, model, request_id, created).await
    } else {
        handle_non_streaming(state, data, meta, model, request_id, created).await
    }
}

/// Handle a non-streaming chat completion request.
async fn handle_non_streaming(
    state: ProxyState,
    data: serde_json::Value,
    meta: serde_json::Map<String, serde_json::Value>,
    model: String,
    request_id: String,
    created: i64,
) -> Result<Response, Error> {
    let (completion_state, completion_rx) = new_completion_channel(state.leaf_count);

    let mut builder = EventBuilder::new()
        .data(EventData::Json(data))
        .subject(state.config.name.to_owned())
        .task_id(state.task_id)
        .task_type(state.task_type)
        .completion_tx(completion_state);

    if !meta.is_empty() {
        builder = builder.meta(meta);
    }

    let e = builder
        .build()
        .map_err(|source| Error::EventBuilder { source })?;

    e.send_with_logging(state.tx.as_ref())
        .await
        .map_err(|source| Error::SendMessage { source })?;

    // Wait for pipeline completion.
    let result = match state.config.ack_timeout {
        Some(timeout) => tokio::time::timeout(timeout, completion_rx)
            .await
            .map_err(|_| Error::FlowCompletionFailed)?
            .map_err(|_| Error::FlowCompletionFailed)?,
        None => completion_rx
            .await
            .map_err(|_| Error::FlowCompletionFailed)?,
    };

    let completion_data = result.map_err(|_| Error::FlowCompletionFailed)?;

    let content = completion_data
        .and_then(|v| v.get("content").and_then(|c| c.as_str()).map(String::from))
        .unwrap_or_default();

    let response = ChatCompletionResponse::new(request_id, created, model, content);

    Ok(axum::Json(response).into_response())
}

/// Handle a streaming chat completion request with SSE.
async fn handle_streaming(
    state: ProxyState,
    data: serde_json::Value,
    meta: serde_json::Map<String, serde_json::Value>,
    model: String,
    request_id: String,
    created: i64,
) -> Result<Response, Error> {
    let correlation_id = uuid::Uuid::now_v7().to_string();

    // Set up progress channel for streaming intermediate events.
    let (progress_tx, mut progress_rx) = mpsc::channel::<ProgressEvent>(32);

    state
        .response_registry
        .insert(
            correlation_id.clone(),
            ResponseSender {
                progress_tx,
                result_tx: None,
            },
        )
        .await;

    // Inject correlation_id into event meta.
    let mut meta = meta;
    meta.insert(
        flowgen_core::registry::CORRELATION_ID.to_string(),
        serde_json::Value::String(correlation_id.clone()),
    );

    let (completion_state_tx, completion_rx) = new_completion_channel(state.leaf_count);

    let e = EventBuilder::new()
        .data(EventData::Json(data))
        .subject(state.config.name.to_owned())
        .task_id(state.task_id)
        .task_type(state.task_type)
        .meta(meta)
        .completion_tx(completion_state_tx)
        .build()
        .map_err(|source| Error::EventBuilder { source })?;

    e.send_with_logging(state.tx.as_ref())
        .await
        .map_err(|source| Error::SendMessage { source })?;

    info!(
        proxy = %state.config.name,
        correlation_id = %correlation_id,
        "Streaming AI gateway request accepted."
    );

    // Spawn background task to stream progress + final result as OpenAI SSE.
    let registry = state.response_registry.clone();
    let cid = correlation_id.clone();
    let ack_timeout = state.config.ack_timeout;
    let (sse_tx, sse_rx) = mpsc::channel::<Result<String, std::convert::Infallible>>(32);

    tokio::spawn(async move {
        /// Send an SSE chunk to the client, logging and skipping on serialization failure.
        async fn send_sse(
            tx: &mpsc::Sender<Result<String, std::convert::Infallible>>,
            chunk: &ChatCompletionChunk,
        ) -> bool {
            match chunk.to_sse() {
                Ok(data) => tx.send(Ok(data)).await.is_ok(),
                Err(e) => {
                    tracing::error!(error = %e, "Failed to serialize SSE chunk.");
                    true // Continue streaming, skip this chunk.
                }
            }
        }

        if !send_sse(
            &sse_tx,
            &ChatCompletionChunk::role(&request_id, created, &model),
        )
        .await
        {
            return;
        }

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
                            let chunk = ChatCompletionChunk::content(
                                &request_id, created, &model, evt.status.clone(),
                            );
                            if !send_sse(&sse_tx, &chunk).await {
                                registry.remove(&cid).await;
                                return;
                            }
                        }
                        None => break None,
                    }
                }
                completion = async {
                    match ack_timeout {
                        Some(timeout) => {
                            match tokio::time::timeout(timeout, &mut completion_rx).await {
                                Ok(r) => Some(r),
                                Err(_) => { registry.remove(&cid).await; None }
                            }
                        }
                        None => Some((&mut completion_rx).await),
                    }
                } => {
                    registry.remove(&cid).await;

                    // Drain remaining progress events.
                    while let Ok(evt) = progress_rx.try_recv() {
                        let chunk = ChatCompletionChunk::content(
                            &request_id, created, &model, evt.status.clone(),
                        );
                        send_sse(&sse_tx, &chunk).await;
                    }

                    match completion {
                        Some(r) => break Some(r),
                        None => return,
                    }
                }
            }
        };

        // Send final content from pipeline result if present.
        if let Some(Ok(Ok(Some(data)))) = &result {
            if let Some(content) = data.get("content").and_then(|c| c.as_str()) {
                let chunk =
                    ChatCompletionChunk::content(&request_id, created, &model, content.to_string());
                send_sse(&sse_tx, &chunk).await;
            }
        }

        send_sse(
            &sse_tx,
            &ChatCompletionChunk::stop(&request_id, created, &model),
        )
        .await;
        let _ = sse_tx.send(Ok(SSE_DONE.to_string())).await;
    });

    let stream = ReceiverStream::new(sse_rx);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .map_err(|e| {
            error!(error = %e, "Failed to build SSE response.");
            Error::FlowCompletionFailed
        })
}

/// AI gateway processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<config::Processor>,
    /// Event sender channel into the pipeline.
    tx: Option<mpsc::Sender<Event>>,
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

        // Load endpoint credentials.
        let credentials = match &init_config.credentials_path {
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

        // Get auth provider from the worker-level HTTP server.
        let auth_provider = self
            .task_context
            .http_server
            .as_ref()
            .and_then(|server| server.auth_provider());

        let response_registry = self
            .task_context
            .response_registry
            .clone()
            .unwrap_or_else(|| Arc::new(ResponseRegistry::new()));

        let state = ProxyState {
            cancellation_token: self.task_context.cancellation_token.clone(),
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            credentials,
            auth_provider,
            response_registry,
            leaf_count: self.task_context.leaf_count,
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
                "AI gateway endpoint registered."
            );
        }

        Ok(())
    }
}

/// Builder for AI gateway processor.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<config::Processor>>,
    tx: Option<mpsc::Sender<Event>>,
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

    pub fn sender(mut self, sender: mpsc::Sender<Event>) -> Self {
        self.tx = Some(sender);
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
            tx: self.tx,
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
