//! AI gateway role for the generic HTTP server.
//!
//! Implements the OpenAI-compatible chat completions surface: a single
//! `POST <path>/chat/completions` route + a `GET <path>/models` model-listing
//! route. Per-flow routing is driven by the request body's `model` field
//! (`model: "<gateway-name>/<downstream-model>"`); the dispatcher splits on
//! the first `/`, looks the gateway up by name, and forwards the request to
//! the pipeline with the model field rewritten to the downstream portion.
//!
//! The server lifecycle, dispatch table, and hot-reload semantics live in
//! `flowgen_core::http_server`; this module owns only the AI-gateway-specific
//! URL layout and OpenAI translation logic.

use crate::ai_gateway::config::{
    self, ChatCompletionChunk, ChatCompletionRequest, ChatCompletionResponse, Message, Protocol,
    SSE_DONE,
};
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use base64::Engine;
use flowgen_core::auth::{extract_bearer_token, AuthProvider};
use flowgen_core::credentials::HttpCredentials;
use flowgen_core::event::{new_completion_channel, Event, EventBuilder, EventData, EventExt};
use flowgen_core::http_server::{DispatchState, Dispatcher, HasFlowName, HttpServer};
use flowgen_core::registry::{ProgressEvent, ResponseRegistry, ResponseSender};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

/// Default port for the AI gateway server.
pub const DEFAULT_AI_GATEWAY_PORT: u16 = 3002;

/// Default path prefix for AI gateway routes. Matches the de-facto
/// OpenAI-compatible convention used by vLLM, Ollama, LiteLLM, OpenRouter,
/// and the OpenAI SDKs.
pub const DEFAULT_AI_GATEWAY_PATH: &str = "/v1";

/// Convenience type alias for the AI gateway server.
pub type AiGatewayServer = HttpServer<AiGatewayDispatcher>;

/// Dispatch-table entry describing one registered LLM proxy.
///
/// Stored in the AI gateway server's dispatch table keyed by the proxy's
/// `name` (the `name` field of the `llm_proxy` task config). For OpenAI
/// protocol the dispatcher routes by the request body's `model` field
/// (`"<name>/<downstream-model>"`); the remainder is forwarded as the real
/// model name in the pipeline event.
///
/// `flow_name` lets the server bulk-deregister every proxy owned by a flow
/// when the flow is stopped or hot-reloaded. `protocol` lets the dispatcher
/// reject requests on the wrong URL for this registration's protocol.
#[derive(Clone)]
pub struct LlmProxyRegistration {
    /// Name of the flow that registered this proxy.
    pub flow_name: String,
    /// Wire protocol exposed for this proxy. Determines which URL handler
    /// is allowed to dispatch to it.
    pub protocol: Protocol,
    /// Full processor configuration. The dispatcher reads `auth`, `ack_timeout`,
    /// and `name` from here.
    pub config: Arc<config::Processor>,
    /// Optional bearer-token credentials loaded from `config.credentials_path`.
    pub credentials: Option<HttpCredentials>,
    /// Optional auth provider for user identity resolution (JWT, OIDC, session).
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
    /// Channel to send the inbound chat-completion event into the flow pipeline.
    pub tx: Sender<Event>,
    /// Task identifier used when constructing pipeline events.
    pub task_id: usize,
    /// Task type label used when constructing pipeline events.
    pub task_type: &'static str,
    /// Shared response registry for streaming chunks back to the OpenAI client
    /// (SSE) or awaiting the final completion (non-streaming).
    pub response_registry: Arc<ResponseRegistry>,
    /// Number of leaf tasks reachable from this gateway source.
    pub leaf_count: usize,
    /// Cancellation token from the owning flow's task tenure.
    pub cancellation_token: CancellationToken,
}

impl HasFlowName for LlmProxyRegistration {
    fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

/// Dispatcher for AI gateway traffic.
///
/// Wires `POST <path>/chat/completions` and `GET <path>/models` and routes
/// chat completions by the `model` field of the request body.
pub struct AiGatewayDispatcher;

impl Dispatcher for AiGatewayDispatcher {
    type Registration = LlmProxyRegistration;
    type Extras = ();

    fn build_router(state: DispatchState<Self::Registration, Self::Extras>) -> Router {
        let prefix = state.path.trim_end_matches('/').to_string();
        let chat_route = format!("{prefix}/chat/completions");
        let models_route = format!("{prefix}/models");
        Router::new()
            .route(&chat_route, post(dispatch_chat_completions))
            .route(&models_route, get(list_models))
            .with_state(state)
    }
}

// ---------------------------------------------------------------------------
// Dispatcher entry points
// ---------------------------------------------------------------------------

/// Compact OpenAI-compatible response for `GET <path>/models`.
#[derive(Serialize)]
struct ModelsResponse {
    object: &'static str,
    data: Vec<ModelEntry>,
}

#[derive(Serialize)]
struct ModelEntry {
    id: String,
    object: &'static str,
    created: i64,
    owned_by: &'static str,
}

/// Returns the list of currently-registered gateway names in the OpenAI
/// models schema.
async fn list_models(State(state): State<DispatchState<LlmProxyRegistration>>) -> Response {
    let created = chrono::Utc::now().timestamp();
    let data = state
        .table
        .iter()
        .map(|entry| ModelEntry {
            id: entry.key().clone(),
            object: "model",
            created,
            owned_by: "flowgen",
        })
        .collect();
    axum::Json(ModelsResponse {
        object: "list",
        data,
    })
    .into_response()
}

/// Handler for `POST <path>/chat/completions`. Peeks `body.model` to find the
/// registration, splits on the first `/`, and dispatches to the matching
/// gateway with the downstream-model portion of `model`.
async fn dispatch_chat_completions(
    State(state): State<DispatchState<LlmProxyRegistration>>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<ChatCompletionRequest>,
) -> Response {
    let model = match request.model.as_deref() {
        Some(m) if !m.is_empty() => m.to_string(),
        _ => return DispatchError::MissingModelField.into_response(),
    };

    let (proxy_name, downstream_model) = match model.split_once('/') {
        Some((name, rest)) if !name.is_empty() && !rest.is_empty() => {
            (name.to_string(), rest.to_string())
        }
        _ => return DispatchError::MissingProxyPrefix.into_response(),
    };

    let registration = match state.table.get(&proxy_name) {
        Some(entry) => entry.clone(),
        None => {
            return DispatchError::UnknownProxy { name: proxy_name }.into_response();
        }
    };

    // Reject if the registration speaks a different protocol than this URL.
    // Today only OpenAI exists so this is always true; the check exists so
    // adding a second protocol later does not require rewriting the dispatcher.
    if registration.protocol != Protocol::Openai {
        return DispatchError::WrongProtocolForUrl { name: proxy_name }.into_response();
    }

    if registration.cancellation_token.is_cancelled() {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }

    if let Err(e) = validate_endpoint_auth(registration.credentials.as_ref(), &headers) {
        return e.into_response();
    }

    let user_context = match validate_user_auth(&registration, &headers).await {
        Ok(ctx) => ctx,
        Err(e) => return e.into_response(),
    };

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

    let request_id = format!("chatcmpl-{}", uuid::Uuid::now_v7());
    let created = chrono::Utc::now().timestamp();
    let is_stream = request.stream;

    let data = serde_json::json!({
        "prompt": prompt,
        "system_prompt": system_prompt,
        "model": downstream_model,
        "temperature": request.temperature,
        "max_tokens": request.max_tokens,
        "stream": is_stream,
    });

    let mut meta = serde_json::Map::new();
    if let Some(ref ctx) = user_context {
        if let Ok(value) = serde_json::to_value(ctx) {
            meta.insert(flowgen_core::auth::AUTH.to_string(), value);
        }
    }

    let result = if is_stream {
        dispatch_streaming(
            &registration,
            data,
            meta,
            downstream_model,
            request_id,
            created,
        )
        .await
    } else {
        dispatch_blocking(
            &registration,
            data,
            meta,
            downstream_model,
            request_id,
            created,
        )
        .await
    };

    match result {
        Ok(response) => response,
        Err(e) => {
            error!(error = %e, "AI gateway dispatch failed");
            e.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/// Validates endpoint-level credentials (bearer/basic) on the request.
fn validate_endpoint_auth(
    credentials: Option<&HttpCredentials>,
    headers: &HeaderMap,
) -> Result<(), DispatchError> {
    let credentials = match credentials {
        Some(creds) => creds,
        None => return Ok(()),
    };

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .ok_or(DispatchError::NoCredentials)?;

    let auth_value = auth_header
        .to_str()
        .map_err(|_| DispatchError::MalformedCredentials)?;

    if let Some(expected_token) = &credentials.bearer_auth {
        match extract_bearer_token(auth_value) {
            Some(token) if token == expected_token => return Ok(()),
            Some(_) => return Err(DispatchError::InvalidCredentials),
            None => {}
        }
    }

    if let Some(basic_auth) = &credentials.basic_auth {
        if let Some(encoded) = auth_value.strip_prefix("Basic ") {
            let decoded_bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| DispatchError::MalformedCredentials)?;
            let decoded_str = String::from_utf8(decoded_bytes)
                .map_err(|_| DispatchError::MalformedCredentials)?;
            let expected = format!("{}:{}", basic_auth.username, basic_auth.password);
            return if decoded_str == expected {
                Ok(())
            } else {
                Err(DispatchError::InvalidCredentials)
            };
        }
    }

    Err(DispatchError::InvalidCredentials)
}

/// Validates user-level auth via the worker auth provider when
/// `config.auth.required` is true.
async fn validate_user_auth(
    registration: &LlmProxyRegistration,
    headers: &HeaderMap,
) -> Result<Option<flowgen_core::auth::UserContext>, DispatchError> {
    match &registration.config.auth {
        Some(config) if config.required => {}
        _ => return Ok(None),
    }

    let provider = registration
        .auth_provider
        .as_ref()
        .ok_or(DispatchError::AuthProviderMissing)?;

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(DispatchError::NoCredentials)?;

    let token = extract_bearer_token(auth_header).ok_or(DispatchError::MalformedCredentials)?;

    provider
        .validate(token)
        .await
        .map(Some)
        .map_err(|_| DispatchError::InvalidCredentials)
}

// ---------------------------------------------------------------------------
// Dispatch implementations
// ---------------------------------------------------------------------------

/// Non-streaming chat completion: send event, await leaf completion, return
/// an OpenAI `ChatCompletionResponse`.
async fn dispatch_blocking(
    registration: &LlmProxyRegistration,
    data: serde_json::Value,
    meta: serde_json::Map<String, serde_json::Value>,
    model: String,
    request_id: String,
    created: i64,
) -> Result<Response, DispatchError> {
    let (completion_state, completion_rx) = new_completion_channel(registration.leaf_count);

    let mut builder = EventBuilder::new()
        .data(EventData::Json(data))
        .subject(registration.config.name.to_owned())
        .task_id(registration.task_id)
        .task_type(registration.task_type)
        .completion_tx(completion_state);

    if !meta.is_empty() {
        builder = builder.meta(meta);
    }

    let e = builder
        .build()
        .map_err(|source| DispatchError::EventBuilder { source })?;

    e.send_with_logging(Some(&registration.tx))
        .await
        .map_err(|source| DispatchError::SendMessage { source })?;

    let result = match registration.config.ack_timeout {
        Some(timeout) => tokio::time::timeout(timeout, completion_rx)
            .await
            .map_err(|_| DispatchError::FlowCompletionFailed)?
            .map_err(|_| DispatchError::FlowCompletionFailed)?,
        None => completion_rx
            .await
            .map_err(|_| DispatchError::FlowCompletionFailed)?,
    };

    let completion_data = result.map_err(|_| DispatchError::FlowCompletionFailed)?;

    let content = completion_data
        .and_then(|v| v.get("content").and_then(|c| c.as_str()).map(String::from))
        .unwrap_or_default();

    let response = ChatCompletionResponse::new(request_id, created, model, content);
    Ok(axum::Json(response).into_response())
}

/// Streaming chat completion: open SSE response stream backed by the response
/// registry, forwarding progress events as `ChatCompletionChunk` content
/// chunks and the final completion as a stop chunk plus `[DONE]`.
async fn dispatch_streaming(
    registration: &LlmProxyRegistration,
    data: serde_json::Value,
    mut meta: serde_json::Map<String, serde_json::Value>,
    model: String,
    request_id: String,
    created: i64,
) -> Result<Response, DispatchError> {
    let correlation_id = uuid::Uuid::now_v7().to_string();

    let (progress_tx, mut progress_rx) = mpsc::channel::<ProgressEvent>(32);

    registration
        .response_registry
        .insert(
            correlation_id.clone(),
            ResponseSender {
                progress_tx,
                result_tx: None,
            },
        )
        .await;

    meta.insert(
        flowgen_core::registry::CORRELATION_ID.to_string(),
        serde_json::Value::String(correlation_id.clone()),
    );

    let (completion_state_tx, completion_rx) = new_completion_channel(registration.leaf_count);

    let e = EventBuilder::new()
        .data(EventData::Json(data))
        .subject(registration.config.name.to_owned())
        .task_id(registration.task_id)
        .task_type(registration.task_type)
        .meta(meta)
        .completion_tx(completion_state_tx)
        .build()
        .map_err(|source| DispatchError::EventBuilder { source })?;

    e.send_with_logging(Some(&registration.tx))
        .await
        .map_err(|source| DispatchError::SendMessage { source })?;

    info!(
        gateway = %registration.config.name,
        correlation_id = %correlation_id,
        "Streaming AI gateway request accepted."
    );

    let registry = Arc::clone(&registration.response_registry);
    let cid = correlation_id.clone();
    let ack_timeout = registration.config.ack_timeout;
    let (sse_tx, sse_rx) = mpsc::channel::<Result<String, std::convert::Infallible>>(32);

    tokio::spawn(async move {
        async fn send_sse(
            tx: &mpsc::Sender<Result<String, std::convert::Infallible>>,
            chunk: &ChatCompletionChunk,
        ) -> bool {
            match chunk.to_sse() {
                Ok(data) => tx.send(Ok(data)).await.is_ok(),
                Err(e) => {
                    error!(error = %e, "Failed to serialize SSE chunk.");
                    true
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
        .map_err(|_| DispatchError::FlowCompletionFailed)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the dispatcher path. Implements `IntoResponse` for HTTP
/// status mapping.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
enum DispatchError {
    #[error("Request body is missing the required 'model' field")]
    MissingModelField,
    #[error("Request 'model' field must be of the form '<proxy-name>/<downstream-model>'")]
    MissingProxyPrefix,
    #[error("Unknown LLM proxy '{name}'")]
    UnknownProxy { name: String },
    #[error("LLM proxy '{name}' does not speak the protocol expected at this URL")]
    WrongProtocolForUrl { name: String },
    #[error("No authorization header provided")]
    NoCredentials,
    #[error("Invalid authorization credentials")]
    InvalidCredentials,
    #[error("Malformed authorization header")]
    MalformedCredentials,
    #[error("Auth provider not configured but auth.required is true")]
    AuthProviderMissing,
    #[error("Failed to build pipeline event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to send pipeline event: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Flow completion failed or timed out")]
    FlowCompletionFailed,
}

impl IntoResponse for DispatchError {
    fn into_response(self) -> Response {
        let status = match &self {
            DispatchError::MissingModelField | DispatchError::MissingProxyPrefix => {
                StatusCode::BAD_REQUEST
            }
            DispatchError::UnknownProxy { .. } | DispatchError::WrongProtocolForUrl { .. } => {
                StatusCode::NOT_FOUND
            }
            DispatchError::NoCredentials
            | DispatchError::InvalidCredentials
            | DispatchError::MalformedCredentials
            | DispatchError::AuthProviderMissing => StatusCode::UNAUTHORIZED,
            DispatchError::EventBuilder { .. }
            | DispatchError::SendMessage { .. }
            | DispatchError::FlowCompletionFailed => StatusCode::INTERNAL_SERVER_ERROR,
        };
        (status, self.to_string()).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_AI_GATEWAY_PORT, 3002);
        assert_eq!(DEFAULT_AI_GATEWAY_PATH, "/v1");
    }

    #[test]
    fn test_dispatch_error_status_codes() {
        assert_eq!(
            DispatchError::MissingModelField.into_response().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            DispatchError::MissingProxyPrefix.into_response().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            DispatchError::UnknownProxy {
                name: "x".to_string()
            }
            .into_response()
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            DispatchError::WrongProtocolForUrl {
                name: "x".to_string()
            }
            .into_response()
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            DispatchError::NoCredentials.into_response().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            DispatchError::FlowCompletionFailed.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }
}
