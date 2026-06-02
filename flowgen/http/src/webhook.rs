//! HTTP webhook processor for handling incoming requests.
//!
//! Processes incoming HTTP webhook requests, extracting headers and payload
//! data and converting them into events for further processing in the pipeline.

use axum::{body::Body, response::IntoResponse};
use base64::{engine::general_purpose::STANDARD, Engine};
use flowgen_core::auth::{extract_bearer_token, AuthProvider};
use flowgen_core::config::ConfigExt;
use flowgen_core::credentials::HttpCredentials;
use flowgen_core::event::{
    new_completion_channel, CompletionRx, Event, EventBuilder, EventData, EventExt,
};
use flowgen_core::registry::{ProgressEvent, ResponseRegistry, ResponseSender};
use reqwest::{
    header::{HeaderMap, AUTHORIZATION},
    StatusCode,
};
use serde_json::{json, Map, Value};
use std::{fs, sync::Arc};
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tracing::{error, info};

/// JSON key for HTTP headers in webhook events.
const DEFAULT_HEADERS_KEY: &str = "headers";
/// JSON key for HTTP payload in webhook events.
const DEFAULT_PAYLOAD_KEY: &str = "payload";

/// Errors that can occur during webhook processing.
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
    #[error("No authorization header provided")]
    NoCredentials,
    #[error("Invalid authorization credentials")]
    InvalidCredentials,
    #[error("Malformed authorization header")]
    MalformedCredentials,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Flow completion failed or timed out for webhook request")]
    FlowCompletionFailed,
    #[error("Request body exceeds configured max_body_bytes limit of {limit} bytes")]
    BodyTooLarge { limit: usize },
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Error::SerdeJson { .. } => StatusCode::BAD_REQUEST,
            Error::BodyTooLarge { .. } => StatusCode::PAYLOAD_TOO_LARGE,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        status.into_response()
    }
}

/// Resolved init-time data for a webhook task.
///
/// Returned by `Processor::init` and consumed by `Processor::run` to populate
/// a [`crate::server::WebhookRegistration`]. Carries only the fields whose
/// resolution requires file I/O or trait downcasts at init time; everything
/// else needed by the dispatcher already lives on the `Processor` itself.
///
/// Mirrors the role of `flowgen_mcp::processor::EventHandler`: a lightweight
/// carrier returned by `Runner::init` so `run()` can hand the values off to
/// the server's dispatch table. The actual request-handling logic lives in
/// the free-function `dispatch_*` family at the bottom of this module.
#[derive(Clone, Debug)]
pub struct EventHandler {
    /// Pre-loaded endpoint authentication credentials (task-level path
    /// overrides the global webhook server credentials path).
    credentials: Option<HttpCredentials>,
    /// Shared response registry, used when the webhook is configured for
    /// SSE streaming so the dispatcher can surface progress events back to
    /// the client.
    response_registry: Option<Arc<ResponseRegistry>>,
    /// Auth provider for user identity resolution. Resolved from the worker
    /// HTTP server config at init time.
    auth_provider: Option<Arc<dyn AuthProvider>>,
}

/// HTTP webhook processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel. Required for webhook tasks because they are
    /// always sources — the builder rejects construction without a sender.
    tx: Sender<Event>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Shared response registry for SSE streaming.
    response_registry: Option<Arc<ResponseRegistry>>,
    /// Shared webhook HTTP server. The processor inserts a `WebhookRegistration`
    /// into this server during `run()`; the dispatcher routes inbound requests
    /// for the configured endpoint to it.
    http_server: Arc<crate::server::WebhookServer>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        // Resolve credentials: task-level overrides the server's global path.
        let server_credentials_path = self.http_server.credentials_path().map(|p| p.to_path_buf());
        let credentials_path = init_config
            .credentials_path
            .as_ref()
            .or(server_credentials_path.as_ref());

        let credentials = match credentials_path {
            Some(path) => {
                let content = fs::read_to_string(path).map_err(|e| Error::ReadCredentials {
                    path: path.clone(),
                    source: e,
                })?;
                Some(serde_json::from_str(&content).map_err(|e| Error::SerdeJson { source: e })?)
            }
            None => None,
        };

        let event_handler = EventHandler {
            credentials,
            response_registry: self
                .response_registry
                .clone()
                .or_else(|| self.task_context.response_registry.clone()),
            auth_provider: self.http_server.auth_provider(),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize webhook processor");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => handler,
            Err(e) => {
                return Err(e);
            }
        };

        let registration = crate::server::WebhookRegistration {
            flow_name: self.task_context.flow.name.clone(),
            config: Arc::clone(&self.config),
            credentials: event_handler.credentials.clone(),
            auth_provider: event_handler.auth_provider.clone(),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            response_registry: event_handler
                .response_registry
                .clone()
                .unwrap_or_else(|| Arc::new(ResponseRegistry::new())),
            leaf_count: self.task_context.leaf_count,
            cancellation_token: self.task_context.cancellation_token.clone(),
        };

        self.http_server
            .register(self.config.endpoint.clone(), registration);

        Ok(())
    }
}

/// Builder for HTTP webhook processor.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Optional processor configuration.
    config: Option<Arc<super::config::Processor>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
    /// Shared response registry for SSE streaming.
    response_registry: Option<Arc<ResponseRegistry>>,
    /// Shared webhook HTTP server.
    http_server: Option<Arc<crate::server::WebhookServer>>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
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

    pub fn response_registry(mut self, registry: Arc<ResponseRegistry>) -> Self {
        self.response_registry = Some(registry);
        self
    }

    /// Sets the shared webhook HTTP server. Required.
    pub fn http_server(mut self, server: Arc<crate::server::WebhookServer>) -> Self {
        self.http_server = Some(server);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingBuilderAttribute("sender".to_string()))?,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
            response_registry: self.response_registry,
            http_server: self
                .http_server
                .ok_or_else(|| Error::MissingBuilderAttribute("http_server".to_string()))?,
        })
    }
}

/// Per-request dispatcher for the static webhook catch-all route.
///
/// Called by the HTTP server's catch-all handler after method validation.
/// Reproduces the behaviour of `EventHandler::handle` / `handle_stream`
/// against `&WebhookRegistration` so request-handling no longer requires a
/// dedicated axum route per webhook. Branches on `registration.config.stream`
/// to either await flow completion and return 200/500, or open an SSE
/// response stream backed by the response registry.
pub async fn dispatch(
    registration: &crate::server::WebhookRegistration,
    headers: HeaderMap,
    body: Body,
) -> axum::response::Response<Body> {
    if registration.config.stream {
        dispatch_stream(registration, headers, body)
            .await
            .unwrap_or_else(|e| {
                error!(error = %e, "Webhook stream dispatch failed");
                e.into_response()
            })
    } else {
        match dispatch_blocking(registration, headers, body).await {
            Ok(status) => status.into_response(),
            Err(e) => {
                error!(error = %e, "Webhook dispatch failed");
                e.into_response()
            }
        }
    }
}

/// Validates the bearer/basic credentials on the request against `registration.credentials`.
fn validate_endpoint_auth(
    credentials: Option<&HttpCredentials>,
    headers: &HeaderMap,
) -> Result<(), Error> {
    let credentials = match credentials {
        Some(creds) => creds,
        None => return Ok(()),
    };

    let auth_header = match headers.get(AUTHORIZATION) {
        Some(header) => header,
        None => return Err(Error::NoCredentials),
    };

    let auth_value = match auth_header.to_str() {
        Ok(value) => value,
        Err(_) => return Err(Error::MalformedCredentials),
    };

    if let Some(expected_token) = &credentials.bearer_auth {
        match extract_bearer_token(auth_value) {
            Some(token) if token == expected_token => return Ok(()),
            Some(_) => return Err(Error::InvalidCredentials),
            None => {}
        }
    }

    if let Some(basic_auth) = &credentials.basic_auth {
        if let Some(encoded) = auth_value.strip_prefix("Basic ") {
            match STANDARD.decode(encoded) {
                Ok(decoded_bytes) => match String::from_utf8(decoded_bytes) {
                    Ok(decoded_str) => {
                        let expected = format!("{}:{}", basic_auth.username, basic_auth.password);
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

/// Validates user-level authentication via the worker auth provider when
/// `config.auth.required` is true. Returns the resolved user context, or
/// `None` when user auth is not required.
async fn validate_user_auth(
    registration: &crate::server::WebhookRegistration,
    headers: &HeaderMap,
) -> Result<Option<flowgen_core::auth::UserContext>, Error> {
    match &registration.config.auth {
        Some(config) if config.required => {}
        _ => return Ok(None),
    }

    let provider = match &registration.auth_provider {
        Some(p) => p,
        None => return Err(Error::NoCredentials),
    };

    let auth_header = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(Error::NoCredentials)?;

    let token = extract_bearer_token(auth_header).ok_or(Error::MalformedCredentials)?;

    provider
        .validate(token)
        .await
        .map(Some)
        .map_err(|_| Error::InvalidCredentials)
}

/// Reads the request body (bounded by `config.max_body_bytes`), parses it as
/// JSON, projects configured headers into the event payload, and runs auth.
/// Returns the event-payload `Value` and any user context.
async fn parse_request(
    registration: &crate::server::WebhookRegistration,
    headers: &HeaderMap,
    body: Body,
) -> Result<(Value, Option<flowgen_core::auth::UserContext>), Error> {
    if registration.cancellation_token.is_cancelled() {
        return Err(Error::FlowCompletionFailed);
    }

    validate_endpoint_auth(registration.credentials.as_ref(), headers)?;
    let user_context = validate_user_auth(registration, headers).await?;

    let limit = registration.config.max_body_bytes;
    let body_bytes = axum::body::to_bytes(body, limit)
        .await
        .map_err(|_| Error::BodyTooLarge { limit })?;

    let json_body = match body_bytes.is_empty() {
        true => Value::Null,
        false => {
            serde_json::from_slice(&body_bytes).map_err(|source| Error::SerdeJson { source })?
        }
    };

    let mut headers_map = Map::new();
    if let Some(configured_headers) = &registration.config.headers {
        for (key, value) in headers.iter() {
            let header_name = key.as_str();
            if configured_headers.contains_key(header_name) {
                headers_map.insert(
                    header_name.to_string(),
                    Value::String(value.to_str().unwrap_or("").to_string()),
                );
            }
        }
    }

    let data = json!({
        DEFAULT_HEADERS_KEY: Value::Object(headers_map),
        DEFAULT_PAYLOAD_KEY: json_body,
    });

    Ok((data, user_context))
}

/// Builds an event with a per-request completion channel sized to the flow's
/// leaf count and pushes it onto `registration.tx`. Returns the receiver the
/// dispatcher waits on for flow completion.
async fn inject_event(
    registration: &crate::server::WebhookRegistration,
    data: Value,
    meta: Option<serde_json::Map<String, Value>>,
) -> Result<CompletionRx, Error> {
    let (completion_state, completion_rx) = new_completion_channel(registration.leaf_count);

    let mut builder = EventBuilder::new()
        .data(EventData::Json(data))
        .subject(registration.config.name.to_owned())
        .task_id(registration.task_id)
        .task_type(registration.task_type)
        .completion_tx(completion_state);

    if let Some(meta) = meta {
        builder = builder.meta(meta);
    }

    let e = builder
        .build()
        .map_err(|source| Error::EventBuilder { source })?;

    e.send_with_logging(Some(&registration.tx))
        .await
        .map_err(|source| Error::SendMessage { source })?;

    Ok(completion_rx)
}

/// Non-streaming dispatch: parses the request, injects an event, awaits flow
/// completion (bounded by `ack_timeout` if configured), and returns
/// `200 OK` on success or `500` on completion failure.
async fn dispatch_blocking(
    registration: &crate::server::WebhookRegistration,
    headers: HeaderMap,
    body: Body,
) -> Result<StatusCode, Error> {
    let (data, user_context) = match parse_request(registration, &headers, body).await {
        Ok(result) => result,
        Err(Error::FlowCompletionFailed) => return Ok(StatusCode::SERVICE_UNAVAILABLE),
        Err(
            e @ (Error::NoCredentials | Error::InvalidCredentials | Error::MalformedCredentials),
        ) => return Ok(e.into_response().status()),
        Err(e) => return Err(e),
    };

    let meta = user_context.map(|ctx| {
        let mut meta = serde_json::Map::new();
        if let Ok(value) = serde_json::to_value(ctx) {
            meta.insert(flowgen_core::auth::AUTH.to_string(), value);
        }
        meta
    });

    let completion_rx = inject_event(registration, data, meta).await?;

    match registration.config.ack_timeout {
        Some(timeout) => match tokio::time::timeout(timeout, completion_rx).await {
            Ok(Ok(Ok(_))) => Ok(StatusCode::OK),
            Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                error!("{}", Error::FlowCompletionFailed);
                Ok(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
        None => match completion_rx.await {
            Ok(Ok(_)) => Ok(StatusCode::OK),
            Ok(Err(_)) | Err(_) => {
                error!("{}", Error::FlowCompletionFailed);
                Ok(StatusCode::INTERNAL_SERVER_ERROR)
            }
        },
    }
}

/// Streaming dispatch: opens an SSE response stream backed by the response
/// registry. Progress events from downstream tasks are forwarded as they
/// arrive; the final completion result is sent as the last SSE message.
async fn dispatch_stream(
    registration: &crate::server::WebhookRegistration,
    headers: HeaderMap,
    body: Body,
) -> Result<axum::response::Response<Body>, Error> {
    let (data, user_context) = match parse_request(registration, &headers, body).await {
        Ok(result) => result,
        Err(Error::FlowCompletionFailed) => {
            return Ok(StatusCode::SERVICE_UNAVAILABLE.into_response());
        }
        Err(
            e @ (Error::NoCredentials | Error::InvalidCredentials | Error::MalformedCredentials),
        ) => return Ok(e.into_response()),
        Err(e) => return Err(e),
    };

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

    let mut meta = serde_json::Map::new();
    meta.insert(
        flowgen_core::registry::CORRELATION_ID.to_string(),
        Value::String(correlation_id.clone()),
    );
    if let Some(ctx) = user_context {
        if let Ok(value) = serde_json::to_value(ctx) {
            meta.insert(flowgen_core::auth::AUTH.to_string(), value);
        }
    }

    let completion_rx = inject_event(registration, data, Some(meta)).await?;

    info!(
        webhook = %registration.config.name,
        correlation_id = %correlation_id,
        "Streaming webhook request accepted."
    );

    let registry = Arc::clone(&registration.response_registry);
    let cid = correlation_id.clone();
    let ack_timeout = registration.config.ack_timeout;
    let (sse_tx, sse_rx) = mpsc::channel::<Result<String, std::convert::Infallible>>(32);

    tokio::spawn(async move {
        let format_data = |value: &Value| -> Option<String> {
            serde_json::to_string(value)
                .ok()
                .map(|data| format!("data: {data}\n\n"))
        };

        tokio::pin!(completion_rx);

        type CompletionResult = Result<
            Result<Option<Value>, Box<dyn std::error::Error + Send + Sync>>,
            tokio::sync::oneshot::error::RecvError,
        >;

        let result: Option<CompletionResult> = loop {
            tokio::select! {
                progress = progress_rx.recv() => {
                    match progress {
                        Some(evt) => {
                            if let Ok(value) = serde_json::to_value(&evt) {
                                if let Some(msg) = format_data(&value) {
                                    if sse_tx.send(Ok(msg)).await.is_err() {
                                        registry.remove(&cid).await;
                                        return;
                                    }
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
                                    registry.remove(&cid).await;
                                    let err = json!({"error": "Flow completion timed out."});
                                    if let Some(msg) = format_data(&err) {
                                        let _ = sse_tx.send(Ok(msg)).await;
                                    }
                                    None
                                }
                            }
                        }
                        None => Some((&mut completion_rx).await),
                    }
                } => {
                    registry.remove(&cid).await;

                    while let Ok(evt) = progress_rx.try_recv() {
                        if let Ok(value) = serde_json::to_value(&evt) {
                            if let Some(msg) = format_data(&value) {
                                let _ = sse_tx.send(Ok(msg)).await;
                            }
                        }
                    }

                    match result {
                        Some(completion) => break Some(completion),
                        None => return,
                    }
                }
            }
        };

        if let Some(Ok(Ok(Some(data)))) = result {
            if let Some(msg) = format_data(&data) {
                let _ = sse_tx.send(Ok(msg)).await;
            }
        }
    });

    let stream = ReceiverStream::new(sse_rx);

    axum::response::Response::builder()
        .status(StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, "text/event-stream")
        .header(axum::http::header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .map_err(|e| {
            error!(error = %e, "Failed to build SSE response.");
            Error::FlowCompletionFailed
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value};
    use tokio::sync::mpsc;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_error_serde_json_structure() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error = Error::SerdeJson { source: json_error };
        assert!(matches!(error, Error::SerdeJson { .. }));
    }

    #[test]
    fn test_error_read_credentials_structure() {
        use std::path::PathBuf;
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error = Error::ReadCredentials {
            path: PathBuf::from("/test/credentials.json"),
            source: io_error,
        };
        assert!(matches!(error, Error::ReadCredentials { .. }));
    }

    #[test]
    fn test_error_into_response() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err = Error::SerdeJson { source: json_error };
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let err = Error::MissingBuilderAttribute("test".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::config::Processor {
            name: "test_webhook".to_string(),
            endpoint: "/webhook".to_string(),
            method: crate::config::Method::Post,
            payload: None,
            headers: None,
            credentials_path: None,
            ack_timeout: None,
            timeout: crate::config::default_request_timeout(),
            connect_timeout: crate::config::default_connect_timeout(),
            max_body_bytes: crate::config::default_max_body_bytes(),
            stream: false,
            auth: None,
            depends_on: None,
            retry: None,
        });
        let (tx, _rx) = mpsc::channel(100);

        let server = Arc::new(flowgen_core::http_server::HttpServer::<
            crate::server::WebhookDispatcher,
        >::new(
            crate::server::DEFAULT_WEBHOOK_PATH.to_string()
        ));

        // Success case.
        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .http_server(server)
            .build()
            .await;
        assert!(processor.is_ok());

        // Error case - missing config.
        let (tx2, _rx2) = mpsc::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_HEADERS_KEY, "headers");
        assert_eq!(DEFAULT_PAYLOAD_KEY, "payload");
    }
}
