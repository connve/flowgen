//! HTTP webhook processor for handling incoming requests.
//!
//! Processes incoming HTTP webhook requests, extracting headers and payload
//! data and converting them into events for further processing in the pipeline.

use axum::{body::Body, extract::Request, response::IntoResponse, routing::MethodRouter};
use base64::{engine::general_purpose::STANDARD, Engine};
use flowgen_core::auth::{extract_bearer_token, AuthProvider};
use flowgen_core::config::ConfigExt;
use flowgen_core::credentials::HttpCredentials;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
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
use tracing::{error, info, Instrument};

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
    #[error("Axum HTTP error: {source}")]
    Axum {
        #[source]
        source: axum::Error,
    },
    #[error("Error reading credentials file at {path}: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Task manager error: {source}")]
    TaskManager {
        #[source]
        source: flowgen_core::task::manager::Error,
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
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Error::SerdeJson { .. } | Error::Axum { .. } => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        status.into_response()
    }
}

/// Event handler for processing incoming webhooks.
#[derive(Clone, Debug)]
pub struct EventHandler {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Option<Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Pre-loaded authentication credentials.
    credentials: Option<HttpCredentials>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Shared response registry for SSE streaming (only used when stream: true).
    response_registry: Option<Arc<ResponseRegistry>>,
    /// Auth provider for user identity resolution (from worker config).
    auth_provider: Option<Arc<dyn AuthProvider>>,
}

impl EventHandler {
    /// Validate authentication credentials against incoming request headers.
    fn validate_authentication(&self, headers: &HeaderMap) -> Result<(), Error> {
        let credentials = match &self.credentials {
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

        // Check bearer authentication.
        if let Some(expected_token) = &credentials.bearer_auth {
            match extract_bearer_token(auth_value) {
                Some(token) if token == expected_token => return Ok(()),
                Some(_) => return Err(Error::InvalidCredentials),
                None => {}
            }
        }

        // Check basic authentication.
        if let Some(basic_auth) = &credentials.basic_auth {
            if let Some(encoded) = auth_value.strip_prefix("Basic ") {
                match STANDARD.decode(encoded) {
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
    /// Returns the user context if auth is required and valid, None otherwise.
    async fn validate_user_auth(
        &self,
        headers: &HeaderMap,
    ) -> Result<Option<flowgen_core::auth::UserContext>, Error> {
        match &self.config.auth {
            Some(config) if config.required => {}
            _ => return Ok(None),
        }

        let provider = match &self.auth_provider {
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

    /// Parse and validate the incoming request, returning the JSON data and optional user context.
    async fn parse_request(
        &self,
        headers: &HeaderMap,
        request: Request<Body>,
    ) -> Result<(Value, Option<flowgen_core::auth::UserContext>), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Err(Error::FlowCompletionFailed);
        }

        self.validate_authentication(headers)?;

        let user_context = self.validate_user_auth(headers).await?;

        let body = axum::body::to_bytes(request.into_body(), usize::MAX)
            .await
            .map_err(|e| Error::Axum { source: e })?;

        let json_body = match body.is_empty() {
            true => Value::Null,
            false => serde_json::from_slice(&body).map_err(|source| Error::SerdeJson { source })?,
        };

        let mut headers_map = Map::new();
        if let Some(configured_headers) = &self.config.headers {
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
            DEFAULT_PAYLOAD_KEY: json_body
        });

        Ok((data, user_context))
    }

    /// Build an event with completion channel and inject into pipeline.
    async fn inject_event(
        &self,
        data: Value,
        meta: Option<serde_json::Map<String, Value>>,
    ) -> Result<
        tokio::sync::oneshot::Receiver<
            Result<Option<Value>, Box<dyn std::error::Error + Send + Sync>>,
        >,
        Error,
    > {
        let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();

        let mut builder = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(self.config.name.to_owned())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .completion_tx(completion_tx);

        if let Some(meta) = meta {
            builder = builder.meta(meta);
        }

        let e = builder
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })?;

        Ok(completion_rx)
    }

    async fn handle(
        &self,
        headers: HeaderMap,
        request: Request<Body>,
    ) -> Result<StatusCode, Error> {
        let (data, user_context) = match self.parse_request(&headers, request).await {
            Ok(result) => result,
            Err(Error::FlowCompletionFailed) => return Ok(StatusCode::SERVICE_UNAVAILABLE),
            Err(
                e
                @ (Error::NoCredentials | Error::InvalidCredentials | Error::MalformedCredentials),
            ) => {
                return Ok(e.into_response().status());
            }
            Err(e) => return Err(e),
        };

        let meta = user_context.map(|ctx| {
            let mut meta = serde_json::Map::new();
            if let Ok(value) = serde_json::to_value(ctx) {
                meta.insert(flowgen_core::auth::AUTH.to_string(), value);
            }
            meta
        });

        let completion_rx = self.inject_event(data, meta).await?;

        // Wait for flow completion before responding to HTTP request.
        match self.config.ack_timeout {
            Some(timeout) => match tokio::time::timeout(timeout, completion_rx).await {
                Ok(Ok(Ok(_))) => Ok(StatusCode::OK),
                Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                    error!("{}", Error::FlowCompletionFailed);
                    Ok(StatusCode::INTERNAL_SERVER_ERROR)
                }
            },
            None => {
                // No timeout configured, wait indefinitely.
                match completion_rx.await {
                    Ok(Ok(_)) => Ok(StatusCode::OK),
                    Ok(Err(_)) | Err(_) => {
                        error!("{}", Error::FlowCompletionFailed);
                        Ok(StatusCode::INTERNAL_SERVER_ERROR)
                    }
                }
            }
        }
    }

    /// Handle an incoming request with SSE streaming response.
    async fn handle_stream(
        &self,
        headers: HeaderMap,
        request: Request<Body>,
    ) -> Result<axum::response::Response, Error> {
        let (data, user_context) = match self.parse_request(&headers, request).await {
            Ok(result) => result,
            Err(Error::FlowCompletionFailed) => {
                return Ok(StatusCode::SERVICE_UNAVAILABLE.into_response());
            }
            Err(
                e
                @ (Error::NoCredentials | Error::InvalidCredentials | Error::MalformedCredentials),
            ) => {
                return Ok(e.into_response());
            }
            Err(e) => return Err(e),
        };

        let correlation_id = uuid::Uuid::new_v4().to_string();

        let (progress_tx, mut progress_rx) = mpsc::channel::<ProgressEvent>(32);

        let response_registry = match &self.response_registry {
            Some(registry) => registry,
            None => return Err(Error::FlowCompletionFailed),
        };

        response_registry
            .insert(
                correlation_id.clone(),
                ResponseSender {
                    progress_tx,
                    result_tx: None,
                },
            )
            .await;

        // Build meta with correlation_id and optional user context.
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

        let completion_rx = self.inject_event(data, Some(meta)).await?;

        info!(
            webhook = %self.config.name,
            correlation_id = %correlation_id,
            "Streaming webhook request accepted."
        );

        // Spawn background task to stream progress + final result as SSE.
        let registry = response_registry.clone();
        let cid = correlation_id.clone();
        let ack_timeout = self.config.ack_timeout;
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

                        // Drain remaining progress events.
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

            // Send final result.
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
}

/// HTTP webhook processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Option<Sender<Event>>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Shared response registry for SSE streaming.
    response_registry: Option<Arc<ResponseRegistry>>,
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

        // Resolve credentials: task-level overrides global.
        let server_credentials_path = self
            .task_context
            .http_server
            .as_ref()
            .and_then(|server| server.as_any().downcast_ref::<super::server::HttpServer>())
            .and_then(|server| server.credentials_path())
            .map(|p| p.to_path_buf());

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

        // Get auth provider from the worker-level HTTP server.
        let auth_provider = self
            .task_context
            .http_server
            .as_ref()
            .and_then(|server| server.auth_provider());

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            credentials,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            response_registry: self
                .response_registry
                .clone()
                .or_else(|| self.task_context.response_registry.clone()),
            auth_provider,
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

        let config = Arc::clone(&self.config);
        let span = tracing::Span::current();
        let stream = config.stream;

        let method_router: MethodRouter = if stream {
            let handler = move |headers: HeaderMap, request: Request<Body>| {
                let span = span.clone();
                let event_handler = event_handler.clone();
                async move { event_handler.handle_stream(headers, request).await }.instrument(span)
            };
            match config.method {
                crate::config::Method::Get => MethodRouter::new().get(handler),
                crate::config::Method::Post => MethodRouter::new().post(handler),
                crate::config::Method::Put => MethodRouter::new().put(handler),
                crate::config::Method::Delete => MethodRouter::new().delete(handler),
                crate::config::Method::Patch => MethodRouter::new().patch(handler),
                crate::config::Method::Head => MethodRouter::new().head(handler),
            }
        } else {
            let handler = move |headers: HeaderMap, request: Request<Body>| {
                let span = span.clone();
                let event_handler = event_handler.clone();
                async move { event_handler.handle(headers, request).await }.instrument(span)
            };
            match config.method {
                crate::config::Method::Get => MethodRouter::new().get(handler),
                crate::config::Method::Post => MethodRouter::new().post(handler),
                crate::config::Method::Put => MethodRouter::new().put(handler),
                crate::config::Method::Delete => MethodRouter::new().delete(handler),
                crate::config::Method::Patch => MethodRouter::new().patch(handler),
                crate::config::Method::Head => MethodRouter::new().head(handler),
            }
        };

        if let Some(http_server) = &self.task_context.http_server {
            http_server
                .register_route(config.endpoint.clone(), Box::new(method_router))
                .await;
        }

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
            response_registry: self.response_registry,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value};
    use std::collections::HashMap;
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
            stream: false,
            auth: None,
            depends_on: None,
            retry: None,
        });
        let (tx, _rx) = mpsc::channel(100);

        // Success case.
        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
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

    #[test]
    fn test_event_handler_structure() {
        let (tx, _rx) = mpsc::channel(1);
        let config = Arc::new(crate::config::Processor::default());

        let _handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 0,
            credentials: None,
            task_type: "test",
            task_context: create_mock_task_context(),
            response_registry: None,
            auth_provider: None,
        };
    }

    #[test]
    fn test_event_handler_with_configured_headers() {
        let mut configured_headers = HashMap::new();
        configured_headers.insert("x-custom-header".to_string(), "".to_string());
        configured_headers.insert("x-request-id".to_string(), "".to_string());

        let config = Arc::new(crate::config::Processor {
            name: "test_webhook".to_string(),
            endpoint: "/webhook".to_string(),
            method: crate::config::Method::Post,
            payload: None,
            headers: Some(configured_headers),
            credentials_path: None,
            ack_timeout: None,
            stream: false,
            auth: None,
            depends_on: None,
            retry: None,
        });

        let (tx, _rx) = mpsc::channel(100);

        let handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            credentials: None,
            task_type: "test",
            task_context: create_mock_task_context(),
            response_registry: None,
            auth_provider: None,
        };

        assert!(handler.config.headers.is_some());
        let headers = handler.config.headers.as_ref().unwrap();
        assert_eq!(headers.len(), 2);
        assert!(headers.contains_key("x-custom-header"));
        assert!(headers.contains_key("x-request-id"));
    }

    #[test]
    fn test_event_handler_without_configured_headers() {
        let config = Arc::new(crate::config::Processor {
            name: "test_webhook".to_string(),
            endpoint: "/webhook".to_string(),
            method: crate::config::Method::Post,
            payload: None,
            headers: None,
            credentials_path: None,
            ack_timeout: None,
            stream: false,
            auth: None,
            depends_on: None,
            retry: None,
        });

        let (tx, _rx) = mpsc::channel(100);

        let handler = EventHandler {
            config,
            tx: Some(tx),
            task_id: 1,
            credentials: None,
            task_type: "test",
            task_context: create_mock_task_context(),
            response_registry: None,
            auth_provider: None,
        };

        assert!(handler.config.headers.is_none());
    }
}
