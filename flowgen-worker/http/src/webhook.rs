//! HTTP webhook processor for handling incoming requests.
//!
//! Processes incoming HTTP webhook requests, extracting headers and payload
//! data and converting them into events for further processing in the pipeline.

use crate::config::Credentials;
use axum::{body::Body, extract::Request, response::IntoResponse, routing::MethodRouter};
use base64::{engine::general_purpose::STANDARD, Engine};
use flowgen_core::event::{Event, EventBuilder, EventData, SenderExt};
use reqwest::{header::HeaderMap, StatusCode};
use serde_json::{json, Map, Value};
use std::{fs, sync::Arc};
use tokio::sync::broadcast::Sender;
use tracing::{error, Instrument};

/// JSON key for HTTP headers in webhook events.
const DEFAULT_HEADERS_KEY: &str = "headers";
/// JSON key for HTTP payload in webhook events.
const DEFAULT_PAYLOAD_KEY: &str = "payload";

/// Errors that can occur during webhook processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Webhook event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("JSON serialization/deserialization failed with error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Axum HTTP processing failed with error: {source}")]
    Axum {
        #[source]
        source: axum::Error,
    },
    #[error("Failed to read credentials file at {path} with error: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Task manager failed with error: {source}")]
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
    MissingRequiredAttribute(String),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Error::SerdeJson { .. } | Error::Axum { .. } => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        error!("webhook error: {}", self);
        status.into_response()
    }
}

/// Event handler for processing incoming webhooks.
#[derive(Clone, Debug)]
pub struct EventHandler {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Sender<Event>,
    /// Task identifier.
    task_id: usize,
    /// Pre-loaded authentication credentials.
    credentials: Option<Credentials>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Validate authentication credentials against incoming request headers.
    fn validate_authentication(&self, headers: &HeaderMap) -> Result<(), Error> {
        let credentials = match &self.credentials {
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

        // Check bearer authentication.
        if let Some(expected_token) = &credentials.bearer_auth {
            match auth_value.strip_prefix("Bearer ") {
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

    async fn handle(
        &self,
        headers: HeaderMap,
        request: Request<Body>,
    ) -> Result<StatusCode, Error> {
        // Validate the authentication and return error if request is not authorized.
        if let Err(auth_error) = self.validate_authentication(&headers) {
            error!("Webhook authentication failed for: {}", auth_error);
            return Ok(StatusCode::UNAUTHORIZED);
        }

        let body = axum::body::to_bytes(request.into_body(), usize::MAX)
            .await
            .map_err(|e| Error::Axum { source: e })?;

        let json_body = match body.is_empty() {
            true => Value::Null,
            false => serde_json::from_slice(&body).map_err(|e| Error::SerdeJson { source: e })?,
        };

        // Only store headers that are specified in the configuration.
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

        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(self.config.name.to_owned())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        self.tx
            .send_with_logging(e)
            .map_err(|source| Error::SendMessage { source })?;
        Ok(StatusCode::OK)
    }
}

/// HTTP webhook processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Sender<Event>,
    /// Task identifier.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = Arc::clone(&self.config);

        // Load credentials at task creation time.
        let credentials = match &config.credentials_path {
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
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            credentials,
            task_type: self.task_type,
            _task_context: Arc::clone(&self._task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        // Initialize runner task.
        let event_handler = match self.init().await {
            Ok(handler) => handler,
            Err(e) => {
                error!("{}", e);
                return Ok(());
            }
        };

        let config = Arc::clone(&self.config);
        let span = tracing::Span::current();
        let handler = move |headers: HeaderMap, request: Request<Body>| {
            let span = span.clone();
            let event_handler = event_handler.clone();
            async move { event_handler.handle(headers, request).await }.instrument(span)
        };

        let method_router = match config.method {
            crate::config::Method::GET => MethodRouter::new().get(handler),
            crate::config::Method::POST => MethodRouter::new().post(handler),
            crate::config::Method::PUT => MethodRouter::new().put(handler),
            crate::config::Method::DELETE => MethodRouter::new().delete(handler),
            crate::config::Method::PATCH => MethodRouter::new().patch(handler),
            crate::config::Method::HEAD => MethodRouter::new().head(handler),
        };

        if let Some(http_server) = &self._task_context.http_server {
            // Downcast the trait object to the concrete HttpServer type
            if let Some(server) = http_server
                .as_any()
                .downcast_ref::<super::server::HttpServer>()
            {
                server
                    .register_route(config.endpoint.clone(), method_router)
                    .await;
            }
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

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self.task_id,
            _task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Map, Value};
    use std::collections::HashMap;
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(flowgen_core::task::manager::TaskManagerBuilder::new().build());
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
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

        let err = Error::MissingRequiredAttribute("test".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::config::Processor {
            name: "test_webhook".to_string(),
            endpoint: "/webhook".to_string(),
            method: crate::config::Method::POST,
            payload: None,
            headers: None,
            credentials_path: None,
        });
        let (tx, _rx) = broadcast::channel(100);

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
        let (tx2, _rx2) = broadcast::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_HEADERS_KEY, "headers");
        assert_eq!(DEFAULT_PAYLOAD_KEY, "payload");
    }

    #[test]
    fn test_event_handler_structure() {
        let (tx, _rx) = broadcast::channel(1);
        let config = Arc::new(crate::config::Processor::default());

        let _handler = EventHandler {
            config,
            tx,
            task_id: 0,
            credentials: None,
            task_type: "test",
            _task_context: create_mock_task_context(),
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
            method: crate::config::Method::POST,
            payload: None,
            headers: Some(configured_headers),
            credentials_path: None,
        });

        let (tx, _rx) = broadcast::channel(100);

        let handler = EventHandler {
            config,
            tx,
            task_id: 1,
            credentials: None,
            task_type: "test",
            _task_context: create_mock_task_context(),
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
            method: crate::config::Method::POST,
            payload: None,
            headers: None,
            credentials_path: None,
        });

        let (tx, _rx) = broadcast::channel(100);

        let handler = EventHandler {
            config,
            tx,
            task_id: 1,
            credentials: None,
            task_type: "test",
            _task_context: create_mock_task_context(),
        };

        assert!(handler.config.headers.is_none());
    }
}
