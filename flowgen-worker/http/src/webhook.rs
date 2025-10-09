//! HTTP webhook processor for handling incoming requests.
//!
//! Processes incoming HTTP webhook requests, extracting headers and payload
//! data and converting them into events for further processing in the pipeline.

use axum::{body::Body, extract::Request, response::IntoResponse, routing::MethodRouter};
use flowgen_core::event::{
    generate_subject, Event, EventBuilder, EventData, SubjectSuffix, DEFAULT_LOG_MESSAGE,
};
use reqwest::{header::HeaderMap, StatusCode};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tracing::{event, Level};

/// Default subject for webhook events.
const DEFAULT_MESSAGE_SUBJECT: &str = "http.webhook.in";
/// JSON key for HTTP headers in webhook events.
const DEFAULT_HEADERS_KEY: &str = "headers";
/// JSON key for HTTP payload in webhook events.
const DEFAULT_PAYLOAD_KEY: &str = "payload";

/// Errors that can occur during webhook processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to send event message.
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    /// Event building or processing failed.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// JSON serialization/deserialization failed.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    /// Axum HTTP processing failed.
    #[error(transparent)]
    Axum(#[from] axum::Error),
    /// Input/output operation failed.
    #[error(transparent)]
    IO(#[from] std::io::Error),
    /// Required configuration attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

impl IntoResponse for Error {
    fn into_response(self) -> axum::response::Response {
        let status = match &self {
            Error::SerdeJson(_) | Error::Axum(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        };
        event!(Level::ERROR, "webhook error: {}", self);
        status.into_response()
    }
}

/// Event handler for processing incoming webhooks.
#[derive(Clone, Debug)]
struct EventHandler {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Sender<Event>,
    /// Current task identifier.
    current_task_id: usize,
}

impl EventHandler {
    async fn handle(
        &self,
        headers: HeaderMap,
        request: Request<Body>,
    ) -> Result<StatusCode, Error> {
        let body = axum::body::to_bytes(request.into_body(), usize::MAX).await?;

        let json_body = match body.is_empty() {
            true => Value::Null,
            false => serde_json::from_slice(&body)?,
        };

        let mut headers_map = Map::new();
        for (key, value) in headers.iter() {
            headers_map.insert(
                key.as_str().to_string(),
                Value::String(value.to_str().unwrap_or("").to_string()),
            );
        }

        let data = json!({
            DEFAULT_HEADERS_KEY: Value::Object(headers_map),
            DEFAULT_PAYLOAD_KEY: json_body
        });

        let subject = generate_subject(
            self.config.label.as_deref(),
            DEFAULT_MESSAGE_SUBJECT,
            SubjectSuffix::Timestamp,
        );

        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(subject)
            .current_task_id(self.current_task_id)
            .build()?;

        event!(Level::INFO, "{}: {}", DEFAULT_LOG_MESSAGE, e.subject);
        self.tx.send(e)?;
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
    /// Current task identifier.
    current_task_id: usize,
    /// Shared HTTP server instance.
    http_server: Arc<super::server::HttpServer>,
}

impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        let config = Arc::clone(&self.config);
        let event_handler = EventHandler {
            config: self.config,
            tx: self.tx,
            current_task_id: self.current_task_id,
        };

        let handler = move |headers: HeaderMap, request: Request<Body>| async move {
            event_handler.handle(headers, request).await
        };

        let method_router = match config.method {
            crate::config::Method::GET => MethodRouter::new().get(handler),
            crate::config::Method::POST => MethodRouter::new().post(handler),
            crate::config::Method::PUT => MethodRouter::new().put(handler),
            crate::config::Method::DELETE => MethodRouter::new().delete(handler),
            crate::config::Method::PATCH => MethodRouter::new().patch(handler),
            crate::config::Method::HEAD => MethodRouter::new().head(handler),
        };

        self.http_server
            .register_route(config.endpoint.clone(), method_router)
            .await;

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
    /// Current task identifier.
    current_task_id: usize,
    /// Optional HTTP server instance.
    http_server: Option<Arc<super::server::HttpServer>>,
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

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub fn http_server(mut self, server: Arc<super::server::HttpServer>) -> Self {
        self.http_server = Some(server);
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
            current_task_id: self.current_task_id,
            http_server: self
                .http_server
                .ok_or_else(|| Error::MissingRequiredAttribute("http_server".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use tokio::sync::broadcast;

    #[test]
    fn test_error_from_serde_json_error() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error: Error = json_error.into();
        assert!(matches!(error, Error::SerdeJson(_)));
    }

    #[test]
    fn test_error_from_io_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error: Error = io_error.into();
        assert!(matches!(error, Error::IO(_)));
    }

    #[test]
    fn test_error_into_response() {
        let err =
            Error::SerdeJson(serde_json::from_str::<serde_json::Value>("invalid").unwrap_err());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let err = Error::MissingRequiredAttribute("test".to_string());
        let response = err.into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn test_processor_builder_new() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert_eq!(builder.current_task_id, 0);
        assert!(builder.http_server.is_none());
    }

    #[tokio::test]
    async fn test_processor_builder_default() {
        let builder = ProcessorBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert_eq!(builder.current_task_id, 0);
        assert!(builder.http_server.is_none());
    }

    #[tokio::test]
    async fn test_processor_builder_config() {
        let config = Arc::new(crate::config::Processor {
            label: Some("webhook_test".to_string()),
            endpoint: "/webhook".to_string(),
            method: crate::config::Method::POST,
            payload: None,
            headers: None,
            credentials: None,
        });

        let builder = ProcessorBuilder::new().config(config.clone());
        assert_eq!(builder.config, Some(config));
    }

    #[tokio::test]
    async fn test_processor_builder_sender() {
        let (tx, _rx) = broadcast::channel(100);
        let builder = ProcessorBuilder::new().sender(tx);
        assert!(builder.tx.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_current_task_id() {
        let builder = ProcessorBuilder::new().current_task_id(42);
        assert_eq!(builder.current_task_id, 42);
    }

    #[tokio::test]
    async fn test_processor_builder_http_server() {
        let server = Arc::new(crate::server::HttpServer::new());
        let builder = ProcessorBuilder::new().http_server(server.clone());
        assert!(builder.http_server.is_some());
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_config() {
        let (tx, _rx) = broadcast::channel(100);
        let server = Arc::new(crate::server::HttpServer::new());

        let result = ProcessorBuilder::new()
            .sender(tx)
            .http_server(server)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_sender() {
        let config = Arc::new(crate::config::Processor {
            label: None,
            endpoint: "/test".to_string(),
            method: crate::config::Method::GET,
            payload: None,
            headers: None,
            credentials: None,
        });
        let server = Arc::new(crate::server::HttpServer::new());

        let result = ProcessorBuilder::new()
            .config(config)
            .http_server(server)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "sender")
        );
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_http_server() {
        let (tx, _rx) = broadcast::channel(100);
        let config = Arc::new(crate::config::Processor {
            label: None,
            endpoint: "/test".to_string(),
            method: crate::config::Method::GET,
            payload: None,
            headers: None,
            credentials: None,
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "http_server")
        );
    }

    #[tokio::test]
    async fn test_processor_builder_build_success() {
        let (tx, _rx) = broadcast::channel(100);
        let config = Arc::new(crate::config::Processor {
            label: Some("success_webhook".to_string()),
            endpoint: "/success".to_string(),
            method: crate::config::Method::POST,
            payload: Some(crate::config::Payload {
                object: None,
                input: Some("{\"webhook\": \"data\"}".to_string()),
                send_as: crate::config::PayloadSendAs::Json,
            }),
            headers: Some({
                let mut headers = HashMap::new();
                headers.insert("X-Webhook".to_string(), "test".to_string());
                headers
            }),
            credentials: None,
        });
        let server = Arc::new(crate::server::HttpServer::new());

        let result = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx)
            .http_server(server.clone())
            .current_task_id(5)
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.config, config);
        assert_eq!(processor.current_task_id, 5);
    }

    #[tokio::test]
    async fn test_processor_builder_chain() {
        let (tx, _rx) = broadcast::channel(50);
        let config = Arc::new(crate::config::Processor {
            label: Some("chain_webhook".to_string()),
            endpoint: "/chain".to_string(),
            method: crate::config::Method::PUT,
            payload: None,
            headers: None,
            credentials: None,
        });
        let server = Arc::new(crate::server::HttpServer::new());

        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx)
            .http_server(server)
            .current_task_id(10)
            .build()
            .await
            .unwrap();

        assert_eq!(processor.config, config);
        assert_eq!(processor.current_task_id, 10);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "http.webhook.in");
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
            current_task_id: 0,
        };
    }
}
