use axum::{body::Body, extract::Request, response::IntoResponse, routing::MethodRouter};
use chrono::Utc;
use flowgen_core::event::{Event, EventBuilder, EventData};
use reqwest::{header::HeaderMap, StatusCode};
use serde_json::{json, Map, Value};
use std::sync::Arc;
use tokio::sync::broadcast::Sender;
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "http.webhook.in";
const DEFAULT_HEADERS_KEY: &str = "headers";
const DEFAULT_PAYLOAD_KEY: &str = "payload";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Axum(#[from] axum::Error),
    #[error(transparent)]
    IO(#[from] std::io::Error),
    #[error("missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

/// Implements Axum IntoResponse trait so that errors
/// can be propagated from handle function.
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

/// Handles processing of incoming HTTP webhooks.
#[derive(Clone, Debug)]
struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Channel sender for processed events.
    tx: Sender<Event>,
    /// Task identifier for event tracking.
    current_task_id: usize,
}

impl EventHandler {
    async fn handle(
        &self,
        headers: HeaderMap,
        request: Request<Body>,
    ) -> Result<StatusCode, Error> {
        // Get bytes out of the body.
        let body = axum::body::to_bytes(request.into_body(), usize::MAX).await?;

        // Convert bytes body to JSON object.
        let json_body = match body.is_empty() {
            true => Value::Null,
            false => serde_json::from_slice(&body)?,
        };

        // Convert headers to a JSON object.
        let mut headers_map = Map::new();
        for (key, value) in headers.iter() {
            headers_map.insert(
                key.as_str().to_string(),
                Value::String(value.to_str().unwrap_or("").to_string()),
            );
        }

        // Merge headers and body into one JSON value.
        let data = json!({
            DEFAULT_HEADERS_KEY: Value::Object(headers_map),
            DEFAULT_PAYLOAD_KEY: json_body
        });

        // Create event subject.
        let timestamp = Utc::now().timestamp_micros();
        let subject = match &self.config.label {
            Some(label) => format!(
                "{}.{}.{}",
                DEFAULT_MESSAGE_SUBJECT,
                label.to_lowercase(),
                timestamp
            ),
            None => format!("{DEFAULT_MESSAGE_SUBJECT}.{timestamp}"),
        };

        // Send processor output as event.
        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(subject.clone())
            .current_task_id(self.current_task_id)
            .build()?;

        self.tx.send(e)?;
        event!(Level::INFO, "Event processed: {}", subject);
        Ok(StatusCode::OK)
    }
}

#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Processor>,
    tx: Sender<Event>,
    current_task_id: usize,
    http_server: Arc<super::server::HttpServer>,
}

impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    async fn run(self) -> Result<(), Error> {
        // Init event handler with required setup.
        let config = Arc::clone(&self.config);
        let event_handler = EventHandler {
            config: self.config,
            tx: self.tx,
            current_task_id: self.current_task_id,
        };

        // Prepare closure for MethodRouter.
        let handler = move |headers: HeaderMap, request: Request<Body>| async move {
            event_handler.handle(headers, request).await
        };

        // Handle routing according to selected method.
        let method_router = match config.method {
            crate::config::Method::GET => MethodRouter::new().get(handler),
            crate::config::Method::POST => MethodRouter::new().post(handler),
            crate::config::Method::PUT => MethodRouter::new().put(handler),
            crate::config::Method::DELETE => MethodRouter::new().delete(handler),
            crate::config::Method::PATCH => MethodRouter::new().patch(handler),
            crate::config::Method::HEAD => MethodRouter::new().head(handler),
        };

        // Register route with the shared HTTP Server.
        self.http_server
            .register_route(config.endpoint.clone(), method_router)
            .await;

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Processor>>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
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
