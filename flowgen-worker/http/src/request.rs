//! HTTP request processor for making outbound HTTP calls.
//!
//! Handles HTTP request processing with authentication, headers,
//! and various payload formats. Processes events by making HTTP requests
//! and publishing the responses as new events.

use crate::config::Credentials;
use flowgen_core::{
    config::ConfigExt,
    event::{generate_subject, Event, EventBuilder, EventData, SubjectSuffix},
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::{
    fs,
    sync::broadcast::{Receiver, Sender},
};
use tracing::{debug, error, info, Instrument};

/// Default subject for HTTP response events.
const DEFAULT_MESSAGE_SUBJECT: &str = "http_request";

/// Errors that can occur during HTTP request processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Input/output operation failed.
    #[error("IO operation failed on path {path}: {source}")]
    IO {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to send event message.
    #[error(transparent)]
    SendMessage(#[from] tokio::sync::broadcast::error::SendError<Event>),
    /// Event building or processing failed.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// JSON serialization/deserialization failed.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    /// Configuration template rendering failed.
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    /// HTTP request failed.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    /// Invalid HTTP header name.
    #[error(transparent)]
    ReqwestInvalidHeaderName(#[from] reqwest::header::InvalidHeaderName),
    /// Invalid HTTP header value.
    #[error(transparent)]
    ReqwestInvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),
    /// Required configuration attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Payload configuration is invalid.
    #[error("Either payload json or payload input is required")]
    PayloadConfig(),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] flowgen_core::host::Error),
    /// Task manager error.
    #[error(transparent)]
    TaskManager(#[from] flowgen_core::task::manager::Error),
}

/// Event handler for processing HTTP requests.
#[derive(Debug)]
struct EventHandler {
    /// HTTP client instance.
    client: Arc<reqwest::Client>,
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Sender<Event>,
    /// Current task identifier.
    current_task_id: usize,
}

impl EventHandler {
    /// Processes an event by making an HTTP request.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let config = self.config.render(&event.data)?;

        let mut client = match config.method {
            crate::config::Method::GET => self.client.get(config.endpoint),
            crate::config::Method::POST => self.client.post(config.endpoint),
            crate::config::Method::PUT => self.client.put(config.endpoint),
            crate::config::Method::DELETE => self.client.delete(config.endpoint),
            crate::config::Method::PATCH => self.client.patch(config.endpoint),
            crate::config::Method::HEAD => self.client.head(config.endpoint),
        };

        if let Some(headers) = self.config.headers.to_owned() {
            let mut header_map = HeaderMap::new();
            for (key, value) in headers {
                let header_name = HeaderName::try_from(key)?;
                let header_value = HeaderValue::try_from(value)?;
                header_map.insert(header_name, header_value);
            }
            client = client.headers(header_map);
        }

        if let Some(payload) = &self.config.payload {
            let json = match &payload.object {
                Some(obj) => Value::Object(obj.to_owned()),
                None => match &payload.input {
                    Some(input) => serde_json::from_str::<serde_json::Value>(input.as_str())?,
                    None => return Err(Error::PayloadConfig()),
                },
            };

            client = match payload.send_as {
                crate::config::PayloadSendAs::Json => client.json(&json),
                crate::config::PayloadSendAs::UrlEncoded => client.form(&json),
                crate::config::PayloadSendAs::QueryParams => client.query(&json),
            }
        }

        if let Some(credentials) = &self.config.credentials {
            let credentials_string =
                fs::read_to_string(credentials)
                    .await
                    .map_err(|e| Error::IO {
                        path: credentials.clone(),
                        source: e,
                    })?;
            let credentials: Credentials = serde_json::from_str(&credentials_string)?;

            if let Some(bearer_token) = credentials.bearer_auth {
                client = client.bearer_auth(bearer_token);
            }

            if let Some(basic_auth) = credentials.basic_auth {
                client = client.basic_auth(basic_auth.username, Some(basic_auth.password));
            }
        };

        let resp = client.send().await?.text().await?;

        let data = json!(resp);

        let subject = generate_subject(
            Some(&self.config.name),
            DEFAULT_MESSAGE_SUBJECT,
            SubjectSuffix::Timestamp,
        );
        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(subject.clone())
            .current_task_id(self.current_task_id)
            .build()?;

        self.tx.send(e)?;
        info!("event processeds: {}", subject);
        Ok(())
    }
}

/// HTTP request processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Sender<Event>,
    /// Event receiver channel.
    rx: Receiver<Event>,
    /// Current task identifier.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;

    #[tracing::instrument(skip(self), name = DEFAULT_MESSAGE_SUBJECT, fields(task = %self.config.name, task_id = self.current_task_id))]
    async fn run(mut self) -> Result<(), Error> {
        // Register task with task manager.
        let task_id = format!(
            "{}.{}.{}",
            self.task_context.flow.name, DEFAULT_MESSAGE_SUBJECT, self.config.name
        );
        let mut task_manager_rx = self
            .task_context
            .task_manager
            .register(
                task_id,
                Some(flowgen_core::task::manager::LeaderElectionOptions {}),
            )
            .await?;

        let client = reqwest::ClientBuilder::new().https_only(true).build()?;
        let client = Arc::new(client);

        let event_handler = Arc::new(EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.current_task_id,
            tx: self.tx.clone(),
            client,
        });

        loop {
            tokio::select! {
                biased;

                // Check for leadership changes.
                Some(status) = task_manager_rx.recv() => {
                    if status == flowgen_core::task::manager::LeaderElectionResult::NotLeader {
                        debug!("Lost leadership for task: {}", self.config.name);
                        return Ok(());
                    }
                }

                // Process events.
                result = self.rx.recv() => {
                    match result {
                        Ok(event) => {
                            if event.current_task_id == Some(self.current_task_id - 1) {
                                let handler = Arc::clone(&event_handler);
                                tokio::spawn(async move {
                                    if let Err(err) = handler.handle(event).await {
                                        error!("{}", err);
                                    }
                                }.instrument(tracing::Span::current()));
                            }
                        }
                        Err(_) => return Ok(()),
                    }
                }
            }
        }
    }
}

/// Builder for HTTP request processor.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Optional processor configuration.
    config: Option<Arc<super::config::Processor>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Optional event receiver.
    rx: Option<Receiver<Event>>,
    /// Current task identifier.
    current_task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
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

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
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

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            current_task_id: self.current_task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BasicAuth;
    use serde_json::Map;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tokio::sync::broadcast;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_credentials_default() {
        let creds = Credentials::default();
        assert_eq!(creds.bearer_auth, None);
        assert_eq!(creds.basic_auth, None);
    }

    #[test]
    fn test_credentials_creation() {
        let basic_auth = BasicAuth {
            username: "testuser".to_string(),
            password: "testpass".to_string(),
        };

        let creds = Credentials {
            bearer_auth: Some("bearer_token_123".to_string()),
            basic_auth: Some(basic_auth.clone()),
        };

        assert_eq!(creds.bearer_auth, Some("bearer_token_123".to_string()));
        assert_eq!(creds.basic_auth, Some(basic_auth));
    }

    #[test]
    fn test_credentials_serialization() {
        let basic_auth = BasicAuth {
            username: "user".to_string(),
            password: "pass".to_string(),
        };

        let creds = Credentials {
            bearer_auth: Some("token".to_string()),
            basic_auth: Some(basic_auth),
        };

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
    }

    #[test]
    fn test_basic_auth_default() {
        let basic_auth = BasicAuth::default();
        assert_eq!(basic_auth.username, "");
        assert_eq!(basic_auth.password, "");
    }

    #[test]
    fn test_basic_auth_creation() {
        let basic_auth = BasicAuth {
            username: "admin".to_string(),
            password: "secret123".to_string(),
        };

        assert_eq!(basic_auth.username, "admin");
        assert_eq!(basic_auth.password, "secret123");
    }

    #[test]
    fn test_basic_auth_serialization() {
        let basic_auth = BasicAuth {
            username: "test_user".to_string(),
            password: "test_password".to_string(),
        };

        let json = serde_json::to_string(&basic_auth).unwrap();
        let deserialized: BasicAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(basic_auth, deserialized);
    }

    #[test]
    fn test_error_io_structure() {
        use std::path::PathBuf;
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error = Error::IO {
            path: PathBuf::from("/test/file.json"),
            source: io_error,
        };
        assert!(matches!(error, Error::IO { .. }));
    }

    #[test]
    fn test_error_from_serde_json_error() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error: Error = json_error.into();
        assert!(matches!(error, Error::SerdeJson(_)));
    }

    #[tokio::test]
    async fn test_processor_builder_new() {
        let builder = ProcessorBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert!(builder.task_context.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_default() {
        let builder = ProcessorBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.tx.is_none());
        assert!(builder.rx.is_none());
        assert!(builder.task_context.is_none());
        assert_eq!(builder.current_task_id, 0);
    }

    #[tokio::test]
    async fn test_processor_builder_config() {
        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://api.example.com".to_string(),
            method: crate::config::Method::POST,
            payload: None,
            headers: None,
            credentials: None,
        });

        let builder = ProcessorBuilder::new().config(config.clone());
        assert_eq!(builder.config, Some(config));
    }

    #[tokio::test]
    async fn test_processor_builder_receiver() {
        let (_tx, rx) = broadcast::channel(100);
        let builder = ProcessorBuilder::new().receiver(rx);
        assert!(builder.rx.is_some());
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
    async fn test_processor_builder_build_missing_config() {
        let (tx, rx) = broadcast::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx)
            .receiver(rx)
            .current_task_id(1)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_receiver() {
        let (tx, _rx) = broadcast::channel(100);
        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://test.com".to_string(),
            method: crate::config::Method::GET,
            payload: None,
            headers: None,
            credentials: None,
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .current_task_id(1)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "receiver")
        );
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_sender() {
        let (_tx, rx) = broadcast::channel(100);
        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://test.com".to_string(),
            method: crate::config::Method::GET,
            payload: None,
            headers: None,
            credentials: None,
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .current_task_id(1)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "sender")
        );
    }

    #[tokio::test]
    async fn test_processor_builder_build_missing_task_context() {
        let (tx, rx) = broadcast::channel(100);
        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://test.com".to_string(),
            method: crate::config::Method::GET,
            payload: None,
            headers: None,
            credentials: None,
        });

        let result = ProcessorBuilder::new()
            .config(config)
            .sender(tx)
            .receiver(rx)
            .current_task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "task_context")
        );
    }

    #[tokio::test]
    async fn test_processor_builder_build_success() {
        let (tx, rx) = broadcast::channel(100);
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());

        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://success.test.com".to_string(),
            method: crate::config::Method::POST,
            payload: Some(crate::config::Payload {
                object: None,
                input: Some("{\"test\": \"data\"}".to_string()),
                send_as: crate::config::PayloadSendAs::Json,
            }),
            headers: Some(headers),
            credentials: Some(PathBuf::from("/test/creds.json")),
        });

        let result = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx)
            .receiver(rx)
            .current_task_id(5)
            .task_context(create_mock_task_context())
            .build()
            .await;

        assert!(result.is_ok());
        let processor = result.unwrap();
        assert_eq!(processor.config, config);
        assert_eq!(processor.current_task_id, 5);
    }

    #[tokio::test]
    async fn test_processor_builder_chain() {
        let (tx, rx) = broadcast::channel(50);
        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://chain.test.com".to_string(),
            method: crate::config::Method::PUT,
            payload: None,
            headers: None,
            credentials: None,
        });

        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx)
            .receiver(rx)
            .current_task_id(10)
            .task_context(create_mock_task_context())
            .build()
            .await
            .unwrap();

        assert_eq!(processor.config, config);
        assert_eq!(processor.current_task_id, 10);
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_MESSAGE_SUBJECT, "http.response.out");
    }

    #[test]
    fn test_event_handler_structure() {
        let (tx, _rx) = broadcast::channel(1);
        let config = Arc::new(crate::config::Processor::default());
        let client = Arc::new(reqwest::Client::new());

        let _handler = EventHandler {
            client,
            config,
            tx,
            current_task_id: 0,
        };
    }
}
