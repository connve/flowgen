//! HTTP request processor for making outbound HTTP calls.
//!
//! Handles HTTP request processing with authentication, headers,
//! and various payload formats. Processes events by making HTTP requests
//! and publishing the responses as new events.

use crate::config::Credentials;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, SenderExt},
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::{
    fs,
    sync::broadcast::{Receiver, Sender},
};
use tracing::{error, Instrument};

/// Errors that can occur during HTTP request processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Sending event to channel failed with error: {source}")]
    SendMessage {
        #[source]
        source: Box<tokio::sync::broadcast::error::SendError<Event>>,
    },
    #[error("Request event builder failed with error: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to read credentials file at {path} with error: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("JSON serialization/deserialization failed with error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Configuration template rendering failed with error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("HTTP request failed with error: {source}. This may be caused by invalid headers, malformed URL, unsupported request body format, or network issues")]
    Reqwest {
        #[source]
        source: reqwest::Error,
    },
    #[error("Invalid HTTP header name with error: {source}")]
    ReqwestInvalidHeaderName {
        #[source]
        source: reqwest::header::InvalidHeaderName,
    },
    #[error("Invalid HTTP header value with error: {source}")]
    ReqwestInvalidHeaderValue {
        #[source]
        source: reqwest::header::InvalidHeaderValue,
    },
    #[error("Host coordination failed with error: {source}")]
    Host {
        #[source]
        source: flowgen_core::host::Error,
    },
    #[error("Either payload json or payload input is required")]
    PayloadConfig,
    #[error("Event data was not found on the event payload")]
    MissingEventData,
    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Event handler for processing HTTP requests.
#[derive(Debug)]
pub struct EventHandler {
    /// HTTP client instance.
    client: Arc<reqwest::Client>,
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Sender<Event>,
    /// Current task identifier.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    _task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by making an HTTP request.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.task_id.checked_sub(1) {
            return Ok(());
        }

        // Render config with to support templates inside configuration.
        let event_value = serde_json::value::Value::try_from(&event)
            .map_err(|source| Error::EventBuilder { source })?;
        let config = self
            .config
            .render(&event_value)
            .map_err(|source| Error::ConfigRender { source })?;

        let mut client = match config.method {
            crate::config::Method::GET => self.client.get(config.endpoint),
            crate::config::Method::POST => self.client.post(config.endpoint),
            crate::config::Method::PUT => self.client.put(config.endpoint),
            crate::config::Method::DELETE => self.client.delete(config.endpoint),
            crate::config::Method::PATCH => self.client.patch(config.endpoint),
            crate::config::Method::HEAD => self.client.head(config.endpoint),
        };

        if let Some(headers) = config.headers.to_owned() {
            let mut header_map = HeaderMap::new();
            for (key, value) in headers {
                let header_name = HeaderName::try_from(key)
                    .map_err(|source| Error::ReqwestInvalidHeaderName { source })?;
                let header_value = HeaderValue::try_from(value)
                    .map_err(|source| Error::ReqwestInvalidHeaderValue { source })?;
                header_map.insert(header_name, header_value);
            }
            client = client.headers(header_map);
        }

        if let Some(payload) = &config.payload {
            let event_data = if payload.from_event {
                event_value
                    .get("event")
                    .and_then(|e| e.get("data"))
                    .ok_or_else(|| Error::MissingEventData)?
            } else {
                &match &payload.object {
                    Some(obj) => Value::Object(obj.to_owned()),
                    None => match &payload.input {
                        Some(input) => serde_json::from_str::<serde_json::Value>(input.as_str())
                            .map_err(|source| Error::SerdeJson { source })?,
                        None => return Err(Error::PayloadConfig),
                    },
                }
            };

            client = match payload.send_as {
                crate::config::PayloadSendAs::Json => client.json(&event_data),
                crate::config::PayloadSendAs::UrlEncoded => client.form(&event_data),
                crate::config::PayloadSendAs::QueryParams => client.query(&event_data),
            };
        }

        if let Some(credentials_path) = &config.credentials_path {
            let credentials_string =
                fs::read_to_string(credentials_path)
                    .await
                    .map_err(|e| Error::ReadCredentials {
                        path: credentials_path.clone(),
                        source: e,
                    })?;
            let credentials: Credentials = serde_json::from_str(&credentials_string)
                .map_err(|source| Error::SerdeJson { source })?;

            if let Some(bearer_token) = credentials.bearer_auth {
                client = client.bearer_auth(bearer_token);
            }

            if let Some(basic_auth) = credentials.basic_auth {
                client = client.basic_auth(basic_auth.username, Some(basic_auth.password));
            }
        };

        let resp = client
            .send()
            .await
            .map_err(|source| Error::Reqwest { source })?
            .text()
            .await
            .map_err(|source| Error::Reqwest { source })?;

        let data = serde_json::from_str::<Value>(&resp).unwrap_or_else(|_| json!(resp));

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

    /// Initializes the processor by building the HTTP client.
    async fn init(&self) -> Result<EventHandler, Error> {
        let client = reqwest::ClientBuilder::new()
            .https_only(true)
            .build()
            .map_err(|e| Error::Reqwest { source: e })?;
        let client = Arc::new(client);

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            client,
            task_type: self.task_type,
            _task_context: Arc::clone(&self._task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self._task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!("{}", e);
                    Err(e)
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                error!(
                    "{}",
                    Error::RetryExhausted {
                        source: Box::new(e)
                    }
                );
                return Ok(());
            }
        };

        loop {
            match self.rx.recv().await {
                Ok(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    tokio::spawn(
                        async move {
                            if let Err(err) = event_handler.handle(event).await {
                                error!("{}", err);
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                }
                Err(_) => return Ok(()),
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
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Current task identifier.
    task_id: usize,
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

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
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
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
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
    use crate::config::BasicAuth;
    use serde_json::Map;
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
    fn test_error_from_serde_json_error() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error = Error::SerdeJson { source: json_error };
        assert!(matches!(error, Error::SerdeJson { .. }));
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://test.com".to_string(),
            method: crate::config::Method::GET,
            payload: None,
            headers: None,
            credentials_path: None,
            retry: None,
        });
        let (tx, rx) = broadcast::channel(100);

        // Success case.
        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .receiver(rx)
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(processor.is_ok());

        // Error case - missing config.
        let (tx2, rx2) = broadcast::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingRequiredAttribute(_)
        ));
    }
}
