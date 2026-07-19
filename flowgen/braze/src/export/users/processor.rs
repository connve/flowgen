//! Processor for the Braze `export_user_ids` task.
//!
//! Calls `POST /users/export/ids` once per incoming event and emits the Braze
//! response as a JSON event downstream.

use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during Braze export user IDs processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error sending event to channel.
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        /// Underlying event send error.
        #[source]
        source: flowgen_core::event::Error,
    },
    /// Error building event.
    #[error("Error building event: {source}")]
    EventBuilder {
        /// Underlying event builder error.
        #[source]
        source: flowgen_core::event::Error,
    },
    /// Failed to convert the incoming event to JSON for template rendering.
    #[error("Failed to convert event to JSON: {source}")]
    EventToJson {
        /// Underlying conversion error.
        #[source]
        source: flowgen_core::event::Error,
    },
    /// JSON serialization or deserialization error.
    #[error("JSON error: {source}")]
    SerdeJson {
        /// Underlying serde error.
        #[source]
        source: serde_json::Error,
    },
    /// Config template rendering error.
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        /// Underlying render error.
        #[source]
        source: flowgen_core::config::Error,
    },
    /// Identifier template failed to render against the event.
    #[error("Failed to render identifier template: {source}")]
    IdentifierRender {
        /// Underlying render error.
        #[source]
        source: flowgen_core::config::Error,
    },
    /// Failed to initialize the Braze client.
    #[error("Braze client initialization failed: {source}")]
    ClientInit {
        /// Underlying Braze SDK error.
        #[source]
        source: braze::Error,
    },
    /// Braze API returned an error.
    #[error("Braze API error: {source}")]
    BrazeApi {
        /// Underlying Braze SDK error.
        #[source]
        source: braze::Error,
    },
    /// No identifiers were provided or all resolved to empty values.
    #[error("No identifiers provided for Braze user export")]
    MissingIdentifiers,
    /// Missing required builder attribute.
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    /// Task failed after all retry attempts.
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        /// Original error that caused retry exhaustion.
        #[source]
        source: Box<Error>,
    },
    /// Client registry type mismatch.
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

impl Error {
    /// Whether this error is a permanent configuration or data error that
    /// should not be retried.
    fn is_permanent(&self) -> bool {
        matches!(
            self,
            Error::ConfigRender { .. }
                | Error::IdentifierRender { .. }
                | Error::SerdeJson { .. }
                | Error::EventToJson { .. }
                | Error::MissingIdentifiers
                | Error::MissingBuilderAttribute(_)
                | Error::ClientRegistryMismatch
        )
    }
}

/// Event handler for processing individual export user IDs events.
pub struct EventHandler {
    client: Arc<braze::Client>,
    config: Arc<super::config::Processor>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(activity = true, duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventToJson { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            let request = build_request(&config, &event_value)?;

            let response = self
                .client
                .export()
                .users_by_ids(&request)
                .send()
                .await
                .map_err(|source| Error::BrazeApi { source })?;

            let response_value =
                serde_json::to_value(&response).map_err(|source| Error::SerdeJson { source })?;

            let mut e = EventBuilder::new()
                .data(EventData::Json(response_value))
                .subject(format!("{}.{}", event.subject, config.name))
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            match self.tx {
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .context("users", response.users.len())
                .context("invalid_user_ids", response.invalid_user_ids.len())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

/// Builds a Braze [`ExportUsersByIdsRequest`](braze::export::ExportUsersByIdsRequest)
/// from rendered configuration and event data.
///
/// Template strings for `external_ids`, `device_id`, `braze_id`, `email_address`,
/// and `phone` are rendered against the incoming event. Identifiers that render
/// to an empty string are ignored. Returns [`Error::MissingIdentifiers`] when
/// no usable identifier remains.
fn build_request(
    config: &super::config::Processor,
    event_value: &Value,
) -> Result<braze::export::ExportUsersByIdsRequest, Error> {
    let external_ids = match config.external_ids.as_ref() {
        Some(ids) => {
            let mut rendered = Vec::with_capacity(ids.len());
            for id in ids {
                if let Some(v) = render_non_empty(id, event_value)? {
                    rendered.push(v);
                }
            }
            Some(rendered)
        }
        None => None,
    };

    let device_id = render_optional(config.device_id.as_ref(), event_value)?;
    let braze_id = render_optional(config.braze_id.as_ref(), event_value)?;
    let email_address = render_optional(config.email_address.as_ref(), event_value)?;
    let phone = render_optional(config.phone.as_ref(), event_value)?;

    let has_identifier = external_ids.as_ref().is_some_and(|v| !v.is_empty())
        || device_id.is_some()
        || braze_id.is_some()
        || email_address.is_some()
        || phone.is_some()
        || config.user_aliases.as_ref().is_some_and(|v| !v.is_empty());

    if !has_identifier {
        return Err(Error::MissingIdentifiers);
    }

    Ok(braze::export::ExportUsersByIdsRequest {
        external_ids,
        user_aliases: config.user_aliases.clone().map(|aliases| {
            aliases
                .into_iter()
                .map(braze::export::UserAlias::from)
                .collect()
        }),
        device_id,
        braze_id,
        email_address,
        phone,
        fields_to_export: config.fields_to_export.clone(),
    })
}

/// Renders a Handlebars template against the event and returns `Some` only when
/// the result is a non-empty string. Propagates render errors so misconfigured
/// templates surface as task failures instead of silently dropping identifiers.
fn render_non_empty(template: &str, event_value: &Value) -> Result<Option<String>, Error> {
    let rendered = flowgen_core::config::render_template(template, event_value)
        .map_err(|source| Error::IdentifierRender { source })?;
    let trimmed = rendered.trim();
    match trimmed.is_empty() {
        true => Ok(None),
        false => Ok(Some(trimmed.to_string())),
    }
}

/// Convenience wrapper for optional single-value identifier fields.
fn render_optional(
    template: Option<&String>,
    event_value: &Value,
) -> Result<Option<String>, Error> {
    match template {
        Some(t) => render_non_empty(t, event_value),
        None => Ok(None),
    }
}

/// Braze export user IDs processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Processor>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let credentials_path = init_config.credentials_path.clone();
        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKey::new(&credentials_path),
                || async {
                    let contents =
                        tokio::fs::read_to_string(&credentials_path)
                            .await
                            .map_err(|source| Error::ClientInit {
                                source: braze::Error::CredentialsIo { source },
                            })?;
                    let credentials = serde_json::from_str::<braze::Credentials>(&contents)
                        .map_err(|source| Error::ClientInit {
                            source: braze::Error::CredentialsParse { source },
                        })?;
                    braze::Client::builder()
                        .credentials(credentials)
                        .build()
                        .map_err(|source| Error::ClientInit { source })
                },
            )
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        Ok(EventHandler {
            client,
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) if e.is_permanent() => {
                        error!(error = %e, "Failed to initialize Braze export user IDs processor with permanent error");
                        Err(tokio_retry::RetryError::permanent(e))
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to initialize Braze export user IDs processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        let mut handlers = Vec::new();

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let handle = tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) if e.is_permanent() => {
                                        error!(error = %e, "Failed to export Braze user IDs with permanent error");
                                        Err(tokio_retry::RetryError::permanent(e))
                                    }
                                    Err(e) => {
                                        error!(error = %e, "Failed to export Braze user IDs");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Braze export user IDs failed after all retry attempts");
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                } else if let Some(arc) = event.completion_tx.as_ref() {
                                    arc.signal_completion_with_error(err.to_string());
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                }
                None => {
                    futures_util::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for creating Braze export user IDs processor instances.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Processor>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    /// Creates a new builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the processor configuration.
    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the input channel receiver.
    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    /// Sets the output channel sender.
    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    /// Sets the task identifier.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Sets the task execution context.
    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Sets the task type.
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Builds the processor.
    ///
    /// # Errors
    /// Returns [`Error::MissingBuilderAttribute`] when a required field is missing.
    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingBuilderAttribute("task_id".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_request_uses_rendered_external_ids() {
        let config = super::super::config::Processor {
            name: "test".to_string(),
            credentials_path: std::path::PathBuf::from("/dev/null"),
            external_ids: Some(vec!["{{event.data.user_id}}".to_string()]),
            fields_to_export: Some(vec!["email".to_string()]),
            ..Default::default()
        };

        let event_value = json!({
            "event": {
                "data": { "user_id": "user-123" },
            }
        });

        let request = build_request(&config, &event_value).unwrap();
        assert_eq!(request.external_ids, Some(vec!["user-123".to_string()]));
        assert_eq!(request.fields_to_export, Some(vec!["email".to_string()]));
    }

    #[test]
    fn build_request_rejects_empty_identifiers() {
        let config = super::super::config::Processor {
            name: "test".to_string(),
            credentials_path: std::path::PathBuf::from("/dev/null"),
            external_ids: Some(vec!["{{event.data.missing}}".to_string()]),
            ..Default::default()
        };

        let event_value = json!({ "event": { "data": {} } });
        let result = build_request(&config, &event_value);
        assert!(matches!(result, Err(Error::MissingIdentifiers)));
    }
}
