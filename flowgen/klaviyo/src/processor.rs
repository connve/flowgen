//! What every Klaviyo task shares: one client per credentials file and API
//! address, the JSON:API document built around `payload`, and the event
//! plumbing.

use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt, SharedCompletionTx};
use flowgen_core::task::TaskConfig;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Errors of the Klaviyo tasks.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Klaviyo rejected the call, or it didn't reach Klaviyo.
    #[error("Klaviyo API call failed: {source}")]
    Api {
        #[source]
        source: klaviyo::Error,
    },
    /// The credentials file couldn't be read or parsed.
    #[error("Failed to load the Klaviyo credentials: {source}")]
    Credentials {
        #[source]
        source: flowgen_core::credentials::Error,
    },
    /// The client couldn't be built from the API key and address.
    #[error("Failed to build the Klaviyo client: {source}")]
    ClientBuild {
        #[source]
        source: klaviyo::Error,
    },
    /// A config template couldn't be rendered.
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    /// The operation needs a config field that isn't set.
    #[error("This operation needs `{0}`")]
    MissingField(&'static str),
    /// The payload doesn't deserialize into the Klaviyo request.
    #[error("The payload doesn't fit the Klaviyo request: {source}")]
    InvalidPayload {
        #[source]
        source: serde_json::Error,
    },
    /// `from_event: true` was set, but the event data isn't a JSON object.
    #[error("The payload with `from_event: true` needs the event data to be a JSON object")]
    EventDataNotObject,
    /// A filter value contains a quote that would end the filter string.
    #[error("`{0}` contains a single quote, which a Klaviyo filter value can't hold")]
    InvalidFilterValue(String),
    /// A Klaviyo response couldn't be turned into event data.
    #[error("Failed to read the Klaviyo response: {source}")]
    Response {
        #[source]
        source: serde_json::Error,
    },
    /// The call finished without a response to emit.
    #[error("The Klaviyo call returned nothing to emit")]
    NoResponse,
    /// The event couldn't be built or its data read.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// The next task didn't take the event.
    #[error("Failed to send event to the next task: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    /// The builder is missing a required attribute.
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(&'static str),
    /// The client registry holds another client type for these credentials.
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

impl From<klaviyo::Error> for Error {
    fn from(source: klaviyo::Error) -> Self {
        Error::Api { source }
    }
}

/// The resource's attributes: written in the config, or the event data.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Payload {
    /// Use the incoming event data as the attributes.
    FromEvent { from_event: bool },
    /// Attributes written in the config.
    Attributes(Map<String, Value>),
}

/// A Klaviyo task: its config, and the call it makes for one event.
#[async_trait::async_trait]
pub trait Operation:
    TaskConfig + ConfigExt + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The JSON file with the account's private API key.
    fn credentials_path(&self) -> &Path;

    /// The Klaviyo API address, when it isn't Klaviyo's own.
    fn base_url(&self) -> Option<&str>;

    /// Checks at startup that the operation has the fields it needs.
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Whether repeating a failed call can't do the work twice.
    fn retries_safely(&self) -> bool {
        true
    }

    /// Calls Klaviyo for one event and emits each response.
    async fn call(
        &self,
        client: &klaviyo::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error>;
}

#[derive(Serialize)]
struct Document<'a> {
    data: Resource<'a>,
}

#[derive(Serialize)]
struct Resource<'a> {
    #[serde(rename = "type")]
    resource_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    id: Option<&'a str>,
    attributes: Map<String, Value>,
}

/// What a delete emits, since Klaviyo answers it without a body.
#[derive(Serialize)]
pub(crate) struct Done<'a> {
    pub(crate) id: &'a str,
    pub(crate) done: bool,
}

/// Sends each response of one call downstream as its own event. The last
/// event carries the completion signal, so the flow completes once.
pub struct Emitter<'a> {
    tx: Option<&'a Sender<Event>>,
    subject: &'a str,
    task_id: usize,
    task_type: &'static str,
    completion_tx: Option<SharedCompletionTx>,
    pending: Option<Event>,
}

impl Emitter<'_> {
    /// Emits `response`, the SDK's own response type.
    pub(crate) async fn emit<T: Serialize>(&mut self, response: &T) -> Result<(), Error> {
        let data = serde_json::to_value(response).map_err(|source| Error::Response { source })?;
        let event = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(self.subject.to_owned())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()?;
        match self.pending.replace(event) {
            Some(previous) => previous
                .send_with_logging(self.tx)
                .await
                .map_err(|source| Error::SendMessage { source }),
            None => Ok(()),
        }
    }

    async fn finish(mut self) -> Result<(), Error> {
        let mut last = self.pending.take().ok_or(Error::NoResponse)?;
        match self.tx {
            Some(_) => last.completion_tx = self.completion_tx,
            None => {
                if let Some(completion) = self.completion_tx.as_ref() {
                    completion.signal_completion(last.data_as_json().ok());
                }
            }
        }
        last.send_with_logging(self.tx)
            .await
            .map_err(|source| Error::SendMessage { source })
    }
}

/// The attributes `payload` resolves to for this event.
pub(crate) fn attributes(
    payload: Option<&Payload>,
    event: &Event,
) -> Result<Map<String, Value>, Error> {
    match payload {
        Some(Payload::FromEvent { from_event: true }) => match event.data_as_json()? {
            Value::Object(attributes) => Ok(attributes),
            _ => Err(Error::EventDataNotObject),
        },
        Some(Payload::Attributes(attributes)) => Ok(attributes.clone()),
        Some(Payload::FromEvent { from_event: false }) | None => {
            Err(Error::MissingField("payload"))
        }
    }
}

/// Wraps `attributes` in a JSON:API document and reads it as the SDK request.
pub(crate) fn request<T: DeserializeOwned>(
    resource_type: &str,
    id: Option<&str>,
    attributes: Map<String, Value>,
) -> Result<T, Error> {
    let document = Document {
        data: Resource {
            resource_type,
            id,
            attributes,
        },
    };
    let value =
        serde_json::to_value(document).map_err(|source| Error::InvalidPayload { source })?;
    serde_json::from_value(value).map_err(|source| Error::InvalidPayload { source })
}

/// The value of a field the operation needs.
pub(crate) fn required<'a>(value: Option<&'a str>, field: &'static str) -> Result<&'a str, Error> {
    match value {
        Some(value) => Ok(value),
        None => Err(Error::MissingField(field)),
    }
}

/// Fails when a field the operation needs isn't set.
pub(crate) fn present<T>(value: Option<&T>, field: &'static str) -> Result<(), Error> {
    match value {
        Some(_) => Ok(()),
        None => Err(Error::MissingField(field)),
    }
}

/// A Klaviyo filter matching `field` exactly, e.g. `equals(name,'Welcome')`.
pub(crate) fn equals(field: &str, value: &str) -> Result<String, Error> {
    match value.contains('\'') {
        true => Err(Error::InvalidFilterValue(value.to_owned())),
        false => Ok(format!("equals({field},'{value}')")),
    }
}

/// Handles the events of one Klaviyo task.
pub struct EventHandler<C> {
    client: Arc<klaviyo::Client>,
    config: Arc<C>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl<C: Operation> flowgen_core::task::runner::Handler for EventHandler<C> {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        match error {
            Error::Api { source } => source.is_retryable() && self.config.retries_safely(),
            Error::SendMessage { .. } => true,
            _ => false,
        }
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let event = Arc::new(event);
        let completion_tx = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let config = self
                .config
                .render_for_event(&event)
                .map_err(|source| Error::ConfigRender { source })?;
            let mut emitter = Emitter {
                tx: self.tx.as_ref(),
                subject: config.name(),
                task_id: self.task_id,
                task_type: self.task_type,
                completion_tx,
                pending: None,
            };
            config.call(&self.client, &event, &mut emitter).await?;
            emitter.finish().await
        })
        .await
    }
}

/// Runs one Klaviyo task.
#[derive(Debug)]
pub struct Processor<C> {
    config: Arc<C>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl<C: Operation> flowgen_core::task::runner::Runner for Processor<C> {
    type Error = Error;
    type EventHandler = EventHandler<C>;

    async fn init(&self) -> Result<EventHandler<C>, Error> {
        let config = self
            .config
            .render(&Map::new())
            .map_err(|source| Error::ConfigRender { source })?;
        let credentials_path = config.credentials_path().to_path_buf();
        let base_url = config.base_url().map(str::to_owned);

        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKeyBuilder::new(crate::CONNECTOR.name)
                    .field("credentials_path", &credentials_path)
                    .field("base_url", &base_url)
                    .build(),
                || async {
                    let credentials: klaviyo::Credentials =
                        flowgen_core::credentials::load_credentials(&credentials_path)
                            .await
                            .map_err(|source| Error::Credentials { source })?;
                    let builder = klaviyo::Client::builder().credentials(credentials);
                    let builder = match base_url {
                        Some(base_url) => builder.base_url(base_url),
                        None => builder,
                    };
                    builder
                        .build()
                        .map_err(|source| Error::ClientBuild { source })
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
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name(), task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        self.config.validate()?;
        let retry_config = flowgen_core::retry::RetryConfig::merge(
            &self.task_context.retry,
            &self.config.retry().cloned(),
        );

        let handler = tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                self.init().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize the Klaviyo task");
                    tokio_retry::RetryError::transient(e)
                })
            },
        )
        .await?;

        flowgen_core::task::runner::process_events(
            self.rx,
            Arc::new(handler),
            &retry_config,
            self.config.max_in_flight(),
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builds a [`Processor`].
#[derive(Debug)]
pub struct ProcessorBuilder<C> {
    config: Option<Arc<C>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl<C> Default for ProcessorBuilder<C> {
    fn default() -> Self {
        Self {
            config: None,
            rx: None,
            tx: None,
            task_id: None,
            task_context: None,
            task_type: None,
        }
    }
}

impl<C> ProcessorBuilder<C> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<C>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
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

    pub fn build(self) -> Result<Processor<C>, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or(Error::MissingBuilderAttribute("config"))?,
            rx: self.rx.ok_or(Error::MissingBuilderAttribute("receiver"))?,
            tx: self.tx,
            task_id: self
                .task_id
                .ok_or(Error::MissingBuilderAttribute("task_id"))?,
            task_context: self
                .task_context
                .ok_or(Error::MissingBuilderAttribute("task_context"))?,
            task_type: self
                .task_type
                .ok_or(Error::MissingBuilderAttribute("task_type"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(data: Value) -> Event {
        EventBuilder::new()
            .data(EventData::Json(data))
            .subject("a".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap()
    }

    #[test]
    fn attributes_are_wrapped_in_a_json_api_document() {
        let mut attributes = Map::new();
        attributes.insert("name".to_string(), Value::from("Welcome"));
        attributes.insert("editor_type".to_string(), Value::from("CODE"));

        let request: klaviyo::types::TemplateCreateHtmlOrDndQuery =
            request("template", None, attributes).unwrap();

        assert_eq!(request.data.attributes.name, "Welcome");
        assert_eq!(request.data.attributes.editor_type, "CODE");
    }

    #[test]
    fn attributes_missing_a_required_field_are_rejected() {
        let result: Result<klaviyo::types::TemplateCreateHtmlOrDndQuery, Error> =
            request("template", None, Map::new());

        assert!(matches!(result, Err(Error::InvalidPayload { .. })));
    }

    #[test]
    fn a_filter_value_with_a_single_quote_is_rejected() {
        assert_eq!(equals("name", "Welcome").unwrap(), "equals(name,'Welcome')");
        assert!(matches!(
            equals("name", "Mum's day"),
            Err(Error::InvalidFilterValue(_))
        ));
    }

    #[test]
    fn the_payload_can_come_from_the_event() {
        let mut data = Map::new();
        data.insert("name".to_string(), Value::from("Welcome"));
        let payload = Payload::FromEvent { from_event: true };

        let from_event = attributes(Some(&payload), &event(Value::Object(data.clone()))).unwrap();

        assert_eq!(from_event, data);
        assert!(matches!(
            attributes(Some(&payload), &event(Value::from("html"))),
            Err(Error::EventDataNotObject)
        ));
        assert!(matches!(
            attributes(None, &event(Value::Null)),
            Err(Error::MissingField("payload"))
        ));
    }
}
