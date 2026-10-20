//! What every Braze task shares: one client per credentials file and
//! instance, the request read from `payload`, and the event plumbing.

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

/// Errors of the Braze tasks.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Braze rejected the call, or it didn't reach Braze.
    #[error("Braze API call failed: {source}")]
    Api {
        /// The underlying error.
        #[source]
        source: braze::Error,
    },
    /// The credentials file couldn't be read or parsed.
    #[error("Failed to load the Braze credentials: {source}")]
    Credentials {
        /// The underlying error.
        #[source]
        source: flowgen_core::credentials::Error,
    },
    /// The client couldn't be built from the API key and instance address.
    #[error("Failed to build the Braze client: {source}")]
    ClientBuild {
        /// The underlying error.
        #[source]
        source: braze::Error,
    },
    /// A config template couldn't be rendered.
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        /// The underlying error.
        #[source]
        source: flowgen_core::config::Error,
    },
    /// The operation needs a config field that isn't set.
    #[error("This operation needs `{0}`")]
    MissingField(&'static str),
    /// The payload doesn't deserialize into the Braze request.
    #[error("The payload doesn't fit the Braze request: {source}")]
    InvalidPayload {
        /// The underlying error.
        #[source]
        source: serde_json::Error,
    },
    /// `from_event: true` was set, but the event data isn't a JSON object.
    #[error("The payload with `from_event: true` needs the event data to be a JSON object")]
    EventDataNotObject,
    /// A Braze response couldn't be turned into event data.
    #[error("Failed to read the Braze response: {source}")]
    Response {
        /// The underlying error.
        #[source]
        source: serde_json::Error,
    },
    /// The call finished without a response to emit.
    #[error("The Braze call returned nothing to emit")]
    NoResponse,
    /// The event couldn't be built or its data read.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// The next task didn't take the event.
    #[error("Failed to send event to the next task: {source}")]
    SendMessage {
        /// The underlying error.
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

impl From<braze::Error> for Error {
    fn from(source: braze::Error) -> Self {
        Error::Api { source }
    }
}

/// The request body: written in the config, or the event data.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Payload {
    /// Use the incoming event data as the request body.
    FromEvent {
        /// `true` to use the event data.
        from_event: bool,
    },
    /// Request body written in the config.
    Body(Map<String, Value>),
}

/// A Braze task: its config, and the call it makes for one event.
#[async_trait::async_trait]
pub trait Operation:
    TaskConfig + ConfigExt + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The JSON file with the Braze REST API key.
    fn credentials_path(&self) -> &Path;

    /// The REST endpoint of the Braze instance.
    fn base_url(&self) -> &str;

    /// Checks at startup that the operation has the fields it needs.
    fn validate(&self) -> Result<(), Error> {
        Ok(())
    }

    /// Whether repeating a failed call can't do the work twice.
    fn retries_safely(&self) -> bool {
        true
    }

    /// Calls Braze for one event and emits the response.
    async fn call(
        &self,
        client: &braze::Client,
        event: &Event,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error>;
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

/// Reads `payload` for this event as the SDK request.
pub(crate) fn request<T: DeserializeOwned>(
    payload: Option<&Payload>,
    event: &Event,
) -> Result<T, Error> {
    let body = match payload {
        Some(Payload::FromEvent { from_event: true }) => match event.data_as_json()? {
            Value::Object(body) => body,
            _ => return Err(Error::EventDataNotObject),
        },
        Some(Payload::Body(body)) => body.clone(),
        Some(Payload::FromEvent { from_event: false }) | None => {
            return Err(Error::MissingField("payload"))
        }
    };
    serde_json::from_value(Value::Object(body)).map_err(|source| Error::InvalidPayload { source })
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

/// Handles the events of one Braze task.
pub struct EventHandler<C> {
    client: Arc<braze::Client>,
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

/// Runs one Braze task.
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
        let base_url = config.base_url().to_owned();

        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKeyBuilder::new(crate::CONNECTOR.name)
                    .field("credentials_path", &credentials_path)
                    .field("base_url", &base_url)
                    .build(),
                || async {
                    let credentials: braze::Credentials =
                        flowgen_core::credentials::load_credentials(&credentials_path)
                            .await
                            .map_err(|source| Error::Credentials { source })?;
                    braze::Client::builder()
                        .credentials(credentials)
                        .base_url(base_url)
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
                    error!(error = %e, "Failed to initialize the Braze task");
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
    /// An empty builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// The task's config.
    pub fn config(mut self, config: Arc<C>) -> Self {
        self.config = Some(config);
        self
    }

    /// Where the task receives events.
    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    /// Where the task sends events; unset for the last task of a flow.
    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    /// The task's position in its flow.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// The flow's shared context: cache, retry defaults, cancellation.
    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// The task type, as written in the flow.
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// The processor, or the first missing attribute.
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
    fn the_payload_is_read_as_the_braze_request() {
        let mut body = Map::new();
        body.insert("external_ids".to_string(), Value::from(vec!["user-1"]));

        let request: braze::export::ExportUsersByIdsRequest =
            request(Some(&Payload::Body(body)), &event(Value::Null)).unwrap();

        assert_eq!(request.external_ids, ["user-1"]);
    }

    #[test]
    fn the_payload_can_come_from_the_event() {
        let mut body = Map::new();
        body.insert("external_ids".to_string(), Value::from(vec!["user-1"]));
        let payload = Payload::FromEvent { from_event: true };

        let exported: braze::export::ExportUsersByIdsRequest =
            request(Some(&payload), &event(Value::Object(body))).unwrap();

        assert_eq!(exported.external_ids, ["user-1"]);
        assert!(matches!(
            request::<braze::export::ExportUsersByIdsRequest>(
                Some(&payload),
                &event(Value::from("ids"))
            ),
            Err(Error::EventDataNotObject)
        ));
        assert!(matches!(
            request::<braze::export::ExportUsersByIdsRequest>(None, &event(Value::Null)),
            Err(Error::MissingField("payload"))
        ));
    }

    #[test]
    fn a_payload_of_the_wrong_shape_is_rejected() {
        let mut body = Map::new();
        body.insert("external_ids".to_string(), Value::from("user-1"));

        let result: Result<braze::export::ExportUsersByIdsRequest, Error> =
            request(Some(&Payload::Body(body)), &event(Value::Null));

        assert!(matches!(result, Err(Error::InvalidPayload { .. })));
    }
}
