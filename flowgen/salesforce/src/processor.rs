//! What every Salesforce task shares: one logged-in client per credentials
//! file, the error type and how each error is retried, result pages retried in
//! place, and the event plumbing.

use flowgen_core::client::Client as _;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt, SharedCompletionTx};
use flowgen_core::task::context::TaskContext;
use flowgen_core::task::TaskConfig;
use salesforce_core::pubsubapi::PubSubError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::Mutex;
use tracing::{error, warn};

/// Header that lets a save through a duplicate rule that would block it.
pub(crate) const DUPLICATE_RULE_HEADER: &str = "Sforce-Duplicate-Rule-Header";
/// Value of [`DUPLICATE_RULE_HEADER`] that allows the save.
pub(crate) const ALLOW_SAVE_VALUE: &str = "allowSave=true";

/// Default Salesforce Pub/Sub API URL.
pub const DEFAULT_PUBSUB_URL: &str = "https://api.pubsub.salesforce.com";
/// Default Salesforce Pub/Sub API port.
pub const DEFAULT_PUBSUB_PORT: &str = "443";

/// Errors of the Salesforce tasks.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to load the Salesforce credentials: {source}")]
    Credentials {
        #[source]
        source: flowgen_core::credentials::Error,
    },
    #[error("Salesforce authentication error: {source}")]
    Auth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
    #[error("Failed to build the REST API client: {source}")]
    RestClient {
        #[source]
        source: salesforce_core::restapi::ClientError,
    },
    #[error("Failed to build the Bulk API client: {source}")]
    BulkClient {
        #[source]
        source: salesforce_core::bulkapi::ClientError,
    },
    #[error("Failed to build the SOAP API client: {source}")]
    SoapClient {
        #[source]
        source: salesforce_core::soapapi::ClientError,
    },
    #[error("SObject request failed: {source}")]
    SObject {
        #[source]
        source: Box<salesforce_core::restapi::SObjectError>,
    },
    #[error("Failed to describe {object} for the results schema: {source}")]
    Describe {
        object: String,
        #[source]
        source: Box<salesforce_core::restapi::SObjectError>,
    },
    #[error("Composite request failed: {source}")]
    Composite {
        #[source]
        source: Box<salesforce_core::restapi::CompositeError>,
    },
    #[error("SOSL search failed: {source}")]
    Search {
        #[source]
        source: Box<salesforce_core::restapi::SearchError>,
    },
    #[error("SOQL query failed: {source}")]
    Query {
        #[source]
        source: Box<salesforce_core::restapi::QueryError>,
    },
    #[error("Bulk API query request failed: {source}")]
    BulkQuery {
        #[source]
        source: Box<salesforce_core::bulkapi::QueryError>,
    },
    #[error("Failed to read the query results: {source}")]
    Stream {
        #[source]
        source: Box<reqwest::Error>,
    },
    #[error("Failed to read the query results as Arrow: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("SOAP merge failed: {source}")]
    Merge {
        #[source]
        source: salesforce_core::soapapi::MergeError,
    },
    #[error("Tooling API request failed: {source}")]
    Tooling {
        #[source]
        source: Box<salesforce_core::toolingapi::Error>,
    },
    #[error("Pub/Sub API request failed: {source}")]
    PubSub {
        #[source]
        source: PubSubError,
    },
    #[error("Failed to connect to the Pub/Sub API: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    #[error("Failed to encode the Pub/Sub event: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("The Pub/Sub stream ended, the connection may have been lost")]
    StreamEnded,
    #[error("Failed to bind the query parameters: {source}")]
    Bind {
        #[source]
        source: crate::soql::Error,
    },
    #[error("Failed to load the resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("This operation needs `{0}`")]
    MissingField(&'static str),
    #[error("Invalid `{field}` datetime: {source}")]
    InvalidDateTime {
        field: &'static str,
        #[source]
        source: chrono::ParseError,
    },
    #[error("`record_ids_to_merge` must contain one or two IDs, got {count}")]
    InvalidMergeCount { count: usize },
    #[error("The record found by its external ID has no Id")]
    MissingRecordId,
    #[error("The payload with `from_event: true` needs the event data to be a JSON object")]
    EventDataNotObject,
    #[error("The payload with `from_event: true` needs the event data to be an array of records")]
    EventDataNotArray,
    #[error("Record at index {index} is not a JSON object")]
    RecordNotObject { index: usize },
    #[error(
        "Record at index {index} is missing `attributes` — \
         Salesforce Composite API requires each record to include \
         `attributes: {{ type: \"...\" }}`. Set `sobject_type` in the \
         task config to auto-populate it, or add `attributes` to \
         each record in the payload."
    )]
    MissingAttributes { index: usize },
    #[error("The records don't fit the Composite request: {source}")]
    InvalidRecords {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to read the Salesforce response: {source}")]
    Response {
        #[source]
        source: serde_json::Error,
    },
    #[error("The Salesforce call returned nothing to emit")]
    NoResponse,
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error("Failed to send event to the next task: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(&'static str),
}

/// How a failed request is retried.
pub(crate) enum Failure {
    /// The session expired or was revoked: log in again, then retry.
    Auth,
    /// Retrying might help.
    Transient,
    /// Retrying won't help.
    Permanent,
}

/// How `error` is retried.
pub(crate) fn failure(error: &Error) -> Failure {
    use salesforce_core::{bulkapi, restapi, soapapi, toolingapi};
    match error {
        Error::Auth { .. } => Failure::Auth,
        Error::SObject { source } | Error::Describe { source, .. } => api_failure(
            matches!(**source, restapi::SObjectError::Auth { .. }),
            source.is_retryable(),
        ),
        Error::Composite { source } => api_failure(
            matches!(**source, restapi::CompositeError::Auth { .. }),
            source.is_retryable(),
        ),
        Error::Search { source } => api_failure(
            matches!(**source, restapi::SearchError::Auth { .. }),
            source.is_retryable(),
        ),
        Error::Query { source } => api_failure(
            matches!(**source, restapi::QueryError::Auth { .. }),
            source.is_retryable(),
        ),
        Error::BulkQuery { source } => api_failure(
            matches!(**source, bulkapi::QueryError::Auth { .. }),
            source.is_retryable(),
        ),
        Error::Merge { source } => api_failure(
            matches!(source, soapapi::MergeError::Auth { .. }),
            source.is_retryable(),
        ),
        Error::Tooling { source } => api_failure(
            matches!(**source, toolingapi::Error::Auth { .. }),
            source.is_retryable(),
        ),
        Error::PubSub { source } => {
            api_failure(is_pubsub_auth_error(source), source.is_retryable())
        }
        Error::Stream { .. } => Failure::Transient,
        _ => Failure::Permanent,
    }
}

fn api_failure(auth: bool, retryable: bool) -> Failure {
    match (auth, retryable) {
        (true, _) => Failure::Auth,
        (false, true) => Failure::Transient,
        (false, false) => Failure::Permanent,
    }
}

/// Whether a gRPC error says the session is no longer valid.
pub(crate) fn is_pubsub_auth_error(error: &PubSubError) -> bool {
    match error {
        PubSubError::Tonic(status) => {
            let message = status.message();
            message.contains("does not have valid authentication credentials")
                || message.contains("authentication exception occurred")
        }
        _ => false,
    }
}

/// Runs `fetch` until it succeeds, waiting between attempts as `retry` says
/// and logging in again after an authentication failure. Retrying the page
/// itself, not the event, keeps pages already sent downstream from being sent
/// again. Returns the last error once retries run out, the error is
/// permanent, or the task is cancelled.
pub(crate) async fn fetch<T, E, F, Fut>(
    retry: &flowgen_core::retry::RetryConfig,
    cancellation: &tokio_util::sync::CancellationToken,
    auth: &Mutex<salesforce_core::client::Client>,
    classify: impl Fn(&E) -> Failure,
    mut fetch: F,
) -> Result<T, E>
where
    E: std::fmt::Display,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
{
    let mut delays = retry.strategy();
    loop {
        let error = match fetch().await {
            Ok(page) => return Ok(page),
            Err(error) => error,
        };
        let delay = match classify(&error) {
            Failure::Permanent => None,
            Failure::Transient => delays.next(),
            Failure::Auth => {
                if let Err(source) = auth.lock().await.reconnect().await {
                    warn!(error = %source, "Reconnecting after an authentication error failed");
                }
                delays.next()
            }
        };
        let delay = match delay {
            Some(delay) => delay,
            None => return Err(error),
        };
        warn!(error = %error, "Failed to fetch a page of results, retrying it");
        tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Err(error),
            _ = tokio::time::sleep(delay) => {}
        }
    }
}

/// The client registry key of the client for `credentials_path`.
pub(crate) fn client_key(
    task_type: &'static str,
    credentials_path: &Path,
) -> flowgen_core::client_registry::ClientKey {
    flowgen_core::client_registry::ClientKey::new(task_type, &credentials_path)
}

/// The client logged in with the credentials file at `credentials_path`,
/// shared through the client registry.
pub(crate) async fn connect(
    task_context: &TaskContext,
    task_type: &'static str,
    credentials_path: &Path,
) -> Result<Arc<Mutex<salesforce_core::client::Client>>, Error> {
    task_context
        .client_registry
        .get_or_init(client_key(task_type, credentials_path), || async {
            let credentials = flowgen_core::credentials::load_credentials(credentials_path)
                .await
                .map_err(|source| Error::Credentials { source })?;
            let client = salesforce_core::client::Builder::new()
                .credentials(credentials)
                .build()
                .map_err(|source| Error::Auth { source })?
                .connect()
                .await
                .map_err(|source| Error::Auth { source })?;
            Ok(Mutex::new(client))
        })
        .await
        .map_err(|error| match error {
            flowgen_core::client_registry::Error::Init { source } => source,
            flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
        })
}

/// The REST API client of a logged-in client.
pub(crate) fn rest_client(
    auth: &salesforce_core::client::Client,
) -> Result<salesforce_core::restapi::Client, Error> {
    salesforce_core::restapi::ClientBuilder::new(auth.clone())
        .build()
        .map_err(|source| Error::RestClient { source })
}

/// A Pub/Sub API client and the registry keys it was built from.
pub(crate) struct PubSub {
    pub(crate) client: Arc<Mutex<salesforce_core::pubsubapi::Client>>,
    /// Key of the gRPC channel shared per endpoint.
    pub(crate) channel_key: flowgen_core::client_registry::ClientKey,
    /// Key of the logged-in client shared per credentials file.
    pub(crate) client_key: flowgen_core::client_registry::ClientKey,
}

impl PubSub {
    /// Logs in with `credentials_path` and opens the Pub/Sub API at
    /// `endpoint`. One gRPC channel per endpoint is shared through the client
    /// registry, so subscriptions and publishers against one org multiplex
    /// over one HTTP/2 connection instead of each holding its own.
    pub(crate) async fn connect(
        task_context: &TaskContext,
        task_type: &'static str,
        credentials_path: &Path,
        endpoint: Option<&str>,
    ) -> Result<Self, Error> {
        let endpoint = match endpoint {
            Some(endpoint) => endpoint.to_string(),
            None => format!("{DEFAULT_PUBSUB_URL}:{DEFAULT_PUBSUB_PORT}"),
        };
        let channel_key = flowgen_core::client_registry::ClientKeyBuilder::new("salesforce_pubsub")
            .field("grpc_endpoint", &endpoint)
            .build();
        let channel = task_context
            .client_registry
            .get_or_init(channel_key.clone(), || async {
                let service = flowgen_core::service::ServiceBuilder::new()
                    .endpoint(endpoint)
                    .build()
                    .map_err(|source| Error::Service { source })?
                    .connect()
                    .await
                    .map_err(|source| Error::Service { source })?;
                service.channel.ok_or(Error::Service {
                    source: flowgen_core::service::Error::MissingEndpoint(),
                })
            })
            .await
            .map_err(|error| match error {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let auth = connect(task_context, task_type, credentials_path).await?;
        let client =
            salesforce_core::pubsubapi::Client::new((*channel).clone(), auth.lock().await.clone())
                .map_err(|source| Error::PubSub { source })?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            channel_key,
            client_key: client_key(task_type, credentials_path),
        })
    }
}

/// Operation of `query` and `queryAll` queries.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum QueryOperation {
    /// Query active records only.
    #[default]
    #[serde(rename = "query")]
    Query,
    /// Query including deleted and archived records.
    #[serde(rename = "queryAll")]
    QueryAll,
}

/// Record data: written in the config, or the event data.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum Payload {
    /// Use the incoming event data.
    FromEvent { from_event: bool },
    /// Field values written in the config.
    Fields(Map<String, Value>),
}

impl Default for Payload {
    fn default() -> Self {
        Payload::Fields(Map::new())
    }
}

impl Payload {
    /// The record fields for this event.
    pub(crate) fn fields(&self, event: &Event) -> Result<Map<String, Value>, Error> {
        match self {
            Payload::FromEvent { from_event: true } => match event.data_as_json()? {
                Value::Object(fields) => Ok(fields),
                _ => Err(Error::EventDataNotObject),
            },
            Payload::FromEvent { from_event: false } => Err(Error::MissingField("payload")),
            Payload::Fields(fields) => Ok(fields.clone()),
        }
    }
}

/// The value of a field the operation needs.
pub(crate) fn required<T>(value: Option<T>, field: &'static str) -> Result<T, Error> {
    value.ok_or(Error::MissingField(field))
}

/// `response` as event data.
pub(crate) fn to_json<T: Serialize>(response: &T) -> Result<Value, Error> {
    serde_json::to_value(response).map_err(|source| Error::Response { source })
}

/// A Salesforce task: its config, and the call it makes for one event.
#[async_trait::async_trait]
pub trait Operation:
    TaskConfig + ConfigExt + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The API client the task calls.
    type Client: Send + Sync + 'static;

    /// Builds the API client from the logged-in client.
    fn client(auth: &salesforce_core::client::Client) -> Result<Self::Client, Error>;

    /// The JSON file with the org's credentials.
    fn credentials_path(&self) -> &Path;

    /// Whether repeating a failed call can't do the work twice.
    fn retries_safely(&self) -> bool {
        true
    }

    /// Calls Salesforce for one event and emits the response.
    async fn call(
        &self,
        client: &Self::Client,
        call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error>;
}

/// What a call needs besides its config and client.
pub struct Call<'a> {
    /// The event the call is made for.
    pub event: &'a Event,
    task_context: &'a TaskContext,
    retry: &'a flowgen_core::retry::RetryConfig,
    auth: &'a Mutex<salesforce_core::client::Client>,
}

impl Call<'_> {
    /// Whether the task is stopping.
    pub(crate) fn is_cancelled(&self) -> bool {
        self.task_context.cancellation_token.is_cancelled()
    }

    /// One page of results, retried in place.
    pub(crate) async fn page<T, F, Fut>(&self, fetch_page: F) -> Result<T, Error>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T, Error>>,
    {
        fetch(
            self.retry,
            &self.task_context.cancellation_token,
            self.auth,
            failure,
            fetch_page,
        )
        .await
        .map_err(|source| Error::RetryExhausted {
            source: Box::new(source),
        })
    }

    /// The SOQL query to send: an inline query as rendered with the config,
    /// a resource file rendered for the event, then `parameters` bound.
    pub(crate) async fn soql(
        &self,
        query: &flowgen_core::resource::Source,
        parameters: Option<&HashMap<String, crate::soql::Parameter>>,
    ) -> Result<String, Error> {
        let query = match query {
            flowgen_core::resource::Source::Inline(query) => query.clone(),
            source => source
                .render(
                    self.task_context.resource_loader.as_ref(),
                    &flowgen_core::event::template_value(self.event, true)?,
                )
                .await
                .map_err(|source| Error::ResourceLoad { source })?,
        };
        match parameters {
            Some(parameters) => {
                crate::soql::bind(&query, parameters).map_err(|source| Error::Bind { source })
            }
            None => Ok(query),
        }
    }
}

/// One event a call emits.
pub(crate) struct Output {
    data: EventData,
    id: Option<String>,
    num_records: Option<usize>,
}

impl Output {
    /// A JSON response.
    pub(crate) fn json(data: Value) -> Self {
        Self {
            data: EventData::Json(data),
            id: None,
            num_records: None,
        }
    }

    /// A batch of query results.
    pub(crate) fn batch(batch: arrow::record_batch::RecordBatch) -> Self {
        Self {
            num_records: Some(batch.num_rows()),
            data: EventData::ArrowRecordBatch(batch),
            id: None,
        }
    }

    /// Sets the event id.
    pub(crate) fn id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }

    /// Logs the number of records the event holds.
    pub(crate) fn num_records(mut self, num_records: usize) -> Self {
        self.num_records = Some(num_records);
        self
    }
}

/// Sends each output of one call downstream as its own event. The last
/// event carries the completion signal, so the flow completes once.
pub struct Emitter<'a> {
    tx: Option<&'a Sender<Event>>,
    subject: &'a str,
    task_id: usize,
    task_type: &'static str,
    completion_tx: Option<SharedCompletionTx>,
    pending: Option<(Event, Option<usize>)>,
}

impl Emitter<'_> {
    /// Emits `output`.
    pub(crate) async fn emit(&mut self, output: Output) -> Result<(), Error> {
        let mut builder = EventBuilder::new()
            .data(output.data)
            .subject(self.subject.to_owned())
            .task_id(self.task_id)
            .task_type(self.task_type);
        if let Some(id) = output.id {
            builder = builder.id(id);
        }
        let event = builder.build()?;
        match self.pending.replace((event, output.num_records)) {
            Some((previous, num_records)) => send(previous, num_records, self.tx).await,
            None => Ok(()),
        }
    }

    async fn finish(mut self) -> Result<(), Error> {
        let (mut last, num_records) = self.pending.take().ok_or(Error::NoResponse)?;
        match self.tx {
            Some(_) => last.completion_tx = self.completion_tx,
            None => {
                if let Some(completion) = self.completion_tx.as_ref() {
                    completion.signal_completion(last.data_as_json().ok());
                }
            }
        }
        send(last, num_records, self.tx).await
    }
}

async fn send(
    event: Event,
    num_records: Option<usize>,
    tx: Option<&Sender<Event>>,
) -> Result<(), Error> {
    let logger = event.send_with_logging(tx);
    let logger = match num_records {
        Some(num_records) => logger.context("num_records", num_records),
        None => logger,
    };
    logger.await.map_err(|source| Error::SendMessage { source })
}

/// Handles the events of one Salesforce task.
pub struct EventHandler<C: Operation> {
    client: C::Client,
    auth: Arc<Mutex<salesforce_core::client::Client>>,
    config: Arc<C>,
    retry: flowgen_core::retry::RetryConfig,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<TaskContext>,
}

impl<C: Operation> EventHandler<C> {
    async fn reconnect_on_auth_error(&self, error: Error) -> Error {
        match failure(&error) {
            Failure::Auth => match self.auth.lock().await.reconnect().await {
                Ok(()) => error,
                Err(source) => {
                    error!(error = %error, "Reconnecting after an authentication error failed");
                    Error::Auth { source }
                }
            },
            Failure::Transient | Failure::Permanent => error,
        }
    }
}

#[async_trait::async_trait]
impl<C: Operation> flowgen_core::task::runner::Handler for EventHandler<C> {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        match failure(error) {
            Failure::Auth => true,
            Failure::Transient => self.config.retries_safely(),
            Failure::Permanent => false,
        }
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let event = Arc::new(event);
        let completion_tx = event.completion_tx.clone();

        let result = flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let config = self
                .config
                .render_for_event(&event)
                .map_err(|source| Error::ConfigRender { source })?;
            let call = Call {
                event: &event,
                task_context: &self.task_context,
                retry: &self.retry,
                auth: &self.auth,
            };
            let mut emitter = Emitter {
                tx: self.tx.as_ref(),
                subject: config.name(),
                task_id: self.task_id,
                task_type: self.task_type,
                completion_tx,
                pending: None,
            };
            config.call(&self.client, &call, &mut emitter).await?;
            match call.is_cancelled() {
                true => Ok(()),
                false => emitter.finish().await,
            }
        })
        .await;
        match result {
            Ok(()) => Ok(()),
            Err(error) => Err(self.reconnect_on_auth_error(error).await),
        }
    }
}

/// Runs one Salesforce task.
#[derive(Debug)]
pub struct Processor<C> {
    config: Arc<C>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<TaskContext>,
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
        let auth = connect(
            &self.task_context,
            self.task_type,
            config.credentials_path(),
        )
        .await?;
        let client = C::client(&*auth.lock().await)?;

        Ok(EventHandler {
            client,
            auth,
            config: Arc::clone(&self.config),
            retry: flowgen_core::retry::RetryConfig::merge(
                &self.task_context.retry,
                &self.config.retry().cloned(),
            ),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name(), task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config = flowgen_core::retry::RetryConfig::merge(
            &self.task_context.retry,
            &self.config.retry().cloned(),
        );

        let handler = tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                self.init().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize the Salesforce task");
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
    task_context: Option<Arc<TaskContext>>,
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
    pub fn task_context(mut self, task_context: Arc<TaskContext>) -> Self {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    fn retry() -> flowgen_core::retry::RetryConfig {
        flowgen_core::retry::RetryConfig {
            max_attempts: Some(3),
            initial_backoff: Duration::from_millis(1),
        }
    }

    fn client() -> Mutex<salesforce_core::client::Client> {
        Mutex::new(
            salesforce_core::client::Builder::new()
                .client_id("a")
                .client_secret("b")
                .instance_url("https://a.my.salesforce.com")
                .build()
                .unwrap(),
        )
    }

    async fn attempts(failure: fn() -> Failure, failures: usize) -> (Result<usize, String>, usize) {
        let calls = AtomicUsize::new(0);
        let result = fetch(
            &retry(),
            &tokio_util::sync::CancellationToken::new(),
            &client(),
            |_: &String| failure(),
            || async {
                match calls.fetch_add(1, Ordering::SeqCst) < failures {
                    true => Err("down".to_string()),
                    false => Ok(7),
                }
            },
        )
        .await;
        (result, calls.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn a_transient_failure_is_retried_until_the_page_arrives() {
        assert_eq!(attempts(|| Failure::Transient, 2).await, (Ok(7), 3));
    }

    #[tokio::test]
    async fn retries_stop_after_the_configured_attempts() {
        assert_eq!(
            attempts(|| Failure::Transient, 5).await,
            (Err("down".to_string()), 3)
        );
    }

    #[tokio::test]
    async fn a_permanent_failure_is_not_retried() {
        assert_eq!(
            attempts(|| Failure::Permanent, 1).await,
            (Err("down".to_string()), 1)
        );
    }

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
    fn the_payload_is_the_config_fields_or_the_event_data() {
        let fields = Map::from_iter([("Name".to_string(), Value::from("Acme"))]);

        assert_eq!(
            Payload::Fields(fields.clone())
                .fields(&event(Value::Null))
                .unwrap(),
            fields
        );
        assert_eq!(
            Payload::FromEvent { from_event: true }
                .fields(&event(Value::Object(fields.clone())))
                .unwrap(),
            fields
        );
        assert!(matches!(
            Payload::FromEvent { from_event: true }.fields(&event(Value::from("Acme"))),
            Err(Error::EventDataNotObject)
        ));
        assert!(matches!(
            Payload::FromEvent { from_event: false }.fields(&event(Value::Null)),
            Err(Error::MissingField("payload"))
        ));
    }

    #[test]
    fn query_operation_serializes_as_the_api_names() {
        assert_eq!(
            serde_json::to_string(&QueryOperation::QueryAll).unwrap(),
            "\"queryAll\""
        );
        assert_eq!(
            serde_json::from_str::<QueryOperation>("\"query\"").unwrap(),
            QueryOperation::Query
        );
    }
}
