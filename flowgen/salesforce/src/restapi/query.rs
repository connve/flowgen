//! Salesforce SOQL query processor.
//!
//! Executes SOQL queries via the REST API, follows `nextRecordsUrl` through
//! every page, and emits each page as an event.

use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use salesforce_core::restapi::QueryResponse;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Errors for Salesforce SOQL query operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to send event message to next task: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Salesforce authentication error: {source}")]
    SalesforceAuth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error(transparent)]
    Connect(#[from] crate::client::Error),
    #[error("Failed to bind the query parameters: {source}")]
    Bind {
        #[source]
        source: crate::soql::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Salesforce SOQL query failed: {source}")]
    QueryOperation {
        #[source]
        source: Box<salesforce_core::restapi::QueryError>,
    },
    #[error("Failed to serialize or deserialize data: {source}")]
    SerdeExt {
        #[source]
        source: flowgen_core::serde::Error,
    },
    #[error(transparent)]
    EventError(#[from] flowgen_core::event::Error),
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Error loading resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Failed to build REST API client: {source}")]
    RestClientBuild {
        #[source]
        source: salesforce_core::restapi::ClientError,
    },
}

/// Event handler for processing individual SOQL query requests.
pub struct EventHandler {
    client: Arc<salesforce_core::restapi::Client>,
    retry: flowgen_core::retry::RetryConfig,
    config: Arc<super::config::Query>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    async fn process(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let config = self
                .config
                .render_for_event(&event)
                .map_err(|source| Error::ConfigRender { source })?;
            // An inline query was rendered with the config; a resource file is rendered here.
            let query = match &config.query {
                flowgen_core::resource::Source::Inline(query) => query.clone(),
                source => source
                    .render(
                        self.task_context.resource_loader.as_ref(),
                        &flowgen_core::event::template_value(&event, true)?,
                    )
                    .await
                    .map_err(|source| Error::ResourceLoad { source })?,
            };
            let soql = match &config.parameters {
                Some(parameters) => crate::soql::bind(&query, parameters)
                    .map_err(|source| Error::Bind { source })?,
                None => query,
            };
            let query_all = matches!(
                config.query_operation,
                Some(crate::bulkapi::config::QueryOperation::QueryAll)
            );

            let mut next_records_url: Option<String> = None;
            let mut pending_event: Option<(Event, usize)> = None;
            loop {
                if self.task_context.cancellation_token.is_cancelled() {
                    return Ok(());
                }
                let mut page = crate::page::fetch(
                    &self.retry,
                    &self.task_context.cancellation_token,
                    &self.sfdc_client,
                    failure,
                    || self.fetch_page(&soql, query_all, next_records_url.as_deref()),
                )
                .await
                .map_err(|source| Error::RetryExhausted {
                    source: Box::new(source),
                })?;
                let next = page.next_records_url.take();
                if let Some((prev, prev_records)) = pending_event.take() {
                    prev.send_with_logging(self.tx.as_ref())
                        .context("num_records", prev_records)
                        .await
                        .map_err(|source| Error::SendMessage { source })?;
                }
                pending_event = Some(self.page_event(&config.name, page)?);
                next_records_url = match next {
                    Some(url) => Some(url),
                    None => break,
                };
            }

            if let Some((mut last, last_records)) = pending_event {
                match self.tx {
                    None => {
                        if let Some(arc) = completion_tx_arc.as_ref() {
                            arc.signal_completion(last.data_as_json().ok());
                        }
                    }
                    Some(_) => {
                        last.completion_tx = completion_tx_arc.clone();
                    }
                }
                last.send_with_logging(self.tx.as_ref())
                    .context("num_records", last_records)
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }

            Ok(())
        })
        .await
    }

    /// The first page of the query, or the page at `next_records_url`.
    async fn fetch_page(
        &self,
        soql: &str,
        query_all: bool,
        next_records_url: Option<&str>,
    ) -> Result<QueryResponse, Error> {
        let page = match (next_records_url, query_all) {
            (Some(url), _) => self.client.query_more(url).send().await,
            (None, false) => self.client.query(soql).send().await,
            (None, true) => self.client.query_all(soql).send().await,
        };
        page.map_err(|source| Error::QueryOperation {
            source: Box::new(source),
        })
    }

    /// One page of results as an event, with its record count.
    fn page_event(&self, name: &str, page: QueryResponse) -> Result<(Event, usize), Error> {
        let num_records = page.records.len();
        let data = serde_json::to_value(page).map_err(|source| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source },
        })?;
        let event = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(name.to_owned())
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;
        Ok((event, num_records))
    }

    async fn reconnect_on_auth_error(&self, error: Error) -> Error {
        match failure(&error) {
            crate::page::Failure::Auth => match self.sfdc_client.lock().await.reconnect().await {
                Ok(()) => error,
                Err(source) => {
                    error!(error = %error, "Reconnecting after an authentication error failed");
                    Error::SalesforceAuth { source }
                }
            },
            crate::page::Failure::Transient | crate::page::Failure::Permanent => error,
        }
    }
}

/// How a failed request is retried.
fn failure(error: &Error) -> crate::page::Failure {
    use crate::page::Failure;
    match error {
        Error::SalesforceAuth { .. } => Failure::Auth,
        Error::QueryOperation { source } => match source.as_ref() {
            salesforce_core::restapi::QueryError::Auth { .. } => Failure::Auth,
            source if source.is_retryable() => Failure::Transient,
            _ => Failure::Permanent,
        },
        _ => Failure::Permanent,
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        !matches!(failure(error), crate::page::Failure::Permanent)
    }

    /// Ignores events addressed to another task.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if Some(event.task_id) != self.current_task_id.checked_sub(1) {
            return Ok(());
        }
        match self.process(event).await {
            Ok(()) => Ok(()),
            Err(error) => Err(self.reconnect_on_auth_error(error).await),
        }
    }
}

/// Salesforce SOQL query processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Query>,
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
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let sfdc_client = crate::client::connect(
            &self.task_context,
            self.task_type,
            &init_config.credentials_path,
        )
        .await?;

        let rest_client = {
            let guard = sfdc_client.lock().await;
            salesforce_core::restapi::ClientBuilder::new(guard.clone())
                .build()
                .map_err(|source| Error::RestClientBuild { source })?
        };

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(rest_client),
            retry: flowgen_core::retry::RetryConfig::merge(
                &self.task_context.retry,
                &self.config.retry,
            ),
            sfdc_client,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize SOQL query processor");
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

        flowgen_core::task::runner::process_events(
            self.rx,
            event_handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for creating query Processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Query>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            rx: None,
            tx: None,
            task_id: None,
            task_context: None,
            task_type: None,
        }
    }

    pub fn config(mut self, config: Arc<super::config::Query>) -> Self {
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

    pub fn build(self) -> Result<Processor, Error> {
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
    use super::super::config::Query;
    use crate::bulkapi::config::QueryOperation;
    use flowgen_core::resource::Source;

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "recent_accounts",
            "credentials_path": "/creds.json",
            "query": "SELECT Id, Name FROM Account"
        }"#;
        let config: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.query,
            Source::Inline("SELECT Id, Name FROM Account".to_string())
        );
        assert!(config.query_operation.is_none());
    }

    #[test]
    fn config_deser_query_all_from_resource() {
        let json = r#"{
            "name": "deleted_accounts",
            "credentials_path": "/creds.json",
            "query": {"resource": "queries/deleted_accounts.soql"},
            "query_operation": "queryAll"
        }"#;
        let config: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.query,
            Source::Resource {
                resource: "queries/deleted_accounts.soql".to_string()
            }
        );
        assert_eq!(config.query_operation, Some(QueryOperation::QueryAll));
    }

    #[test]
    fn config_deser_rejects_unknown_fields() {
        let json = r#"{
            "name": "typo",
            "credentials_path": "/creds.json",
            "query": "SELECT Id FROM Account",
            "operation": "query"
        }"#;
        assert!(serde_json::from_str::<Query>(json).is_err());
    }

    #[test]
    fn config_deser_missing_query_fails() {
        let json = r#"{
            "name": "missing",
            "credentials_path": "/creds.json"
        }"#;
        assert!(serde_json::from_str::<Query>(json).is_err());
    }
}
