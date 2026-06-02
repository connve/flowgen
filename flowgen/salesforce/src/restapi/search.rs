//! Salesforce SOSL search processor.
//!
//! Executes SOSL queries via the REST API and emits matching records as events.

use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors for Salesforce SOSL search operations.
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
    #[error("Salesforce SOSL search failed: {source}")]
    SearchOperation {
        #[source]
        source: Box<salesforce_core::restapi::search::Error>,
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
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Failed to build REST API client: {source}")]
    RestClientBuild {
        #[source]
        source: salesforce_core::restapi::ClientError,
    },
}

/// Event handler for processing individual SOSL search requests.
pub struct EventHandler {
    client: Arc<salesforce_core::restapi::Client>,
    config: Arc<super::config::Search>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle")]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|e| Error::ConfigRender { source: e })?;

            let response =
                self.client
                    .search(&config.query)
                    .await
                    .map_err(|e| Error::SearchOperation {
                        source: Box::new(e),
                    })?;

            let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
                source: flowgen_core::serde::Error::Serde { source: e },
            })?;

            let mut e = EventBuilder::new()
                .data(EventData::Json(resp))
                .subject(config.name.to_owned())
                .task_id(self.current_task_id)
                .task_type(self.task_type)
                .build()?;

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
                .await
                .map_err(|e| Error::SendMessage { source: e })?;

            Ok(())
        })
        .await
    }
}

/// Salesforce SOSL search processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Search>,
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
            .map_err(|e| Error::ConfigRender { source: e })?;

        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(init_config.credentials_path.clone())
            .build()
            .map_err(|e| Error::SalesforceAuth { source: e })?
            .connect()
            .await
            .map_err(|e| Error::SalesforceAuth { source: e })?;

        let rest_client = salesforce_core::restapi::ClientBuilder::new(sfdc_client.clone())
            .build()
            .map_err(|e| Error::RestClientBuild { source: e })?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(rest_client),
            sfdc_client: Arc::new(tokio::sync::Mutex::new(sfdc_client)),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };
        Ok(event_handler)
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
                    Err(e) => {
                        error!(error = %e, "Failed to initialize SOSL search processor.");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        let mut handlers = Vec::new();
        loop {
            match self.rx.recv().await {
                Some(event) => {
                    if Some(event.task_id) == event_handler.current_task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();
                        let event_clone = event.clone();
                        let handle = tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event_clone.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!(error = %e, "Failed to process SOSL search.");
                                            let needs_reconnect = matches!(&e, Error::SalesforceAuth { .. })
                                                || matches!(&e,
                                                    Error::SearchOperation { source }
                                                        if matches!(source.as_ref(), salesforce_core::restapi::search::Error::Auth { .. })
                                                );

                                            if needs_reconnect {
                                                let mut sfdc_client =
                                                    event_handler.sfdc_client.lock().await;
                                                if let Err(reconnect_err) =
                                                    (*sfdc_client).reconnect().await
                                                {
                                                    error!(error = %reconnect_err, "Failed to reconnect.");
                                                    return Err(tokio_retry::RetryError::transient(Error::SalesforceAuth {
                                                        source: reconnect_err,
                                                    }));
                                                }
                                            }
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(error = %err, "SOSL search failed after all retry attempts.");
                                    let mut error_event = event_clone.clone();
                                    error_event.error = Some(err.to_string());
                                    if let Some(ref tx) = event_handler.tx {
                                        tx.send(error_event).await.ok();
                                    }
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                        handlers.push(handle);
                    }
                }
                None => {
                    futures_util::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for creating search Processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Search>>,
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

    pub fn config(mut self, config: Arc<super::config::Search>) -> Self {
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
    use super::super::config::Search;

    // ── Config Deserialization ───────────────────────────────────────

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "find_accounts",
            "credentials_path": "/path/to/creds.json",
            "query": "FIND {Acme} IN ALL FIELDS RETURNING Account(Id, Name)"
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "find_accounts");
        assert_eq!(
            config.credentials_path,
            std::path::PathBuf::from("/path/to/creds.json")
        );
        assert_eq!(
            config.query,
            "FIND {Acme} IN ALL FIELDS RETURNING Account(Id, Name)"
        );
        assert!(config.depends_on.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn config_deser_with_depends_on() {
        let json = r#"{
            "name": "search_contacts",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "depends_on": ["upstream_task"]
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let deps = config.depends_on.unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0], "upstream_task");
    }

    #[test]
    fn config_deser_with_retry() {
        let json = r#"{
            "name": "retryable_search",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "retry": { "max_retries": 5, "initial_interval": "2s" }
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        assert!(config.retry.is_some());
    }

    #[test]
    fn config_deser_missing_name_fails() {
        let json = r#"{
            "credentials_path": "/creds.json",
            "query": "FIND {test}"
        }"#;
        let result = serde_json::from_str::<Search>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_credentials_path_fails() {
        let json = r#"{
            "name": "test",
            "query": "FIND {test}"
        }"#;
        let result = serde_json::from_str::<Search>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_query_fails() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/creds.json"
        }"#;
        let result = serde_json::from_str::<Search>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_roundtrip_serde() {
        let json = r#"{
            "name": "roundtrip",
            "credentials_path": "/creds.json",
            "query": "FIND {Acme} RETURNING Account"
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Search = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn config_deser_with_handlebars_template_in_query() {
        let json = r#"{
            "name": "templated_search",
            "credentials_path": "/creds.json",
            "query": "FIND {{{event.data.search_term}}} IN ALL FIELDS RETURNING Account(Id, Name)"
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        assert!(config.query.contains("{{event.data.search_term}}"));
    }

    #[test]
    fn config_deser_with_multiple_depends_on() {
        let json = r#"{
            "name": "multi_dep",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "depends_on": ["task_a", "task_b", "task_c"]
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let deps = config.depends_on.unwrap();
        assert_eq!(deps.len(), 3);
        assert_eq!(deps[2], "task_c");
    }

    #[test]
    fn config_deser_empty_depends_on_is_empty_vec() {
        let json = r#"{
            "name": "empty_dep",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "depends_on": []
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let deps = config.depends_on.unwrap();
        assert!(deps.is_empty());
    }
}
