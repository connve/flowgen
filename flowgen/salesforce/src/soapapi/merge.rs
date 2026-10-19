//! Salesforce SOAP API merge processor.
//!
//! Merges duplicate SObject records (Account, Contact, Lead, Individual) into a
//! single master record via the Salesforce SOAP API. Related records from the
//! victim records are automatically reparented to the master.

use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;

/// Errors for Salesforce SOAP API merge operations.
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
    #[error("Salesforce SOAP merge operation failed: {source}")]
    MergeOperation {
        #[source]
        source: salesforce_core::soapapi::MergeError,
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
    #[error("record_ids_to_merge must contain one or two IDs, got {count}")]
    InvalidMergeCount { count: usize },
    #[error("Failed to build SOAP API client: {source}")]
    SoapClientBuild {
        #[source]
        source: salesforce_core::soapapi::ClientError,
    },
}

/// Event handler for processing individual merge requests.
pub struct EventHandler {
    client: Arc<salesforce_core::soapapi::Client>,
    config: Arc<super::config::Merge>,
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
                .map_err(|e| Error::ConfigRender { source: e })?;

            let count = config.record_ids_to_merge.len();
            if count == 0 || count > 2 {
                return Err(Error::InvalidMergeCount { count });
            }

            let response = self
                .client
                .merge(
                    &config.sobject_type,
                    &config.master_record_id,
                    &config.record_ids_to_merge,
                    config.master_field_overrides.as_ref(),
                    config.allow_duplicate_save,
                )
                .await
                .map_err(|e| Error::MergeOperation { source: e })?;

            let resp = serde_json::json!({
                "success": response.success,
                "merged_record_ids": response.merged_record_ids,
                "updated_related_ids": response.updated_related_ids,
            });

            let mut e = EventBuilder::new()
                .data(EventData::Json(resp))
                .subject(config.name.to_owned())
                .id(config.master_record_id.clone())
                .task_id(self.current_task_id)
                .task_type(self.task_type)
                .build()?;

            match self.tx {
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|e| Error::SendMessage { source: e })?;

            Ok(())
        })
        .await
    }

    async fn reconnect_on_auth_error(&self, error: Error) -> Error {
        match error {
            Error::SalesforceAuth { .. } => match self.sfdc_client.lock().await.reconnect().await {
                Ok(()) => error,
                Err(source) => {
                    error!(error = %error, "Reconnecting after an authentication error failed");
                    Error::SalesforceAuth { source }
                }
            },
            _ => error,
        }
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        match error {
            Error::MergeOperation { source } => source.is_retryable(),
            _ => true,
        }
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

/// Salesforce SOAP API merge processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Merge>,
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

        let sfdc_client = crate::client::connect(
            &self.task_context,
            self.task_type,
            &init_config.credentials_path,
        )
        .await?;

        let soap_client = {
            let guard = sfdc_client.lock().await;
            salesforce_core::soapapi::ClientBuilder::new(guard.clone())
                .build()
                .map_err(|e| Error::SoapClientBuild { source: e })?
        };

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(soap_client),
            sfdc_client,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };
        Ok(event_handler)
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
                        error!(error = %e, "Failed to initialize SOAP API merge processor");
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

/// Builder for creating merge Processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Merge>>,
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

    pub fn config(mut self, config: Arc<super::config::Merge>) -> Self {
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
