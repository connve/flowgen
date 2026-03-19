//! Salesforce Tooling API processor.
//!
//! Handles Tooling API operations such as creating managed event subscriptions.

use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors for Salesforce Tooling API operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event message: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error(transparent)]
    SalesforceAuth(#[from] salesforce_core::client::Error),
    #[error("Tooling API error: {source}")]
    ToolingApi {
        #[source]
        source: Box<salesforce_core::tooling::Error>,
    },
    #[error("Serialization error: {source}")]
    SerdeExt {
        #[source]
        source: flowgen_core::serde::Error,
    },
    #[error(transparent)]
    EventError(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Operation requires full_name and metadata")]
    MissingSubscriptionData,
}

/// Event handler for processing individual Tooling API requests.
pub struct EventHandler {
    client: Arc<salesforce_core::tooling::Client>,
    config: Arc<super::config::Tooling>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Run handler with event context for automatic meta preservation.
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
            let config = self.config.render(&event_value)?;

            // Execute operation based on type.
            match config.operation {
                super::config::ToolingOperation::CreateManagedEventSubscription => {
                    self.create_managed_event_subscription(&config, completion_tx_arc)
                        .await?
                }
            }

            Ok(())
        })
        .await
    }

    /// Creates a managed event subscription using the Tooling API.
    async fn create_managed_event_subscription(
        &self,
        config: &super::config::Tooling,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let full_name = config
            .full_name
            .as_ref()
            .ok_or(Error::MissingSubscriptionData)?;
        let metadata = config
            .metadata
            .as_ref()
            .ok_or(Error::MissingSubscriptionData)?;

        // Build SDK request.
        let request = salesforce_core::tooling::CreateManagedEventSubscriptionRequest {
            full_name: full_name.clone(),
            metadata: salesforce_core::tooling::ManagedEventSubscriptionMetadata {
                label: metadata.label.clone(),
                topic_name: metadata.topic_name.clone(),
                default_replay: super::config::to_sdk_replay_preset(&metadata.default_replay),
                state: super::config::to_sdk_subscription_state(&metadata.state),
                error_recovery_replay: super::config::to_sdk_replay_preset(
                    &metadata.error_recovery_replay,
                ),
            },
        };

        // Create subscription using SDK.
        let response = self
            .client
            .create_managed_event_subscription(request)
            .await
            .map_err(|e| Error::ToolingApi {
                source: Box::new(e),
            })?;

        // Serialize the response to JSON.
        let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp))
            .subject(config.name.to_owned())
            .id(response.id.clone())
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        // Handle completion signal for the operation.
        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    if let Ok(mut guard) = arc.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(Ok(())).ok();
                        }
                    }
                }
            }
        }

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }
}

/// Salesforce Tooling API processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Tooling>,
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
        let init_config = self.config.render(&serde_json::json!({}))?;

        let sfdc_client = salesforce_core::client::Builder::new()
            .credentials_path(init_config.credentials_path.clone())
            .build()?
            .connect()
            .await?;

        // Create Tooling API client.
        let tooling_client =
            salesforce_core::tooling::ClientBuilder::new(sfdc_client.clone()).build();

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(tooling_client),
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

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize Tooling API processor");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    if Some(event.task_id) == event_handler.current_task_id.checked_sub(1) {
                        let event_handler = Arc::clone(&event_handler);
                        let retry_strategy = retry_config.strategy();
                        let event_clone = event.clone();
                        tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event_clone.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!(error = %e, "Failed to process Tooling API operation");
                                            let needs_reconnect = matches!(&e, Error::SalesforceAuth(_))
                                                || matches!(&e,
                                                    Error::ToolingApi { source }
                                                        if matches!(source.as_ref(), salesforce_core::tooling::Error::Auth { .. })
                                                );

                                            if needs_reconnect {
                                                let mut sfdc_client =
                                                    event_handler.sfdc_client.lock().await;
                                                if let Err(reconnect_err) =
                                                    (*sfdc_client).reconnect().await
                                                {
                                                    error!(error = %reconnect_err, "Failed to reconnect");
                                                    return Err(tokio_retry::RetryError::transient(Error::SalesforceAuth(
                                                        reconnect_err,
                                                    )));
                                                }
                                            }
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(error = %err, "Tooling API operation failed after all retry attempts");
                                }
                            }
                            .instrument(tracing::Span::current()),
                        );
                    }
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for creating Processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Tooling>>,
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

    pub fn config(mut self, config: Arc<super::config::Tooling>) -> Self {
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
            task_id: self.task_id.unwrap_or(0),
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}
