//! Salesforce REST API processor.
//!
//! Handles SObject CRUD operations: create, get, get_by_external_id, update, upsert, and delete.

use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn, Instrument};

/// Errors for Salesforce REST API CRUD operations.
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
    #[error("Salesforce REST API operation failed: {source}")]
    RestApiOperation {
        #[source]
        source: Box<salesforce_core::restapi::sobject::Error>,
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
    #[error("Operation requires record_id field to be specified")]
    MissingRecordId,
    #[error("Operation requires payload data (explicit fields or from_event: true)")]
    MissingPayload,
    #[error("Operation requires both external_id_field and external_id_value to be specified")]
    MissingExternalId,
    #[error("Failed to build REST API client: {source}")]
    RestClientBuild {
        #[source]
        source: salesforce_core::restapi::ClientError,
    },
}

/// Event handler for processing individual SObject CRUD requests.
pub struct EventHandler {
    client: Arc<salesforce_core::restapi::Client>,
    config: Arc<super::config::SObject>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Log SObject operation failure as a warning if the response indicates failure.
    /// The event is still forwarded to downstream tasks with the full Salesforce response.
    fn log_failure(response: &serde_json::Value) {
        let success = response
            .get("success")
            .and_then(|s| s.as_bool())
            .unwrap_or(true);
        if !success {
            let errors = response
                .get("errors")
                .cloned()
                .unwrap_or(serde_json::Value::Null);
            warn!(
                errors = %errors,
                "SObject operation completed with failure."
            );
        }
    }

    #[tracing::instrument(skip(self, event), name = "task.handle", fields(task = %self.config.name, task_id = self.current_task_id, task_type = %self.task_type))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        // Run handler with event context for automatic meta preservation.
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|e| Error::ConfigRender { source: e })?;

            // Extract event.data for payload operations
            let event_data = event.data_as_json()?;

            // Execute operation based on type.
            match config.operation {
                super::config::SObjectOperation::Create => {
                    self.create(&config, &event_data, completion_tx_arc).await?
                }
                super::config::SObjectOperation::Get => {
                    self.get(&config, completion_tx_arc).await?
                }
                super::config::SObjectOperation::GetByExternalId => {
                    self.get_by_external_id(&config, completion_tx_arc).await?
                }
                super::config::SObjectOperation::Update => {
                    self.update(&config, &event_data, completion_tx_arc).await?
                }
                super::config::SObjectOperation::Upsert => {
                    self.upsert(&config, &event_data, completion_tx_arc).await?
                }
                super::config::SObjectOperation::Delete => {
                    self.delete(&config, completion_tx_arc).await?
                }
            }

            Ok(())
        })
        .await
    }

    /// Extracts payload data from config, handling both from_event and explicit fields.
    fn get_payload_data(
        &self,
        config: &super::config::SObject,
        event_data: &serde_json::Value,
    ) -> Result<serde_json::Value, Error> {
        let payload = config.payload.as_ref().ok_or(Error::MissingPayload)?;

        match payload {
            super::config::Payload::FromEvent { from_event } if *from_event => {
                // Use event.data as the payload
                Ok(event_data.clone())
            }
            super::config::Payload::Fields(fields) => {
                // Use explicit fields
                Ok(serde_json::Value::Object(fields.clone()))
            }
            _ => Err(Error::MissingPayload),
        }
    }

    /// Creates a new SObject record.
    async fn create(
        &self,
        config: &super::config::SObject,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let data = self.get_payload_data(config, event_data)?;

        let response = self
            .client
            .create(&config.sobject_type, data.clone())
            .await
            .map_err(|e| Error::RestApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(response.id)
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

        Self::log_failure(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    /// Gets a record by its Salesforce ID.
    async fn get(
        &self,
        config: &super::config::SObject,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let record_id = config.record_id.as_ref().ok_or(Error::MissingRecordId)?;

        let record = self
            .client
            .get(&config.sobject_type, record_id, config.fields.as_deref())
            .await
            .map_err(|e| Error::RestApiOperation {
                source: Box::new(e),
            })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(record))
            .subject(config.name.to_owned())
            .id(record_id.clone())
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        // Signal completion or pass through to next task.
        match self.tx {
            None => {
                // Final task, signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    if let Ok(mut guard) = arc.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(Ok(())).ok();
                        }
                    }
                }
            }
            Some(_) => {
                // Pass through completion_tx to next task.
                e.completion_tx = completion_tx_arc.clone();
            }
        }

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    /// Gets a record by an external ID field.
    async fn get_by_external_id(
        &self,
        config: &super::config::SObject,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let external_id_field = config
            .external_id_field
            .as_ref()
            .ok_or(Error::MissingExternalId)?;
        let external_id_value = config
            .external_id_value
            .as_ref()
            .ok_or(Error::MissingExternalId)?;

        let record = self
            .client
            .get_by_external_id(
                &config.sobject_type,
                external_id_field,
                external_id_value,
                config.fields.as_deref(),
            )
            .await
            .map_err(|e| Error::RestApiOperation {
                source: Box::new(e),
            })?;

        // Extract ID from record if available.
        let record_id = record
            .get("Id")
            .and_then(|v| v.as_str())
            .unwrap_or(external_id_value)
            .to_string();

        let mut e = EventBuilder::new()
            .data(EventData::Json(record))
            .subject(config.name.to_owned())
            .id(record_id)
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        // Signal completion or pass through to next task.
        match self.tx {
            None => {
                // Final task, signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    if let Ok(mut guard) = arc.lock() {
                        if let Some(tx) = guard.take() {
                            tx.send(Ok(())).ok();
                        }
                    }
                }
            }
            Some(_) => {
                // Pass through completion_tx to next task.
                e.completion_tx = completion_tx_arc.clone();
            }
        }

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    /// Updates an existing record.
    async fn update(
        &self,
        config: &super::config::SObject,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let record_id = config.record_id.as_ref().ok_or(Error::MissingRecordId)?;
        let data = self.get_payload_data(config, event_data)?;

        self.client
            .update(&config.sobject_type, record_id, data.clone())
            .await
            .map_err(|e| Error::RestApiOperation {
                source: Box::new(e),
            })?;

        // Salesforce update API returns nothing on success.
        // Send empty event to maintain event chain.
        let resp = serde_json::json!({});

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp))
            .subject(config.name.to_owned())
            .id(record_id.clone())
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

    /// Upserts a record using external ID (update if exists, create if not).
    async fn upsert(
        &self,
        config: &super::config::SObject,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let external_id_field = config
            .external_id_field
            .as_ref()
            .ok_or(Error::MissingExternalId)?;
        let external_id_value = config
            .external_id_value
            .as_ref()
            .ok_or(Error::MissingExternalId)?;
        let data = self.get_payload_data(config, event_data)?;

        // Try to get existing record by external ID.
        let existing_record = self
            .client
            .get_by_external_id(
                &config.sobject_type,
                external_id_field,
                external_id_value,
                Some("Id"),
            )
            .await;

        match existing_record {
            Ok(record) => {
                // Record exists, update it.
                let record_id = record
                    .get("Id")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| Error::RestApiOperation {
                        source: Box::new(
                            salesforce_core::restapi::sobject::Error::InvalidDataType {
                                actual_type: "Missing Id field in response".to_string(),
                            },
                        ),
                    })?
                    .to_string();

                self.client
                    .update(&config.sobject_type, &record_id, data.clone())
                    .await
                    .map_err(|e| Error::RestApiOperation {
                        source: Box::new(e),
                    })?;

                // Salesforce update API returns nothing on success.
                // Send empty event to maintain event chain.
                let resp = serde_json::json!({});

                let mut e = EventBuilder::new()
                    .data(EventData::Json(resp))
                    .subject(config.name.to_owned())
                    .id(record_id)
                    .task_id(self.current_task_id)
                    .task_type(self.task_type)
                    .build()?;

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
            Err(_) => {
                // Record doesn't exist, create it.
                let response = self
                    .client
                    .create(&config.sobject_type, data.clone())
                    .await
                    .map_err(|e| Error::RestApiOperation {
                        source: Box::new(e),
                    })?;

                let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
                    source: flowgen_core::serde::Error::Serde { source: e },
                })?;

                let mut e = EventBuilder::new()
                    .data(EventData::Json(resp.clone()))
                    .subject(config.name.to_owned())
                    .id(response.id)
                    .task_id(self.current_task_id)
                    .task_type(self.task_type)
                    .build()?;

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

                Self::log_failure(&resp);

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|e| Error::SendMessage { source: e })?;

                Ok(())
            }
        }
    }

    /// Deletes a record.
    async fn delete(
        &self,
        config: &super::config::SObject,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let record_id = config.record_id.as_ref().ok_or(Error::MissingRecordId)?;

        self.client
            .delete(&config.sobject_type, record_id)
            .await
            .map_err(|e| Error::RestApiOperation {
                source: Box::new(e),
            })?;

        // Salesforce delete API returns nothing on success.
        // Send empty event to maintain event chain.
        let resp = serde_json::json!({});

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp))
            .subject(config.name.to_owned())
            .id(record_id.to_string())
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

/// Salesforce REST API processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::SObject>,
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

        let sobject_client = salesforce_core::restapi::ClientBuilder::new(sfdc_client.clone())
            .build()
            .map_err(|e| Error::RestClientBuild { source: e })?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            current_task_id: self.task_id,
            tx: self.tx.clone(),
            client: Arc::new(sobject_client),
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
                    error!(error = %e, "Failed to initialize REST API processor");
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
                                            error!(error = %e, "Failed to process REST API operation");
                                            let needs_reconnect = matches!(&e, Error::SalesforceAuth { .. })
                                                || matches!(&e,
                                                    Error::RestApiOperation { source }
                                                        if matches!(source.as_ref(), salesforce_core::restapi::sobject::Error::Auth { .. })
                                                );

                                            if needs_reconnect {
                                                let mut sfdc_client =
                                                    event_handler.sfdc_client.lock().await;
                                                if let Err(reconnect_err) =
                                                    (*sfdc_client).reconnect().await
                                                {
                                                    error!(error = %reconnect_err, "Failed to reconnect");
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
                                    error!(error = %err, "REST API operation failed after all retry attempts");
                                    // Emit error event downstream for error handling.
                                    let mut error_event = event_clone.clone();
                                    error_event.error = Some(err.to_string());
                                    if let Some(ref tx) = event_handler.tx {
                                        tx.send(error_event).await.ok();
                                    }
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
    config: Option<Arc<super::config::SObject>>,
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

    pub fn config(mut self, config: Arc<super::config::SObject>) -> Self {
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
