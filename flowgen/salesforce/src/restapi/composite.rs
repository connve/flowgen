use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use salesforce_core::restapi::{
    CompositeCollectionCreateRequest, CompositeCollectionRetrieveRequest,
    CompositeCollectionUpdateRequest, CompositeCollectionUpsertRequest, CompositeRecordRequest,
    CompositeTreeRequest,
};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn, Instrument};

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
    #[error("Salesforce Composite API operation failed: {source}")]
    CompositeApiOperation {
        #[source]
        source: Box<salesforce_core::restapi::CompositeError>,
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
    #[error("Operation requires sobject_type field to be specified")]
    MissingSObjectType,
    #[error("Operation requires payload data (explicit records or from_event: true)")]
    MissingPayload,
    #[error("Operation requires ids field to be specified")]
    MissingIds,
    #[error("Operation requires external_id_field to be specified")]
    MissingExternalIdField,
    #[error("Failed to build REST API client: {source}")]
    RestClientBuild {
        #[source]
        source: salesforce_core::restapi::ClientError,
    },
    #[error("Expected array of objects for composite records")]
    InvalidCompositeRecordFormat,
    #[error("Expected array data for composite operation")]
    InvalidCompositeDataFormat,
}

pub struct EventHandler {
    client: Arc<salesforce_core::restapi::Client>,
    config: Arc<super::config::Composite>,
    tx: Option<Sender<Event>>,
    current_task_id: usize,
    sfdc_client: Arc<tokio::sync::Mutex<salesforce_core::client::Client>>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Log any failed sub-requests from the composite response as warnings.
    /// The event is still forwarded to downstream tasks with the full Salesforce response,
    /// allowing them to process successful records.
    fn log_failures(response: &serde_json::Value) {
        if let Some(array) = response.as_array() {
            let failures: Vec<(usize, &serde_json::Value)> = array
                .iter()
                .enumerate()
                .filter(|(_, item)| {
                    item.get("success")
                        .and_then(|s| s.as_bool())
                        .is_some_and(|success| !success)
                })
                .collect();

            if !failures.is_empty() {
                for (i, item) in &failures {
                    let errors = item
                        .get("errors")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    warn!(
                        index = i,
                        errors = %errors,
                        "Composite operation completed with one or more failures."
                    );
                }
            }
        }
    }

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

            let event_data = event.data_as_json()?;

            match config.operation {
                super::config::CompositeOperation::Create => {
                    self.create(&config, &event_data, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Get => {
                    self.get(&config, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Update => {
                    self.update(&config, &event_data, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Upsert => {
                    self.upsert(&config, &event_data, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Delete => {
                    self.delete(&config, completion_tx_arc).await?
                }
                super::config::CompositeOperation::Tree => {
                    self.tree(&config, &event_data, completion_tx_arc).await?
                }
            }

            Ok(())
        })
        .await
    }

    fn get_payload_records(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
    ) -> Result<Vec<serde_json::Map<String, serde_json::Value>>, Error> {
        let payload = config.payload.as_ref().ok_or(Error::MissingPayload)?;

        match payload {
            super::config::CompositePayload::FromEvent { from_event } if *from_event => {
                if let serde_json::Value::Array(records) = event_data {
                    records
                        .iter()
                        .map(|r| {
                            r.as_object()
                                .cloned()
                                .ok_or(Error::InvalidCompositeRecordFormat)
                        })
                        .collect()
                } else {
                    Err(Error::InvalidCompositeDataFormat)
                }
            }
            super::config::CompositePayload::Records(records) => Ok(records.clone()),
            _ => Err(Error::MissingPayload),
        }
    }

    async fn create(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let records = self.get_payload_records(config, event_data)?;

        let composite_records: Result<Vec<CompositeRecordRequest>, Error> = records
            .iter()
            .map(|r| {
                serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                    Error::SerdeExt {
                        source: flowgen_core::serde::Error::Serde { source: e },
                    }
                })
            })
            .collect();

        let request = CompositeCollectionCreateRequest {
            all_or_none: config.all_or_none.unwrap_or(false),
            records: composite_records?,
        };

        let response = self
            .client
            .composite()
            .create_records(&request)
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_create_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn get(
        &self,
        config: &super::config::Composite,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let sobject_type = config
            .sobject_type
            .as_ref()
            .ok_or(Error::MissingSObjectType)?;
        let ids = config.ids.as_ref().ok_or(Error::MissingIds)?;

        let request = CompositeCollectionRetrieveRequest {
            ids: ids.clone(),
            fields: config.fields.clone().unwrap_or_default(),
        };

        let response = self
            .client
            .composite()
            .get_records(sobject_type, &request)
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_get_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn update(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let records = self.get_payload_records(config, event_data)?;

        let composite_records: Result<Vec<serde_json::Map<String, serde_json::Value>>, Error> =
            records.iter().map(|r| Ok(r.clone())).collect();

        let request = CompositeCollectionUpdateRequest {
            all_or_none: config.all_or_none.unwrap_or(false),
            records: composite_records?
                .iter()
                .map(|r| {
                    serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                        Error::SerdeExt {
                            source: flowgen_core::serde::Error::Serde { source: e },
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        let response = self
            .client
            .composite()
            .update_records(&request)
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_update_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn upsert(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let sobject_type = config
            .sobject_type
            .as_ref()
            .ok_or(Error::MissingSObjectType)?;
        let external_id_field = config
            .external_id_field
            .as_ref()
            .ok_or(Error::MissingExternalIdField)?;
        let records = self.get_payload_records(config, event_data)?;

        let composite_records: Result<Vec<serde_json::Map<String, serde_json::Value>>, Error> =
            records.iter().map(|r| Ok(r.clone())).collect();

        let request = CompositeCollectionUpsertRequest {
            all_or_none: config.all_or_none.unwrap_or(false),
            records: composite_records?
                .iter()
                .map(|r| {
                    serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                        Error::SerdeExt {
                            source: flowgen_core::serde::Error::Serde { source: e },
                        }
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        let response = self
            .client
            .composite()
            .upsert_records(sobject_type, external_id_field, &request)
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_upsert_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn delete(
        &self,
        config: &super::config::Composite,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let ids = config.ids.as_ref().ok_or(Error::MissingIds)?;

        let ids_str = ids.join(",");
        let response = self
            .client
            .composite()
            .delete_records(&ids_str, config.all_or_none)
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&*response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_delete_{}", response.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }

    async fn tree(
        &self,
        config: &super::config::Composite,
        event_data: &serde_json::Value,
        completion_tx_arc: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Result<(), Error> {
        let sobject_type = config
            .sobject_type
            .as_ref()
            .ok_or(Error::MissingSObjectType)?;
        let records = self.get_payload_records(config, event_data)?;

        let tree_records: Result<Vec<_>, Error> = records
            .iter()
            .map(|r| {
                serde_json::from_value(serde_json::Value::Object(r.clone())).map_err(|e| {
                    Error::SerdeExt {
                        source: flowgen_core::serde::Error::Serde { source: e },
                    }
                })
            })
            .collect();

        let request = CompositeTreeRequest {
            records: tree_records?,
        };

        let response = self
            .client
            .composite()
            .create_record_tree(sobject_type, &request)
            .await
            .map_err(|e| Error::CompositeApiOperation {
                source: Box::new(e),
            })?;

        let resp = serde_json::to_value(&response).map_err(|e| Error::SerdeExt {
            source: flowgen_core::serde::Error::Serde { source: e },
        })?;

        let mut e = EventBuilder::new()
            .data(EventData::Json(resp.clone()))
            .subject(config.name.to_owned())
            .id(format!("composite_tree_{}", response.results.len()))
            .task_id(self.current_task_id)
            .task_type(self.task_type)
            .build()?;

        match self.tx {
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
        }

        Self::log_failures(&resp);

        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|e| Error::SendMessage { source: e })?;

        Ok(())
    }
}

#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Composite>,
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

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize Composite API processor");
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
                                            error!(error = %e, "Failed to process Composite API operation");
                                            let needs_reconnect = matches!(&e, Error::SalesforceAuth { .. })
                                                || matches!(&e,
                                                    Error::CompositeApiOperation { .. }
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
                                    error!(error = %err, "Composite API operation failed after all retry attempts");
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

pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Composite>>,
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

    pub fn config(mut self, config: Arc<super::config::Composite>) -> Self {
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
