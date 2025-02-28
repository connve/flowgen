use chrono::Utc;
use flowgen_core::{
    client::Client,
    event::Event,
    render::Render,
    serde::{MapExt, StringExt},
};
use salesforce_pubsub::eventbus::v1::{ProducerEvent, PublishRequest, SchemaRequest, TopicRequest};
use serde_avro_fast::{ser, Schema};
use serde_json::{Map, Value};
use std::{path::Path, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};

#[derive(thiserror::Error, Debug)]
pub enum PublisherError {
    #[error("There was an error with PubSub context.")]
    SalesforcePubSubError(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    SalesforceAuthError(#[source] crate::client::Error),
    #[error("There was an error with parsing a given value.")]
    SerdeError(#[source] flowgen_core::serde::SerdeError),
    #[error("There was an error with parsing a given value.")]
    SerdeJsonError(#[source] serde_json::error::Error),
    #[error("There was an error with rendering a given value.")]
    RenderError(#[source] flowgen_core::render::RenderError),
    #[error("Missing required event attrubute.")]
    MissingRequiredAttributeError(String),
}

pub struct Publisher {
    service: flowgen_core::service::Service,
    config: Arc<super::config::Target>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::publisher::Publisher for Publisher {
    type Error = PublisherError;
    async fn publish(mut self) -> Result<(), Self::Error> {
        let config = self.config.as_ref();
        let a = Path::new(&config.credentials);
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(a.to_path_buf())
            .build()
            .map_err(PublisherError::SalesforceAuthError)?
            .connect()
            .await
            .map_err(PublisherError::SalesforceAuthError)?;

        let pubsub = super::context::Builder::new(self.service)
            .with_client(sfdc_client)
            .build()
            .map_err(PublisherError::SalesforcePubSubError)?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await
            .map_err(PublisherError::SalesforcePubSubError)?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(PublisherError::SalesforcePubSubError)?
            .into_inner();

        let pubsub = pubsub.clone();

        let topic_name = &self.config.topic;
        let schema_id = &schema_info.schema_id;

        let schema: Schema = schema_info.schema_json.parse().unwrap();
        let serializer_config = &mut ser::SerializerConfig::new(&schema);

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let mut data = Map::new();
                if let Some(inputs) = &self.config.inputs {
                    for (key, input) in inputs {
                        let value = input.extract(&event.data, &event.extensions);
                        if let Ok(value) = value {
                            data.insert(key.to_owned(), value);
                        }
                    }
                }

                let payload = self
                    .config
                    .payload
                    .to_string()
                    .map_err(PublisherError::SerdeError)?
                    .render(&data)
                    .map_err(PublisherError::RenderError)?
                    .to_value()
                    .map_err(PublisherError::SerdeError)?;

                let mut publish_payload: Map<String, Value> = Map::new();
                for (k, v) in payload.as_object().unwrap() {
                    publish_payload.insert(k.to_owned(), v.to_owned());
                }
                let now = Utc::now().timestamp_millis();
                publish_payload.insert("CreatedDate".to_string(), Value::Number(now.into()));

                let serialized_payload: Vec<u8> =
                    serde_avro_fast::to_datum_vec(&publish_payload, serializer_config).unwrap();

                let mut events = Vec::new();
                let pe = ProducerEvent {
                    schema_id: schema_id.to_string(),
                    payload: serialized_payload,
                    ..Default::default()
                };
                events.push(pe);

                let resp = pubsub
                    .lock()
                    .await
                    .publish(PublishRequest {
                        topic_name: topic_name.to_string(),
                        events,
                        ..Default::default()
                    })
                    .await
                    .map_err(PublisherError::SalesforcePubSubError)?;

                println!("{:?}", resp);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    service: Option<flowgen_core::service::Service>,
    config: Option<Arc<super::config::Target>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl PublisherBuilder {
    pub fn new() -> PublisherBuilder {
        PublisherBuilder {
            ..Default::default()
        }
    }

    pub fn service(mut self, service: flowgen_core::service::Service) -> Self {
        self.service = Some(service);
        self
    }

    pub fn config(mut self, config: Arc<super::config::Target>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Publisher, PublisherError> {
        Ok(Publisher {
            service: self.service.ok_or_else(|| {
                PublisherError::MissingRequiredAttributeError("service".to_string())
            })?,
            config: self.config.ok_or_else(|| {
                PublisherError::MissingRequiredAttributeError("config".to_string())
            })?,
            rx: self.rx.ok_or_else(|| {
                PublisherError::MissingRequiredAttributeError("receiver".to_string())
            })?,
            current_task_id: self.current_task_id,
        })
    }
}
