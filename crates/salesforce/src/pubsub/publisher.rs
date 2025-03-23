use chrono::Utc;
use flowgen_core::{
    client::Client,
    conversion::render::Render,
    conversion::serde::{MapExt, StringExt},
    event::Event,
};
use salesforce_pubsub::eventbus::v1::{ProducerEvent, PublishRequest, SchemaRequest, TopicRequest};
use serde_avro_fast::{ser, Schema};
use serde_json::{Map, Value};
use std::{path::Path, sync::Arc};
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "salesforce.pubsub.out";
const DEFAULT_PUBSUB_URI: &str = "https://api.pubsub.salesforce.com";
const DEFAULT_PUBSUB_PORT: &str = "443";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with PubSub context")]
    SalesforcePubSub(#[source] super::context::Error),
    #[error("error with Salesforce authentication")]
    SalesforceAuth(#[source] crate::client::Error),
    #[error("error with parsing a given value")]
    Serde(#[source] flowgen_core::conversion::serde::Error),
    #[error("error with parsing a given value")]
    SerdeJson(#[source] serde_json::error::Error),
    #[error("error with rendering a given value")]
    Render(#[source] flowgen_core::conversion::render::Error),
    #[error("error with processing recordbatch")]
    RecordBatch(#[source] flowgen_core::conversion::recordbatch::Error),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
    #[error("error with sending event over channel")]
    SendMessage(#[source] tokio::sync::broadcast::error::SendError<Event>),
    #[error("error with creating event")]
    Event(#[source] flowgen_core::event::Error),
    #[error("error setting up flowgen grpc service")]
    Service(#[source] flowgen_core::service::Error),
}

pub struct Publisher {
    config: Arc<super::config::Target>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::publisher::Publisher for Publisher {
    type Error = Error;
    async fn publish(mut self) -> Result<(), Self::Error> {
        let config = self.config.as_ref();
        let a = Path::new(&config.credentials);

        let service = flowgen_core::service::ServiceBuilder::new()
            .endpoint(format!("{0}:{1}", DEFAULT_PUBSUB_URI, DEFAULT_PUBSUB_PORT))
            .build()
            .map_err(Error::Service)?
            .connect()
            .await
            .map_err(Error::Service)?;

        let sfdc_client = crate::client::Builder::new()
            .credentials_path(a.to_path_buf())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        let pubsub = super::context::Builder::new(service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::SalesforcePubSub)?;

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.clone(),
            })
            .await
            .map_err(Error::SalesforcePubSub)?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(Error::SalesforcePubSub)?
            .into_inner();

        let pubsub = pubsub.clone();

        let topic = &self.config.topic;
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
                    .map_err(Error::Serde)?
                    .render(&data)
                    .map_err(Error::Render)?
                    .to_value()
                    .map_err(Error::Serde)?;

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

                let _ = pubsub
                    .lock()
                    .await
                    .publish(PublishRequest {
                        topic_name: topic.to_string(),
                        events,
                        ..Default::default()
                    })
                    .await
                    .map_err(Error::SalesforcePubSub)?;

                let timestamp = Utc::now().timestamp_micros();
                let topic = topic_info.topic_name.replace('/', ".").to_lowercase();
                let subject = format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, &topic[1..], timestamp);

                event!(Level::INFO, "event published: {}", subject);
            }
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
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

    pub async fn build(self) -> Result<Publisher, Error> {
        Ok(Publisher {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
