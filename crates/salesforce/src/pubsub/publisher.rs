use flowgen_core::{
    client::Client,
    event::Event,
    render::Render,
    serde::{MapExt, StringExt},
};
use salesforce_pubsub::eventbus::v1::{ProducerEvent, PublishRequest, SchemaRequest, TopicRequest};
use serde_json::{Map, Value};
use std::{future::Future, sync::Arc};
use tokio::{
    sync::{broadcast::Receiver, Mutex},
    task::JoinHandle,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context.")]
    SalesforcePubSub(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    SalesforceAuth(#[source] crate::client::Error),
    #[error("There was an error with parsing a given value.")]
    Serde(#[source] flowgen_core::serde::Error),
    #[error("There was an error with rendering a given value.")]
    Render(#[source] flowgen_core::render::Error),
    #[error("Missing required event attrubute.")]
    MissingRequiredAttribute(String),
}

pub struct Publisher {
    service: flowgen_core::service::Service,
    config: super::config::Target,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::publisher::Publisher for Publisher {
    async fn publish(mut self) -> Result<(), Error> {
        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::SalesforceAuth)?
            .connect()
            .await
            .map_err(Error::SalesforceAuth)?;

        let pubsub = super::context::Builder::new(self.service)
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
            .unwrap()
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .unwrap()
            .into_inner();

        let pubsub = pubsub.clone();

        let topic_name = &self.config.topic;
        let schema_id = &schema_info.schema_id;

        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                let mut data = Map::new();
                if let Some(inputs) = &self.config.inputs {
                    for (key, input) in inputs {
                        let value = input.extract_from(&event.data, &event.extensions);
                        if let Ok(value) = value {
                            data.insert(key.to_string(), Value::String(value.to_string()));
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

                let mut bytes: Vec<u8> = Vec::new();
                serde_json::to_writer(&mut bytes, &payload).unwrap();

                let mut events = Vec::new();
                let pe = ProducerEvent {
                    schema_id: schema_id.to_string(),
                    payload: bytes,
                    ..Default::default()
                };

                println!("{:?}", payload);

                events.push(pe);
                let test = pubsub
                    .lock()
                    .await
                    .publish(PublishRequest {
                        topic_name: topic_name.to_string(),
                        events,
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                println!("{:?}", test);
            }
        }
        Ok(())
    }

    type Error = Error;
}

#[derive(Default)]
pub struct PublisherBuilder {
    service: Option<flowgen_core::service::Service>,
    config: Option<super::config::Target>,
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

    pub fn config(mut self, config: super::config::Target) -> Self {
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
            service: self
                .service
                .ok_or_else(|| Error::MissingRequiredAttribute("data".to_string()))?,
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("subject".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
