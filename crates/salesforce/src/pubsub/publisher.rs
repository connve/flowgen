use arrow::array::StringArray;
use flowgen_core::{client::Client, event::Event};
use handlebars::Handlebars;
use std::{collections::HashMap, sync::Arc};
use tokio::{sync::broadcast::Receiver, task::JoinHandle};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with PubSub context.")]
    FlowgenSalesforcePubSub(#[source] super::context::Error),
    #[error("There was an error with Salesforce authentication.")]
    FlowgenSalesforceAuth(#[source] crate::client::Error),
    #[error("Missing required event attrubute.")]
    MissingRequiredAttribute(String),
}

pub struct Publisher {
    service: flowgen_core::service::Service,
    config: super::config::Target,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl Publisher {
    pub async fn publish(mut self) -> Result<(), Error> {
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let handlebars = Handlebars::new();

        let sfdc_client = crate::client::Builder::new()
            .with_credentials_path(self.config.credentials.into())
            .build()
            .map_err(Error::FlowgenSalesforceAuth)?
            .connect()
            .await
            .map_err(Error::FlowgenSalesforceAuth)?;

        let pubsub = super::context::Builder::new(self.service)
            .with_client(sfdc_client)
            .build()
            .map_err(Error::FlowgenSalesforcePubSub)?;

        tokio::spawn(async move {
            while let Ok(e) = self.rx.recv().await {
                if e.current_task_id == Some(self.current_task_id - 1) {
                    println!("{:?}", e);
                    let mut data = HashMap::new();
                    if let Some(inputs) = &self.config.inputs {
                        for (key, input) in inputs {
                            if !input.is_static && !input.is_extension {
                                let array: StringArray = e
                                    .data
                                    .column_by_name(&input.value)
                                    .unwrap()
                                    .to_data()
                                    .into();

                                for item in (&array).into_iter().flatten() {
                                    data.insert(key.to_string(), item.to_string());
                                }
                            }
                        }
                    }

                    let serialized_payload = serde_json::to_string(&self.config.payload).unwrap();
                    let payload = handlebars
                        .render_template(&serialized_payload, &data)
                        .unwrap();

                    println!("{:?}", payload);
                }
            }
        });
        Ok(())
    }
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
