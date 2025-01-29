use crate::config::Task;

use super::config;
use flowgen_core::{client::Client, event::Event, publisher::Publisher};
use flowgen_nats::jetstream::message::FlowgenMessageExt;
use std::{path::PathBuf, sync::Arc};
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{error, event, Level};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse config file")]
    ParseConfig(#[source] serde_json::Error),
    #[error("Cannot setup Flowgen Client")]
    FlowgenService(#[source] flowgen_core::service::Error),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    FlowgenSalesforcePubSubSubscriberError(#[source] flowgen_salesforce::pubsub::subscriber::Error),
    #[error("Failed to setup Salesforce PubSub as flow source.")]
    SalesforcePubsubPublisher(#[source] flowgen_salesforce::pubsub::publisher::PublisherError),
    #[error("There was an error with processing http request.")]
    HttpProcessorError(#[source] flowgen_http::processor::ProcessorError),
    #[error("There was an error with Flowgen Nats JetStream Publisher.")]
    FlowgenNatsJetStreamPublisher(#[source] flowgen_nats::jetstream::publisher::Error),
    #[error("There was an error with Flowgen Nats JetStream Subscriber.")]
    FlowgenNatsJetStreamSubscriber(#[source] flowgen_nats::jetstream::subscriber::Error),
    #[error("There was an error with Flowgen Nats JetStream Event.")]
    FlowgenNatsJetStreamEventError(#[source] flowgen_nats::jetstream::message::Error),
    #[error("There was an error with Flowgen File Subscriber.")]
    FlowgenFileSubscriberError(#[source] flowgen_file::subscriber::SubscriberError),
    #[error("Failed to publish message to Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Cannot execute async task.")]
    TokioJoin(#[source] tokio::task::JoinError),
}

#[derive(Debug)]
pub struct Flow {
    config: config::Config,
    pub handle_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
}

impl Flow {
    pub async fn run(mut self) -> Result<Self, Error> {
        let service = flowgen_core::service::Builder::new()
            .with_endpoint(format!("{0}:443", "https://api.pubsub.salesforce.com"))
            .build()
            .map_err(Error::FlowgenService)?
            .connect()
            .await
            .map_err(Error::FlowgenService)?;

        let config = &self.config;
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let (tx, _): (Sender<Event>, Receiver<Event>) = tokio::sync::broadcast::channel(1000);

        for (i, task) in config.flow.tasks.iter().enumerate() {
            match task {
                Task::source(source) => match source {
                    config::Source::salesforce_pubsub(config) => {
                        flowgen_salesforce::pubsub::subscriber::Builder::new(
                            service.clone(),
                            config.clone(),
                            &tx,
                            i,
                        )
                        .build()
                        .await
                        .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?
                        .subscribe()
                        .await
                        .map_err(Error::FlowgenSalesforcePubSubSubscriberError)?;
                    }
                    config::Source::nats_jetstream(config) => {
                        flowgen_nats::jetstream::subscriber::Builder::new(config.clone(), &tx, i)
                            .build()
                            .await
                            .map_err(Error::FlowgenNatsJetStreamSubscriber)?
                            .subscribe()
                            .await
                            .map_err(Error::FlowgenNatsJetStreamSubscriber)?;
                    }
                    _ => {}
                },
                Task::processor(processor) => match processor {
                    config::Processor::http(config) => {
                        flowgen_http::processor::Builder::new(config.clone(), &tx, i)
                            .build()
                            .await
                            .map_err(Error::HttpProcessorError)?
                            .process()
                            .await
                            .map_err(Error::HttpProcessorError)?
                    }
                },
                Task::target(target) => match target {
                    config::Target::nats_jetstream(config) => {
                        let publisher =
                            flowgen_nats::jetstream::publisher::Builder::new(config.clone())
                                .build()
                                .await
                                .map_err(Error::FlowgenNatsJetStreamPublisher)?;
                        let publisher = Arc::new(publisher);

                        {
                            let publisher = publisher.clone();
                            let mut rx = tx.subscribe();
                            let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                                while let Ok(e) = rx.recv().await {
                                    if e.current_task_id == Some(i - 1) {
                                        let event = e
                                            .to_publish()
                                            .map_err(Error::FlowgenNatsJetStreamEventError)?;

                                        publisher
                                            .jetstream
                                            .send_publish(e.subject.clone(), event)
                                            .await
                                            .map_err(Error::NatsPublish)?;

                                        event!(Level::INFO, "event processed: {}", e.subject);
                                    }
                                }
                                Ok(())
                            });
                            handle_list.push(handle);
                        }
                    }
                    config::Target::salesforce_pubsub(config) => {
                        let config = Arc::new(config.to_owned());
                        let rx = tx.subscribe();
                        let service = service.clone();
                        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                            flowgen_salesforce::pubsub::publisher::PublisherBuilder::new()
                                .service(service)
                                .config(config)
                                .receiver(rx)
                                .current_task_id(i)
                                .build()
                                .await
                                .map_err(Error::SalesforcePubsubPublisher)?
                                .publish()
                                .await
                                .map_err(Error::SalesforcePubsubPublisher)?;
                            Ok(())
                        });
                        handle_list.push(handle);
                    }
                    _ => {}
                },
            }
        }
        self.handle_list = Some(handle_list);
        Ok(self)
    }
}

#[derive(Default, Debug)]
pub struct Builder {
    config_path: PathBuf,
}

impl Builder {
    pub fn new(config_path: PathBuf) -> Builder {
        Builder { config_path }
    }
    pub fn build(&mut self) -> Result<Flow, Error> {
        let c = std::fs::read_to_string(&self.config_path)
            .map_err(|e| Error::OpenFile(e, self.config_path.clone()))?;
        let config: config::Config = serde_json::from_str(&c).map_err(Error::ParseConfig)?;
        let f = Flow {
            config,
            handle_list: None,
        };
        Ok(f)
    }
}
