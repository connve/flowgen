use super::config;
use crate::config::Task;
use flowgen_core::{stream::event::Event, task::runner::Runner};
use std::{path::PathBuf, sync::Arc};
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("error reading a credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("error parsing config file")]
    ParseConfig(#[source] serde_json::Error),
    #[error("error setting up Salesforce PubSub as flow source")]
    SalesforcePubSubSubscriber(#[source] flowgen_salesforce::pubsub::subscriber::Error),
    #[error("error setting up Salesforce PubSub as flow source")]
    SalesforcePubsubPublisher(#[source] flowgen_salesforce::pubsub::publisher::Error),
    #[error("error processing http request")]
    HttpProcessor(#[source] flowgen_http::processor::Error),
    #[error("error processing element during enumaration")]
    EnumerateProcessor(#[source] flowgen_core::task::enumerate::processor::Error),
    #[error("error with NATS JetStream Publisher")]
    NatsJetStreamPublisher(#[source] flowgen_nats::jetstream::publisher::Error),
    #[error("error with NATS JetStream Subscriber")]
    NatsJetStreamSubscriber(#[source] flowgen_nats::jetstream::subscriber::Error),
    #[error("error with file subscriber")]
    FileReader(#[source] flowgen_file::reader::Error),
    #[error("error with file publisher")]
    FileWriter(#[source] flowgen_file::writer::Error),
    #[error("error with generate subscriber")]
    GenerateSubscriber(#[source] flowgen_core::task::generate::subscriber::Error),
    #[error("error with NATS JetStream Subscriber")]
    NatsJetStreamObjectStoreSubscriber(#[source] flowgen_nats::jetstream::object_store::reader::Error),

}

#[derive(Debug)]
pub struct Flow {
    config: config::Config,
    pub handle_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
}

impl Flow {
    pub async fn run(mut self) -> Result<Self, Error> {
        let config = &self.config;
        let mut handle_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let (tx, _): (Sender<Event>, Receiver<Event>) = tokio::sync::broadcast::channel(1000);

        for (i, task) in config.flow.tasks.iter().enumerate() {
            match task {
                Task::enumerate(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_core::task::enumerate::processor::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::EnumerateProcessor)?
                            .run()
                            .await
                            .map_err(Error::EnumerateProcessor)?;

                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::file_reader(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_file::reader::ReaderBuilder::new()
                            .config(config)
                            .sender(tx)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::FileReader)?
                            .run()
                            .await
                            .map_err(Error::FileReader)?;
                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::file_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_file::writer::WriterBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::FileWriter)?
                            .run()
                            .await
                            .map_err(Error::FileWriter)?;
                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::generate(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_core::task::generate::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::GenerateSubscriber)?
                            .run()
                            .await
                            .map_err(Error::GenerateSubscriber)?;
                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::http(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_http::processor::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::HttpProcessor)?
                            .run()
                            .await
                            .map_err(Error::HttpProcessor)?;

                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::nats_jetstream_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_nats::jetstream::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::NatsJetStreamSubscriber)?
                            .run()
                            .await
                            .map_err(Error::NatsJetStreamSubscriber)?;
                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::nats_jetstream_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_nats::jetstream::publisher::PublisherBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::NatsJetStreamPublisher)?
                            .run()
                            .await
                            .map_err(Error::NatsJetStreamPublisher)?;
                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::object_store_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                        let tx = tx.clone();
                        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                            flowgen_nats::jetstream::object_store::reader::NatsObjectReaderBuilder::new()
                                .config(config)
                                .sender(tx)
                                .current_task_id(i)
                                .build()
                                .await
                                .map_err(Error::NatsJetStreamObjectStoreSubscriber)?
                                .subscribe()
                                .await
                                .map_err(Error::NatsJetStreamObjectStoreSubscriber)?;
                            Ok(())
                        });
                        handle_list.push(handle);

                }
                Task::salesforce_pubsub_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_salesforce::pubsub::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::SalesforcePubSubSubscriber)?
                            .run()
                            .await
                            .map_err(Error::SalesforcePubSubSubscriber)?;
                        Ok(())
                    });
                    handle_list.push(handle);
                }
                Task::salesforce_pubsub_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_salesforce::pubsub::publisher::PublisherBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::SalesforcePubsubPublisher)?
                            .run()
                            .await
                            .map_err(Error::SalesforcePubsubPublisher)?;
                        Ok(())
                    });
                    handle_list.push(handle);
                }
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
