use super::config;
use crate::config::Task;
use flowgen_core::{cache::Cache, stream::event::Event, task::runner::Runner};
use std::{path::PathBuf, sync::Arc};
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::error;

/// Default alias for the source data (in-memory table) in MERGE operations.
const DEFAULT_CACHE_NAME: &str = "flowgen_cache";

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    DeltalakeWriter(#[from] flowgen_deltalake::writer::Error),
    #[error("error processing element during enumaration")]
    EnumerateProcessor(#[source] flowgen_core::task::enumerate::processor::Error),
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
    NatsJetStreamObjectStoreSubscriber(
        #[source] flowgen_nats::jetstream::object_store::reader::Error,
    ),
    #[error("error rendering content")]
    RenderProcessor(#[source] flowgen_core::task::render::processor::Error),
    /// An expected attribute or configuration value was missing.
    #[error("missing required event attribute")]
    MissingRequiredAttribute(String),
}

#[derive(Debug)]
pub struct Flow {
    config_path: PathBuf,
    cache_credential_path: PathBuf,
    pub task_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
}

impl Flow {
    pub async fn run(mut self) -> Result<Self, Error> {
        let c = std::fs::read_to_string(&self.config_path)
            .map_err(|e| Error::OpenFile(e, self.config_path.clone()))?;
        let config: config::Config = serde_json::from_str(&c).map_err(Error::ParseConfig)?;

        let mut task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let (tx, _): (Sender<Event>, Receiver<Event>) = tokio::sync::broadcast::channel(1000);

        let cache = flowgen_nats::cache::CacheBuilder::new()
            .credentials_path(self.cache_credential_path.clone())
            .build()
            .unwrap()
            .init(DEFAULT_CACHE_NAME)
            .await
            .unwrap();

        let cache = Arc::new(cache);

        for (i, task) in config.flow.tasks.iter().enumerate() {
            match task {
                Task::deltalake_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let cache = Arc::clone(&cache);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_deltalake::writer::WriterBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .cache(cache)
                            .build()
                            .map_err(Error::DeltalakeWriter)?
                            .run()
                            .await
                            .map_err(Error::DeltalakeWriter)?;
                        Ok(())
                    });
                    task_list.push(task);
                }

                Task::enumerate(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::file_reader(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let cache = Arc::clone(&cache);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_file::reader::ReaderBuilder::new()
                            .config(config)
                            .sender(tx)
                            .receiver(rx)
                            .cache(cache)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::FileReader)?
                            .run()
                            .await
                            .map_err(Error::FileReader)?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::file_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::generate(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::http(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::nats_jetstream_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::nats_jetstream_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::object_store_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_nats::jetstream::object_store::reader::ReaderBuilder::new()
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
                    task_list.push(task);
                }
                Task::salesforce_pubsub_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::salesforce_pubsub_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
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
                    task_list.push(task);
                }
                Task::render(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_core::task::render::processor::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(Error::RenderProcessor)?
                            .run()
                            .await
                            .map_err(Error::RenderProcessor)?;

                        Ok(())
                    });
                    task_list.push(task);
                }
            }
        }
        self.task_list = Some(task_list);
        Ok(self)
    }
}

#[derive(Default)]
pub struct FlowBuilder {
    config_path: Option<PathBuf>,
    cache_credentials_path: Option<PathBuf>,
}

impl FlowBuilder {
    pub fn new() -> FlowBuilder {
        FlowBuilder {
            ..Default::default()
        }
    }

    pub fn config_path(mut self, config_path: PathBuf) -> Self {
        self.config_path = Some(config_path);
        self
    }

    pub fn cache_credentials_path(mut self, cache_credentials_path: PathBuf) -> Self {
        self.cache_credentials_path = Some(cache_credentials_path);
        self
    }

    pub fn build(self) -> Result<Flow, Error> {
        Ok(Flow {
            config_path: self
                .config_path
                .ok_or_else(|| Error::MissingRequiredAttribute("config_path".to_string()))?,
            cache_credential_path: self.cache_credentials_path.ok_or_else(|| {
                Error::MissingRequiredAttribute("cache_credential_path".to_string())
            })?,
            task_list: None,
        })
    }
}
