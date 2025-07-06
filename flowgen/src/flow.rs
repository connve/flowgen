use crate::config::{FlowConfig, Task};
use flowgen_core::{cache::Cache, stream::event::Event, task::runner::Runner};
use std::{path::Path, sync::Arc};
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
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    DeltalakeWriter {
        #[source]
        source: flowgen_deltalake::writer::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    EnumerateProcessor {
        #[source]
        source: flowgen_core::task::enumerate::processor::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    SalesforcePubSubSubscriber {
        #[source]
        source: flowgen_salesforce::pubsub::subscriber::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    SalesforcePubsubPublisher {
        #[source]
        source: flowgen_salesforce::pubsub::publisher::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    HttpProcessor {
        #[source]
        source: flowgen_http::processor::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    NatsJetStreamPublisher {
        #[source]
        source: flowgen_nats::jetstream::publisher::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    NatsJetStreamSubscriber {
        #[source]
        source: flowgen_nats::jetstream::subscriber::Error,
        flow: String,
        task_id: usize,
    },
    #[error(transparent)]
    Cache(#[from] flowgen_nats::cache::Error),
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    FileReader {
        #[source]
        source: flowgen_file::reader::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    FileWriter {
        #[source]
        source: flowgen_file::writer::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    GenerateSubscriber {
        #[source]
        source: flowgen_core::task::generate::subscriber::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    NatsJetStreamObjectStoreSubscriber {
        #[source]
        source: flowgen_nats::jetstream::object_store::reader::Error,
        flow: String,
        task_id: usize,
    },
    #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    RenderProcessor {
        #[source]
        source: flowgen_core::task::render::processor::Error,
        flow: String,
        task_id: usize,
    },
    #[error("missing required event attribute")]
    MissingRequiredAttribute(String),
}

#[derive(Debug)]
pub struct Flow<'a> {
    config: Arc<FlowConfig>,
    cache_credential_path: &'a Path,
    pub task_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
}

impl Flow<'_> {
    pub async fn run(mut self) -> Result<Self, Error> {
        let mut task_list: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        let (tx, _): (Sender<Event>, Receiver<Event>) = tokio::sync::broadcast::channel(1000);

        let cache = flowgen_nats::cache::CacheBuilder::new()
            .credentials_path(self.cache_credential_path.to_path_buf())
            .build()
            .map_err(Error::Cache)?
            .init(DEFAULT_CACHE_NAME)
            .await
            .map_err(Error::Cache)?;

        let cache = Arc::new(cache);

        for (i, task) in self.config.flow.tasks.iter().enumerate() {
            match task {
                Task::deltalake_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let cache = Arc::clone(&cache);
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_deltalake::writer::WriterBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .cache(cache)
                            .build()
                            .map_err(|e| Error::DeltalakeWriter {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::DeltalakeWriter {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }

                Task::enumerate(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_core::task::enumerate::processor::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::EnumerateProcessor {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::EnumerateProcessor {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;

                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::file_reader(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let cache = Arc::clone(&cache);
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_file::reader::ReaderBuilder::new()
                            .config(config)
                            .sender(tx)
                            .receiver(rx)
                            .cache(cache)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::FileReader {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::FileReader {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::file_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_file::writer::WriterBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::FileWriter {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::FileWriter {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::generate(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_core::task::generate::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::GenerateSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::GenerateSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::http(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_http::processor::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::HttpProcessor {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::HttpProcessor {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;

                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::nats_jetstream_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_nats::jetstream::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::NatsJetStreamSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::NatsJetStreamSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::nats_jetstream_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let flow_config = Arc::clone(&self.config);
                    let rx = tx.subscribe();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_nats::jetstream::publisher::PublisherBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::NatsJetStreamPublisher {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::NatsJetStreamPublisher {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::object_store_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_nats::jetstream::object_store::reader::ReaderBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::NatsJetStreamObjectStoreSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .subscribe()
                            .await
                            .map_err(|e| Error::NatsJetStreamObjectStoreSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::salesforce_pubsub_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let cache = Arc::clone(&cache);
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_salesforce::pubsub::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .cache(cache)
                            .build()
                            .await
                            .map_err(|e| Error::SalesforcePubSubSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::SalesforcePubSubSubscriber {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::salesforce_pubsub_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_salesforce::pubsub::publisher::PublisherBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::SalesforcePubsubPublisher {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::SalesforcePubsubPublisher {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::render(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_core::task::render::processor::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::RenderProcessor {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::RenderProcessor {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;

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
pub struct FlowBuilder<'a> {
    config: Option<Arc<FlowConfig>>,
    cache_credentials_path: Option<&'a Path>,
}

impl<'a> FlowBuilder<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<FlowConfig>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn cache_credentials_path(mut self, path: &'a Path) -> Self {
        self.cache_credentials_path = Some(path);
        self
    }

    pub fn build(self) -> Result<Flow<'a>, Error> {
        Ok(Flow {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            cache_credential_path: self.cache_credentials_path.ok_or_else(|| {
                Error::MissingRequiredAttribute("cache_credential_path".to_string())
            })?,
            task_list: None,
        })
    }
}
