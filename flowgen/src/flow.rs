//! Flow execution and task orchestration.
//!
//! Manages the execution of individual flows by creating and orchestrating
//! tasks from different processor types. Handles task lifecycle, error
//! propagation, and resource sharing between tasks.

use crate::config::{FlowConfig, Task};
use flowgen_core::{cache::Cache, event::Event, task::runner::Runner};
use std::{path::Path, sync::Arc};
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::error;

/// Default cache bucket name for flow caching.
const DEFAULT_CACHE_NAME: &str = "flowgen_cache";

/// Errors that can occur during flow execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error in convert processor task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    ConverProcessor {
        #[source]
        source: Box<flowgen_core::task::convert::processor::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in Salesforce Pub/Sub subscriber task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    SalesforcePubSubSubscriber {
        #[source]
        source: Box<flowgen_salesforce::pubsub::subscriber::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in Salesforce Pub/Sub publisher task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    SalesforcePubsubPublisher {
        #[source]
        source: Box<flowgen_salesforce::pubsub::publisher::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in HTTP request processor task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    HttpRequestProcessor {
        #[source]
        source: Box<flowgen_http::request::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in HTTP webhook processor task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    HttpWebhookProcessor {
        #[source]
        source: Box<flowgen_http::webhook::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in HTTP server task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    HttpServer {
        #[source]
        source: Box<flowgen_http::server::Error>,
        flow: String,
        task_id: usize,
    },

     #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    BulkapiJobRetrieverError {
        #[source]
        source: flowgen_salesforce::bulkapi::job_retriever::Error,
        flow: String,
        task_id: usize,
    },
    
    /// Error in NATS JetStream publisher task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    NatsJetStreamPublisher {
        #[source]
        source: Box<flowgen_nats::jetstream::publisher::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in NATS JetStream subscriber task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    NatsJetStreamSubscriber {
        #[source]
        source: Box<flowgen_nats::jetstream::subscriber::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in object store reader task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    ObjectStoreReader {
        #[source]
        source: Box<flowgen_object_store::reader::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in object store writer task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    ObjectStoreWriter {
        #[source]
        source: Box<flowgen_object_store::writer::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in generate subscriber task.
    #[error("Flow: {flow}, task_id: {task_id}, source: {source}")]
    GenerateSubscriber {
        #[source]
        source: Box<flowgen_core::task::generate::subscriber::Error>,
        flow: String,
        task_id: usize,
    },
    /// Error in cache operations.
    #[error(transparent)]
    Cache(#[from] flowgen_nats::cache::Error),
    /// Missing required configuration attribute.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),

     #[error("flow: {flow}, task_id: {task_id}, source: {source}")]
    BulkapiJobCreatorError {
        #[source]
        source: flowgen_salesforce::bulkapi::job_creator::Error,
        flow: String,
        task_id: usize,
    },
}

/// A flow execution context managing tasks and resources.
#[derive(Debug)]
pub struct Flow<'a> {
    /// Flow configuration defining name and tasks.
    config: Arc<FlowConfig>,
    /// Path to cache credentials for task authentication.
    cache_credential_path: &'a Path,
    /// List of spawned task handles for concurrent execution.
    pub task_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
    /// Shared HTTP server for webhook tasks.
    http_server: Arc<flowgen_http::server::HttpServer>,
}

impl Flow<'_> {
    /// Executes all tasks in the flow concurrently.
    ///
    /// Creates a shared event channel, initializes a cache, and spawns
    /// each configured task as a separate tokio task. Tasks are connected
    /// via broadcast channels for event communication.
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
                Task::convert(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_core::task::convert::processor::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::ConverProcessor {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::ConverProcessor {
                                source: Box::new(e),
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
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::GenerateSubscriber {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::http_request(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_http::request::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::HttpRequestProcessor {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::HttpRequestProcessor {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;

                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::http_webhook(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let http_server = Arc::clone(&self.http_server);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_http::webhook::ProcessorBuilder::new()
                            .config(config)
                            .sender(tx)
                            .current_task_id(i)
                            .http_server(http_server)
                            .build()
                            .await
                            .map_err(|e| Error::HttpWebhookProcessor {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::HttpWebhookProcessor {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;

                        Ok(())
                    });
                    task_list.push(task);
                }

                Task::salesforce_bulkapi_job_creator(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_salesforce::bulkapi::job_creator::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::BulkapiJobCreatorError {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::BulkapiJobCreatorError {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;

                        Ok(())
                    });
                    task_list.push(task);
                }


                Task::salesforce_bulkapi_job_retriever(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_salesforce::bulkapi::job_retriever::ProcessorBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .sender(tx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::BulkapiJobRetrieverError {
                                source: e,
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::BulkapiJobRetrieverError {
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
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::NatsJetStreamSubscriber {
                                source: Box::new(e),
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
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::NatsJetStreamPublisher {
                                source: Box::new(e),
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
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::SalesforcePubSubSubscriber {
                                source: Box::new(e),
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
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::SalesforcePubsubPublisher {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::object_store_reader(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let cache = Arc::clone(&cache);
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_object_store::reader::ReaderBuilder::new()
                            .config(config)
                            .sender(tx)
                            .receiver(rx)
                            .cache(cache)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::ObjectStoreReader {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::ObjectStoreReader {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?;
                        Ok(())
                    });
                    task_list.push(task);
                }
                Task::object_store_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let flow_config = Arc::clone(&self.config);
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        flowgen_object_store::writer::WriterBuilder::new()
                            .config(config)
                            .receiver(rx)
                            .current_task_id(i)
                            .build()
                            .await
                            .map_err(|e| Error::ObjectStoreWriter {
                                source: Box::new(e),
                                flow: flow_config.flow.name.to_owned(),
                                task_id: i,
                            })?
                            .run()
                            .await
                            .map_err(|e| Error::ObjectStoreWriter {
                                source: Box::new(e),
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

/// Builder for creating Flow instances.
#[derive(Default)]
pub struct FlowBuilder<'a> {
    /// Optional flow configuration.
    config: Option<Arc<FlowConfig>>,
    /// Optional path to cache credentials.
    cache_credentials_path: Option<&'a Path>,
    /// Optional shared HTTP server instance.
    http_server: Option<Arc<flowgen_http::server::HttpServer>>,
}

impl<'a> FlowBuilder<'a> {
    /// Creates a new FlowBuilder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the flow configuration.
    pub fn config(mut self, config: Arc<FlowConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the cache credentials path.
    pub fn cache_credentials_path(mut self, path: &'a Path) -> Self {
        self.cache_credentials_path = Some(path);
        self
    }

    /// Sets the shared HTTP server instance.
    pub fn http_server(mut self, server: Arc<flowgen_http::server::HttpServer>) -> Self {
        self.http_server = Some(server);
        self
    }

    /// Builds a Flow instance from the configured options.
    ///
    /// # Errors
    /// Returns `Error::MissingRequiredAttribute` if required fields are not set.
    pub fn build(self) -> Result<Flow<'a>, Error> {
        Ok(Flow {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            cache_credential_path: self.cache_credentials_path.ok_or_else(|| {
                Error::MissingRequiredAttribute("cache_credential_path".to_string())
            })?,
            task_list: None,
            http_server: self
                .http_server
                .ok_or_else(|| Error::MissingRequiredAttribute("http_server".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Flow, FlowConfig};
    use std::path::Path;

    #[test]
    fn test_flow_builder_new() {
        let builder = FlowBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.cache_credentials_path.is_none());
        assert!(builder.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_default() {
        let builder = FlowBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.cache_credentials_path.is_none());
        assert!(builder.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_config() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                tasks: vec![],
            },
        });

        let builder = FlowBuilder::new().config(flow_config.clone());
        assert_eq!(builder.config, Some(flow_config));
    }

    #[test]
    fn test_flow_builder_cache_credentials_path() {
        let path = Path::new("/test/credentials");
        let builder = FlowBuilder::new().cache_credentials_path(path);
        assert_eq!(builder.cache_credentials_path, Some(path));
    }

    #[test]
    fn test_flow_builder_http_server() {
        let server = Arc::new(flowgen_http::server::HttpServer::new());
        let builder = FlowBuilder::new().http_server(server.clone());
        assert!(builder.http_server.is_some());
    }

    #[test]
    fn test_flow_builder_build_missing_config() {
        let path = Path::new("/test/credentials");
        let server = Arc::new(flowgen_http::server::HttpServer::new());

        let result = FlowBuilder::new()
            .cache_credentials_path(path)
            .http_server(server)
            .build();

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[test]
    fn test_flow_builder_build_missing_cache_path() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                tasks: vec![],
            },
        });
        let server = Arc::new(flowgen_http::server::HttpServer::new());

        let result = FlowBuilder::new()
            .config(flow_config)
            .http_server(server)
            .build();

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "cache_credential_path")
        );
    }

    #[test]
    fn test_flow_builder_build_missing_http_server() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                tasks: vec![],
            },
        });
        let path = Path::new("/test/credentials");

        let result = FlowBuilder::new()
            .config(flow_config)
            .cache_credentials_path(path)
            .build();

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "http_server")
        );
    }

    #[test]
    fn test_flow_builder_build_success() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "success_flow".to_string(),
                tasks: vec![],
            },
        });
        let path = Path::new("/test/credentials");
        let server = Arc::new(flowgen_http::server::HttpServer::new());

        let result = FlowBuilder::new()
            .config(flow_config.clone())
            .cache_credentials_path(path)
            .http_server(server)
            .build();

        assert!(result.is_ok());
        let flow = result.unwrap();
        assert_eq!(flow.config, flow_config);
        assert_eq!(flow.cache_credential_path, path);
        assert!(flow.task_list.is_none());
    }

    #[test]
    fn test_flow_builder_chain() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "chain_flow".to_string(),
                tasks: vec![],
            },
        });
        let path = Path::new("/chain/credentials");
        let server = Arc::new(flowgen_http::server::HttpServer::new());

        let flow = FlowBuilder::new()
            .config(flow_config.clone())
            .cache_credentials_path(path)
            .http_server(server)
            .build()
            .unwrap();

        assert_eq!(flow.config, flow_config);
        assert_eq!(flow.cache_credential_path, path);
    }

    #[test]
    fn test_error_display() {
        let error = Error::MissingRequiredAttribute("test_field".to_string());
        assert_eq!(error.to_string(), "Missing required attribute: test_field");
    }

    #[test]
    fn test_error_convert_processor() {
        let convert_error = flowgen_core::task::convert::processor::Error::MissingRequiredAttribute(
            "test".to_string(),
        );
        let error = Error::ConverProcessor {
            source: Box::new(convert_error),
            flow: "test_flow".to_string(),
            task_id: 0,
        };

        let error_str = error.to_string();
        assert!(error_str.contains("Flow: test_flow"));
        assert!(error_str.contains("task_id: 0"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_CACHE_NAME, "flowgen_cache");
    }
}
