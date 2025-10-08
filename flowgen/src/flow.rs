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
use tracing::{error, Instrument};

/// Default cache bucket name for flow caching.
const DEFAULT_CACHE_NAME: &str = "flowgen_cache";

/// Errors that can occur during flow execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error in convert processor task.
    #[error(transparent)]
    ConverProcessor(#[from] flowgen_core::convert::processor::Error),
    /// Error in Salesforce Pub/Sub subscriber task.
    #[error(transparent)]
    SalesforcePubSubSubscriber(#[from] flowgen_salesforce::pubsub::subscriber::Error),
    /// Error in Salesforce Pub/Sub publisher task.
    #[error(transparent)]
    SalesforcePubsubPublisher(#[from] flowgen_salesforce::pubsub::publisher::Error),
    /// Error in HTTP request processor task.
    #[error(transparent)]
    HttpRequestProcessor(#[from] flowgen_http::request::Error),
    /// Error in HTTP webhook processor task.
    #[error(transparent)]
    HttpWebhookProcessor(#[from] flowgen_http::webhook::Error),
    /// Error in HTTP server task.
    #[error(transparent)]
    HttpServer(#[from] flowgen_http::server::Error),
    /// Error in NATS JetStream publisher task.
    #[error(transparent)]
    NatsJetStreamPublisher(#[from] flowgen_nats::jetstream::publisher::Error),
    /// Error in NATS JetStream subscriber task.
    #[error(transparent)]
    NatsJetStreamSubscriber(#[from] flowgen_nats::jetstream::subscriber::Error),
    /// Error in object store reader task.
    #[error(transparent)]
    ObjectStoreReader(#[from] flowgen_object_store::reader::Error),
    /// Error in object store writer task.
    #[error(transparent)]
    ObjectStoreWriter(#[from] flowgen_object_store::writer::Error),
    /// Error in generate subscriber task.
    #[error(transparent)]
    GenerateSubscriber(#[from] flowgen_core::generate::subscriber::Error),
    /// Error in cache operations.
    #[error(transparent)]
    Cache(#[from] flowgen_nats::cache::Error),
    /// Missing required configuration attribute.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
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
    /// Optional host client for coordination.
    host: Option<Arc<flowgen_core::task::context::HostClient>>,
}

impl Flow<'_> {
    /// Executes all tasks in the flow concurrently.
    ///
    /// Creates a shared event channel, initializes a cache, and spawns
    /// each configured task as a separate tokio task. Tasks are connected
    /// via broadcast channels for event communication.
    #[tracing::instrument(skip(self), fields(flow = %self.config.flow.name))]
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

        // Create task manager with host if available.
        let mut task_manager = flowgen_core::task::manager::TaskManagerBuilder::new();
        if let Some(ref host) = self.host {
            task_manager = task_manager.host(host.client.clone());
        }
        let task_manager = Arc::new(task_manager.build().start().await);

        // Create task context for execution.
        let task_context = Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name(self.config.flow.name.clone())
                .flow_labels(self.config.flow.labels.clone())
                .task_manager(task_manager)
                .build()
                .map_err(|e| Error::MissingRequiredAttribute(e.to_string()))?,
        );

        for (i, task) in self.config.flow.tasks.iter().enumerate() {
            match task {
                Task::convert(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_core::convert::processor::ProcessorBuilder::new()
                                .config(config)
                                .receiver(rx)
                                .sender(tx)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;

                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::generate(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let cache = Arc::clone(&cache);
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_core::generate::subscriber::SubscriberBuilder::new()
                                .config(config)
                                .sender(tx)
                                .cache(cache)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;
                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::http_request(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_http::request::ProcessorBuilder::new()
                                .config(config)
                                .receiver(rx)
                                .sender(tx)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;

                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::http_webhook(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let http_server = Arc::clone(&self.http_server);
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_http::webhook::ProcessorBuilder::new()
                                .config(config)
                                .sender(tx)
                                .current_task_id(i)
                                .http_server(http_server)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;

                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }

                Task::nats_jetstream_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_nats::jetstream::subscriber::SubscriberBuilder::new()
                                .config(config)
                                .sender(tx)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;
                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::nats_jetstream_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_nats::jetstream::publisher::PublisherBuilder::new()
                                .config(config)
                                .receiver(rx)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;
                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::salesforce_pubsub_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let cache = Arc::clone(&cache);
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_salesforce::pubsub::subscriber::SubscriberBuilder::new()
                                .config(config)
                                .sender(tx)
                                .current_task_id(i)
                                .cache(cache)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;
                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::salesforce_pubsub_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_salesforce::pubsub::publisher::PublisherBuilder::new()
                                .config(config)
                                .receiver(rx)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;
                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::object_store_reader(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let cache = Arc::clone(&cache);
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current().clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_object_store::reader::ReaderBuilder::new()
                                .config(config)
                                .sender(tx)
                                .receiver(rx)
                                .cache(cache)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;
                            Ok(())
                        }
                        .instrument(span),
                    );
                    task_list.push(task);
                }
                Task::object_store_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task_context = Arc::clone(&task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_object_store::writer::WriterBuilder::new()
                                .config(config)
                                .receiver(rx)
                                .current_task_id(i)
                                .task_context(task_context)
                                .build()
                                .await?
                                .run()
                                .await?;
                            Ok(())
                        }
                        .instrument(span),
                    );
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
    /// Optional host client for coordination.
    host: Option<Arc<flowgen_core::task::context::HostClient>>,
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

    /// Sets the host client for coordination.
    pub fn host(mut self, client: Option<Arc<flowgen_core::task::context::HostClient>>) -> Self {
        self.host = client;
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
            host: self.host,
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
                labels: None,
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
                labels: None,
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
                labels: None,
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
                labels: None,
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
                labels: None,
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
    fn test_error_convert_processor() {
        let convert_error =
            flowgen_core::convert::processor::Error::MissingRequiredAttribute("test".to_string());
        let error = Error::ConverProcessor(convert_error);

        let error_str = error.to_string();
        assert!(error_str.contains("Missing required attribute: test"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_CACHE_NAME, "flowgen_cache");
    }
}
