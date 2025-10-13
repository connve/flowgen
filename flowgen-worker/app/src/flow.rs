//! Flow execution and task orchestration.
//!
//! Manages the execution of individual flows by creating and orchestrating
//! tasks from different processor types. Handles task lifecycle, error
//! propagation, and resource sharing between tasks.

use crate::config::{FlowConfig, Task};
use flowgen_core::{event::Event, task::runner::Runner};
use std::sync::Arc;
use tokio::{
    sync::broadcast::{Receiver, Sender},
    task::JoinHandle,
};
use tracing::{debug, error, info, Instrument};

const DEFAULT_EVENT_BUFFER_SIZE: usize = 1000;

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
    /// Leadership channel closed unexpectedly.
    #[error("Leadership channel closed unexpectedly")]
    LeadershipChannelClosed,
}

/// A flow execution context managing tasks and resources.
#[derive(Debug)]
pub struct Flow {
    /// Flow configuration defining name and tasks.
    config: Arc<FlowConfig>,
    /// List of spawned task handles for concurrent execution.
    pub task_list: Option<Vec<JoinHandle<Result<(), Error>>>>,
    /// Optional shared HTTP server for webhook tasks.
    http_server: Option<Arc<dyn flowgen_core::http_server::HttpServer>>,
    /// Optional host client for coordination.
    host: Option<Arc<dyn flowgen_core::host::Host>>,
    /// Optional shared cache for task operations.
    cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
}

impl Flow {
    /// Determines if this flow should wait for ready based on its tasks.
    ///
    /// Returns true if the flow contains webhook tasks and doesn't require leader election.
    /// Webhook flows need to complete route registration before the HTTP server starts.
    pub fn should_wait_for_ready(&self) -> bool {
        // Auto-detect: flows with webhooks and no leader election should wait
        let has_webhooks = self
            .config
            .flow
            .tasks
            .iter()
            .any(|task| matches!(task, Task::http_webhook(_)));
        let requires_election = self.config.flow.require_lease_election.unwrap_or(false);

        has_webhooks && !requires_election
    }

    /// Executes all tasks in the flow concurrently.
    ///
    /// Creates a shared event channel and spawns each configured task as a
    /// separate tokio task. Tasks are connected via broadcast channels for
    /// event communication.
    #[tracing::instrument(skip(self), fields(flow = %self.config.flow.name))]
    pub async fn run(self) -> Result<Self, Error> {
        let (tx, _): (Sender<Event>, Receiver<Event>) =
            tokio::sync::broadcast::channel(DEFAULT_EVENT_BUFFER_SIZE);

        // Create task manager with host if available.
        let mut task_manager = flowgen_core::task::manager::TaskManagerBuilder::new();
        if let Some(ref host) = self.host {
            task_manager = task_manager.host(host.clone());
        }
        let task_manager = Arc::new(task_manager.build().start().await);

        // Create task context for this execution.
        let task_context = Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name(self.config.flow.name.clone())
                .flow_labels(self.config.flow.labels.clone())
                .task_manager(Arc::clone(&task_manager))
                .cache(self.cache.clone())
                .http_server(self.http_server.clone())
                .build()
                .map_err(|e| Error::MissingRequiredAttribute(e.to_string()))?,
        );

        // Register flow for leader election if required.
        let flow_id = self.config.flow.name.clone();
        let require_lease_election = self.config.flow.require_lease_election.unwrap_or(false);

        let leader_election_options = if require_lease_election {
            Some(flowgen_core::task::manager::LeaderElectionOptions {})
        } else {
            None
        };

        let mut leadership_rx = task_manager
            .register(flow_id.clone(), leader_election_options)
            .await
            .map_err(|e| {
                Error::MissingRequiredAttribute(format!(
                    "Failed to register flow for leader election: {e}"
                ))
            })?;

        // Leadership monitoring and task spawning loop.
        let mut task_list: Vec<JoinHandle<Result<(), Error>>>;
        loop {
            // Wait for leadership acquisition.
            loop {
                match leadership_rx.recv().await {
                    Some(flowgen_core::task::manager::LeaderElectionResult::Leader) => {
                        info!("Flow {} acquired leadership, spawning tasks", flow_id);
                        break;
                    }
                    Some(flowgen_core::task::manager::LeaderElectionResult::NotLeader) => {
                        debug!("Flow {} is not leader, waiting for leadership", flow_id);
                    }
                    Some(flowgen_core::task::manager::LeaderElectionResult::NoElection) => {
                        debug!(
                            "No leader election configured for flow {}, spawning tasks immediately",
                            flow_id
                        );
                        break;
                    }
                    None => {
                        return Err(Error::LeadershipChannelClosed);
                    }
                }
            }

            // Spawn all tasks as leader.
            let (blocking_tasks, background_tasks) =
                Self::spawn_tasks(&self.config.flow.tasks, &tx, &task_context).await;

            // For flows without leader election, wait for blocking tasks to complete
            if !require_lease_election && !blocking_tasks.is_empty() {
                futures::future::join_all(blocking_tasks).await;
            }

            task_list = background_tasks;

            // Monitor for leadership changes while tasks are running (only if election enabled).
            if require_lease_election {
                loop {
                    tokio::select! {
                        biased;

                        // Check for leadership changes.
                        Some(status) = leadership_rx.recv() => {
                            if status == flowgen_core::task::manager::LeaderElectionResult::NotLeader {
                                debug!("Flow {} lost leadership, aborting all tasks", flow_id);
                                for task in &task_list {
                                    task.abort();
                                }
                                task_list.clear();
                                break;
                            }
                        }

                        // Wait for all tasks to complete.
                        _ = futures::future::join_all(&mut task_list), if !task_list.is_empty() => {
                            error!("All tasks completed unexpectedly for flow {}", flow_id);
                            task_list.clear();
                            break;
                        }
                    }
                }
            } else {
                // No leader election - return immediately with spawned tasks.
                // Tasks will run in the background.
                return Ok(Self {
                    config: self.config,
                    task_list: Some(task_list),
                    http_server: self.http_server,
                    host: self.host,
                    cache: self.cache,
                });
            }
        }
    }

    /// Spawns all tasks for the flow.
    /// Returns (blocking_tasks, background_tasks) where blocking_tasks complete quickly
    /// and must be awaited before the application is ready (e.g., webhooks registering routes),
    /// while background_tasks run indefinitely.
    async fn spawn_tasks(
        tasks: &[Task],
        tx: &Sender<Event>,
        task_context: &Arc<flowgen_core::task::context::TaskContext>,
    ) -> (
        Vec<JoinHandle<Result<(), Error>>>,
        Vec<JoinHandle<Result<(), Error>>>,
    ) {
        let mut blocking_tasks = Vec::new();
        let mut background_tasks = Vec::new();

        for (i, task) in tasks.iter().enumerate() {
            match task {
                Task::convert(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task_context = Arc::clone(task_context);
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
                    background_tasks.push(task);
                }
                Task::generate(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task_context = Arc::clone(task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_core::generate::subscriber::SubscriberBuilder::new()
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
                    background_tasks.push(task);
                }
                Task::http_request(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task_context = Arc::clone(task_context);
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
                    background_tasks.push(task);
                }
                Task::http_webhook(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task_context = Arc::clone(task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_http::webhook::ProcessorBuilder::new()
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
                    blocking_tasks.push(task);
                }

                Task::nats_jetstream_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task_context = Arc::clone(task_context);
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
                    background_tasks.push(task);
                }
                Task::nats_jetstream_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task_context = Arc::clone(task_context);
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
                    background_tasks.push(task);
                }
                Task::salesforce_pubsub_subscriber(config) => {
                    let config = Arc::new(config.to_owned());
                    let tx = tx.clone();
                    let task_context = Arc::clone(task_context);
                    let span = tracing::Span::current();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_salesforce::pubsub::subscriber::SubscriberBuilder::new()
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
                    background_tasks.push(task);
                }
                Task::salesforce_pubsub_publisher(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task_context = Arc::clone(task_context);
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
                    background_tasks.push(task);
                }
                Task::object_store_reader(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let tx = tx.clone();
                    let task_context = Arc::clone(task_context);
                    let span = tracing::Span::current().clone();
                    let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                        async move {
                            flowgen_object_store::reader::ReaderBuilder::new()
                                .config(config)
                                .sender(tx)
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
                    background_tasks.push(task);
                }
                Task::object_store_writer(config) => {
                    let config = Arc::new(config.to_owned());
                    let rx = tx.subscribe();
                    let task_context = Arc::clone(task_context);
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
                    background_tasks.push(task);
                }
            }
        }

        (blocking_tasks, background_tasks)
    }
}

/// Builder for creating Flow instances.
#[derive(Default)]
pub struct FlowBuilder {
    /// Optional flow configuration.
    config: Option<Arc<FlowConfig>>,
    /// Optional shared HTTP server instance.
    http_server: Option<Arc<dyn flowgen_core::http_server::HttpServer>>,
    /// Optional host client for coordination.
    host: Option<Arc<dyn flowgen_core::host::Host>>,
    /// Optional shared cache instance.
    cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
}

impl FlowBuilder {
    /// Creates a new FlowBuilder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the flow configuration.
    pub fn config(mut self, config: Arc<FlowConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the shared HTTP server instance.
    pub fn http_server(mut self, server: Arc<dyn flowgen_core::http_server::HttpServer>) -> Self {
        self.http_server = Some(server);
        self
    }

    /// Sets the host client for coordination.
    pub fn host(mut self, client: Option<Arc<dyn flowgen_core::host::Host>>) -> Self {
        self.host = client;
        self
    }

    /// Sets the shared cache instance.
    pub fn cache(mut self, cache: Option<Arc<dyn flowgen_core::cache::Cache>>) -> Self {
        self.cache = cache;
        self
    }

    /// Builds a Flow instance from the configured options.
    ///
    /// # Errors
    /// Returns `Error::MissingRequiredAttribute` if required fields are not set.
    pub fn build(self) -> Result<Flow, Error> {
        Ok(Flow {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            task_list: None,
            http_server: self.http_server,
            host: self.host,
            cache: self.cache,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Flow, FlowConfig};

    #[test]
    fn test_flow_builder_new() {
        let builder = FlowBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_default() {
        let builder = FlowBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_config() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![],
                require_lease_election: None,
            },
        });

        let builder = FlowBuilder::new().config(flow_config.clone());
        assert_eq!(builder.config, Some(flow_config));
    }

    #[test]
    fn test_flow_builder_http_server() {
        let server = Arc::new(flowgen_http::server::HttpServerBuilder::new().build());
        let builder = FlowBuilder::new().http_server(server.clone());
        assert!(builder.http_server.is_some());
    }

    #[test]
    fn test_flow_builder_build_missing_config() {
        let server = Arc::new(flowgen_http::server::HttpServerBuilder::new().build());

        let result = FlowBuilder::new().http_server(server).build();

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "config")
        );
    }

    #[test]
    fn test_flow_builder_build_without_http_server() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![],
                require_lease_election: None,
            },
        });

        let result = FlowBuilder::new().config(flow_config).build();

        assert!(result.is_ok());
        let flow = result.unwrap();
        assert!(flow.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_build_success() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "success_flow".to_string(),
                labels: None,
                tasks: vec![],
                require_lease_election: None,
            },
        });
        let server = Arc::new(flowgen_http::server::HttpServerBuilder::new().build());

        let result = FlowBuilder::new()
            .config(flow_config.clone())
            .http_server(server)
            .build();

        assert!(result.is_ok());
        let flow = result.unwrap();
        assert_eq!(flow.config, flow_config);

        assert!(flow.task_list.is_none());
    }

    #[test]
    fn test_flow_builder_chain() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "chain_flow".to_string(),
                labels: None,
                tasks: vec![],
                require_lease_election: None,
            },
        });
        let server = Arc::new(flowgen_http::server::HttpServerBuilder::new().build());

        let flow = FlowBuilder::new()
            .config(flow_config.clone())
            .http_server(server)
            .build()
            .unwrap();

        assert_eq!(flow.config, flow_config);
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
        assert_eq!(DEFAULT_EVENT_BUFFER_SIZE, 1000);
    }
}
