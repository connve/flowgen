//! Flow execution and task orchestration.
//!
//! Manages the execution of individual flows by creating and orchestrating
//! tasks from different processor types. Handles task lifecycle, error
//! propagation, and resource sharing between tasks.

use crate::config::{FlowConfig, Task};
use flowgen_core::{event::Event, task::runner::Runner};
use std::sync::Arc;
use tokio::{
    sync::broadcast::{self, Sender},
    task::JoinHandle,
};
use tracing::{debug, error, info, Instrument};

const DEFAULT_EVENT_BUFFER_SIZE: usize = 10000;

/// Errors that can occur during flow execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error in convert processor task.
    #[error(transparent)]
    ConverProcessor(#[from] flowgen_core::task::convert::processor::Error),
    /// Error in iterate processor task.
    #[error(transparent)]
    IterateProcessor(#[from] flowgen_core::task::iterate::processor::Error),
    /// Error in log processor task.
    #[error(transparent)]
    LogProcessor(#[from] flowgen_core::task::log::processor::Error),
    /// Error in script processor task.
    #[error(transparent)]
    ScriptProcessor(#[from] flowgen_core::task::script::processor::Error),
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
    GenerateSubscriber(#[from] flowgen_core::task::generate::subscriber::Error),
    /// Error in cache operations.
    #[error(transparent)]
    Cache(#[from] flowgen_nats::cache::Error),
    /// Missing required configuration attribute.
    #[error("Missing required attribute: {0}")]
    MissingRequiredAttribute(String),
    /// Leadership channel closed unexpectedly.
    #[error("Leadership channel closed unexpectedly")]
    LeadershipChannelClosed,
}

pub struct Flow {
    /// The flow's static configuration, loaded from a file.
    pub config: Arc<FlowConfig>,
    /// An optional shared HTTP server instance, passed in from the main application.
    http_server: Option<Arc<dyn flowgen_core::http_server::HttpServer>>,
    /// An optional client for host-level coordination (e.g., Kubernetes), passed in from the main application.
    host: Option<Arc<dyn flowgen_core::host::Host>>,
    /// An optional shared cache, passed in from the main application.
    cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
    /// Event channel buffer size for this flow (from app config or DEFAULT).
    event_buffer_size: Option<usize>,
    /// The task manager, responsible for leader election. Initialized by `init()`.,
    task_manager: Option<Arc<flowgen_core::task::manager::TaskManager>>,
    /// The shared context for all tasks in this flow. Initialized by `init()`.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// The broadcast channel sender for events within this flow. Initialized by `init()`.
    tx: Option<Sender<Event>>,
}

impl Flow {
    /// Returns the name of the flow.
    pub fn name(&self) -> &str {
        &self.config.flow.name
    }

    /// Determines if the flow should run with leader election.
    ///
    /// If a flow contains any webhook tasks, it will always be treated as
    /// non-leader-elected by disregarding the `required_leader_election` flag.
    fn is_leader_elected(&self) -> bool {
        let has_webhooks = self
            .config
            .flow
            .tasks
            .iter()
            .any(|task| matches!(task, Task::http_webhook(_)));

        if has_webhooks {
            if self.config.flow.require_leader_election.unwrap_or(false) {
                info!(
                    "Flow {} contains a webhook; `required_leader_election` flag will be ignored.",
                    self.config.flow.name
                );
            }
            return false; // Webhook flows are never leader elected.
        }

        // Otherwise, respect the configuration.
        self.config.flow.require_leader_election.unwrap_or(false)
    }

    /// Initializes shared resources for the flow, such as the TaskManager and TaskContext.
    /// This must be called before any other run methods.
    #[tracing::instrument(skip(self), name = "flow.init", fields(flow = %self.config.flow.name))]
    pub async fn init(&mut self) -> Result<(), Error> {
        if self.task_manager.is_some() {
            return Ok(()); // Already initialized
        }

        let mut task_manager_builder = flowgen_core::task::manager::TaskManagerBuilder::new();
        if let Some(ref host) = self.host {
            task_manager_builder = task_manager_builder.host(host.clone());
        }
        let task_manager = Arc::new(task_manager_builder.build().start().await);

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

        let buffer_size = self.event_buffer_size.unwrap_or(DEFAULT_EVENT_BUFFER_SIZE);
        let (tx, _) = broadcast::channel(buffer_size);

        self.task_manager = Some(task_manager);
        self.task_context = Some(task_context);
        self.tx = Some(tx);

        Ok(())
    }

    /// Spawns initial setup tasks that must complete before the HTTP server starts.
    ///
    /// This is specifically for registering webhooks for non-leader-elected flows.
    #[tracing::instrument(skip(self), name = "flow.run_http_handlers", fields(flow = %self.config.flow.name))]
    pub async fn run_http_handlers(&self) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
        // Only non-leader-elected flows can have webhook handlers that run at setup.
        if self.is_leader_elected() {
            return Ok(Vec::new());
        }

        let task_context = self.task_context.as_ref().ok_or_else(|| {
            Error::MissingRequiredAttribute("task_context: init() must be called first".to_string())
        })?;
        let tx = self.tx.as_ref().ok_or_else(|| {
            Error::MissingRequiredAttribute("tx: init() must be called first".to_string())
        })?;

        let webhook_task_configs: Vec<(usize, Task)> = self
            .config
            .flow
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| matches!(task, Task::http_webhook(_)))
            .map(|(i, task)| (i, task.clone()))
            .collect();

        if webhook_task_configs.is_empty() {
            return Ok(Vec::new());
        }

        // Spawn the webhook registration tasks.
        let (setup_handles, background_handles) =
            spawn_tasks(&webhook_task_configs, tx, task_context).await;

        // The `spawn_tasks` for webhooks should not produce background tasks.
        assert!(background_handles.is_empty());

        Ok(setup_handles)
    }

    /// Starts the main, long-running execution of the flow.
    ///
    /// This spawns a single master task that manages the flow's lifecycle,
    /// including leader election and running all background tasks.
    #[tracing::instrument(skip(self), name = "flow.run", fields(flow = %self.config.flow.name))]
    pub fn run(self) -> JoinHandle<()> {
        let flow_name = self.config.flow.name.clone();
        tokio::spawn(
            async move {
                if let Err(e) = self.run_background_tasks().await {
                    error!("Flow {} terminated with an error: {}", flow_name, e);
                }
            }
            .instrument(tracing::Span::current()),
        )
    }

    /// The main internal run loop for the flow.
    async fn run_background_tasks(self) -> Result<(), Error> {
        let is_leader_elected = self.is_leader_elected();
        let task_manager = self.task_manager.ok_or_else(|| {
            Error::MissingRequiredAttribute("task_manager: init() must be called first".to_string())
        })?;
        let task_context = self.task_context.ok_or_else(|| {
            Error::MissingRequiredAttribute("task_context: init() must be called first".to_string())
        })?;
        let tx = self.tx.ok_or_else(|| {
            Error::MissingRequiredAttribute("tx: init() must be called first".to_string())
        })?;

        // Determine which tasks to run in the main phase.
        // Setup-only tasks (i.e., non-elected webhooks) are excluded from this phase.
        let main_tasks_configs: Vec<(usize, Task)> = if !is_leader_elected {
            self.config
                .flow
                .tasks
                .iter()
                .enumerate()
                .filter(|(_, task)| !matches!(task, Task::http_webhook(_)))
                .map(|(i, task)| (i, task.clone()))
                .collect()
        } else {
            self.config
                .flow
                .tasks
                .iter()
                .enumerate()
                .map(|(i, task)| (i, task.clone()))
                .collect()
        };

        if main_tasks_configs.is_empty() {
            info!("Flow {} has no main tasks to run.", self.config.flow.name);
            return Ok(());
        }

        let flow_id = self.config.flow.name.clone();

        let leader_election_options = if is_leader_elected {
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

        // Main lifecycle loop.
        loop {
            // 1. Wait for leadership state.
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
                            "No leader election for flow {}, spawning tasks immediately",
                            flow_id
                        );
                        break;
                    }
                    None => return Err(Error::LeadershipChannelClosed),
                }
            }

            // 2. Spawn main tasks.
            let (_, mut background_tasks) =
                spawn_tasks(&main_tasks_configs, &tx, &task_context).await;

            // 3. Monitor tasks.
            if is_leader_elected {
                // For leader-elected flows, monitor leadership and abort tasks if leadership is lost.
                loop {
                    tokio::select! {
                        biased;
                        Some(status) = leadership_rx.recv() => {
                            if status == flowgen_core::task::manager::LeaderElectionResult::NotLeader {
                                debug!("Flow {} lost leadership, aborting all tasks", flow_id);
                                for task in &background_tasks {
                                    task.abort();
                                }
                                break; // Break inner loop to re-evaluate leadership.
                            }
                        }
                        _ = futures::future::join_all(&mut background_tasks), if !background_tasks.is_empty() => {
                            error!("All tasks completed unexpectedly for flow {}", flow_id);
                            break; // Break inner loop.
                        }
                    }
                }
            } else {
                // For non-leader-elected flows, just wait for all tasks to complete.
                futures::future::join_all(background_tasks).await;
                info!("All tasks completed for flow {}", flow_id);
                break; // Exit main loop as the work is done.
            }
        }
        Ok(())
    }
}

/// Spawns all tasks for the flow.
/// Returns (blocking_tasks, background_tasks) where blocking_tasks complete quickly
/// and must be awaited before the application is ready (e.g., webhooks registering routes),
/// while background_tasks run indefinitely.
async fn spawn_tasks(
    tasks: &[(usize, Task)],
    tx: &Sender<Event>,
    task_context: &Arc<flowgen_core::task::context::TaskContext>,
) -> (
    Vec<JoinHandle<Result<(), Error>>>,
    Vec<JoinHandle<Result<(), Error>>>,
) {
    let mut blocking_tasks = Vec::new();
    let mut background_tasks = Vec::new();

    for (i, task) in tasks.iter() {
        let i = *i; // Copy the index value so it can be moved into async blocks
        match task {
            Task::convert(config) => {
                let config = Arc::new(config.to_owned());
                let rx = tx.subscribe();
                let tx = tx.clone();
                let task_context = Arc::clone(task_context);
                let span = tracing::Span::current();
                let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                    async move {
                        flowgen_core::task::convert::processor::ProcessorBuilder::new()
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
            Task::iterate(config) => {
                let config = Arc::new(config.to_owned());
                let rx = tx.subscribe();
                let tx = tx.clone();
                let task_context = Arc::clone(task_context);
                let span = tracing::Span::current();
                let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                    async move {
                        flowgen_core::task::iterate::processor::ProcessorBuilder::new()
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
            Task::log(config) => {
                let config = Arc::new(config.to_owned());
                let rx = tx.subscribe();
                let tx = tx.clone();
                let task_context = Arc::clone(task_context);
                let span = tracing::Span::current();
                let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                    async move {
                        flowgen_core::task::log::processor::ProcessorBuilder::new()
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
            Task::script(config) => {
                let config = Arc::new(config.to_owned());
                let rx = tx.subscribe();
                let tx = tx.clone();
                let task_context = Arc::clone(task_context);
                let span = tracing::Span::current();
                let task: JoinHandle<Result<(), Error>> = tokio::spawn(
                    async move {
                        flowgen_core::task::script::processor::ProcessorBuilder::new()
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
                        flowgen_core::task::generate::subscriber::SubscriberBuilder::new()
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
    /// Optional event channel buffer size.
    event_buffer_size: Option<usize>,
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

    /// Sets the event channel buffer size.
    pub fn event_buffer_size(mut self, size: usize) -> Self {
        self.event_buffer_size = Some(size);
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
            http_server: self.http_server,
            host: self.host,
            cache: self.cache,
            event_buffer_size: self.event_buffer_size,
            task_manager: None,
            task_context: None,
            tx: None,
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
                require_leader_election: None,
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
        match result {
            Err(Error::MissingRequiredAttribute(attr)) => assert_eq!(attr, "config"),
            _ => panic!("Expected MissingRequiredAttribute error"),
        }
    }

    #[test]
    fn test_flow_builder_build_without_http_server() {
        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![],
                require_leader_election: None,
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
                require_leader_election: None,
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
        assert!(flow.task_manager.is_none());
        assert!(flow.task_context.is_none());
    }

    #[test]
    fn test_error_convert_processor() {
        let convert_error = flowgen_core::task::convert::processor::Error::MissingRequiredAttribute(
            "test".to_string(),
        );
        let error = Error::ConverProcessor(convert_error);

        let error_str = error.to_string();
        assert!(error_str.contains("Missing required attribute: test"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_EVENT_BUFFER_SIZE, 10_000);
    }
}
