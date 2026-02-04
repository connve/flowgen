//! Flow execution and task orchestration.
//!
//! This module manages the lifecycle of flows, including task registry creation,
//! channel wiring, leader election, and event propagation through task chains.
//!
//! ## Architecture
//!
//! The core pattern uses a `TaskRegistry` that builds a linear chain of tasks
//! connected by MPSC channels:
//!
//! ```text
//! [Task 0] --channel[0]--> [Task 1] --channel[1]--> [Task 2]
//! ```
//!
//! For N tasks, we create N-1 channels. Each task receives:
//! - `input_rx`: Receiver from previous channel (None for first task)
//! - `output_tx`: Sender to next channel (None for last task)
//!
//! ## Channel Ownership
//!
//! All tasks are spawned together using the SAME `TaskRegistry` to ensure
//! channels are properly connected. This is critical for webhook flows where
//! blocking tasks (webhooks) must send events to background tasks:
//!
//! 1. `run_http_handlers()` spawns ALL tasks (webhooks and background)
//! 2. Returns only blocking handles to wait for webhook registration
//! 3. Background tasks are already running with connected channels
//! 4. `run_background_tasks()` monitors the pre-spawned tasks
//!
//! This prevents channel mismatch bugs where webhooks send to one channel
//! while background tasks receive from a different channel.

use crate::config::{FlowConfig, TaskType};
use flowgen_core::{event::Event, task::runner::Runner};
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, info, warn, Instrument};

// Event buffer size for MPSC channels. This needs to be large enough to handle
// bursts from tasks like iterate that fan-out large arrays (e.g., 100k+ rows).
// Set to 10M (~1.2 GB memory) to provide ample headroom for high-volume processing.
// With MPSC, this buffer is distributed across N-1 channels for N tasks.
const DEFAULT_EVENT_BUFFER_SIZE: usize = 10_000_000;

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
    /// Error in buffer processor task.
    #[error(transparent)]
    BufferProcessor(#[from] flowgen_core::task::buffer::processor::Error),
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
    /// Error in object store read task.
    #[error(transparent)]
    ObjectStoreRead(#[from] flowgen_object_store::read::Error),
    /// Error in object store write task.
    #[error(transparent)]
    ObjectStoreWrite(#[from] flowgen_object_store::write::Error),
    /// Error in generate subscriber task.
    #[error(transparent)]
    GenerateSubscriber(#[from] flowgen_core::task::generate::subscriber::Error),
    /// Error in cache operations.
    #[error(transparent)]
    Cache(#[from] flowgen_nats::cache::Error),
    /// Missing required builder attribute.
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    /// Task context not initialized (init must be called first).
    #[error("Task context not initialized: init() must be called first")]
    TaskContextNotInitialized,
    /// Task manager not initialized (init must be called first).
    #[error("Task manager not initialized: init() must be called first")]
    TaskManagerNotInitialized,
    /// Failed to register flow for leader election.
    #[error("Failed to register flow for leader election: {0}")]
    LeaderElectionRegistrationFailed(String),
    /// Leadership channel closed unexpectedly.
    #[error("Leadership channel closed unexpectedly")]
    LeadershipChannelClosed,
    /// Error in Salesforce Bulk API unified job operations.
    #[error(transparent)]
    SalesforceBulkApiJob(#[from] flowgen_salesforce::bulkapi::job::Error),
    /// Error in GCP BigQuery query task.
    #[error(transparent)]
    GcpBigQueryQuery(#[from] flowgen_gcp::bigquery::query::Error),
    /// Error in GCP BigQuery Storage Read task.
    #[error(transparent)]
    GcpBigQueryStorageRead(#[from] flowgen_gcp::bigquery::storage_read::Error),
    /// Error in GCP BigQuery unified job operations.
    #[error(transparent)]
    GcpBigQueryJob(#[from] flowgen_gcp::bigquery::job::Error),
    /// Failed to store background task handles for later monitoring.
    #[error("Failed to store background task handles")]
    BackgroundHandlesStoreFailed,
    /// Failed to retrieve background task handles for monitoring.
    #[error("Failed to retrieve background task handles")]
    BackgroundHandlesRetrieveFailed,
}

/// Descriptor for a task with its channel endpoints.
#[derive(Debug)]
struct TaskDescriptor {
    /// Unique task identifier (from original task array index).
    id: usize,
    /// Task configuration and type.
    task_type: TaskType,
    /// Input channel receiver (None for source tasks).
    input_rx: Option<mpsc::Receiver<Event>>,
    /// Output channel sender (None for sink tasks).
    output_tx: Option<mpsc::Sender<Event>>,
    /// Whether this is a blocking setup task (e.g., webhook registration).
    is_blocking: bool,
}

/// Central registry for managing all tasks in a flow.
#[derive(Debug)]
struct TaskRegistry {
    /// All tasks in execution order.
    tasks: Vec<TaskDescriptor>,
}

/// Type alias for a task join handle.
type TaskHandle = JoinHandle<Result<(), Error>>;

/// Handle collections separated by task type.
pub struct TaskHandles {
    /// Handles for blocking setup tasks (e.g., webhook route registration).
    pub blocking_handles: Vec<TaskHandle>,
    /// Handles for long-running background tasks.
    pub background_handles: Vec<TaskHandle>,
}

impl TaskRegistry {
    /// Creates a new builder for constructing a task registry.
    fn builder(flow_config: Arc<FlowConfig>, buffer_size: usize) -> TaskRegistryBuilder {
        TaskRegistryBuilder {
            flow_config,
            buffer_size,
        }
    }

    /// Separates tasks into blocking (setup) and background tasks.
    fn partition(self) -> (Vec<TaskDescriptor>, Vec<TaskDescriptor>) {
        let mut blocking = Vec::new();
        let mut background = Vec::new();

        for task in self.tasks {
            if task.is_blocking {
                blocking.push(task);
            } else {
                background.push(task);
            }
        }

        (blocking, background)
    }
}

/// Builder for constructing a task registry with proper channel wiring.
#[derive(Debug)]
struct TaskRegistryBuilder {
    flow_config: Arc<FlowConfig>,
    buffer_size: usize,
}

impl TaskRegistryBuilder {
    /// Builds a complete task registry with all channels properly wired.
    fn build(self) -> Result<TaskRegistry, Error> {
        let tasks_config = &self.flow_config.flow.tasks;
        let task_count = tasks_config.len();

        if task_count == 0 {
            return Ok(TaskRegistry { tasks: Vec::new() });
        }

        // Create channels for the linear task chain.
        // For N tasks, we need N-1 channels connecting them.
        let mut channels: Vec<(mpsc::Sender<Event>, mpsc::Receiver<Event>)> = (0..task_count
            .saturating_sub(1))
            .map(|_| mpsc::channel(self.buffer_size))
            .collect();

        let mut task_descriptors = Vec::with_capacity(task_count);

        for (idx, task_type) in tasks_config.iter().enumerate() {
            // Determine blocking status (only webhooks are blocking).
            let is_blocking = matches!(task_type, TaskType::http_webhook(_));

            // Wire input: task receives from the previous channel (if not the first task).
            let input_rx = if idx > 0 {
                channels.get_mut(idx - 1).map(|(_, rx)| {
                    // Take ownership of the receiver by replacing it with a dummy channel.
                    std::mem::replace(rx, mpsc::channel(1).1)
                })
            } else {
                None
            };

            // Wire output: task sends to the next channel (if not the last task).
            let output_tx = if idx < task_count - 1 {
                channels.get(idx).map(|(tx, _)| tx.clone())
            } else {
                None
            };

            task_descriptors.push(TaskDescriptor {
                id: idx,
                task_type: task_type.clone(),
                input_rx,
                output_tx,
                is_blocking,
            });
        }

        Ok(TaskRegistry {
            tasks: task_descriptors,
        })
    }
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
    /// Optional app-level retry configuration, passed in from the main application.
    retry: Option<flowgen_core::retry::RetryConfig>,
    /// Optional resource loader for loading external files, passed in from the main application.
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    /// The task manager, responsible for leader election. Initialized by `init()`.
    task_manager: Option<Arc<flowgen_core::task::manager::TaskManager>>,
    /// The shared context for all tasks in this flow. Initialized by `init()`.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Background task handles spawned by run_http_handlers for run_background_tasks to monitor.
    background_handles: Arc<std::sync::Mutex<Option<Vec<TaskHandle>>>>,
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
            .any(|task| matches!(task, TaskType::http_webhook(_)));

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

        let mut task_context_builder = flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name(self.config.flow.name.clone())
            .flow_labels(self.config.flow.labels.clone())
            .task_manager(Arc::clone(&task_manager))
            .cache(self.cache.clone())
            .http_server(self.http_server.clone())
            .resource_loader(self.resource_loader.clone());

        if let Some(retry_config) = &self.retry {
            task_context_builder = task_context_builder.retry(retry_config.clone());
        }

        let task_context = Arc::new(
            task_context_builder
                .build()
                .map_err(|e| Error::MissingBuilderAttribute(e.to_string()))?,
        );

        self.task_manager = Some(task_manager);
        self.task_context = Some(task_context);

        Ok(())
    }

    /// Spawns all tasks and returns handles separated by type.
    ///
    /// This method spawns ALL tasks together (both blocking and background) using a
    /// single `TaskRegistry`. This ensures all tasks use the SAME channels for event
    /// propagation, which is critical for webhook flows.
    ///
    /// ## Why All Tasks Are Spawned Together
    ///
    /// For webhook flows, the webhook (blocking) task must send events to background
    /// tasks through a shared channel. If we spawn them separately with different
    /// registries, they get different channels and events are lost.
    ///
    /// ## Example Flow
    ///
    /// ```text
    /// TaskRegistry creates:
    ///   channel[0]: webhook -> script
    ///   channel[1]: script -> nats
    ///
    /// Spawns:
    ///   webhook (blocking, registers route, keeps running)
    ///   script (background, processes events)
    ///   nats (background, publishes to NATS)
    ///
    /// Returns:
    ///   blocking_handles: [webhook_handle]
    ///   background_handles: [script_handle, nats_handle]
    /// ```
    ///
    /// The caller should await `blocking_handles` before starting the HTTP server,
    /// but `background_handles` are already running and connected.
    pub async fn spawn_all_tasks(&self) -> Result<TaskHandles, Error> {
        let task_context = self
            .task_context
            .as_ref()
            .ok_or_else(|| Error::TaskContextNotInitialized)?
            .clone();

        // Build task registry with all tasks properly wired.
        let buffer_size = self.event_buffer_size.unwrap_or(DEFAULT_EVENT_BUFFER_SIZE);
        let registry = TaskRegistry::builder(self.config.clone(), buffer_size).build()?;

        // Separate blocking (setup) tasks from background tasks.
        let (blocking_tasks, background_tasks) = registry.partition();

        // Spawn all blocking tasks (webhooks).
        let mut blocking_handles = Vec::new();
        for task_desc in blocking_tasks {
            let handle = spawn_task(task_desc, task_context.clone()).await?;
            blocking_handles.push(handle);
        }

        // Spawn all background tasks.
        let mut background_handles = Vec::new();
        for task_desc in background_tasks {
            let handle = spawn_task(task_desc, task_context.clone()).await?;
            background_handles.push(handle);
        }

        Ok(TaskHandles {
            blocking_handles,
            background_handles,
        })
    }

    /// Spawns initial setup tasks that must complete before the HTTP server starts.
    ///
    /// This is specifically for registering webhooks for non-leader-elected flows.
    pub async fn run_http_handlers(&self) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
        // Only non-leader-elected flows can have webhook handlers that run at setup.
        if self.is_leader_elected() {
            return Ok(Vec::new());
        }

        // Spawn all tasks and return only the blocking handles.
        let handles = self.spawn_all_tasks().await?;

        // Store background handles for run_background_tasks to monitor.
        let mut lock = self
            .background_handles
            .lock()
            .map_err(|_| Error::BackgroundHandlesStoreFailed)?;
        *lock = Some(handles.background_handles);

        Ok(handles.blocking_handles)
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
        let task_manager = self
            .task_manager
            .as_ref()
            .ok_or_else(|| Error::TaskManagerNotInitialized)?
            .clone();
        let flow_id = self.config.flow.name.clone();

        let leader_election_options = if is_leader_elected {
            Some(flowgen_core::task::manager::LeaderElectionOptions {})
        } else {
            None
        };

        let mut leadership_rx = task_manager
            .register(flow_id.clone(), leader_election_options)
            .await
            .map_err(|e| Error::LeaderElectionRegistrationFailed(e.to_string()))?;

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
                    None => {
                        info!("Flow {} leadership channel closed, exiting", flow_id);
                        return Ok(());
                    }
                }
            }

            // 2. Get or spawn tasks.
            let mut background_tasks = if !is_leader_elected {
                // For non-leader-elected flows, retrieve the already-spawned background tasks.
                let mut lock = self
                    .background_handles
                    .lock()
                    .map_err(|_| Error::BackgroundHandlesRetrieveFailed)?;

                lock.take()
                    .ok_or_else(|| Error::BackgroundHandlesRetrieveFailed)?
            } else {
                // For leader-elected flows, spawn all tasks fresh.
                let handles = self.spawn_all_tasks().await?;
                handles
                    .blocking_handles
                    .into_iter()
                    .chain(handles.background_handles)
                    .collect()
            };

            if background_tasks.is_empty() {
                info!("Flow {} has no tasks to monitor.", flow_id);
                return Ok(());
            }

            // 3. Monitor tasks.
            if is_leader_elected {
                // For leader-elected flows, monitor leadership and abort tasks if leadership is lost.
                let all_completed = loop {
                    tokio::select! {
                        biased;
                        Some(status) = leadership_rx.recv() => {
                            if status == flowgen_core::task::manager::LeaderElectionResult::NotLeader {
                                debug!("Flow {} lost leadership, aborting all tasks", flow_id);
                                for task in &background_tasks {
                                    task.abort();
                                }
                                break false; // Lost leadership, may need to re-acquire.
                            }
                        }
                        results = futures::future::join_all(&mut background_tasks), if !background_tasks.is_empty() => {
                            // Check if any tasks failed.
                            for (idx, result) in results.iter().enumerate() {
                                if let Err(e) = result {
                                    error!("Task {} failed: {}", idx, e);
                                }
                            }
                            info!("All tasks completed for flow {}", flow_id);
                            break true; // All tasks completed successfully.
                        }
                    }
                };

                if all_completed {
                    // All tasks finished, exit main loop.
                    break;
                }
                // Otherwise, lost leadership - clear aborted tasks and loop back to re-acquire.
                background_tasks.clear();
            } else {
                // For non-leader-elected flows, just wait for all tasks to complete.
                let results = futures::future::join_all(background_tasks).await;

                // Check if any tasks failed.
                for (idx, result) in results.iter().enumerate() {
                    if let Err(e) = result {
                        error!("Task {} failed: {}", idx, e);
                    }
                }
                info!("All tasks completed for flow {}", flow_id);
                break; // Exit main loop as the work is done.
            }
        }
        Ok(())
    }
}

/// Spawns a single task based on its descriptor with proper channel wiring.
///
/// Returns a JoinHandle for the spawned task.
async fn spawn_task(
    task_desc: TaskDescriptor,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    let task_id = task_desc.id;
    let rx = task_desc.input_rx;
    let tx = task_desc.output_tx;
    let task_type_str = task_desc.task_type.as_str();
    let span = tracing::Span::current();

    let handle = match task_desc.task_type {
        TaskType::convert(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::convert::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::iterate(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::iterate::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::log(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_core::task::log::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::buffer(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::buffer::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::script(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::script::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::generate(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::generate::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::http_request(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_http::request::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::http_webhook(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_http::webhook::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::nats_jetstream_subscriber(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_nats::jetstream::subscriber::SubscriberBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::nats_jetstream_publisher(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_nats::jetstream::publisher::PublisherBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_pubsub_subscriber(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_salesforce::pubsub::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_pubsub_publisher(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_salesforce::pubsub::publisher::PublisherBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_bulkapi_job(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_salesforce::bulkapi::job::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::object_store_read(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_object_store::read::ReadProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::object_store_write(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_object_store::write::WriteProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::gcp_bigquery_query(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_gcp::bigquery::query::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::gcp_bigquery_storage_read(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_gcp::bigquery::storage_read::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::gcp_bigquery_job(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_gcp::bigquery::job::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
    };

    Ok(handle)
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
    /// Optional app-level retry configuration.
    retry: Option<flowgen_core::retry::RetryConfig>,
    /// Resource loader for loading external files.
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
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

    /// Sets the app-level retry configuration.
    pub fn retry(mut self, retry: flowgen_core::retry::RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Sets the resource loader for loading external files.
    pub fn resource_loader(
        mut self,
        resource_loader: flowgen_core::resource::ResourceLoader,
    ) -> Self {
        self.resource_loader = Some(resource_loader);
        self
    }

    /// Builds a Flow instance from the configured options.
    ///
    /// # Errors
    /// Returns `Error::MissingBuilderAttribute` if required fields are not set.
    pub fn build(self) -> Result<Flow, Error> {
        Ok(Flow {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            http_server: self.http_server,
            host: self.host,
            cache: self.cache,
            event_buffer_size: self.event_buffer_size,
            retry: self.retry,
            resource_loader: self.resource_loader,
            task_manager: None,
            task_context: None,
            background_handles: Arc::new(std::sync::Mutex::new(None)),
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

        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(attr)) if attr == "config"
        ));
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
    fn test_task_registry_creates_n_minus_1_channels() {
        use crate::config::{Flow, FlowConfig, TaskType};

        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                ],
                require_leader_election: None,
            },
        });

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks.len(), 3);

        let task0_has_input = registry.tasks[0].input_rx.is_some();
        let task0_has_output = registry.tasks[0].output_tx.is_some();

        let task1_has_input = registry.tasks[1].input_rx.is_some();
        let task1_has_output = registry.tasks[1].output_tx.is_some();

        let task2_has_input = registry.tasks[2].input_rx.is_some();
        let task2_has_output = registry.tasks[2].output_tx.is_some();

        assert!(!task0_has_input, "First task should not have input");
        assert!(task0_has_output, "First task should have output");

        assert!(task1_has_input, "Middle task should have input");
        assert!(task1_has_output, "Middle task should have output");

        assert!(task2_has_input, "Last task should have input");
        assert!(!task2_has_output, "Last task should not have output");
    }

    #[test]
    fn test_task_registry_single_task() {
        use crate::config::{Flow, FlowConfig, TaskType};

        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![TaskType::script(
                    flowgen_core::task::script::config::Processor::default(),
                )],
                require_leader_election: None,
            },
        });

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks.len(), 1);

        let task = &registry.tasks[0];
        assert!(task.input_rx.is_none(), "Single task should not have input");
        assert!(
            task.output_tx.is_none(),
            "Single task should not have output"
        );
    }

    #[test]
    fn test_task_registry_partition_blocking_vs_background() {
        use crate::config::{Flow, FlowConfig, TaskType};

        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![
                    TaskType::http_webhook(flowgen_http::config::Processor::default()),
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                ],
                require_leader_election: None,
            },
        });

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();
        let (blocking, background) = registry.partition();

        assert_eq!(blocking.len(), 1, "Should have 1 blocking task (webhook)");
        assert_eq!(
            background.len(),
            2,
            "Should have 2 background tasks (scripts)"
        );

        assert!(
            blocking[0].is_blocking,
            "Blocking task should have is_blocking=true"
        );
        assert!(
            !background[0].is_blocking,
            "Background task should have is_blocking=false"
        );
        assert!(
            !background[1].is_blocking,
            "Background task should have is_blocking=false"
        );
    }

    #[test]
    fn test_task_registry_empty_flow() {
        use crate::config::{Flow, FlowConfig};

        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![],
                require_leader_election: None,
            },
        });

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks.len(), 0);
    }

    #[test]
    fn test_task_registry_preserves_task_order() {
        use crate::config::{Flow, FlowConfig, TaskType};

        let flow_config = Arc::new(FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                    TaskType::script(flowgen_core::task::script::config::Processor::default()),
                ],
                require_leader_election: None,
            },
        });

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks[0].id, 0);
        assert_eq!(registry.tasks[1].id, 1);
        assert_eq!(registry.tasks[2].id, 2);
    }
}
