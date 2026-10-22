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
//! 1. `start_tasks()` spawns ALL tasks (webhooks and background)
//! 2. Returns only blocking handles to wait for webhook registration
//! 3. Background tasks are already running with connected channels
//! 4. `monitor_tasks()` monitors the pre-spawned tasks
//!
//! This prevents channel mismatch bugs where webhooks send to one channel
//! while background tasks receive from a different channel.

use crate::config::{FlowConfig, TaskType};
use flowgen_core::event::Event;
use flowgen_core::task::context::Services;
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, info, warn, Instrument};

/// Room for one event between two tasks. Each task limits itself with
/// `max_in_flight`, so a slow task makes the ones before it wait.
const CHANNEL_CAPACITY: usize = 1;

/// Errors that can occur during flow execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// A task failed, with the error of the crate that owns it.
    #[error("Task {task_type} failed: {source}")]
    Task {
        task_type: &'static str,
        source: flowgen_core::task::TaskError,
    },
    /// Missing required builder attribute.
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    /// Task context not initialized (init must be called first).
    #[error("Task context not initialized: init() must be called first")]
    TaskContextNotInitialized,
    /// Task manager not initialized (init must be called first).
    #[error("Task manager not initialized: init() must be called first")]
    TaskManagerNotInitialized,
    /// Invalid lease configuration.
    #[error("Invalid lease configuration: {0}")]
    LeaseConfig(#[from] flowgen_core::executor::LeaseConfigError),
    /// Task manager error.
    #[error("Task manager error: {0}")]
    TaskManager(#[from] flowgen_core::task::manager::Error),
    /// Failed to register flow for leader election.
    #[error("Error registering flow for leader election: {0}")]
    LeaderElectionRegistrationFailed(String),
    /// Leadership channel closed unexpectedly.
    #[error("Leadership channel closed unexpectedly")]
    LeadershipChannelClosed,
    /// Failed to store background task handles for later monitoring.
    #[error("Error storing background task handles")]
    BackgroundHandlesStoreFailed,
    /// Failed to store the task context after starting tasks.
    #[error("Error storing task context after start_tasks")]
    TaskContextStoreFailed,
    /// Failed to retrieve background task handles for monitoring.
    #[error("Error retrieving background task handles")]
    BackgroundHandlesRetrieveFailed,
    /// Flow configuration error (duplicate task names, invalid dependencies, etc.).
    #[error("Flow configuration error: {0}")]
    ConfigError(String),
    /// Task wiring is invalid.
    #[error(transparent)]
    Dag(#[from] DagError),
}

/// Invalid task wiring in a flow.
#[derive(thiserror::Error, Debug, Clone, PartialEq)]
pub enum DagError {
    /// Two tasks in the flow share a name.
    #[error("Duplicate task name '{name}' in flow")]
    DuplicateTask { name: String },
    /// A `depends_on` entry names no task in the flow.
    #[error("Task '{task}' depends on unknown task '{dependency}'")]
    UnknownDependency { task: String, dependency: String },
    /// A `depends_on` entry names a task further down the list, which could form a cycle.
    #[error("Task '{task}' depends on '{dependency}' which appears later in the list")]
    LaterDependency { task: String, dependency: String },
}

/// Resolves each task's parent indices from `depends_on`, or the previous
/// pipeline task when unset. Registration-only tasks get no implicit parent,
/// and the implicit-parent walk skips over them.
pub fn resolve_parents(tasks: &[TaskType]) -> Result<Vec<Vec<usize>>, DagError> {
    let mut name_to_idx: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for (idx, task_type) in tasks.iter().enumerate() {
        if name_to_idx.insert(task_type.name(), idx).is_some() {
            return Err(DagError::DuplicateTask {
                name: task_type.name().to_string(),
            });
        }
    }

    let mut parent_indices: Vec<Vec<usize>> = Vec::with_capacity(tasks.len());
    for (idx, task_type) in tasks.iter().enumerate() {
        let parents = match task_type.depends_on() {
            Some(deps) => {
                let mut indices = Vec::with_capacity(deps.len());
                for dep_name in deps {
                    let parent_idx = match name_to_idx.get(dep_name.as_str()) {
                        Some(&parent_idx) => parent_idx,
                        None => {
                            return Err(DagError::UnknownDependency {
                                task: task_type.name().to_string(),
                                dependency: dep_name.clone(),
                            })
                        }
                    };
                    if parent_idx >= idx {
                        return Err(DagError::LaterDependency {
                            task: task_type.name().to_string(),
                            dependency: dep_name.clone(),
                        });
                    }
                    indices.push(parent_idx);
                }
                indices
            }
            None if !task_type.has_pipeline_io() => vec![],
            None => match (0..idx).rev().find(|&i| tasks[i].has_pipeline_io()) {
                Some(i) => vec![i],
                None => vec![],
            },
        };
        parent_indices.push(parents);
    }
    Ok(parent_indices)
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
    /// Number of leaf tasks reachable through this task's output channel.
    ///
    /// Source tasks use this to size the per-flow completion channel so the
    /// upstream message is acknowledged only after every leaf signals. Leaf
    /// tasks have a value of one (the task itself). The flow builder computes
    /// this from the wired topology.
    downstream_leaves: usize,
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
    /// Task context containing the cancellation token for this tenure.
    pub task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl TaskRegistry {
    /// Creates a new builder for constructing a task registry.
    fn builder(flow_config: Arc<FlowConfig>) -> TaskRegistryBuilder {
        TaskRegistryBuilder { flow_config }
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
}

impl TaskRegistryBuilder {
    /// Builds a task registry with channels wired between tasks.
    ///
    /// Dispatches to the DAG path when any task declares `depends_on`,
    /// otherwise uses the linear chain for backward compatibility.
    fn build(self) -> Result<TaskRegistry, Error> {
        let tasks_config = &self.flow_config.flow.tasks;

        if tasks_config.is_empty() {
            return Ok(TaskRegistry { tasks: Vec::new() });
        }

        // The DAG path also owns the implicit-parent walk that skips
        // registration-only tasks, so route through it whenever any task
        // opts out of pipeline I/O even if none declares `depends_on`.
        let needs_dag = tasks_config.iter().any(|t| t.depends_on().is_some())
            || tasks_config.iter().any(|t| !t.has_pipeline_io());
        if needs_dag {
            self.build_dag(tasks_config)
        } else {
            self.build_linear(tasks_config)
        }
    }

    /// Linear chain: task N receives from task N-1. No branching.
    fn build_linear(&self, tasks_config: &[TaskType]) -> Result<TaskRegistry, Error> {
        let task_count = tasks_config.len();
        let mut channels: Vec<(mpsc::Sender<Event>, mpsc::Receiver<Event>)> = (0..task_count
            .saturating_sub(1))
            .map(|_| mpsc::channel(CHANNEL_CAPACITY))
            .collect();

        let mut task_descriptors = Vec::with_capacity(task_count);

        for (idx, task_type) in tasks_config.iter().enumerate() {
            let input_rx = if idx > 0 {
                channels
                    .get_mut(idx - 1)
                    .map(|(_, rx)| std::mem::replace(rx, mpsc::channel(1).1))
            } else {
                None
            };

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
                is_blocking: task_type.is_blocking(),
                // Linear chains have a single terminal task, so every task
                // sees exactly one leaf in its downstream subgraph.
                downstream_leaves: 1,
            });
        }

        Ok(TaskRegistry {
            tasks: task_descriptors,
        })
    }

    /// DAG wiring: tasks declare their upstream via `depends_on`.
    ///
    /// Tasks without `depends_on` fall back to depending on the previous task
    /// in the list, so mixed flows (some tasks with `depends_on`, some without)
    /// work intuitively. Registration-only tasks (`has_pipeline_io() == false`)
    /// are skipped when resolving the implicit parent and never get an
    /// implicit parent themselves — they neither emit nor consume events, so
    /// wiring them into the DAG would either stall a downstream task on a
    /// channel that never sends or attach them to a channel they will never
    /// read. Fan-out points (multiple tasks depending on the same parent)
    /// use a dispatcher that clones events to each child channel.
    fn build_dag(&self, tasks_config: &[TaskType]) -> Result<TaskRegistry, Error> {
        let task_count = tasks_config.len();
        let parent_indices = resolve_parents(tasks_config)?;

        // Group children by parent to identify fan-out points.
        let mut children_per_parent: std::collections::HashMap<usize, Vec<usize>> =
            std::collections::HashMap::new();
        for (child_idx, parents) in parent_indices.iter().enumerate() {
            for &parent_idx in parents {
                children_per_parent
                    .entry(parent_idx)
                    .or_default()
                    .push(child_idx);
            }
        }

        // Each non-source task owns a single inbound mpsc channel.
        // Multiple parents can therefore feed the same child by sharing
        // clones of that channel's sender, which gives correct fan-in
        // semantics: every event from every parent reaches the child in the
        // order it was produced, with backpressure applied by the channel.
        let mut inbound_tx: Vec<Option<mpsc::Sender<Event>>> =
            (0..task_count).map(|_| None).collect();
        let mut child_rx: Vec<Option<mpsc::Receiver<Event>>> =
            (0..task_count).map(|_| None).collect();
        for (idx, parents) in parent_indices.iter().enumerate() {
            if !parents.is_empty() {
                let (tx, rx) = mpsc::channel(CHANNEL_CAPACITY);
                inbound_tx[idx] = Some(tx);
                child_rx[idx] = Some(rx);
            }
        }

        // Wire each parent's outbound sender to its children's inbound
        // channels. A parent with a single child sends directly into that
        // child's inbound channel. A parent with multiple children gets a
        // dispatcher that clones every event into each child's inbound
        // channel, preserving backpressure on each branch.
        let mut parent_tx: Vec<Option<mpsc::Sender<Event>>> =
            (0..task_count).map(|_| None).collect();

        for (&parent_idx, children) in &children_per_parent {
            if children.len() == 1 {
                let child_inbound = inbound_tx[children[0]].clone().ok_or_else(|| {
                    Error::ConfigError(format!(
                        "Internal error: missing inbound channel for task index {}",
                        children[0]
                    ))
                })?;
                parent_tx[parent_idx] = Some(child_inbound);
            } else {
                let (dispatcher_tx, dispatcher_rx) = mpsc::channel(CHANNEL_CAPACITY);
                parent_tx[parent_idx] = Some(dispatcher_tx);

                let mut child_senders = Vec::with_capacity(children.len());
                for &child_idx in children {
                    let child_inbound = inbound_tx[child_idx].clone().ok_or_else(|| {
                        Error::ConfigError(format!(
                            "Internal error: missing inbound channel for task index {child_idx}"
                        ))
                    })?;
                    child_senders.push(child_inbound);
                }

                tokio::spawn(fan_out(dispatcher_rx, child_senders));
            }
        }
        // Drop the sender side of inbound channels held only by the wiring
        // bookkeeping. Each child still holds clones via parent_tx, so the
        // channel stays open until every parent has finished.
        drop(inbound_tx);

        // Compute the number of distinct leaf tasks reachable from each task's
        // output. Tasks are kept in topological order (parents always precede
        // children) so a single reverse pass propagates leaf sets up the
        // directed acyclic graph. Summing child counts would double-count when
        // two branches re-merge (e.g. fan-out -> buffer fan-in), inflating the
        // source's completion expectation beyond the real number of leaves.
        let mut reachable_leaves: Vec<std::collections::HashSet<usize>> =
            vec![std::collections::HashSet::new(); task_count];
        for idx in (0..task_count).rev() {
            match children_per_parent.get(&idx) {
                None => {
                    reachable_leaves[idx].insert(idx);
                }
                Some(children) => {
                    let mut set = std::collections::HashSet::new();
                    for &c in children {
                        set.extend(reachable_leaves[c].iter().copied());
                    }
                    reachable_leaves[idx] = set;
                }
            }
        }
        let downstream_leaves: Vec<usize> = reachable_leaves.iter().map(|s| s.len()).collect();

        let mut task_descriptors = Vec::with_capacity(task_count);
        for (idx, task_type) in tasks_config.iter().enumerate() {
            task_descriptors.push(TaskDescriptor {
                id: idx,
                task_type: task_type.clone(),
                input_rx: child_rx[idx].take(),
                output_tx: parent_tx[idx].take(),
                is_blocking: task_type.is_blocking(),
                downstream_leaves: downstream_leaves[idx],
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
    /// Shared servers tasks register with, passed in from the main application.
    services: Services,
    /// Shared runtime cache, passed in from the main application. Holds
    /// per-flow state (replay IDs, counters, last-run timestamps, business
    /// data) and is exposed to user scripts via `ctx.cache`.
    cache: Arc<dyn flowgen_core::cache::Cache>,
    /// System cache, passed in from the main application. Holds cluster-wide
    /// coordination state (lease keys for leader election). Kept separate
    /// from the runtime `cache` so user scripts cannot reach lease keys, and
    /// so retention policies can differ (short history plus per-message TTL
    /// for leases, longer retention for per-flow state). In single-binary
    /// or in-memory deployments this is the same `Arc` as `cache`; the
    /// separation is enforced only by the operator in the main config.
    system_cache: Arc<dyn flowgen_core::cache::Cache>,
    /// Optional app-level retry configuration, passed in from the main application.
    retry: Option<flowgen_core::retry::RetryConfig>,
    /// Optional resource loader for loading external files, passed in from the main application.
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    /// Shared client registry for deduplicating connections to external services.
    client_registry: Arc<flowgen_core::client_registry::ClientRegistry>,
    /// Flows callable in-process, shared by every flow.
    inproc: Arc<flowgen_core::task::inproc::registry::InprocRegistry>,
    /// This pod's holder identity, resolved once at the app level so every
    /// flow's Executor agrees on it. `None` falls back to per-flow
    /// resolution (tests).
    holder_identity: Option<String>,
    /// Optional shared peer registry for flow distribution via consistent
    /// hashing, passed in from the main application. One instance per pod.
    peer_registry: Option<Arc<flowgen_core::peer::PeerRegistry>>,
    /// The task manager, responsible for leader election. Initialized by `init()`.
    task_manager: Option<Arc<flowgen_core::task::manager::TaskManager>>,
    /// Background task handles spawned by start_tasks for monitor_tasks to monitor.
    background_handles: Arc<std::sync::Mutex<Option<Vec<TaskHandle>>>>,
    /// Task context from the most recent start_tasks call; used to read registration
    /// info (registered_http_routes, registered_mcp_tools) before run() consumes self.
    last_task_context: Arc<std::sync::Mutex<Option<Arc<flowgen_core::task::context::TaskContext>>>>,
}

impl Flow {
    /// Returns the flow identity — the loader-assigned `source_path` when
    /// present (filesystem/cache load), otherwise the programmatic
    /// `flow.name`. Same value as `identity()`; kept as `name()` for
    /// existing call sites that predate the path-as-identity refactor.
    pub fn name(&self) -> &str {
        self.identity()
    }

    /// Returns the flow identity — used as the tracing `flow=` field, the
    /// registry key, and the cache namespace so activity keys line up with
    /// the web API.
    pub fn identity(&self) -> &str {
        self.config.identity()
    }

    /// Returns a reference to the task manager if initialized.
    pub fn task_manager(&self) -> Option<Arc<flowgen_core::task::manager::TaskManager>> {
        self.task_manager.as_ref().map(Arc::clone)
    }

    /// Returns the cache backing this flow's leases and coordination state.
    pub fn system_cache(&self) -> Arc<dyn flowgen_core::cache::Cache> {
        Arc::clone(&self.system_cache)
    }

    /// Returns the cancellation token for the most recently started task tenure.
    ///
    /// Used by the reconciler to cancel a running flow before replacing it. Returns
    /// `None` if `start_tasks()` has not yet been called (e.g. leader-elected flows
    /// that have not yet acquired leadership).
    pub fn cancellation_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.last_task_context
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|ctx| ctx.cancellation_token.clone()))
    }

    /// Determines if the flow should run with leader election.
    ///
    /// If a flow contains any webhook tasks, it will always be treated as
    /// non-leader-elected by disregarding the `required_leader_election` flag.
    fn is_leader_elected(&self) -> bool {
        let has_blocking_tasks = self.config.flow.tasks.iter().any(TaskType::is_blocking);

        if has_blocking_tasks {
            if self.config.flow.require_leader_election.unwrap_or(false) {
                info!(
                    "Flow {} contains a blocking task (http_endpoint, inproc_endpoint, MCP registration, or llm_proxy); `require_leader_election` flag will be ignored.",
                    self.identity()
                );
            }
            return false; // Flows with blocking tasks are never leader elected.
        }

        // Otherwise, respect the configuration.
        self.config.flow.require_leader_election.unwrap_or(false)
    }

    /// Initializes shared resources for the flow, such as the TaskManager.
    /// This must be called before any other run methods.
    pub async fn init(&mut self) -> Result<(), Error> {
        if self.task_manager.is_some() {
            return Ok(()); // Already initialized
        }

        for task in &self.config.flow.tasks {
            task.check(&self.services).map_err(|source| Error::Task {
                task_type: task.as_str(),
                source,
            })?;
        }

        // Create an Executor backed by the system cache. Lease keys must
        // stay isolated from user-script-accessible runtime cache to keep
        // coordination state outside any flow's reach.
        let lease_config = match &self.holder_identity {
            Some(holder_identity) => flowgen_core::executor::LeaseConfig {
                holder_identity: holder_identity.clone(),
                ..Default::default()
            },
            None => flowgen_core::executor::LeaseConfig::default(),
        };
        let executor =
            flowgen_core::executor::Executor::new(self.system_cache.clone(), lease_config)?;
        let executor = Arc::new(executor);

        let mut task_manager_builder =
            flowgen_core::task::manager::TaskManagerBuilder::new().executor(executor);
        if let Some(registry) = &self.peer_registry {
            task_manager_builder = task_manager_builder.peer_registry(Arc::clone(registry));
        }
        let task_manager = task_manager_builder.build()?;
        let task_manager = Arc::new(task_manager.start().await);

        self.task_manager = Some(task_manager);

        Ok(())
    }

    /// Creates a fresh TaskContext with a new cancellation token.
    /// This is called each time tasks are spawned (e.g., on leadership acquisition).
    fn create_task_context(&self) -> Result<Arc<flowgen_core::task::context::TaskContext>, Error> {
        let task_manager = self
            .task_manager
            .as_ref()
            .ok_or_else(|| Error::TaskManagerNotInitialized)?
            .clone();

        // Create a fresh cancellation token for this task tenure.
        let cancellation_token = tokio_util::sync::CancellationToken::new();

        // Shared response registry for streaming progress between tasks in this flow.
        let response_registry = Arc::new(flowgen_core::registry::ResponseRegistry::new());

        let mut task_context_builder = flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name(self.identity().to_string())
            .source_path(Some(self.identity().to_string()))
            .flow_labels(self.config.flow.labels.clone())
            .task_manager(task_manager)
            .cache(Arc::clone(&self.cache))
            .response_registry(response_registry)
            .resource_loader(self.resource_loader.clone())
            .cancellation_token(cancellation_token)
            .client_registry(Arc::clone(&self.client_registry))
            .inproc(Arc::clone(&self.inproc))
            .services(self.services.clone());

        if let Some(retry_config) = &self.retry {
            task_context_builder = task_context_builder.retry(retry_config.clone());
        }

        Ok(Arc::new(task_context_builder.build().map_err(|e| {
            Error::MissingBuilderAttribute(e.to_string())
        })?))
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
        // Create a fresh task context with a new cancellation token for this tenure.
        let task_context = self.create_task_context()?;

        // Build task registry with all tasks properly wired.
        let registry = TaskRegistry::builder(self.config.clone()).build()?;

        // Separate blocking (setup) tasks from background tasks.
        let (blocking_tasks, background_tasks) = registry.partition();

        let blocking_handles = blocking_tasks
            .into_iter()
            .map(|task_desc| spawn_task(task_desc, &task_context))
            .collect();
        let background_handles = background_tasks
            .into_iter()
            .map(|task_desc| spawn_task(task_desc, &task_context))
            .collect();

        Ok(TaskHandles {
            blocking_handles,
            background_handles,
            task_context,
        })
    }

    /// Spawns multiple independent flow pipelines for higher throughput.
    ///
    /// Creates N separate task registries, each with its own event channels.
    /// All instances share the same cancellation token for coordinated shutdown.
    async fn spawn_parallel_instances(&self, count: usize) -> Result<TaskHandles, Error> {
        let task_context = self.create_task_context()?;

        let mut all_blocking_tasks = Vec::new();
        let mut all_background_tasks = Vec::new();

        // Build all task registries and collect task descriptors.
        for _ in 0..count {
            let registry = TaskRegistry::builder(self.config.clone()).build()?;
            let (blocking_tasks, background_tasks) = registry.partition();
            all_blocking_tasks.extend(blocking_tasks);
            all_background_tasks.extend(background_tasks);
        }

        let blocking_handles = all_blocking_tasks
            .into_iter()
            .map(|task_desc| spawn_task(task_desc, &task_context))
            .collect();
        let background_handles = all_background_tasks
            .into_iter()
            .map(|task_desc| spawn_task(task_desc, &task_context))
            .collect();

        Ok(TaskHandles {
            blocking_handles,
            background_handles,
            task_context,
        })
    }

    /// Starts all tasks for non-leader-elected flows immediately.
    ///
    /// Leader-elected flows return early — their tasks are spawned later by
    /// `monitor_tasks()` after acquiring leadership.
    ///
    /// Returns blocking handles (webhook registrations) that must complete
    /// before the HTTP server starts. Background handles are stored for
    /// `monitor_tasks()` to monitor.
    #[tracing::instrument(skip(self), name = "flow.run", fields(flow = %self.identity()))]
    pub async fn start_tasks(&self) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
        if self.is_leader_elected() {
            return Ok(Vec::new());
        }

        // Spawn all tasks and return only the blocking handles.
        let handles = self.spawn_all_tasks().await?;

        // Store background handles for monitor_tasks to monitor.
        let mut lock = self
            .background_handles
            .lock()
            .map_err(|_| Error::BackgroundHandlesStoreFailed)?;
        *lock = Some(handles.background_handles);

        // Store task context so registration info is accessible before run() consumes self.
        self.last_task_context
            .lock()
            .map_err(|_| Error::TaskContextStoreFailed)
            .map(|mut guard| *guard = Some(Arc::clone(&handles.task_context)))?;

        Ok(handles.blocking_handles)
    }

    /// Starts the main, long-running execution of the flow.
    ///
    /// This spawns a single master task that manages the flow's lifecycle,
    /// including leader election and running all background tasks.
    #[tracing::instrument(skip(self), name = "flow.run", fields(flow = %self.identity()))]
    pub fn run(self) -> JoinHandle<()> {
        let flow_id = self.identity().to_string();
        tokio::spawn(
            async move {
                if let Err(e) = self.monitor_tasks().await {
                    error!("Flow {} terminated with an error: {}", flow_id, e);
                }
            }
            .instrument(tracing::Span::current()),
        )
    }

    /// The main internal run loop for the flow.
    ///
    /// For leader-elected flows: waits for leadership, then spawns and monitors tasks.
    /// For non-leader-elected flows: monitors tasks already spawned by `start_tasks()`.
    async fn monitor_tasks(self) -> Result<(), Error> {
        let is_leader_elected = self.is_leader_elected();
        let task_manager = self
            .task_manager
            .as_ref()
            .ok_or_else(|| Error::TaskManagerNotInitialized)?
            .clone();
        // `register` takes the typed identity: it Displays as the human path in
        // logs and projects to the key-safe form for the lease and peer keys.
        let flow_id = self.identity().to_string();
        let lease_id = self.config.identity_ref().clone();

        let leader_election_options = if is_leader_elected {
            Some(flowgen_core::task::manager::LeaderElectionOptions {})
        } else {
            None
        };

        let mut leadership_rx = task_manager
            .register(lease_id, leader_election_options)
            .await
            .map_err(|e| Error::LeaderElectionRegistrationFailed(e.to_string()))?;

        let mut tasks_spawned = false;

        // Wait for initial leadership state.
        loop {
            match leadership_rx.recv().await {
                Some(flowgen_core::task::manager::LeaderElectionResult::Leader) => {
                    if !tasks_spawned {
                        info!("Acquired leadership, spawning tasks");
                        tasks_spawned = true;

                        // Drain duplicate Leader messages.
                        while let Ok(flowgen_core::task::manager::LeaderElectionResult::Leader) =
                            leadership_rx.try_recv()
                        {
                            debug!("Flow {} received duplicate Leader message during initial drain, ignoring", flow_id);
                        }
                        break;
                    } else {
                        debug!(
                            "Flow {} received duplicate Leader message, ignoring",
                            flow_id
                        );
                    }
                }
                Some(flowgen_core::task::manager::LeaderElectionResult::NotLeader) => {
                    debug!("Flow {} is not leader, waiting for leadership", flow_id);
                    tasks_spawned = false;
                }
                Some(flowgen_core::task::manager::LeaderElectionResult::NoElection) => {
                    debug!(
                        "No leader election for flow {}, spawning tasks immediately",
                        flow_id
                    );
                    tasks_spawned = true;
                    break;
                }
                None => {
                    info!("Leadership channel closed, exiting");
                    return Ok(());
                }
            }
        }

        let mut current_task_handles = if !is_leader_elected {
            // Retrieve already-spawned background tasks.
            let mut lock = self
                .background_handles
                .lock()
                .map_err(|_| Error::BackgroundHandlesRetrieveFailed)?;

            // For non-leader-elected flows, we just need the raw handles
            let handles = lock
                .take()
                .ok_or_else(|| Error::BackgroundHandlesRetrieveFailed)?;

            // Create a dummy TaskHandles since non-leader flows don't use cancellation
            TaskHandles {
                blocking_handles: Vec::new(),
                background_handles: handles,
                task_context: self.create_task_context()?, // Won't be used but needed for the type
            }
        } else {
            // Spawn tasks for this leadership tenure.
            let parallel_instances = self.config.flow.parallel_instances;

            if parallel_instances <= 1 {
                self.spawn_all_tasks().await?
            } else {
                info!(
                    "Spawning {} parallel instances for flow: {}",
                    parallel_instances,
                    self.identity()
                );
                self.spawn_parallel_instances(parallel_instances).await?
            }
        };

        let mut background_tasks: Vec<TaskHandle> = current_task_handles
            .blocking_handles
            .into_iter()
            .chain(current_task_handles.background_handles)
            .collect();

        if background_tasks.is_empty() {
            info!("No tasks to monitor");
            return Ok(());
        }

        if is_leader_elected {
            // Monitor leadership and tasks.
            loop {
                let all_completed = loop {
                    tokio::select! {
                        biased;
                        Some(status) = leadership_rx.recv() => {
                            match status {
                                flowgen_core::task::manager::LeaderElectionResult::NotLeader => {
                                    debug!("Lost leadership, signaling cancellation and aborting tasks");

                                    // Signal all tasks to stop via cancellation token
                                    current_task_handles.task_context.cancellation_token.cancel();

                                    // Also abort tasks immediately
                                    for task in &background_tasks {
                                        task.abort();
                                    }
                                    break false;
                                }
                                flowgen_core::task::manager::LeaderElectionResult::Leader => {
                                    if tasks_spawned {
                                        debug!("Flow {} received duplicate Leader message while tasks running, ignoring", flow_id);
                                    } else {
                                        warn!("Received unexpected Leader message, tasks already running");
                                    }
                                }
                                flowgen_core::task::manager::LeaderElectionResult::NoElection => {}
                            }
                        }
                        results = futures::future::join_all(&mut background_tasks), if !background_tasks.is_empty() => {
                            for (idx, result) in results.iter().enumerate() {
                                if let Err(e) = result {
                                    error!("Task {} failed: {}", idx, e);
                                }
                            }
                            info!("All tasks completed");
                            break true;
                        }
                    }
                };

                if all_completed {
                    break;
                }

                // Lost leadership - wait to re-acquire.
                background_tasks.clear();
                tasks_spawned = false;

                loop {
                    match leadership_rx.recv().await {
                        Some(flowgen_core::task::manager::LeaderElectionResult::Leader) => {
                            if !tasks_spawned {
                                info!("Re-acquired leadership, spawning tasks");
                                tasks_spawned = true;

                                // Drain duplicate Leader messages.
                                while let Ok(
                                    flowgen_core::task::manager::LeaderElectionResult::Leader,
                                ) = leadership_rx.try_recv()
                                {
                                    debug!("Flow {} received duplicate Leader message during re-acquisition drain, ignoring", flow_id);
                                }
                                break;
                            } else {
                                debug!("Flow {} received duplicate Leader message during re-acquisition, ignoring", flow_id);
                            }
                        }
                        Some(flowgen_core::task::manager::LeaderElectionResult::NotLeader) => {
                            debug!("Flow {} still not leader, waiting", flow_id);
                            tasks_spawned = false;
                        }
                        Some(flowgen_core::task::manager::LeaderElectionResult::NoElection) => {
                            tasks_spawned = true;
                            break;
                        }
                        None => {
                            info!("Leadership channel closed, exiting");
                            return Ok(());
                        }
                    }
                }

                // Re-spawn tasks after re-acquiring leadership.
                current_task_handles = self.spawn_all_tasks().await?;
                background_tasks = std::mem::take(&mut current_task_handles.blocking_handles)
                    .into_iter()
                    .chain(std::mem::take(&mut current_task_handles.background_handles))
                    .collect();
            }
        } else {
            // Wait for all tasks to complete.
            let results = futures::future::join_all(background_tasks).await;

            for (idx, result) in results.iter().enumerate() {
                if let Err(e) = result {
                    error!("Task {} failed: {}", idx, e);
                }
            }
            info!("All tasks completed");
        }
        Ok(())
    }
}

/// Sends each event to every branch that still takes events, and stops once none do.
async fn fan_out(mut rx: mpsc::Receiver<Event>, mut branches: Vec<mpsc::Sender<Event>>) {
    while let Some(event) = rx.recv().await {
        branches.retain(|branch| !branch.is_closed());
        if branches.is_empty() {
            return;
        }
        for branch in &branches {
            let _ = branch.send(event.clone()).await;
        }
    }
}

/// Spawns a task with its channels and a context sized for its place in the flow.
fn spawn_task(
    task_desc: TaskDescriptor,
    task_context: &Arc<flowgen_core::task::context::TaskContext>,
) -> TaskHandle {
    let task_type = task_desc.task_type;
    let mut context = (**task_context).clone();
    context.leaf_count = task_desc.downstream_leaves;
    context.max_in_flight = flowgen_core::task::runner::max_in_flight(task_type.cpu_intensive());
    context.handles_failed = task_type.handles_failed();
    if !task_type.starts_at_once() {
        let retry_config = flowgen_core::retry::RetryConfig::merge(&context.retry, &None);
        context.startup_delay = Some(retry_config.startup_jitter());
    }
    let io = flowgen_core::task::TaskIo {
        task_id: task_desc.id,
        task_type: task_type.as_str(),
        rx: task_desc.input_rx,
        tx: task_desc.output_tx,
    };
    task_type.spawn(io, Arc::new(context))
}

/// Builder for creating Flow instances.
#[derive(Default)]
pub struct FlowBuilder {
    /// Optional flow configuration.
    config: Option<Arc<FlowConfig>>,
    /// Shared servers tasks register with.
    services: Services,
    /// Shared runtime cache instance.
    cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
    /// System cache instance. Backs the executor's lease store and is kept
    /// out of user-script reach. Required at build time.
    system_cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
    /// Optional app-level retry configuration.
    retry: Option<flowgen_core::retry::RetryConfig>,
    /// Resource loader for loading external files.
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    /// Shared client registry for connection pooling.
    client_registry: Option<Arc<flowgen_core::client_registry::ClientRegistry>>,
    /// Flows callable in-process.
    inproc: Option<Arc<flowgen_core::task::inproc::registry::InprocRegistry>>,
    /// This pod's holder identity, shared across every flow's Executor.
    holder_identity: Option<String>,
    /// Shared peer registry for flow distribution via consistent hashing.
    peer_registry: Option<Arc<flowgen_core::peer::PeerRegistry>>,
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

    /// Adds a shared server tasks register with, such as the HTTP endpoint server.
    pub fn service<T: Send + Sync + 'static>(mut self, service: Arc<T>) -> Self {
        self.services.insert(service);
        self
    }

    /// Sets the shared runtime cache instance.
    pub fn cache(mut self, cache: Arc<dyn flowgen_core::cache::Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Sets the system cache instance used for leader-election leases.
    ///
    /// In multi-bucket deployments this points at a separate bucket from
    /// the runtime cache; in single-binary or in-memory deployments it may
    /// share the same underlying `Arc`.
    pub fn system_cache(mut self, cache: Arc<dyn flowgen_core::cache::Cache>) -> Self {
        self.system_cache = Some(cache);
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

    /// Sets the shared client registry for connection pooling across tasks.
    pub fn client_registry(
        mut self,
        client_registry: Arc<flowgen_core::client_registry::ClientRegistry>,
    ) -> Self {
        self.client_registry = Some(client_registry);
        self
    }

    /// Sets the registry of flows callable in-process.
    pub fn inproc(
        mut self,
        inproc: Arc<flowgen_core::task::inproc::registry::InprocRegistry>,
    ) -> Self {
        self.inproc = Some(inproc);
        self
    }

    /// Sets this pod's holder identity, shared across every flow's Executor.
    pub fn holder_identity(mut self, holder_identity: String) -> Self {
        self.holder_identity = Some(holder_identity);
        self
    }

    /// Sets the shared peer registry for flow distribution via consistent hashing.
    pub fn peer_registry(mut self, peer_registry: Arc<flowgen_core::peer::PeerRegistry>) -> Self {
        self.peer_registry = Some(peer_registry);
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
            services: self.services,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingBuilderAttribute("cache".to_string()))?,
            system_cache: self
                .system_cache
                .ok_or_else(|| Error::MissingBuilderAttribute("system_cache".to_string()))?,
            retry: self.retry,
            resource_loader: self.resource_loader,
            client_registry: match self.client_registry {
                Some(registry) => registry,
                None => Arc::new(flowgen_core::client_registry::ClientRegistry::new()),
            },
            inproc: match self.inproc {
                Some(inproc) => inproc,
                None => Arc::new(flowgen_core::task::inproc::registry::InprocRegistry::new()),
            },
            holder_identity: self.holder_identity,
            peer_registry: self.peer_registry,
            task_manager: None,
            background_handles: Arc::new(std::sync::Mutex::new(None)),
            last_task_context: Arc::new(std::sync::Mutex::new(None)),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Flow, FlowConfig, FlowConfigRaw};

    fn event() -> Event {
        flowgen_core::event::EventBuilder::new()
            .data(flowgen_core::event::EventData::Json(
                serde_json::Value::Null,
            ))
            .subject("a".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn a_fan_out_keeps_feeding_the_other_branches_when_one_stops() {
        let (parent, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (stopped, stopped_rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (running, mut running_rx) = mpsc::channel(CHANNEL_CAPACITY);
        drop(stopped_rx);
        tokio::spawn(fan_out(rx, vec![stopped, running]));

        tokio::spawn(async move {
            for _ in 0..3 {
                parent.send(event()).await.unwrap();
            }
        });

        let mut received = 0;
        while running_rx.recv().await.is_some() {
            received += 1;
        }
        assert_eq!(received, 3);
    }

    #[tokio::test]
    async fn a_fan_out_with_no_branches_left_stops_taking_events() {
        let (parent, rx) = mpsc::channel(CHANNEL_CAPACITY);
        let (stopped, stopped_rx) = mpsc::channel(CHANNEL_CAPACITY);
        drop(stopped_rx);
        let dispatcher = tokio::spawn(fan_out(rx, vec![stopped]));

        parent.send(event()).await.unwrap();
        dispatcher.await.unwrap();

        assert!(parent.send(event()).await.is_err());
    }

    #[test]
    fn test_flow_builder_new() {
        let builder = FlowBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder
            .services
            .get::<flowgen_http::server::EndpointServer>()
            .is_none());
    }

    #[test]
    fn test_flow_builder_default() {
        let builder = FlowBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder
            .services
            .get::<flowgen_http::server::EndpointServer>()
            .is_none());
    }

    #[test]
    fn test_flow_builder_config() {
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let builder = FlowBuilder::new().config(flow_config.clone());
        assert_eq!(builder.config, Some(flow_config));
    }

    #[test]
    fn test_flow_builder_http_server() {
        let server = Arc::new(flowgen_core::http_server::HttpServer::<
            flowgen_http::server::EndpointDispatcher,
        >::new(
            flowgen_http::server::DEFAULT_ENDPOINT_PATH.to_string()
        ));
        let builder = FlowBuilder::new().service(server);
        assert!(builder
            .services
            .get::<flowgen_http::server::EndpointServer>()
            .is_some());
    }

    #[test]
    fn test_flow_builder_build_missing_config() {
        let server = Arc::new(flowgen_core::http_server::HttpServer::<
            flowgen_http::server::EndpointDispatcher,
        >::new(
            flowgen_http::server::DEFAULT_ENDPOINT_PATH.to_string()
        ));

        let result = FlowBuilder::new().service(server).build();

        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(attr)) if attr == "config"
        ));
    }

    #[test]
    fn test_flow_builder_build_without_http_server() {
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;

        let result = FlowBuilder::new()
            .config(flow_config)
            .cache(cache.clone())
            .system_cache(cache)
            .build();

        assert!(result.is_ok());
        let flow = result.unwrap();
        assert!(flow
            .services
            .get::<flowgen_http::server::EndpointServer>()
            .is_none());
    }

    #[test]
    fn test_flow_builder_build_success() {
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("success_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "success_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );
        let server = Arc::new(flowgen_core::http_server::HttpServer::<
            flowgen_http::server::EndpointDispatcher,
        >::new(
            flowgen_http::server::DEFAULT_ENDPOINT_PATH.to_string()
        ));
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;

        let result = FlowBuilder::new()
            .config(flow_config.clone())
            .service(server)
            .cache(cache.clone())
            .system_cache(cache)
            .build();

        assert!(result.is_ok());
        let flow = result.unwrap();
        assert_eq!(flow.config, flow_config);
        assert!(flow.task_manager.is_none());
    }

    #[test]
    fn test_task_registry_creates_n_minus_1_channels() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

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
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![TaskType::script(flowgen_core::task::script::Script {
                            name: "s".to_string(),
                            ..Default::default()
                        })],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

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
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![
                            TaskType::http_endpoint(flowgen_http::endpoint::Endpoint(
                                flowgen_http::config::Processor {
                                    name: "h".to_string(),
                                    ..Default::default()
                                },
                            )),
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();
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
        use crate::config::{Flow, FlowConfig, FlowConfigRaw};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        assert_eq!(registry.tasks.len(), 0);
    }

    #[test]
    fn test_task_registry_preserves_task_order() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::Script {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        assert_eq!(registry.tasks[0].id, 0);
        assert_eq!(registry.tasks[1].id, 1);
        assert_eq!(registry.tasks[2].id, 2);
    }

    #[tokio::test]
    async fn test_downstream_leaves_fan_out_fan_in() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::Script {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("fan_out_fan_in".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("trigger", None),
                            script_with_deps("read_a", Some(vec!["trigger"])),
                            script_with_deps("convert_a", Some(vec!["read_a"])),
                            script_with_deps("read_b", Some(vec!["trigger"])),
                            script_with_deps("convert_b", Some(vec!["read_b"])),
                            script_with_deps("buffer", Some(vec!["convert_a", "convert_b"])),
                            script_with_deps("join", Some(vec!["buffer"])),
                            script_with_deps("write", Some(vec!["join"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "fan_out_fan_in".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("write"), 1, "write is the only leaf");
        assert_eq!(leaves("join"), 1);
        assert_eq!(leaves("buffer"), 1);
        assert_eq!(leaves("convert_a"), 1);
        assert_eq!(leaves("convert_b"), 1);
        assert_eq!(leaves("read_a"), 1);
        assert_eq!(leaves("read_b"), 1);
        assert_eq!(
            leaves("trigger"),
            1,
            "trigger sees one terminal leaf, not two — both branches re-merge at buffer"
        );
    }

    #[tokio::test]
    async fn test_downstream_leaves_true_fan_out() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::Script {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("fan_out".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("source", None),
                            script_with_deps("leaf_a", Some(vec!["source"])),
                            script_with_deps("leaf_b", Some(vec!["source"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "fan_out".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("leaf_a"), 1);
        assert_eq!(leaves("leaf_b"), 1);
        assert_eq!(leaves("source"), 2, "two distinct downstream leaves");
    }

    #[tokio::test]
    async fn test_downstream_leaves_nested_diamonds() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::Script {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("nested_diamonds".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("src", None),
                            script_with_deps("a1", Some(vec!["src"])),
                            script_with_deps("a2", Some(vec!["src"])),
                            script_with_deps("mid", Some(vec!["a1", "a2"])),
                            script_with_deps("b1", Some(vec!["mid"])),
                            script_with_deps("b2", Some(vec!["mid"])),
                            script_with_deps("sink", Some(vec!["b1", "b2"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "nested_diamonds".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("sink"), 1);
        assert_eq!(leaves("b1"), 1);
        assert_eq!(leaves("b2"), 1);
        assert_eq!(leaves("mid"), 1);
        assert_eq!(leaves("a1"), 1);
        assert_eq!(leaves("a2"), 1);
        assert_eq!(
            leaves("src"),
            1,
            "nested diamonds re-merge — naive sum would be 4"
        );
    }

    #[tokio::test]
    async fn test_downstream_leaves_mixed_fan_out_and_re_merge() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::Script {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("mixed".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("src", None),
                            script_with_deps("a", Some(vec!["src"])),
                            script_with_deps("b", Some(vec!["src"])),
                            script_with_deps("c", Some(vec!["src"])),
                            script_with_deps("merge", Some(vec!["a", "b"])),
                            script_with_deps("leaf_x", Some(vec!["merge"])),
                            script_with_deps("leaf_y", Some(vec!["c"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "mixed".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("leaf_x"), 1);
        assert_eq!(leaves("leaf_y"), 1);
        assert_eq!(leaves("merge"), 1);
        assert_eq!(leaves("a"), 1);
        assert_eq!(leaves("b"), 1);
        assert_eq!(leaves("c"), 1);
        assert_eq!(
            leaves("src"),
            2,
            "merged branch counts once, independent branch adds the second leaf"
        );
    }

    #[tokio::test]
    async fn test_downstream_leaves_linear_chain() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::Script {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("linear".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("t0", None),
                            script_with_deps("t1", Some(vec!["t0"])),
                            script_with_deps("t2", Some(vec!["t1"])),
                            script_with_deps("t3", Some(vec!["t2"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "linear".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        for task in &registry.tasks {
            assert_eq!(
                task.downstream_leaves,
                1,
                "{} should see 1 leaf",
                task.task_type.name()
            );
        }
    }

    #[tokio::test]
    async fn test_registration_only_tasks_get_no_implicit_parent() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let mcp_prompt = |name: &str| {
            TaskType::mcp_prompt(flowgen_mcp::prompt::Prompt {
                name: name.to_string(),
                description: format!("desc for {name}"),
                arguments: Vec::new(),
                template: Some(flowgen_core::resource::Source::Inline("body".to_string())),
                messages: None,
                headers: std::collections::HashMap::new(),
                depends_on: None,
                retry: None,
            })
        };

        // Two mcp_prompts back-to-back without any `depends_on:` — both
        // should register as independent sources, neither should inherit
        // the other as a parent.
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("registration_only".to_string()),
                        labels: None,
                        tasks: vec![mcp_prompt("a"), mcp_prompt("b")],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "registration_only".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        for task in &registry.tasks {
            assert!(
                task.input_rx.is_none(),
                "{} must have no inbound channel",
                task.task_type.name()
            );
            assert!(
                task.output_tx.is_none(),
                "{} must have no outbound channel",
                task.task_type.name()
            );
        }
    }

    #[tokio::test]
    async fn test_implicit_parent_skips_registration_only() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script = |name: &str| {
            TaskType::script(flowgen_core::task::script::Script {
                name: name.to_string(),
                depends_on: None,
                ..Default::default()
            })
        };
        let mcp_prompt = |name: &str| {
            TaskType::mcp_prompt(flowgen_mcp::prompt::Prompt {
                name: name.to_string(),
                description: format!("desc for {name}"),
                arguments: Vec::new(),
                template: Some(flowgen_core::resource::Source::Inline("body".to_string())),
                messages: None,
                headers: std::collections::HashMap::new(),
                depends_on: None,
                retry: None,
            })
        };

        // script → mcp_prompt → script: the trailing script should chain
        // back to the first script through the mcp_prompt, not treat the
        // prompt as its parent.
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("skip_registration".to_string()),
                        labels: None,
                        tasks: vec![script("first"), mcp_prompt("register"), script("last")],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "skip_registration".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config).build().unwrap();

        let prompt = registry
            .tasks
            .iter()
            .find(|t| t.task_type.name() == "register")
            .expect("prompt missing");
        assert!(prompt.input_rx.is_none(), "prompt has no inbound channel");
        assert!(prompt.output_tx.is_none(), "prompt has no outbound channel");

        let first = registry
            .tasks
            .iter()
            .find(|t| t.task_type.name() == "first")
            .expect("first missing");
        let last = registry
            .tasks
            .iter()
            .find(|t| t.task_type.name() == "last")
            .expect("last missing");
        assert!(
            first.output_tx.is_some(),
            "first still emits — its child is `last`, not `register`"
        );
        assert!(
            last.input_rx.is_some(),
            "last consumes — its parent is `first`, walked past `register`"
        );
    }
}
