use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn, Instrument};

/// Task manager errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event: {0}")]
    SendError(#[source] mpsc::error::SendError<TaskRegistration>),
    /// Executor coordination error.
    #[error("Executor coordination error: {0}")]
    Executor(#[source] crate::cache::Error),
    /// Invalid lease configuration.
    #[error("Invalid lease configuration: {0}")]
    InvalidConfig(#[from] crate::executor::LeaseConfigError),
    /// Shutdown operation timed out after 30 seconds.
    #[error("Shutdown timed out after 30 seconds - some leases may not have been released")]
    ShutdownTimeout,
}

/// Spawns a task to continuously renew a lease with retry logic.
///
/// This function spawns a background task that attempts to renew the lease at the configured interval.
/// If renewal fails critically (ownership lost), the task notifies the flow
/// by sending a NotLeader message and terminates.
fn spawn_renewal_task(
    task_id: String,
    lease_name: String,
    executor: Arc<crate::executor::Executor>,
    current_revision: Arc<Mutex<u64>>,
    response_tx: mpsc::UnboundedSender<LeaderElectionResult>,
) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            let mut interval = tokio::time::interval(executor.config.renewal_interval);

            loop {
                interval.tick().await;

                let revision = *current_revision.lock().await;

                match executor.renew_lease(&lease_name, revision).await {
                    Ok(crate::executor::RenewalResult::Renewed {
                        revision: new_revision,
                    }) => {
                        *current_revision.lock().await = new_revision;
                        debug!(task_id = %task_id, revision = %new_revision, "Renewed lease");
                    }
                    Ok(crate::executor::RenewalResult::LostOwnership { holder }) => {
                        warn!(
                            task_id = %task_id,
                            current_holder = %holder,
                            "Lost lease ownership, notifying flow"
                        );

                        if response_tx.send(LeaderElectionResult::NotLeader).is_err() {
                            debug!("Flow already terminated");
                        }
                        break;
                    }
                    Ok(crate::executor::RenewalResult::LeaseDeleted) => {
                        warn!(
                            task_id = %task_id,
                            "Lease was deleted, notifying flow"
                        );

                        if response_tx.send(LeaderElectionResult::NotLeader).is_err() {
                            debug!("Flow already terminated");
                        }
                        break;
                    }
                    Err(e) => {
                        // Transient errors - retry on next interval.
                        warn!(error = %e, "Failed to renew lease, will retry");
                    }
                }
            }
        }
        .instrument(tracing::Span::current()),
    )
}

/// Spawns a task to retry lease acquisition with exponential backoff.
fn spawn_acquisition_retry_task(
    task_id: String,
    lease_name: String,
    executor: Arc<crate::executor::Executor>,
    current_revision: Arc<Mutex<u64>>,
    response_tx: mpsc::UnboundedSender<LeaderElectionResult>,
) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            let mut retry_strategy = executor.config.retry_config.strategy();
            let mut attempt = 0;

            // First attempt without delay.
            loop {
                attempt += 1;

                match executor.acquire_lease(&lease_name).await {
                    Ok(crate::executor::LeaseResult::Acquired { revision })
                    | Ok(crate::executor::LeaseResult::TakenOver { revision }) => {
                        *current_revision.lock().await = revision;

                        debug!(
                            task_id = %task_id,
                            revision = %revision,
                            attempt = %attempt,
                            "Acquired lease, transitioning to leader"
                        );

                        if response_tx.send(LeaderElectionResult::Leader).is_err() {
                            debug!("Flow already terminated");
                            break;
                        }

                        // Switch to renewal task.
                        spawn_renewal_task(
                            task_id.clone(),
                            lease_name.clone(),
                            executor.clone(),
                            current_revision.clone(),
                            response_tx.clone(),
                        );
                        break;
                    }
                    Ok(crate::executor::LeaseResult::HeldByOther { holder }) => {
                        debug!(
                            task_id = %task_id,
                            holder = %holder,
                            attempt = %attempt,
                            "Lease held by other, will retry"
                        );
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            attempt = %attempt,
                            "Failed to acquire lease, will retry"
                        );
                    }
                }

                // Get next delay from retry strategy.
                if let Some(delay) = retry_strategy.next() {
                    debug!(
                        task_id = %task_id,
                        delay_ms = %delay.as_millis(),
                        attempt = %attempt,
                        "Waiting before next lease acquisition attempt"
                    );
                    tokio::time::sleep(delay).await;
                } else {
                    // No more retries allowed by the strategy.
                    error!(
                        task_id = %task_id,
                        attempts = %attempt,
                        "Exceeded maximum retry attempts for lease acquisition, giving up"
                    );
                    // Notify that we're not the leader and won't retry anymore.
                    let _ = response_tx.send(LeaderElectionResult::NotLeader);
                    break;
                }
            }
        }
        .instrument(tracing::Span::current()),
    )
}

/// Leader election options for tasks requiring coordination.
#[derive(Debug, Clone)]
pub struct LeaderElectionOptions {
    // Future: task-specific overrides.
}

/// Result of leader election after task registration.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LeaderElectionResult {
    /// This instance acquired the lease and is the leader.
    Leader,
    /// Another instance holds the lease, this instance is not the leader.
    NotLeader,
    /// No leader election was requested for this task.
    NoElection,
}

/// Task registration event.
pub struct TaskRegistration {
    /// Unique identifier for the task.
    task_id: String,
    /// Optional leader election configuration.
    leader_election_options: Option<LeaderElectionOptions>,
    /// Channel to send leader election result back to task.
    response_tx: mpsc::UnboundedSender<LeaderElectionResult>,
}

/// Active lease tracking data.
struct ActiveLease {
    /// Background task handle for renewal or retry operations.
    handle: JoinHandle<()>,
}

/// Centralized task lifecycle manager.
/// Handles task registration, coordination, and resource management.
pub struct TaskManager {
    /// Channel sender for task registration events.
    tx: Arc<Mutex<Option<UnboundedSender<TaskRegistration>>>>,
    /// Executor for leader election (uses in-memory cache by default).
    executor: Arc<crate::executor::Executor>,
    /// Active lease renewal tasks indexed by task ID.
    active_leases: Arc<Mutex<HashMap<String, ActiveLease>>>,
}

impl TaskManager {
    /// Starts the task manager event loop.
    #[tracing::instrument(skip(self), name = "task_manager.start")]
    pub async fn start(self) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<TaskRegistration>();

        // Store the sender.
        *self.tx.lock().await = Some(tx);

        let executor = self.executor.clone();
        let active_leases = self.active_leases.clone();

        // Event processing loop.
        let span = tracing::Span::current();
        tokio::spawn(
            async move {
                while let Some(registration) = rx.recv().await {
                    debug!("Received task registration: {:?}", registration.task_id);
                    let result = if registration.leader_election_options.is_some() {
                        // Sanitize task_id to be DNS-safe (RFC 1123): replace underscores with hyphens.
                        // Add lease prefix for better organization in the cache namespace.
                        let lease_name = format!(
                            "{}{}",
                            crate::executor::LEASE_KEY_PREFIX,
                            registration.task_id.replace('_', "-").to_lowercase()
                        );
                        match executor.acquire_lease(&lease_name).await {
                            Ok(crate::executor::LeaseResult::Acquired { revision })
                            | Ok(crate::executor::LeaseResult::TakenOver { revision }) => {
                                // Successfully acquired the lease.
                                let current_revision = Arc::new(Mutex::new(revision));
                                let renewal_handle = spawn_renewal_task(
                                    registration.task_id.clone(),
                                    lease_name.clone(),
                                    executor.clone(),
                                    current_revision,
                                    registration.response_tx.clone(),
                                );

                                // Store the renewal handle.
                                active_leases.lock().await.insert(
                                    registration.task_id.clone(),
                                    ActiveLease {
                                        handle: renewal_handle,
                                    },
                                );

                                LeaderElectionResult::Leader
                            }
                            Ok(crate::executor::LeaseResult::HeldByOther { .. }) | Err(_) => {
                                // Failed to acquire lease, spawn retry task.
                                debug!("Did not acquire lease for task: {}", registration.task_id);

                                let current_revision = Arc::new(Mutex::new(0));
                                let retry_handle = spawn_acquisition_retry_task(
                                    registration.task_id.clone(),
                                    lease_name.clone(),
                                    executor.clone(),
                                    current_revision,
                                    registration.response_tx.clone(),
                                );

                                // Store the retry handle.
                                active_leases.lock().await.insert(
                                    registration.task_id.clone(),
                                    ActiveLease {
                                        handle: retry_handle,
                                    },
                                );

                                // Don't send any result now - the retry task will send Leader when it succeeds.
                                // This prevents duplicate task spawning.
                                continue;
                            }
                        }
                    } else {
                        // No leader election required.
                        LeaderElectionResult::NoElection
                    };

                    // Send the result back to the caller.
                    registration
                        .response_tx
                        .send(result)
                        .map_err(|e| {
                            error!(
                                task_id = %registration.task_id,
                                "Failed to send leader election result: {:?}", e
                            );
                        })
                        .ok();
                }
            }
            .instrument(span),
        );

        self
    }

    /// Registers a task with the manager.
    /// Returns a receiver that streams leadership status updates.
    #[tracing::instrument(skip(self), name = "task_manager.register", fields(task_id = %task_id))]
    pub async fn register(
        &self,
        task_id: String,
        leader_election_options: Option<LeaderElectionOptions>,
    ) -> Result<mpsc::UnboundedReceiver<LeaderElectionResult>, Error> {
        let (response_tx, response_rx) = mpsc::unbounded_channel();

        if let Some(tx) = self.tx.lock().await.as_ref() {
            tx.send(TaskRegistration {
                task_id,
                leader_election_options,
                response_tx: response_tx.clone(),
            })
            .map_err(Error::SendError)?;
        } else {
            // No sender available, send NoElection and return.
            let _ = response_tx.send(LeaderElectionResult::NoElection);
        }

        Ok(response_rx)
    }

    /// Cleanup all owned leases on shutdown.
    ///
    /// This method performs a two-phase shutdown to prevent race conditions during pod termination.
    /// First, all background renewal and retry tasks are aborted to ensure no new lease acquisitions
    /// can occur while we are shutting down. Second, all owned leases are deleted from the cache.
    ///
    /// This ordering is critical: aborting tasks before deleting leases prevents a scenario where
    /// a retry task acquires a lease immediately after we delete it, causing duplicate leadership
    /// during rollout restarts.
    ///
    /// The entire shutdown operation has a 30-second timeout to prevent indefinite hangs.
    #[tracing::instrument(skip(self), name = "task_manager.shutdown")]
    pub async fn shutdown(&self) -> Result<(), Error> {
        // Wrap the entire shutdown in a timeout to prevent indefinite hangs.
        tokio::time::timeout(Duration::from_secs(30), self.shutdown_internal())
            .await
            .map_err(|_| Error::ShutdownTimeout)?
    }

    /// Internal shutdown implementation without timeout wrapper.
    async fn shutdown_internal(&self) -> Result<(), Error> {
        let mut handles_lock = self.active_leases.lock().await;

        for (task_id, active_lease) in handles_lock.iter() {
            debug!("Aborting background task for: {}", task_id);
            active_lease.handle.abort();
        }

        let task_ids: Vec<String> = handles_lock.keys().cloned().collect();
        handles_lock.clear();
        drop(handles_lock);

        for task_id in task_ids {
            let lease_name = format!(
                "{}{}",
                crate::executor::LEASE_KEY_PREFIX,
                task_id.replace('_', "-").to_lowercase()
            );

            match self.executor.still_owns_lease(&lease_name).await {
                Ok(Some(revision)) => {
                    if let Err(e) = self
                        .executor
                        .release_lease_with_revision(&lease_name, revision)
                        .await
                    {
                        warn!("Failed to release lease {}: {}", lease_name, e);
                    }
                }
                Ok(None) => {
                    debug!("Lease {} no longer owned, skipping deletion", lease_name);
                }
                Err(e) => {
                    warn!("Failed to check lease ownership for {}: {}", lease_name, e);
                }
            }
        }
        Ok(())
    }
}

/// Builder for TaskManager.
#[derive(Default)]
pub struct TaskManagerBuilder {
    executor: Option<Arc<crate::executor::Executor>>,
}

impl TaskManagerBuilder {
    /// Creates a new TaskManager builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the executor for leader election.
    pub fn executor(mut self, executor: Arc<crate::executor::Executor>) -> Self {
        self.executor = Some(executor);
        self
    }

    /// Builds the TaskManager configuration.
    ///
    /// If no executor is provided, creates one with in-memory cache.
    ///
    /// # Errors
    /// Returns error if the default executor configuration is invalid.
    pub fn build(self) -> Result<TaskManager, Error> {
        let executor = match self.executor {
            Some(executor) => executor,
            None => {
                // Default to in-memory cache for single-instance deployments.
                let cache = Arc::new(crate::cache::memory::MemoryCache::new())
                    as Arc<dyn crate::cache::Cache>;
                let config = crate::executor::LeaseConfig::default();
                Arc::new(crate::executor::Executor::new(cache, config)?)
            }
        };

        Ok(TaskManager {
            tx: Arc::new(Mutex::new(None)),
            executor,
            active_leases: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}
