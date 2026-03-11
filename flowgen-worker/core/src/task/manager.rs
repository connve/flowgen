use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn, Instrument};

/// Lease renewal interval in seconds.
const DEFAULT_LEASE_RENEWAL_INTERVAL_SECS: u64 = 10;
/// Lease acquisition retry interval in seconds.
const DEFAULT_LEASE_RETRY_INTERVAL_SECS: u64 = 5;

/// Task manager errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event: {0}")]
    SendError(#[source] mpsc::error::SendError<TaskRegistration>),
    /// Host coordination error.
    #[error("Host coordination error")]
    Host(#[source] crate::host::Error),
}

/// Spawns a task to continuously renew a Kubernetes lease with retry logic.
///
/// This function spawns a background task that attempts to renew the lease every 10 seconds.
/// If renewal fails critically (lease deleted or ownership lost), the task notifies the flow
/// by sending a NotLeader message and terminates.
fn spawn_renewal_task(
    task_id: String,
    lease_name: String,
    host: Arc<dyn crate::host::Host>,
    response_tx: mpsc::UnboundedSender<LeaderElectionResult>,
) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            let mut interval =
                tokio::time::interval(Duration::from_secs(DEFAULT_LEASE_RENEWAL_INTERVAL_SECS));

            // Create retry strategy: 3 attempts with exponential backoff starting at 100ms.
            let retry_config = crate::retry::RetryConfig {
                max_attempts: Some(3),
                initial_backoff: Duration::from_millis(100),
            };

            loop {
                interval.tick().await;

                let lease_name_clone = lease_name.clone();
                let host_clone = Arc::clone(&host);

                match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
                    host_clone
                        .renew_lease(&lease_name_clone, None)
                        .await
                        .map_err(tokio_retry::RetryError::transient)
                })
                .await
                {
                    Ok(_) => {
                        debug!(task_id = %task_id, "Successfully renewed lease");
                    }
                    Err(e) => {
                        // Check if the error indicates we lost ownership of the lease.
                        // This can happen when:
                        // 1. Lease was deleted (404 Not Found)
                        // 2. Lost takeover race (409 Conflict)
                        // 3. Another pod took over an expired lease (LeaseHeldByOther)
                        // In all cases, this pod no longer owns the lease and must stop processing
                        // to prevent duplicate work across multiple pods.
                        let should_stop = if let Some(k8s_err) = e.downcast_ref::<crate::host::k8s::Error>() {
                            match k8s_err {
                                // LeaseHeldByOther is returned when another pod owns the lease.
                                crate::host::k8s::Error::LeaseHeldByOther { .. } => true,
                                // RenewLease with 404 means lease was deleted.
                                crate::host::k8s::Error::RenewLease { source } => {
                                    matches!(source, kube::Error::Api(api_err) if api_err.code == 404)
                                }
                                // GetLease with 404 also means lease was deleted.
                                crate::host::k8s::Error::GetLease { source } => {
                                    matches!(source, kube::Error::Api(api_err) if api_err.code == 404)
                                }
                                _ => false,
                            }
                        } else {
                            false
                        };

                        if should_stop {
                            debug!(
                                error = %e,
                                task_id = %task_id,
                                "Lost lease ownership, notifying flow to stop processing"
                            );

                            // Notify the flow that it lost leadership so it can abort tasks and stop processing.
                            // This prevents multiple pods from processing the same events during rollout restarts.
                            if response_tx.send(LeaderElectionResult::NotLeader).is_err() {
                                debug!("Flow already terminated, leadership channel closed");
                            }
                            break;
                        }

                        error!(error = %e, task_id = %task_id, "Failed to renew lease after all retry attempts");
                    }
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
    /// Optional K8s host for leader election.
    host: Option<Arc<dyn crate::host::Host>>,
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

        let host = self.host.clone();
        let active_leases = self.active_leases.clone();

        // Event processing loop.
        let span = tracing::Span::current();
        tokio::spawn(
            async move {
                while let Some(registration) = rx.recv().await {
                debug!("Received task registration: {:?}", registration.task_id);
                let result = if registration.leader_election_options.is_some() {
                    if let Some(ref host_client) = host {
                        // Sanitize task_id to be DNS-safe (RFC 1123): replace underscores with hyphens.
                        let lease_name = registration.task_id.replace('_', "-").to_lowercase();
                        match host_client.create_lease(&lease_name).await {
                            Ok(_) => {
                                // Successfully acquired the lease from K8s API (authoritative source).
                                let renewal_handle = spawn_renewal_task(
                                    registration.task_id.clone(),
                                    lease_name.clone(),
                                    host_client.clone(),
                                    registration.response_tx.clone(),
                                );

                                // Store the renewal handle.
                                active_leases
                                    .lock()
                                    .await
                                    .insert(registration.task_id.clone(), ActiveLease {
                                        handle: renewal_handle,
                                    });

                                LeaderElectionResult::Leader
                            }
                            Err(e) => {
                                // Failed to acquire lease, spawn retry task.
                                debug!(
                                    "Did not acquire lease for task: {}, {}",
                                    registration.task_id, e
                                );

                                let task_id = registration.task_id.clone();
                                let lease_name_clone = lease_name.clone();
                                let host = host_client.clone();
                                let response_tx = registration.response_tx.clone();
                                let active_leases_clone = active_leases.clone();

                                let retry_handle = tokio::spawn(
                                    async move {
                                        let mut interval = tokio::time::interval(Duration::from_secs(
                                            DEFAULT_LEASE_RETRY_INTERVAL_SECS,
                                        ));
                                        loop {
                                            interval.tick().await;
                                            match host.create_lease(&lease_name_clone).await {
                                                Ok(_) => {
                                                    // Successfully acquired the lease from K8s API (authoritative source).
                                                    debug!(
                                                        "Acquired lease for task: {} after retry",
                                                        task_id
                                                    );

                                                    // Spawn renewal task.
                                                    let renewal_handle = spawn_renewal_task(
                                                        task_id.clone(),
                                                        lease_name_clone.clone(),
                                                        host.clone(),
                                                        response_tx.clone(),
                                                    );

                                                    // Store the renewal handle, replacing the retry handle.
                                                    active_leases_clone
                                                        .lock()
                                                        .await
                                                        .insert(task_id.clone(), ActiveLease {
                                                            handle: renewal_handle,
                                                        });

                                                    // Notify the task.
                                                    if response_tx
                                                        .send(LeaderElectionResult::Leader)
                                                        .is_err()
                                                    {
                                                        debug!(
                                                            "Failed to notify task: {} of leadership acquisition",
                                                            task_id
                                                        );
                                                    }
                                                    break;
                                                }
                                                Err(e) => {
                                                    debug!(
                                                        "Retry failed to acquire lease for task: {}, {}",
                                                        task_id, e
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    .instrument(tracing::Span::current()),
                                );

                                // Store the retry handle.
                                active_leases
                                    .lock()
                                    .await
                                    .insert(registration.task_id.clone(), ActiveLease {
                                        handle: retry_handle,
                                    });

                                // Don't send any result now - the retry task will send Leader when it succeeds.
                                // This prevents duplicate task spawning.
                                continue;
                            }
                        }
                    } else {
                        // No host available.
                        debug!(
                            "Leader election requested for task: {} but no K8s host configured",
                            registration.task_id
                        );
                        LeaderElectionResult::NoElection
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
    /// can occur while we are shutting down. Second, all owned leases are deleted from Kubernetes.
    ///
    /// This ordering is critical: aborting tasks before deleting leases prevents a scenario where
    /// a retry task acquires a lease immediately after we delete it, causing duplicate leadership
    /// during rollout restarts.
    #[tracing::instrument(skip(self), name = "task_manager.shutdown")]
    pub async fn shutdown(&self) -> Result<(), Error> {
        // Abort all background tasks first to prevent race conditions during shutdown.
        // Renewal tasks continuously update leases every 10 seconds, and retry tasks attempt
        // to acquire leases every 5 seconds. If we delete leases before aborting these tasks,
        // they may immediately re-acquire the leases, causing multiple pods to become leaders
        // simultaneously during a rollout restart.
        let mut handles_lock = self.active_leases.lock().await;

        for (task_id, active_lease) in handles_lock.iter() {
            debug!("Aborting background task for: {}", task_id);
            active_lease.handle.abort();
        }

        // Collect task identifiers before clearing the map. We need these to construct
        // the lease names for deletion from Kubernetes.
        let task_ids: Vec<String> = handles_lock.keys().cloned().collect();

        // Clear the handles map to prevent any future access to the aborted tasks.
        // This ensures the tasks are properly cleaned up and resources are freed.
        handles_lock.clear();

        // Release the mutex lock before making network calls to Kubernetes.
        // Holding locks during network operations can cause deadlocks if other
        // operations need to acquire the same lock while waiting for network responses.
        drop(handles_lock);

        // Delete all leases from Kubernetes after tasks are safely aborted.
        // At this point, no background tasks are running that could race to re-acquire
        // the leases we are about to delete.
        if let Some(ref host) = self.host {
            for task_id in task_ids {
                // Convert task identifier to lease name using the same sanitization
                // applied during registration. Kubernetes lease names must be DNS-safe,
                // so we replace underscores with hyphens and convert to lowercase.
                let lease_name = task_id.replace('_', "-").to_lowercase();

                if let Err(e) = host.delete_lease(&lease_name, None).await {
                    warn!("Failed to delete lease {} on shutdown: {}", lease_name, e);
                }
            }
        }
        Ok(())
    }
}

/// Builder for TaskManager.
#[derive(Default)]
pub struct TaskManagerBuilder {
    host: Option<std::sync::Arc<dyn crate::host::Host>>,
}

impl TaskManagerBuilder {
    /// Creates a new TaskManager builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the host client for leader election.
    pub fn host(mut self, host: std::sync::Arc<dyn crate::host::Host>) -> Self {
        self.host = Some(host);
        self
    }

    /// Builds the TaskManager configuration.
    pub fn build(self) -> TaskManager {
        TaskManager {
            tx: Arc::new(Mutex::new(None)),
            host: self.host,
            active_leases: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}
