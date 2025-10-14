use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn, Instrument};

/// Lease renewal interval in seconds.
const DEFAULT_LEASE_RENEWAL_INTERVAL_SECS: u64 = 10;
/// Lease acquisition retry interval in seconds.
const DEFAULT_LEASE_RETRY_INTERVAL_SECS: u64 = 5;

/// Task manager errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to send event: {0}")]
    SendError(#[source] mpsc::error::SendError<TaskRegistration>),
    /// Host coordination error.
    #[error("Host coordination error")]
    Host(#[source] crate::host::Error),
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
    task_id: String,
    leader_election_options: Option<LeaderElectionOptions>,
    response_tx: mpsc::UnboundedSender<LeaderElectionResult>,
}

/// Centralized task lifecycle manager.
/// Handles task registration, coordination, and resource management.
pub struct TaskManager {
    tx: Arc<Mutex<Option<UnboundedSender<TaskRegistration>>>>,
    host: Option<Arc<dyn crate::host::Host>>,
    active_leases: Arc<Mutex<HashMap<String, JoinHandle<()>>>>,
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
                // Process task registration.
                info!("Received task registration: {:?}", registration.task_id);

                let result = if registration.leader_election_options.is_some() {
                    // Leader election is required.
                    if let Some(ref host_client) = host {
                        // Sanitize task_id to be DNS-safe (RFC 1123): replace underscores with hyphens.
                        let lease_name = registration.task_id.replace('_', "-").to_lowercase();
                        match host_client.create_lease(&lease_name).await {
                            Ok(_) => {
                                // Successfully acquired the lease, spawn renewal task.
                                let task_id = registration.task_id.clone();
                                let lease_name_clone = lease_name.clone();
                                let host = host_client.clone();
                                let renewal_handle = tokio::spawn(async move {
                                    let mut interval = tokio::time::interval(Duration::from_secs(
                                        DEFAULT_LEASE_RENEWAL_INTERVAL_SECS,
                                    ));
                                    loop {
                                        interval.tick().await;
                                        if let Err(e) =
                                            host.renew_lease(&lease_name_clone, None).await
                                        {
                                            error!(
                                                "Failed to renew lease for task: {}, {}",
                                                task_id, e
                                            );
                                        } else {
                                            debug!(
                                                "Successfully renewed lease for task: {}",
                                                task_id
                                            );
                                        }
                                    }
                                });

                                // Store the renewal handle.
                                active_leases
                                    .lock()
                                    .await
                                    .insert(registration.task_id.clone(), renewal_handle);

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

                                let retry_handle = tokio::spawn(async move {
                                    let mut interval = tokio::time::interval(Duration::from_secs(
                                        DEFAULT_LEASE_RETRY_INTERVAL_SECS,
                                    ));
                                    loop {
                                        interval.tick().await;
                                        match host.create_lease(&lease_name_clone).await {
                                            Ok(_) => {
                                                // Successfully acquired the lease, notify task.
                                                debug!(
                                                    "Acquired lease for task: {} after retry",
                                                    task_id
                                                );

                                                // Spawn renewal task.
                                                let task_id_clone = task_id.clone();
                                                let lease_name_renewal = lease_name_clone.clone();
                                                let host_renewal = host.clone();
                                                let renewal_handle = tokio::spawn(async move {
                                                    let mut interval =
                                                        tokio::time::interval(Duration::from_secs(
                                                            DEFAULT_LEASE_RENEWAL_INTERVAL_SECS,
                                                        ));
                                                    loop {
                                                        interval.tick().await;
                                                        if let Err(e) = host_renewal
                                                            .renew_lease(&lease_name_renewal, None)
                                                            .await
                                                        {
                                                            error!(
                                                                "Failed to renew lease for task, {}, {}",
                                                                task_id_clone, e
                                                            );
                                                        } else {
                                                            debug!(
                                                                "Successfully renewed lease for task: {}",
                                                                task_id_clone
                                                            );
                                                        }
                                                    }
                                                });

                                                // Store the renewal handle.
                                                active_leases_clone
                                                    .lock()
                                                    .await
                                                    .insert(task_id.clone(), renewal_handle);

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
                                });

                                // Store the retry handle.
                                active_leases
                                    .lock()
                                    .await
                                    .insert(registration.task_id.clone(), retry_handle);

                                LeaderElectionResult::NotLeader
                            }
                        }
                    } else {
                        // No host available.
                        warn!(
                            "Leader election requested for task: {} but no host configured",
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
                        if result != LeaderElectionResult::NoElection {
                            error!(
                                "Failed to send leader election result for task: {}, {}",
                                registration.task_id, e
                            );
                        }
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
    /// Deletes all leases that this instance currently holds.
    #[tracing::instrument(skip(self), name = "task_manager.shutdown")]
    pub async fn shutdown(&self) -> Result<(), Error> {
        let task_ids: Vec<String> = self.active_leases.lock().await.keys().cloned().collect();

        if let Some(ref host) = self.host {
            for task_id in task_ids {
                // Convert task_id to lease name (same sanitization as in register).
                let lease_name = task_id.replace('_', "-").to_lowercase();

                if let Err(e) = host.delete_lease(&lease_name, None).await {
                    warn!("Failed to delete lease {} on shutdown: {}", lease_name, e);
                } else {
                    debug!("Deleted lease: {} on shutdown", lease_name);
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
