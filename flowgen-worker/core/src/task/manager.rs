use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, UnboundedSender};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

/// Lease renewal interval in seconds.
const DEFAULT_LEASE_RENEWAL_INTERVAL_SECS: u64 = 10;

/// Task manager errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Failed to send event: {0}")]
    SendError(#[source] mpsc::error::SendError<TaskRegistration>),
    /// Host coordination error.
    #[error(transparent)]
    Host(#[from] crate::host::Error),
}
/// Leader election options for tasks requiring coordination.
#[derive(Debug, Clone)]
pub struct LeaderElectionOptions {
    // Future: task-specific overrides.
}

/// Result of leader election after task registration.
#[derive(Debug, Clone, PartialEq)]
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
    pub async fn start(self) -> Self {
        let (tx, mut rx) = mpsc::unbounded_channel::<TaskRegistration>();

        // Store the sender.
        *self.tx.lock().await = Some(tx);

        let host = self.host.clone();
        let active_leases = self.active_leases.clone();

        // Event processing loop.
        tokio::spawn(async move {
            while let Some(registration) = rx.recv().await {
                // Process task registration.
                debug!("Received task registration: {:?}", registration.task_id);

                let result = if registration.leader_election_options.is_some() {
                    // Leader election is required.
                    if let Some(ref host_client) = host {
                        match host_client.create_lease(&registration.task_id).await {
                            Ok(_) => {
                                // Successfully acquired the lease, spawn renewal task.
                                let task_id = registration.task_id.clone();
                                let host = host_client.clone();
                                let renewal_handle = tokio::spawn(async move {
                                    let mut interval = tokio::time::interval(Duration::from_secs(
                                        DEFAULT_LEASE_RENEWAL_INTERVAL_SECS,
                                    ));
                                    loop {
                                        interval.tick().await;
                                        if let Err(e) = host.renew_lease(&task_id, None).await {
                                            error!(
                                                "Failed to renew lease for task {}: {}",
                                                task_id, e
                                            );
                                        } else {
                                            debug!(
                                                "Successfully renewed lease for task {}",
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
                                // Failed to acquire lease, another instance holds it.
                                warn!(
                                    "Did not acquire lease for task {}: {}",
                                    registration.task_id, e
                                );
                                LeaderElectionResult::NotLeader
                            }
                        }
                    } else {
                        // No host available.
                        debug!(
                            "Leader election requested for task {} but no host configured",
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
                            "Failed to send leader election result for task {}: {}",
                            registration.task_id, e
                        );
                    })
                    .ok();
            }
        });

        self
    }

    /// Registers a task with the manager.
    /// Returns a receiver that streams leadership status updates.
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
