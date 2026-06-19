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
                            "Lost lease ownership, notifying flow and re-arming acquisition"
                        );

                        if response_tx.send(LeaderElectionResult::NotLeader).is_err() {
                            debug!("Flow already terminated");
                            break;
                        }

                        // Hand off to an infinite-retry acquisition task so
                        // this pod gets another shot at leadership when the
                        // current holder yields. Without this the flow would
                        // sit waiting on `leadership_rx.recv()` forever.
                        spawn_acquisition_retry_task(
                            task_id.clone(),
                            lease_name.clone(),
                            executor.clone(),
                            current_revision.clone(),
                            response_tx.clone(),
                        );
                        break;
                    }
                    Ok(crate::executor::RenewalResult::LeaseDeleted) => {
                        warn!(
                            task_id = %task_id,
                            "Lease was deleted, notifying flow and re-arming acquisition"
                        );

                        if response_tx.send(LeaderElectionResult::NotLeader).is_err() {
                            debug!("Flow already terminated");
                            break;
                        }

                        spawn_acquisition_retry_task(
                            task_id.clone(),
                            lease_name.clone(),
                            executor.clone(),
                            current_revision.clone(),
                            response_tx.clone(),
                        );
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

/// Spawns a task to retry lease acquisition with infinite exponential backoff.
///
/// Unlike task execution retries, lease acquisition uses infinite retries (no max_attempts).
/// This prevents outages where all pods give up trying to acquire leases during high contention
/// (e.g., rolling restarts with DELETE tombstone races).
fn spawn_acquisition_retry_task(
    task_id: String,
    lease_name: String,
    executor: Arc<crate::executor::Executor>,
    current_revision: Arc<Mutex<u64>>,
    response_tx: mpsc::UnboundedSender<LeaderElectionResult>,
) -> JoinHandle<()> {
    tokio::spawn(
        async move {
            let infinite_retry_config = crate::retry::RetryConfig {
                max_attempts: None,
                initial_backoff: executor.config.retry_config.initial_backoff,
            };
            let mut retry_strategy = infinite_retry_config.strategy();
            let mut attempt = 0;

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

                if let Some(delay) = retry_strategy.next() {
                    debug!(
                        task_id = %task_id,
                        delay_ms = %delay.as_millis(),
                        attempt = %attempt,
                        "Waiting before next lease acquisition attempt"
                    );
                    tokio::time::sleep(delay).await;
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
    /// Optional peer registry for flow distribution via consistent hashing.
    peer_registry: Option<Arc<crate::peer::PeerRegistry>>,
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
        let peer_registry = self.peer_registry.clone();

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

                        // If peer registry is configured, non-preferred pods defer
                        // their acquisition attempt to let the preferred pod win.
                        if let Some(ref registry) = peer_registry {
                            match registry.is_preferred_owner(&registration.task_id).await {
                                Ok(true) => {
                                    debug!(
                                        task_id = %registration.task_id,
                                        "This pod is the preferred owner, acquiring immediately"
                                    );
                                }
                                Ok(false) => {
                                    let deferral = registry.deferral_duration();
                                    debug!(
                                        task_id = %registration.task_id,
                                        deferral_ms = %deferral.as_millis(),
                                        "This pod is not the preferred owner, deferring acquisition"
                                    );
                                    tokio::time::sleep(deferral).await;
                                }
                                Err(e) => {
                                    warn!(
                                        error = %e,
                                        task_id = %registration.task_id,
                                        "Failed to check preferred ownership, proceeding with normal acquisition"
                                    );
                                }
                            }
                        }

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
    /// Lease executor for leader election.
    executor: Option<Arc<crate::executor::Executor>>,
    /// Optional peer registry for flow distribution via consistent hashing.
    peer_registry: Option<Arc<crate::peer::PeerRegistry>>,
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

    /// Sets the peer registry for flow distribution via consistent hashing.
    pub fn peer_registry(mut self, registry: Arc<crate::peer::PeerRegistry>) -> Self {
        self.peer_registry = Some(registry);
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
            peer_registry: self.peer_registry,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::memory::MemoryCache;
    use crate::executor::{Executor, LeaseConfig};
    use std::time::Duration;

    fn make_executor(holder: &str, cache: Arc<dyn crate::cache::Cache>) -> Arc<Executor> {
        let config = LeaseConfig {
            holder_identity: holder.to_string(),
            lease_duration: Duration::from_secs(2),
            renewal_interval: Duration::from_millis(100),
            ..Default::default()
        };
        Arc::new(Executor::new(cache, config).unwrap())
    }

    /// Reproducer for the "lost-then-stranded" regression. When a renewal
    /// loses ownership the renewal task previously just returned after
    /// sending `NotLeader` — nobody re-armed acquisition, so the flow's
    /// `leadership_rx.recv()` hung forever. The fix spawns an acquisition
    /// retry task immediately after the lost-ownership notification.
    ///
    /// We exercise the fix by calling `spawn_renewal_task` directly against
    /// a forged "wrong" current revision so the next renew tick definitely
    /// returns `LostOwnership`, then assert two things:
    ///
    /// 1. The flow side receives `NotLeader`.
    /// 2. Once the lease becomes acquirable again, the flow side receives
    ///    `Leader` — proof the acquisition retry task was spawned.
    #[tokio::test]
    async fn renewal_re_arms_acquisition_after_lost_ownership() {
        let cache: Arc<dyn crate::cache::Cache> = Arc::new(MemoryCache::new());

        let exec1 = make_executor("pod-1", cache.clone());
        let exec2 = make_executor("pod-2", cache.clone());

        // Pod 2 owns the lease — we want pod 1's renewal to lose immediately.
        let res = exec2.acquire_lease("acct_nba").await.unwrap();
        let pod2_revision = match res {
            crate::executor::LeaseResult::Acquired { revision } => revision,
            other => panic!("pod-2 should acquire fresh, got {other:?}"),
        };

        // Spawn a renewal task for pod 1 with a STALE revision. On the very
        // next tick the renewal call will fail with RevisionMismatch and
        // surface `LostOwnership`.
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        let stale_revision = Arc::new(Mutex::new(pod2_revision.saturating_sub(1)));
        spawn_renewal_task(
            "acct_nba".to_string(),
            "acct_nba".to_string(),
            exec1.clone(),
            stale_revision,
            response_tx,
        );

        // 1. NotLeader must arrive within a couple of renewal intervals.
        let not_leader = tokio::time::timeout(Duration::from_secs(2), response_rx.recv())
            .await
            .expect("must report NotLeader within renewal interval")
            .expect("channel still open");
        assert_eq!(not_leader, LeaderElectionResult::NotLeader);

        // 2. Pod 2 releases. The fix has now installed an acquisition retry
        // task on the same response channel; it should grab the free lease
        // and emit Leader. Pre-fix nothing was spawned and this would hang.
        exec2.release_lease("acct_nba").await.unwrap();

        let became_leader = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match response_rx.recv().await {
                    Some(LeaderElectionResult::Leader) => break true,
                    Some(LeaderElectionResult::NotLeader) => continue,
                    Some(LeaderElectionResult::NoElection) => continue,
                    None => break false,
                }
            }
        })
        .await
        .expect("manager must re-arm acquisition after lost ownership");
        assert!(became_leader, "leadership channel closed prematurely");
    }
}
