//! Executor manages distributed coordination via cache-based leases.
//!
//! Provides a universal lease management abstraction that works with any Cache
//! implementation (NATS KV, Redis, etc.) using atomic operations for coordination.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Lease metadata stored in cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseMetadata {
    /// Identity of the lease holder (pod name, hostname, etc.)
    pub holder_identity: String,
    /// Unix timestamp when lease was acquired/renewed (seconds).
    pub renewed_at: u64,
    /// Lease duration in seconds.
    pub lease_duration_secs: u64,
}

/// Configuration for lease management.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// How long a lease remains valid without renewal (default: 60 seconds).
    pub lease_duration: Duration,
    /// How often to renew the lease (default: 10 seconds).
    pub renewal_interval: Duration,
    /// How often to retry lease acquisition when failed (default: 5 seconds).
    pub retry_interval: Duration,
    /// Identity of this executor (pod name, hostname, etc.).
    pub holder_identity: String,
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(60),
            renewal_interval: Duration::from_secs(10),
            retry_interval: Duration::from_secs(5),
            holder_identity: std::env::var("POD_NAME")
                .or_else(|_| std::env::var("HOSTNAME"))
                .unwrap_or_else(|_| "unknown".to_string()),
        }
    }
}

/// Result of lease acquisition attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaseResult {
    /// This executor acquired the lease.
    Acquired { revision: u64 },
    /// Another executor holds the lease.
    HeldByOther { holder: String },
    /// Lease expired and was taken over.
    TakenOver { revision: u64 },
}

/// Result of lease renewal attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum RenewalResult {
    /// Lease renewed successfully.
    Renewed { revision: u64 },
    /// Lost lease ownership (another executor took over).
    LostOwnership { holder: String },
}

/// Executor manages distributed coordination via cache-based leases.
///
/// Unlike the platform-specific Host trait, Executor works with any Cache implementation
/// (NATS KV, Redis, etc.) using atomic operations.
#[derive(Debug, Clone)]
pub struct Executor {
    cache: Arc<dyn crate::cache::Cache>,
    pub config: LeaseConfig,
}

impl Executor {
    /// Creates a new executor with the given cache backend.
    pub fn new(cache: Arc<dyn crate::cache::Cache>, config: LeaseConfig) -> Self {
        Self { cache, config }
    }

    /// Attempts to acquire a lease.
    ///
    /// # Flow
    /// 1. Try to create lease key (atomic create)
    /// 2. If exists, check if expired
    /// 3. If expired, attempt takeover (atomic update with revision check)
    /// 4. If not expired, return HeldByOther
    pub async fn acquire_lease(
        &self,
        lease_name: &str,
    ) -> Result<LeaseResult, crate::cache::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                crate::cache::CacheError::CreateFailed(Box::new(e))
            })?
            .as_secs();

        let metadata = LeaseMetadata {
            holder_identity: self.config.holder_identity.clone(),
            renewed_at: now,
            lease_duration_secs: self.config.lease_duration.as_secs(),
        };

        let value = serde_json::to_vec(&metadata)
            .map_err(|e| crate::cache::CacheError::CreateFailed(Box::new(e)))?;

        // Try to create lease (atomic - only succeeds if key doesn't exist).
        match self
            .cache
            .create(
                lease_name,
                Bytes::from(value.clone()),
                Some(self.config.lease_duration.as_secs()),
            )
            .await
        {
            Ok(revision) => {
                tracing::debug!(
                    lease_name = %lease_name,
                    holder = %self.config.holder_identity,
                    revision = %revision,
                    "Acquired new lease"
                );
                return Ok(LeaseResult::Acquired { revision });
            }
            Err(crate::cache::CacheError::AlreadyExists) => {
                // Lease exists - check if we can take it over.
            }
            Err(e) => return Err(e),
        }

        // Lease exists - get current state with revision.
        let (current_value, current_revision) =
            match self.cache.get_with_revision(lease_name).await? {
                Some(entry) => entry,
                None => {
                    // Lease was deleted between create and get - retry acquisition.
                    return Box::pin(self.acquire_lease(lease_name)).await;
                }
            };

        let current_metadata: LeaseMetadata = serde_json::from_slice(&current_value)
            .map_err(|e| crate::cache::CacheError::GetFailed(Box::new(e)))?;

        // Check if current holder is self (can happen during pod restart).
        if current_metadata.holder_identity == self.config.holder_identity {
            tracing::debug!(
                lease_name = %lease_name,
                holder = %self.config.holder_identity,
                "Already own this lease, renewing"
            );
            return self
                .renew_lease(lease_name, current_revision)
                .await
                .map(|result| match result {
                    RenewalResult::Renewed { revision } => LeaseResult::Acquired { revision },
                    RenewalResult::LostOwnership { holder } => LeaseResult::HeldByOther { holder },
                });
        }

        // Check if lease is expired.
        let elapsed = now.saturating_sub(current_metadata.renewed_at);
        if elapsed > current_metadata.lease_duration_secs {
            // Lease expired - attempt takeover (atomic update with revision check).
            tracing::info!(
                lease_name = %lease_name,
                current_holder = %current_metadata.holder_identity,
                elapsed = %elapsed,
                lease_duration = %current_metadata.lease_duration_secs,
                "Lease expired, attempting takeover"
            );

            match self
                .cache
                .update(
                    lease_name,
                    Bytes::from(value),
                    current_revision,
                    Some(self.config.lease_duration.as_secs()),
                )
                .await
            {
                Ok(new_revision) => {
                    tracing::info!(
                        lease_name = %lease_name,
                        holder = %self.config.holder_identity,
                        revision = %new_revision,
                        "Successfully took over expired lease"
                    );
                    Ok(LeaseResult::TakenOver {
                        revision: new_revision,
                    })
                }
                Err(crate::cache::CacheError::RevisionMismatch { .. }) => {
                    // Another executor took over in the meantime - get new state.
                    let (value, _) = self
                        .cache
                        .get_with_revision(lease_name)
                        .await?
                        .ok_or(crate::cache::CacheError::NotFound)?;
                    let metadata: LeaseMetadata = serde_json::from_slice(&value)
                        .map_err(|e| crate::cache::CacheError::GetFailed(Box::new(e)))?;

                    tracing::debug!(
                        lease_name = %lease_name,
                        holder = %metadata.holder_identity,
                        "Lost takeover race to another executor"
                    );
                    Ok(LeaseResult::HeldByOther {
                        holder: metadata.holder_identity,
                    })
                }
                Err(e) => Err(e),
            }
        } else {
            // Lease is still valid and owned by another executor.
            tracing::debug!(
                lease_name = %lease_name,
                holder = %current_metadata.holder_identity,
                time_remaining = %(current_metadata.lease_duration_secs - elapsed),
                "Lease held by another executor"
            );
            Ok(LeaseResult::HeldByOther {
                holder: current_metadata.holder_identity,
            })
        }
    }

    /// Renews an existing lease that this executor owns.
    ///
    /// # Arguments
    /// * `lease_name` - Name of the lease to renew
    /// * `current_revision` - The revision number from when we last acquired/renewed the lease
    ///
    /// # Returns
    /// * `Renewed` - Lease renewed successfully
    /// * `LostOwnership` - Another executor took over the lease (revision mismatch or different holder)
    pub async fn renew_lease(
        &self,
        lease_name: &str,
        current_revision: u64,
    ) -> Result<RenewalResult, crate::cache::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                crate::cache::CacheError::UpdateFailed(Box::new(e))
            })?
            .as_secs();

        let metadata = LeaseMetadata {
            holder_identity: self.config.holder_identity.clone(),
            renewed_at: now,
            lease_duration_secs: self.config.lease_duration.as_secs(),
        };

        let value = serde_json::to_vec(&metadata)
            .map_err(|e| crate::cache::CacheError::UpdateFailed(Box::new(e)))?;

        // Atomic update - only succeeds if revision matches (prevents split-brain).
        match self
            .cache
            .update(
                lease_name,
                Bytes::from(value),
                current_revision,
                Some(self.config.lease_duration.as_secs()),
            )
            .await
        {
            Ok(new_revision) => {
                tracing::trace!(
                    lease_name = %lease_name,
                    revision = %new_revision,
                    "Successfully renewed lease"
                );
                Ok(RenewalResult::Renewed {
                    revision: new_revision,
                })
            }
            Err(crate::cache::CacheError::RevisionMismatch { .. })
            | Err(crate::cache::CacheError::NotFound) => {
                // Lease was taken over or deleted - get current holder.
                match self.cache.get_with_revision(lease_name).await? {
                    Some((value, _)) => {
                        let metadata: LeaseMetadata = serde_json::from_slice(&value)
                            .map_err(|e| crate::cache::CacheError::GetFailed(Box::new(e)))?;

                        tracing::warn!(
                            lease_name = %lease_name,
                            current_holder = %metadata.holder_identity,
                            "Lost lease ownership during renewal"
                        );
                        Ok(RenewalResult::LostOwnership {
                            holder: metadata.holder_identity,
                        })
                    }
                    None => {
                        tracing::warn!(
                            lease_name = %lease_name,
                            "Lease was deleted"
                        );
                        Ok(RenewalResult::LostOwnership {
                            holder: "deleted".to_string(),
                        })
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Releases a lease that this executor owns.
    pub async fn release_lease(&self, lease_name: &str) -> Result<(), crate::cache::Error> {
        self.cache.delete(lease_name).await?;
        tracing::debug!(
            lease_name = %lease_name,
            holder = %self.config.holder_identity,
            "Released lease"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::memory::MemoryCache;

    fn create_executor(identity: &str) -> Executor {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let config = LeaseConfig {
            holder_identity: identity.to_string(),
            ..Default::default()
        };
        Executor::new(cache, config)
    }

    #[tokio::test]
    async fn test_acquire_new_lease() {
        let executor = create_executor("executor-1");
        let result = executor.acquire_lease("test-lease").await.unwrap();

        assert!(matches!(result, LeaseResult::Acquired { .. }));
    }

    #[tokio::test]
    async fn test_acquire_lease_already_owned_by_self() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let config = LeaseConfig {
            holder_identity: "executor-1".to_string(),
            ..Default::default()
        };
        let executor = Executor::new(cache.clone(), config.clone());

        // Acquire lease first time.
        let result1 = executor.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result1, LeaseResult::Acquired { .. }));

        // Try to acquire same lease with same executor.
        let result2 = executor.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result2, LeaseResult::Acquired { .. }));
    }

    #[tokio::test]
    async fn test_fail_to_acquire_lease_held_by_other() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                ..Default::default()
            },
        );
        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        );

        // Executor 1 acquires lease.
        let result1 = executor1.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result1, LeaseResult::Acquired { .. }));

        // Executor 2 tries to acquire same lease.
        let result2 = executor2.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result2, LeaseResult::HeldByOther { .. }));
    }

    #[tokio::test]
    async fn test_takeover_expired_lease() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                lease_duration: Duration::from_secs(1),
                ..Default::default()
            },
        );
        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        );

        // Executor 1 acquires lease.
        let result1 = executor1.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result1, LeaseResult::Acquired { .. }));

        // Wait for lease to expire.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Executor 2 takes over expired lease.
        let result2 = executor2.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result2, LeaseResult::TakenOver { .. }));
    }

    #[tokio::test]
    async fn test_renew_lease_successfully() {
        let executor = create_executor("executor-1");

        let LeaseResult::Acquired { revision } =
            executor.acquire_lease("test-lease").await.unwrap()
        else {
            panic!("Failed to acquire lease");
        };

        let result = executor.renew_lease("test-lease", revision).await.unwrap();
        assert!(matches!(result, RenewalResult::Renewed { .. }));
    }

    #[tokio::test]
    async fn test_detect_lost_ownership_during_renewal() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                lease_duration: Duration::from_secs(1),
                ..Default::default()
            },
        );
        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        );

        // Executor 1 acquires lease.
        let LeaseResult::Acquired { revision } =
            executor1.acquire_lease("test-lease").await.unwrap()
        else {
            panic!("Failed to acquire lease");
        };

        // Wait for lease to expire.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Executor 2 takes over.
        executor2.acquire_lease("test-lease").await.unwrap();

        // Executor 1 tries to renew with old revision.
        let result = executor1.renew_lease("test-lease", revision).await.unwrap();
        assert!(matches!(result, RenewalResult::LostOwnership { .. }));
    }

    #[tokio::test]
    async fn test_release_lease() {
        let executor = create_executor("executor-1");

        executor.acquire_lease("test-lease").await.unwrap();
        executor.release_lease("test-lease").await.unwrap();

        // Verify lease is gone by trying to acquire it again.
        let result = executor.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result, LeaseResult::Acquired { .. }));
    }
}
