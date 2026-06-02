//! Caching abstractions for persistent storage across workflow executions.
//!
//! Provides traits and configuration for key-value caching systems used by
//! tasks that need to maintain state between runs, such as replay identifiers.

pub mod memory;

use async_trait::async_trait;
use futures_util::stream::BoxStream;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Cache operation errors.
#[derive(Debug, thiserror::Error)]
pub enum CacheError {
    /// Key already exists during create operation.
    #[error("Key already exists")]
    AlreadyExists,

    /// Revision mismatch during update operation (optimistic concurrency control failure).
    #[error("Revision mismatch: expected {expected}, got {actual}")]
    RevisionMismatch { expected: u64, actual: u64 },

    /// Key not found during update or get operation.
    #[error("Key not found")]
    NotFound,

    /// Cache store not initialized.
    #[error("Cache store not initialized")]
    StoreNotInitialized,

    /// Failed to create key in cache.
    #[error("Failed to create key: {0}")]
    CreateFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Failed to update key in cache.
    #[error("Failed to update key: {0}")]
    UpdateFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Failed to get key from cache.
    #[error("Failed to get key: {0}")]
    GetFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Failed to delete key from cache.
    #[error("Failed to delete key: {0}")]
    DeleteFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Failed to put key in cache.
    #[error("Failed to put key: {0}")]
    PutFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Failed to list keys in cache.
    #[error("Failed to list keys: {0}")]
    ListKeysFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Watch stream error.
    #[error("Watch stream error: {0}")]
    WatchFailed(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// Watch is not supported by this cache backend.
    #[error("Watch is not supported by this cache backend")]
    WatchNotSupported,
}

/// A single event emitted by a cache watch subscription.
#[derive(Debug, Clone)]
pub enum WatchEvent {
    /// A key was created or updated with the given value.
    Put { key: String, value: bytes::Bytes },
    /// A key was deleted.
    Delete { key: String },
}

/// Type alias for cache errors.
pub type Error = CacheError;

/// Configuration options for cache operations.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct CacheOptions {
    /// Optional key override for cache insertion operations.
    pub insert_key: Option<String>,
    /// Optional key override for cache retrieval operations.
    pub retrieve_key: Option<String>,
}

/// Trait for asynchronous key-value cache implementations.
///
/// Provides a unified interface for different caching backends like NATS JetStream,
/// Redis, or other persistent storage systems used for workflow state management.
#[async_trait]
pub trait Cache: Debug + Send + Sync + 'static {
    /// Stores a value in the cache with the given key.
    ///
    /// # Arguments
    /// * `key` - The key to store the value under
    /// * `value` - The binary data to store
    /// * `ttl_secs` - Optional time-to-live in seconds. If None, value persists indefinitely.
    async fn put(&self, key: &str, value: bytes::Bytes, ttl_secs: Option<u64>)
        -> Result<(), Error>;

    /// Retrieves a value from the cache by key.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve the value for
    ///
    /// # Returns
    /// * `Ok(Some(data))` - Key exists with data
    /// * `Ok(None)` - Key not found
    /// * `Err(e)` - Operation failed (connection error, auth error, etc.)
    async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>, Error>;

    /// Deletes a value from the cache by key.
    ///
    /// # Arguments
    /// * `key` - The key to delete
    ///
    /// # Returns
    /// Ok if the key was deleted successfully, or an error if the operation failed
    async fn delete(&self, key: &str) -> Result<(), Error>;

    /// Creates a key-value pair only if the key does not already exist (atomic).
    ///
    /// # Arguments
    /// * `key` - The key to create
    /// * `value` - The value to store
    /// * `ttl_secs` - Optional time-to-live in seconds
    ///
    /// # Returns
    /// * `Ok(revision)` - Key created successfully, returns revision number
    /// * `Err(CacheError::AlreadyExists)` - Key already exists
    /// * `Err(e)` - Operation failed
    ///
    /// # Use Case
    /// Lease acquisition - only one worker can create the lease key.
    async fn create(
        &self,
        key: &str,
        value: bytes::Bytes,
        ttl_secs: Option<u64>,
    ) -> Result<u64, Error>;

    /// Updates a key's value only if the current revision matches (atomic compare-and-swap).
    ///
    /// # Arguments
    /// * `key` - The key to update
    /// * `value` - The new value
    /// * `expected_revision` - The revision that must match for update to succeed
    /// * `ttl_secs` - Optional time-to-live in seconds
    ///
    /// # Returns
    /// * `Ok(new_revision)` - Update successful, returns new revision
    /// * `Err(CacheError::RevisionMismatch)` - Revision doesn't match (another process modified it)
    /// * `Err(CacheError::NotFound)` - Key doesn't exist
    /// * `Err(e)` - Operation failed
    ///
    /// # Use Case
    /// Lease renewal - ensures this worker still owns the lease before renewing.
    /// Prevents split-brain where two workers think they own the same lease.
    async fn update(
        &self,
        key: &str,
        value: bytes::Bytes,
        expected_revision: u64,
        ttl_secs: Option<u64>,
    ) -> Result<u64, Error>;

    /// Retrieves a value along with its revision number for optimistic concurrency.
    ///
    /// # Returns
    /// * `Ok(Some((value, revision)))` - Key exists with data and revision
    /// * `Ok(None)` - Key not found
    /// * `Err(e)` - Operation failed
    ///
    /// # Use Case
    /// Lease takeover - get current lease holder and revision to check if expired.
    async fn get_with_revision(&self, key: &str) -> Result<Option<(bytes::Bytes, u64)>, Error>;

    /// Deletes a key only if the current revision matches (atomic compare-and-swap delete).
    ///
    /// # Arguments
    /// * `key` - The key to delete
    /// * `expected_revision` - The revision that must match for deletion to succeed
    ///
    /// # Returns
    /// * `Ok(())` - Delete successful
    /// * `Err(CacheError::RevisionMismatch)` - Revision doesn't match (another process modified it)
    /// * `Err(CacheError::NotFound)` - Key doesn't exist
    /// * `Err(e)` - Operation failed
    ///
    /// # Use Case
    /// Lease release on shutdown - ensures we only delete if we still own the lease.
    async fn delete_with_revision(&self, key: &str, expected_revision: u64) -> Result<(), Error>;

    /// Gets the current revision number, even for deleted keys (tombstones).
    ///
    /// # Returns
    /// * `Ok(Some(revision))` - Key or tombstone exists with revision
    /// * `Ok(None)` - Key never existed
    /// * `Err(e)` - Operation failed
    ///
    /// # Use Case
    /// Overwriting DELETE tombstones during lease acquisition.
    async fn get_revision(&self, key: &str) -> Result<Option<u64>, Error>;

    /// Lists all keys matching the given prefix.
    ///
    /// # Arguments
    /// * `prefix` - The prefix to filter keys by.
    ///
    /// # Returns
    /// A vector of key names that start with the given prefix.
    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, Error>;

    /// Subscribes to key changes under the given prefix, returning a stream of watch events.
    ///
    /// Events are emitted whenever a key matching the prefix is created, updated, or deleted.
    /// The stream ends when the connection is lost; callers should reconnect as needed.
    /// Returns `Err(CacheError::WatchNotSupported)` for backends that do not implement watching.
    async fn watch(
        &self,
        _prefix: &str,
    ) -> Result<BoxStream<'static, Result<WatchEvent, Error>>, Error> {
        Err(Error::WatchNotSupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cache_options_default() {
        let options = CacheOptions {
            insert_key: None,
            retrieve_key: None,
        };

        assert!(options.insert_key.is_none());
        assert!(options.retrieve_key.is_none());
    }

    #[test]
    fn test_cache_options_serialization() {
        let options = CacheOptions {
            insert_key: Some("insert".to_string()),
            retrieve_key: Some("retrieve".to_string()),
        };

        let serialized = serde_json::to_string(&options).unwrap();
        let deserialized: CacheOptions = serde_json::from_str(&serialized).unwrap();

        assert_eq!(options, deserialized);
    }
}
