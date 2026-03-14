//! Caching abstractions for persistent storage across workflow executions.
//!
//! Provides traits and configuration for key-value caching systems used by
//! tasks that need to maintain state between runs, such as replay identifiers.

pub mod memory;

use async_trait::async_trait;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Mock cache implementation for testing.
    #[derive(Debug)]
    struct MockCache {
        data: HashMap<String, bytes::Bytes>,
        should_error: bool,
    }

    #[derive(Debug)]
    struct MockError;

    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Mock cache error")
        }
    }

    impl std::error::Error for MockError {}

    #[async_trait]
    impl Cache for MockCache {
        async fn put(
            &self,
            _key: &str,
            _value: bytes::Bytes,
            _ttl_secs: Option<u64>,
        ) -> Result<(), Error> {
            if self.should_error {
                Err(CacheError::PutFailed(Box::new(MockError)))
            } else {
                Ok(())
            }
        }

        async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>, Error> {
            if self.should_error {
                Err(CacheError::GetFailed(Box::new(MockError)))
            } else {
                Ok(self.data.get(key).cloned())
            }
        }

        async fn delete(&self, _key: &str) -> Result<(), Error> {
            if self.should_error {
                Err(CacheError::DeleteFailed(Box::new(MockError)))
            } else {
                Ok(())
            }
        }

        async fn create(
            &self,
            _key: &str,
            _value: bytes::Bytes,
            _ttl_secs: Option<u64>,
        ) -> Result<u64, Error> {
            if self.should_error {
                Err(CacheError::CreateFailed(Box::new(MockError)))
            } else {
                Ok(1)
            }
        }

        async fn update(
            &self,
            _key: &str,
            _value: bytes::Bytes,
            _expected_revision: u64,
            _ttl_secs: Option<u64>,
        ) -> Result<u64, Error> {
            if self.should_error {
                Err(CacheError::UpdateFailed(Box::new(MockError)))
            } else {
                Ok(2)
            }
        }

        async fn get_with_revision(
            &self,
            key: &str,
        ) -> Result<Option<(bytes::Bytes, u64)>, Error> {
            if self.should_error {
                Err(CacheError::GetFailed(Box::new(MockError)))
            } else {
                Ok(self.data.get(key).cloned().map(|v| (v, 1)))
            }
        }
    }

    #[tokio::test]
    async fn test_cache_put_success() {
        let cache = MockCache {
            data: HashMap::new(),
            should_error: false,
        };

        let result = cache
            .put("test_key", bytes::Bytes::from("test_value"), None)
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_cache_get_success() {
        let mut data = HashMap::new();
        data.insert(
            "existing_key".to_string(),
            bytes::Bytes::from("existing_value"),
        );

        let cache = MockCache {
            data,
            should_error: false,
        };

        let result = cache.get("existing_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Some(bytes::Bytes::from("existing_value")));
    }

    #[tokio::test]
    async fn test_cache_get_not_found() {
        let cache = MockCache {
            data: HashMap::new(),
            should_error: false,
        };

        let result = cache.get("nonexistent_key").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), None);
    }

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
