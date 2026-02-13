//! Caching abstractions for persistent storage across workflow executions.
//!
//! Provides traits and configuration for key-value caching systems used by
//! tasks that need to maintain state between runs, such as replay identifiers.

pub mod memory;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Type alias for cache errors.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

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
    async fn put(&self, key: &str, value: bytes::Bytes) -> Result<(), Error>;

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
        async fn put(&self, _key: &str, _value: bytes::Bytes) -> Result<(), Error> {
            if self.should_error {
                Err(Box::new(MockError))
            } else {
                Ok(())
            }
        }

        async fn get(&self, key: &str) -> Result<Option<bytes::Bytes>, Error> {
            if self.should_error {
                Err(Box::new(MockError))
            } else {
                Ok(self.data.get(key).cloned())
            }
        }

        async fn delete(&self, _key: &str) -> Result<(), Error> {
            if self.should_error {
                Err(Box::new(MockError))
            } else {
                Ok(())
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
            .put("test_key", bytes::Bytes::from("test_value"))
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
