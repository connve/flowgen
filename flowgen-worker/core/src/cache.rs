//! Caching abstractions for persistent storage across workflow executions.
//!
//! Provides traits and configuration for key-value caching systems used by
//! tasks that need to maintain state between runs, such as replay identifiers.

use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Configuration options for cache operations.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
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
pub trait Cache: Debug + Send + Sync + 'static {
    /// Error type for cache operations.
    type Error: Debug + Send + Sync + 'static;
    
    /// Initializes the cache with a specific bucket or namespace.
    ///
    /// # Arguments
    /// * `bucket` - The bucket or namespace identifier for this cache instance
    fn init(
        self,
        bucket: &str,
    ) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;
    
    /// Stores a value in the cache with the given key.
    ///
    /// # Arguments
    /// * `key` - The key to store the value under
    /// * `value` - The binary data to store
    fn put(
        &self,
        key: &str,
        value: bytes::Bytes,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send
    where
        Self: Sized;
    
    /// Retrieves a value from the cache by key.
    ///
    /// # Arguments
    /// * `key` - The key to retrieve the value for
    ///
    /// # Returns
    /// The cached binary data or an error if the key is not found
    fn get(
        &self,
        key: &str,
    ) -> impl std::future::Future<Output = Result<bytes::Bytes, Self::Error>> + Send
    where
        Self: Sized;
}
