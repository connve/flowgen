//! Host coordination and lease management.
//!
//! Defines the abstraction for distributed coordination operations
//! such as lease management across different hosting platforms.

use async_trait::async_trait;

pub mod k8s;

/// Type alias for host errors.
pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// Trait for host coordination operations.
#[async_trait]
pub trait Host: Send + Sync + std::fmt::Debug {
    /// Creates a new lease with the given name.
    async fn create_lease(&self, name: &str) -> Result<(), Error>;

    /// Deletes an existing lease.
    async fn delete_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error>;

    /// Renews an existing lease.
    async fn renew_lease(&self, name: &str, namespace: Option<&str>) -> Result<(), Error>;

    /// Verifies that this instance holds the lease by checking cache.
    /// Returns true if we hold the lease, false otherwise.
    async fn verify_lease_ownership(&self, name: &str) -> Result<bool, Error>;
}
