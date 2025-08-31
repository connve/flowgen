//! Client connection trait for external services.
//!
//! Provides a unified interface for establishing connections to various external
//! services used by flowgen workers such as databases, message brokers, and APIs.

/// Trait for establishing connections to external services.
///
/// Defines a common interface for client connection logic that can be implemented
/// by different service clients like HTTP clients, database connections, or message brokers.
pub trait Client {
    /// Error type for connection operations.
    type Error;
    
    /// Establishes a connection to the external service.
    ///
    /// # Returns
    /// The connected client instance or an error if connection fails
    fn connect(self) -> impl std::future::Future<Output = Result<Self, Self::Error>> + Send
    where
        Self: Sized;
}
