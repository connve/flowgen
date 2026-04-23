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

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock client implementation for testing.
    #[derive(Debug)]
    struct MockClient {
        should_error: bool,
        connected: bool,
    }

    #[derive(Debug)]
    struct MockError;

    impl std::fmt::Display for MockError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "Mock client connection error")
        }
    }

    impl std::error::Error for MockError {}

    impl Client for MockClient {
        type Error = MockError;

        async fn connect(mut self) -> Result<Self, Self::Error> {
            if self.should_error {
                Err(MockError)
            } else {
                self.connected = true;
                Ok(self)
            }
        }
    }

    #[tokio::test]
    async fn test_client_connect_success() {
        let client = MockClient {
            should_error: false,
            connected: false,
        };

        let result = client.connect().await;
        assert!(result.is_ok());
        assert!(result.unwrap().connected);
    }

    #[tokio::test]
    async fn test_client_connect_error() {
        let client = MockClient {
            should_error: true,
            connected: false,
        };

        let result = client.connect().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_mock_client_creation() {
        let client = MockClient {
            should_error: false,
            connected: false,
        };

        assert!(!client.should_error);
        assert!(!client.connected);
    }
}
