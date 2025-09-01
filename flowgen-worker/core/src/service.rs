//! Service discovery and connection management for gRPC services.
//!
//! Provides utilities for establishing secure TLS connections to external gRPC
//! services with proper error handling and connection lifecycle management.

/// Errors that can occur during service connection operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to construct a valid URI from the provided endpoint.
    #[error("Error resulting from a failed attempt to construct a URI")]
    InvalidUri(#[source] tonic::codegen::http::uri::InvalidUri),
    /// Transport layer error during connection establishment.
    #[error("Error that originates from the client or server")]
    TransportError(#[source] tonic::transport::Error),
    /// Service endpoint was not configured before attempting connection.
    #[error("Service endpoint was not configured")]
    MissingEndpoint(),
}

/// gRPC service connection manager with TLS support.
#[derive(Debug, Default, Clone)]
pub struct Service {
    /// Service endpoint URL for connection.
    endpoint: Option<String>,
    /// Established gRPC channel for service communication.
    pub channel: Option<tonic::transport::Channel>,
}

impl super::client::Client for Service {
    type Error = Error;
    async fn connect(mut self) -> Result<Self, Self::Error> {
        if let Some(endpoint) = self.endpoint.take() {
            let tls_config = tonic::transport::ClientTlsConfig::new();
            let channel = tonic::transport::Channel::from_shared(endpoint)
                .map_err(Error::InvalidUri)?
                .tls_config(tls_config)
                .map_err(Error::TransportError)?
                .connect()
                .await
                .map_err(Error::TransportError)?;
            self.channel = Some(channel);
            Ok(self)
        } else {
            Err(Error::MissingEndpoint())
        }
    }
}

/// Builder for constructing Service instances with endpoint configuration.
#[derive(Default)]
pub struct ServiceBuilder {
    /// Service endpoint URL to connect to.
    endpoint: Option<String>,
}

impl ServiceBuilder {
    /// Creates a new ServiceBuilder instance.
    pub fn new() -> Self {
        ServiceBuilder {
            ..Default::default()
        }
    }

    /// Sets the service endpoint URL.
    ///
    /// # Arguments
    /// * `endpoint` - The gRPC service endpoint URL
    pub fn endpoint(&mut self, endpoint: String) -> &mut Self {
        self.endpoint = Some(endpoint);
        self
    }

    /// Builds the Service instance with the configured endpoint.
    ///
    /// # Returns
    /// A Service ready for connection or an error if required fields are missing
    pub fn build(&mut self) -> Result<Service, Error> {
        Ok(Service {
            endpoint: self.endpoint.take(),
            channel: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_service_builder_new() {
        let builder = ServiceBuilder::new();
        assert!(builder.endpoint.is_none());
    }

    #[test]
    fn test_service_builder_endpoint() {
        let mut builder = ServiceBuilder::new();
        builder.endpoint("https://api.example.com".to_string());
        assert_eq!(
            builder.endpoint,
            Some("https://api.example.com".to_string())
        );
    }

    #[test]
    fn test_service_builder_build() {
        let mut builder = ServiceBuilder::new();
        builder.endpoint("https://test.com".to_string());

        let service = builder.build().unwrap();
        assert_eq!(service.endpoint, Some("https://test.com".to_string()));
        assert!(service.channel.is_none());
    }

    #[test]
    fn test_service_build_without_endpoint() {
        let mut builder = ServiceBuilder::new();
        let service = builder.build().unwrap();
        assert!(service.endpoint.is_none());
        assert!(service.channel.is_none());
    }

    #[test]
    fn test_service_default_values() {
        let service = Service {
            endpoint: None,
            channel: None,
        };

        assert!(service.endpoint.is_none());
        assert!(service.channel.is_none());
    }

    #[test]
    fn test_service_with_endpoint() {
        let service = Service {
            endpoint: Some("https://example.com".to_string()),
            channel: None,
        };

        assert_eq!(service.endpoint, Some("https://example.com".to_string()));
        assert!(service.channel.is_none());
    }

    #[test]
    fn test_error_display() {
        let missing_endpoint_error = Error::MissingEndpoint();
        assert!(missing_endpoint_error
            .to_string()
            .contains("error that originate from the client or server"));
    }
}
