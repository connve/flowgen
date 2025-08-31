//! Service discovery and connection management for gRPC services.
//!
//! Provides utilities for establishing secure TLS connections to external gRPC
//! services with proper error handling and connection lifecycle management.

/// Errors that can occur during service connection operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to construct a valid URI from the provided endpoint.
    #[error("error resulting from a failed attempt to construct a URI")]
    InvalidUri(#[source] tonic::codegen::http::uri::InvalidUri),
    /// Transport layer error during connection establishment.
    #[error("errror that originate from the client or server")]
    TransportError(#[source] tonic::transport::Error),
    /// Service endpoint was not configured before attempting connection.
    #[error("error that originate from the client or server")]
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
