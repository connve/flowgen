//! Service discovery and connection management for gRPC services.
//!
//! Provides utilities for establishing secure TLS connections to external gRPC
//! services with proper error handling and connection lifecycle management.

use std::time::Duration;

/// Default interval for HTTP/2 keep-alive pings (20 seconds).
pub const DEFAULT_KEEP_ALIVE_INTERVAL_SECS: u64 = 20;

/// Default timeout for keep-alive ping responses (10 seconds).
pub const DEFAULT_KEEP_ALIVE_TIMEOUT_SECS: u64 = 10;

/// Default connection timeout (30 seconds).
pub const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 30;

/// Errors that can occur during service connection operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to construct URI from endpoint: {source}")]
    InvalidUri {
        #[source]
        source: tonic::codegen::http::uri::InvalidUri,
    },
    #[error("Transport error during connection: {source}")]
    TransportError {
        #[source]
        source: tonic::transport::Error,
    },
    #[error("Service endpoint is missing")]
    MissingEndpoint(),
}

/// gRPC service connection manager with TLS support.
#[derive(Debug, Clone)]
pub struct Service {
    /// Service endpoint URL for connection.
    endpoint: Option<String>,
    /// Established gRPC channel for service communication.
    pub channel: Option<tonic::transport::Channel>,
    /// HTTP/2 keep-alive interval in seconds.
    keep_alive_interval_secs: u64,
    /// Keep-alive timeout in seconds.
    keep_alive_timeout_secs: u64,
    /// Connection timeout in seconds.
    connect_timeout_secs: u64,
    /// Enable keep-alive while connection is idle.
    keep_alive_while_idle: bool,
}

impl Default for Service {
    fn default() -> Self {
        Self {
            endpoint: None,
            channel: None,
            keep_alive_interval_secs: DEFAULT_KEEP_ALIVE_INTERVAL_SECS,
            keep_alive_timeout_secs: DEFAULT_KEEP_ALIVE_TIMEOUT_SECS,
            connect_timeout_secs: DEFAULT_CONNECT_TIMEOUT_SECS,
            keep_alive_while_idle: true,
        }
    }
}

impl super::client::Client for Service {
    type Error = Error;
    async fn connect(mut self) -> Result<Self, Self::Error> {
        if let Some(endpoint) = self.endpoint.take() {
            let tls_config = tonic::transport::ClientTlsConfig::new().with_native_roots();
            let channel = tonic::transport::Channel::from_shared(endpoint)
                .map_err(|e| Error::InvalidUri { source: e })?
                .tls_config(tls_config)
                .map_err(|e| Error::TransportError { source: e })?
                .http2_keep_alive_interval(Duration::from_secs(self.keep_alive_interval_secs))
                .keep_alive_timeout(Duration::from_secs(self.keep_alive_timeout_secs))
                .keep_alive_while_idle(self.keep_alive_while_idle)
                .connect_timeout(Duration::from_secs(self.connect_timeout_secs))
                .connect()
                .await
                .map_err(|e| Error::TransportError { source: e })?;
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
    /// HTTP/2 keep-alive interval in seconds.
    keep_alive_interval_secs: Option<u64>,
    /// Keep-alive timeout in seconds.
    keep_alive_timeout_secs: Option<u64>,
    /// Connection timeout in seconds.
    connect_timeout_secs: Option<u64>,
    /// Enable keep-alive while connection is idle.
    keep_alive_while_idle: Option<bool>,
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

    /// Sets the HTTP/2 keep-alive interval.
    ///
    /// # Arguments
    /// * `secs` - Interval in seconds between keep-alive pings
    pub fn keep_alive_interval(&mut self, secs: u64) -> &mut Self {
        self.keep_alive_interval_secs = Some(secs);
        self
    }

    /// Sets the keep-alive timeout.
    ///
    /// # Arguments
    /// * `secs` - Timeout in seconds for keep-alive responses
    pub fn keep_alive_timeout(&mut self, secs: u64) -> &mut Self {
        self.keep_alive_timeout_secs = Some(secs);
        self
    }

    /// Sets the connection timeout.
    ///
    /// # Arguments
    /// * `secs` - Timeout in seconds for establishing connection
    pub fn connect_timeout(&mut self, secs: u64) -> &mut Self {
        self.connect_timeout_secs = Some(secs);
        self
    }

    /// Sets whether to keep alive while connection is idle.
    ///
    /// # Arguments
    /// * `enabled` - Enable or disable keep-alive while idle
    pub fn keep_alive_while_idle(&mut self, enabled: bool) -> &mut Self {
        self.keep_alive_while_idle = Some(enabled);
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
            keep_alive_interval_secs: self
                .keep_alive_interval_secs
                .take()
                .unwrap_or(DEFAULT_KEEP_ALIVE_INTERVAL_SECS),
            keep_alive_timeout_secs: self
                .keep_alive_timeout_secs
                .take()
                .unwrap_or(DEFAULT_KEEP_ALIVE_TIMEOUT_SECS),
            connect_timeout_secs: self
                .connect_timeout_secs
                .take()
                .unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS),
            keep_alive_while_idle: self.keep_alive_while_idle.take().unwrap_or(true),
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
        let service = Service::default();

        assert!(service.endpoint.is_none());
        assert!(service.channel.is_none());
        assert_eq!(
            service.keep_alive_interval_secs,
            DEFAULT_KEEP_ALIVE_INTERVAL_SECS
        );
        assert_eq!(
            service.keep_alive_timeout_secs,
            DEFAULT_KEEP_ALIVE_TIMEOUT_SECS
        );
        assert_eq!(service.connect_timeout_secs, DEFAULT_CONNECT_TIMEOUT_SECS);
        assert!(service.keep_alive_while_idle);
    }

    #[test]
    fn test_service_builder_with_custom_timeouts() {
        let mut builder = ServiceBuilder::new();
        let service = builder
            .endpoint("https://example.com".to_string())
            .keep_alive_interval(15)
            .keep_alive_timeout(5)
            .connect_timeout(60)
            .keep_alive_while_idle(false)
            .build()
            .unwrap();

        assert_eq!(service.endpoint, Some("https://example.com".to_string()));
        assert_eq!(service.keep_alive_interval_secs, 15);
        assert_eq!(service.keep_alive_timeout_secs, 5);
        assert_eq!(service.connect_timeout_secs, 60);
        assert!(!service.keep_alive_while_idle);
    }
}
