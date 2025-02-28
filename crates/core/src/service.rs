#[derive(thiserror::Error, Debug)]
pub enum ServiceError {
    #[error("An error resulting from a failed attempt to construct a URI")]
    InvalidUri(#[source] tonic::codegen::http::uri::InvalidUri),
    #[error("Error that originate from the client or server")]
    TransportError(#[source] tonic::transport::Error),
    #[error("Error that originate from the client or server")]
    MissingEndpoint(),
}

#[derive(Debug, Default, Clone)]
pub struct Service {
    endpoint: Option<String>,
    pub channel: Option<tonic::transport::Channel>,
}

impl super::client::Client for Service {
    type Error = ServiceError;
    async fn connect(mut self) -> Result<Self, Self::Error> {
        if let Some(endpoint) = self.endpoint.take() {
            let tls_config = tonic::transport::ClientTlsConfig::new();
            let channel = tonic::transport::Channel::from_shared(endpoint)
                .map_err(ServiceError::InvalidUri)?
                .tls_config(tls_config)
                .map_err(ServiceError::TransportError)?
                .connect()
                .await
                .map_err(ServiceError::TransportError)?;
            self.channel = Some(channel);
            Ok(self)
        } else {
            Err(ServiceError::MissingEndpoint())
        }
    }
}

#[derive(Default)]
pub struct Builder {
    endpoint: Option<String>,
}

impl Builder {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> Self {
        Builder {
            ..Default::default()
        }
    }

    pub fn with_endpoint(&mut self, endpoint: String) -> &mut Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn build(&mut self) -> Result<Service, ServiceError> {
        Ok(Service {
            endpoint: self.endpoint.take(),
            channel: None,
        })
    }
}
