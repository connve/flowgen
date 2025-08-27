#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error resulting from a failed attempt to construct a URI")]
    InvalidUri(#[source] tonic::codegen::http::uri::InvalidUri),
    #[error("errror that originate from the client or server")]
    TransportError(#[source] tonic::transport::Error),
    #[error("error that originate from the client or server")]
    MissingEndpoint(),
}

#[derive(Debug, Default, Clone)]
pub struct Service {
    endpoint: Option<String>,
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

#[derive(Default)]
pub struct ServiceBuilder {
    endpoint: Option<String>,
}

impl ServiceBuilder {
    pub fn new() -> Self {
        ServiceBuilder {
            ..Default::default()
        }
    }

    pub fn endpoint(&mut self, endpoint: String) -> &mut Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn build(&mut self) -> Result<Service, Error> {
        Ok(Service {
            endpoint: self.endpoint.take(),
            channel: None,
        })
    }
}
