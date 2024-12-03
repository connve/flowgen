use super::super::storage::v2::storage_client::StorageClient;
use crate::client;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Client is missing")]
    ClientMissing(),
    #[error("TokenResponse is missing")]
    TokenResponseMissing(),
    #[error("Service channel is missing")]
    ServiceChannelMissing(),
    #[error("Invalid metadata value")]
    InvalidMetadataValue(#[source] tonic::metadata::errors::InvalidMetadataValue),
    #[error("There was an error with RPC call")]
    RPCFailed(#[source] tonic::Status),
}

struct ContextInterceptor {
    auth_header: tonic::metadata::AsciiMetadataValue,
}

impl tonic::service::Interceptor for ContextInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("authorization", self.auth_header.to_owned());
        Ok(request)
    }
}

#[derive(Debug)]
pub struct Context {
    client: super::super::storage::v2::storage_client::StorageClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ContextInterceptor,
        >,
    >,
}

impl Context {
    pub async fn read_object(
        &mut self,
        request: super::super::storage::v2::ReadObjectRequest,
    ) -> Result<
        tonic::Response<tonic::codec::Streaming<super::super::storage::v2::ReadObjectResponse>>,
        Error,
    > {
        self.client
            .read_object(tonic::Request::new(request))
            .await
            .map_err(Error::RPCFailed)
    }
}

/// Used to store configure PubSub Context.
pub struct Builder {
    client: Option<client::Client>,
    service: flowgen_core::service::Service,
}

impl Builder {
    // Creates a new instance of ContectBuilder.
    pub fn new(service: flowgen_core::service::Service) -> Self {
        Builder {
            client: None,
            service,
        }
    }
    /// Pass the Salesforce OAuth client.
    pub fn with_client(&mut self, client: client::Client) -> &mut Builder {
        self.client = Some(client);
        self
    }

    /// Generates a new PubSub Context that allow for interacting with Salesforce PubSub API.
    pub fn build(&self) -> Result<Context, Error> {
        let client = self.client.as_ref().ok_or_else(Error::ClientMissing)?;

        let auth_header: tonic::metadata::AsciiMetadataValue = client
            .token_result
            .as_ref()
            .unwrap()
            .value
            .parse()
            .map_err(Error::InvalidMetadataValue)?;

        let interceptor = ContextInterceptor { auth_header };

        let client = StorageClient::with_interceptor(
            self.service
                .channel
                .to_owned()
                .ok_or_else(Error::ServiceChannelMissing)?,
            interceptor,
        );

        Ok(Context { client })
    }
}
