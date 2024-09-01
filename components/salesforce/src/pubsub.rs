use crate::eventbus::v1::pub_sub_client::PubSubClient;
use crate::{auth, eventbus};
/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use oauth2::TokenResponse;
use tokio_stream::StreamExt;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Client is missing")]
    ClientMissing(),
    #[error("TokenResponse is missing")]
    TokenResponseMissing(),
    #[error("Invalid metadata value")]
    InvalidMetadataValue(#[source] tonic::metadata::errors::InvalidMetadataValue),
    #[error("There was an error with gRPC call")]
    GrpcStatus(#[source] tonic::Status),
}

struct ContextInterceptor {
    auth_header: tonic::metadata::AsciiMetadataValue,
    instance_url: tonic::metadata::AsciiMetadataValue,
    tenant_id: tonic::metadata::AsciiMetadataValue,
}

impl tonic::service::Interceptor for ContextInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> Result<tonic::Request<()>, tonic::Status> {
        request
            .metadata_mut()
            .insert("accesstoken", self.auth_header.to_owned());
        request
            .metadata_mut()
            .insert("instanceurl", self.instance_url.to_owned());
        request
            .metadata_mut()
            .insert("tenantid", self.tenant_id.to_owned());
        Ok(request)
    }
}

#[derive(Debug)]
pub struct Context {
    pubsub: eventbus::v1::pub_sub_client::PubSubClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ContextInterceptor,
        >,
    >,
}
fn fetch_requests_iter(
    request: eventbus::v1::FetchRequest,
) -> impl tokio_stream::Stream<Item = eventbus::v1::FetchRequest> {
    tokio_stream::iter(1..usize::MAX).map(move |_| request.to_owned())
}

impl Context {
    #[allow(clippy::new_ret_no_self)]
    pub async fn get_topic(
        &mut self,
        request: eventbus::v1::TopicRequest,
    ) -> Result<eventbus::v1::TopicInfo, Error> {
        self.pubsub
            .get_topic(tonic::Request::new(request))
            .await
            .map_err(Error::GrpcStatus)
            .map(|response| response.into_inner())
    }
    pub async fn get_schema(
        &mut self,
        request: eventbus::v1::SchemaRequest,
    ) -> Result<eventbus::v1::SchemaInfo, Error> {
        self.pubsub
            .get_schema(tonic::Request::new(request))
            .await
            .map_err(Error::GrpcStatus)
            .map(|response| response.into_inner())
    }

    pub async fn subscribe(
        &mut self,
        request: eventbus::v1::FetchRequest,
    ) -> Result<tonic::codec::Streaming<eventbus::v1::FetchResponse>, Error> {
        self.pubsub
            .subscribe(fetch_requests_iter(request).throttle(std::time::Duration::from_millis(100)))
            .await
            .map_err(Error::GrpcStatus)
            .map(|response| response.into_inner())
    }
}
#[derive(Debug)]
/// Used to store configure PubSub Context.
pub struct ContextBuilder {
    client: Option<auth::Client>,
    service: flowgen::core::Service,
}

impl ContextBuilder {
    pub fn new(service: flowgen::core::Service) -> Self {
        ContextBuilder {
            client: None,
            service,
        }
    }
    /// Pass the Salesforce OAuth client.
    pub fn with_client(&mut self, client: auth::Client) -> &mut Self {
        self.client = Some(client);
        self
    }

    /// Generates a new PubSub Context that allow for interacting with Salesforce PubSub API..
    pub fn build(&mut self) -> Result<Context, Error> {
        let client = self.client.as_ref().ok_or_else(Error::ClientMissing)?;

        let auth_header: tonic::metadata::AsciiMetadataValue = client
            .token_result
            .as_ref()
            .ok_or_else(Error::TokenResponseMissing)?
            .access_token()
            .secret()
            .parse()
            .map_err(Error::InvalidMetadataValue)?;

        let instance_url: tonic::metadata::AsciiMetadataValue = client
            .instance_url
            .parse()
            .map_err(Error::InvalidMetadataValue)?;

        let tenant_id: tonic::metadata::AsciiMetadataValue = client
            .tenant_id
            .parse()
            .map_err(Error::InvalidMetadataValue)?;

        let interceptor = ContextInterceptor {
            auth_header,
            instance_url,
            tenant_id,
        };

        let pubsub =
            PubSubClient::with_interceptor(self.service.channel.clone().unwrap(), interceptor);

        Ok(Context { pubsub })
    }
}
