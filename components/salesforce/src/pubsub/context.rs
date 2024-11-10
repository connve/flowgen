//! This Source Code Form is subject to the terms of the Mozilla Public
//! License, v. 2.0. If a copy of the MPL was not distributed with this
//! file, You can obtain one at https://mozilla.org/MPL/2.0/.
use super::eventbus::v1::pub_sub_client::PubSubClient;
use crate::client;
use oauth2::TokenResponse;
use tokio_stream::StreamExt;

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
    pubsub: super::eventbus::v1::pub_sub_client::PubSubClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ContextInterceptor,
        >,
    >,
}

impl Context {
    pub async fn get_topic(
        &mut self,
        request: super::eventbus::v1::TopicRequest,
    ) -> Result<tonic::Response<super::eventbus::v1::TopicInfo>, Error> {
        self.pubsub
            .get_topic(tonic::Request::new(request))
            .await
            .map_err(Error::RPCFailed)
    }
    pub async fn get_schema(
        &mut self,
        request: super::eventbus::v1::SchemaRequest,
    ) -> Result<tonic::Response<super::eventbus::v1::SchemaInfo>, Error> {
        self.pubsub
            .get_schema(tonic::Request::new(request))
            .await
            .map_err(Error::RPCFailed)
    }

    pub async fn publish(
        &mut self,
        request: super::eventbus::v1::PublishRequest,
    ) -> Result<tonic::Response<super::eventbus::v1::PublishResponse>, Error> {
        self.pubsub
            .publish(tonic::Request::new(request))
            .await
            .map_err(Error::RPCFailed)
    }

    pub async fn subscribe(
        &mut self,
        request: super::eventbus::v1::FetchRequest,
    ) -> Result<tonic::Response<tonic::codec::Streaming<super::eventbus::v1::FetchResponse>>, Error>
    {
        self.pubsub
            .subscribe(
                tokio_stream::iter(1..usize::MAX)
                    .map(move |_| request.to_owned())
                    .throttle(std::time::Duration::from_millis(10)),
            )
            .await
            .map_err(Error::RPCFailed)
    }

    pub async fn managed_subscribe(
        &mut self,
        request: super::eventbus::v1::ManagedFetchRequest,
    ) -> Result<
        tonic::Response<tonic::codec::Streaming<super::eventbus::v1::ManagedFetchResponse>>,
        Error,
    > {
        self.pubsub
            .managed_subscribe(
                tokio_stream::iter(1..usize::MAX)
                    .map(move |_| request.to_owned())
                    .throttle(std::time::Duration::from_millis(10)),
            )
            .await
            .map_err(Error::RPCFailed)
    }

    pub async fn publish_stream(
        &mut self,
        request: super::eventbus::v1::PublishRequest,
    ) -> Result<tonic::Response<tonic::codec::Streaming<super::eventbus::v1::PublishResponse>>, Error>
    {
        self.pubsub
            .publish_stream(
                tokio_stream::iter(1..usize::MAX)
                    .map(move |_| request.to_owned())
                    .throttle(std::time::Duration::from_millis(10)),
            )
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

        let pubsub = PubSubClient::with_interceptor(
            self.service
                .channel
                .to_owned()
                .ok_or_else(Error::ServiceChannelMissing)?,
            interceptor,
        );

        Ok(Context { pubsub })
    }
}
#[cfg(test)]
mod tests {

    use std::{fs, path::PathBuf};

    use super::*;

    #[test]
    fn test_build_missing_client() {
        let service = flowgen_core::service::Builder::new().build().unwrap();
        let client = Builder::new(service).build();
        assert!(matches!(client, Err(Error::ClientMissing(..))));
    }
    #[test]
    fn test_build_missing_token() {
        let service = flowgen_core::service::Builder::new().build().unwrap();
        let creds: &str = r#"
            {
                "client_id": "some_client_id",
                "client_secret": "some_client_secret", 
                "instance_url": "https://mydomain.salesforce.com", 
                "tenant_id": "some_tenant_id"
            }"#;
        let mut path = PathBuf::new();
        path.push("credentials.json");
        let _ = fs::write(path.clone(), creds);
        let client = client::Builder::new()
            .with_credentials_path(path.clone())
            .build()
            .unwrap();
        let _ = fs::remove_file(path);
        let pubsub = Builder::new(service).with_client(client).build();
        assert!(matches!(pubsub, Err(Error::TokenResponseMissing(..))));
    }
}
