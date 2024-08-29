/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use oauth2::TokenResponse;
use tonic::metadata::errors::InvalidMetadataValue;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;

use crate::eventbus::v1::pub_sub_client::PubSubClient;
use crate::eventbus::v1::{SchemaInfo, SchemaRequest, TopicInfo, TopicRequest};
use crate::{auth, eventbus};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Client is missing")]
    ClientMissing(),
    #[error("TokenResponse is missing")]
    TokenResponseMissing(),
    #[error("Invalid metadata value")]
    InvalidMetadataValue(#[source] InvalidMetadataValue),
}

struct ContextInterceptor {
    auth_header: AsciiMetadataValue,
    instance_url: AsciiMetadataValue,
    tenant_id: AsciiMetadataValue,
}

impl Interceptor for ContextInterceptor {
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
    pubsub_client: eventbus::v1::pub_sub_client::PubSubClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ContextInterceptor,
        >,
    >,
}

impl Context {
    #[allow(clippy::new_ret_no_self)]
    pub async fn get_topic(
        &mut self,
        topic_request: TopicRequest,
    ) -> Result<TopicInfo, Box<dyn std::error::Error>> {
        self.pubsub_client
            .get_topic(tonic::Request::new(topic_request))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
            .map(|response| response.into_inner())
    }
    pub async fn get_schema(
        &mut self,
        schema_request: SchemaRequest,
    ) -> Result<SchemaInfo, Box<dyn std::error::Error>> {
        self.pubsub_client
            .get_schema(tonic::Request::new(schema_request))
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
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

        let auth_header: AsciiMetadataValue = client
            .token_result
            .as_ref()
            .ok_or_else(Error::TokenResponseMissing)?
            .access_token()
            .secret()
            .parse()
            .map_err(Error::InvalidMetadataValue)?;

        let instance_url: AsciiMetadataValue = client
            .instance_url
            .parse()
            .map_err(Error::InvalidMetadataValue)?;

        let tenant_id: AsciiMetadataValue = client
            .tenant_id
            .parse()
            .map_err(Error::InvalidMetadataValue)?;

        let interceptor = ContextInterceptor {
            auth_header,
            instance_url,
            tenant_id,
        };

        let pubsub_client =
            PubSubClient::with_interceptor(self.service.channel.clone().unwrap(), interceptor);

        Ok(Context { pubsub_client })
    }
}
