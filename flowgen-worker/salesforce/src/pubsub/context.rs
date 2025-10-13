use crate::client;
use oauth2::TokenResponse;
use salesforce_pubsub::eventbus::v1::pub_sub_client::PubSubClient;
use tokio_stream::StreamExt;

/// Errors that can occur during Salesforce Pub/Sub context operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Client was not properly initialized or is missing.
    #[error("Client missing")]
    MissingClient(),
    /// OAuth2 token response is missing or unavailable.
    #[error("Token response missing")]
    MissingTokenResponse(),
    /// gRPC service channel is missing or unavailable.
    #[error("Service channel missing")]
    MissingServiceChannel(),
    /// Required attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// Invalid metadata value for gRPC headers.
    #[error(transparent)]
    InvalidMetadataValue(#[from] tonic::metadata::errors::InvalidMetadataValue),
    /// gRPC transport or communication error.
    #[error(transparent)]
    Tonic(Box<tonic::Status>),
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

/// Salesforce Pub/Sub context that manages gRPC client connections with authentication.
#[derive(Debug)]
pub struct Context {
    pubsub: salesforce_pubsub::eventbus::v1::pub_sub_client::PubSubClient<
        tonic::service::interceptor::InterceptedService<
            tonic::transport::Channel,
            ContextInterceptor,
        >,
    >,
}

impl Context {
    /// Creates a new Pub/Sub context with the provided service and authenticated client.
    pub fn new(
        service: flowgen_core::service::Service,
        client: client::Client,
    ) -> Result<Self, Error> {
        let auth_header: tonic::metadata::AsciiMetadataValue = client
            .token_result
            .as_ref()
            .ok_or_else(Error::MissingTokenResponse)?
            .access_token()
            .secret()
            .parse()?;

        let instance_url: tonic::metadata::AsciiMetadataValue = client
            .instance_url
            .as_ref()
            .ok_or_else(|| Error::MissingRequiredAttribute("instance_url".to_string()))?
            .parse()?;

        let tenant_id: tonic::metadata::AsciiMetadataValue = client
            .tenant_id
            .as_ref()
            .ok_or_else(|| Error::MissingRequiredAttribute("tenant_id".to_string()))?
            .parse()?;

        let interceptor = ContextInterceptor {
            auth_header,
            instance_url,
            tenant_id,
        };

        let pubsub = PubSubClient::with_interceptor(
            service.channel.ok_or_else(Error::MissingServiceChannel)?,
            interceptor,
        );

        Ok(Context { pubsub })
    }
    pub async fn get_topic(
        &mut self,
        request: salesforce_pubsub::eventbus::v1::TopicRequest,
    ) -> Result<tonic::Response<salesforce_pubsub::eventbus::v1::TopicInfo>, Error> {
        self.pubsub
            .get_topic(tonic::Request::new(request))
            .await
            .map_err(|e| Error::Tonic(Box::new(e)))
    }
    pub async fn get_schema(
        &mut self,
        request: salesforce_pubsub::eventbus::v1::SchemaRequest,
    ) -> Result<tonic::Response<salesforce_pubsub::eventbus::v1::SchemaInfo>, Error> {
        self.pubsub
            .get_schema(tonic::Request::new(request))
            .await
            .map_err(|e| Error::Tonic(Box::new(e)))
    }

    pub async fn publish(
        &mut self,
        request: salesforce_pubsub::eventbus::v1::PublishRequest,
    ) -> Result<tonic::Response<salesforce_pubsub::eventbus::v1::PublishResponse>, Error> {
        self.pubsub
            .publish(tonic::Request::new(request))
            .await
            .map_err(|e| Error::Tonic(Box::new(e)))
    }

    pub async fn subscribe(
        &mut self,
        request: salesforce_pubsub::eventbus::v1::FetchRequest,
    ) -> Result<
        tonic::Response<tonic::codec::Streaming<salesforce_pubsub::eventbus::v1::FetchResponse>>,
        Error,
    > {
        self.pubsub
            .subscribe(
                tokio_stream::iter(1..usize::MAX)
                    .map(move |_| request.to_owned())
                    .throttle(std::time::Duration::from_millis(10)),
            )
            .await
            .map_err(|e| Error::Tonic(Box::new(e)))
    }

    pub async fn managed_subscribe(
        &mut self,
        request: salesforce_pubsub::eventbus::v1::ManagedFetchRequest,
    ) -> Result<
        tonic::Response<
            tonic::codec::Streaming<salesforce_pubsub::eventbus::v1::ManagedFetchResponse>,
        >,
        Error,
    > {
        self.pubsub
            .managed_subscribe(
                tokio_stream::iter(1..usize::MAX)
                    .map(move |_| request.to_owned())
                    .throttle(std::time::Duration::from_millis(10)),
            )
            .await
            .map_err(|e| Error::Tonic(Box::new(e)))
    }

    pub async fn publish_stream(
        &mut self,
        request: salesforce_pubsub::eventbus::v1::PublishRequest,
    ) -> Result<
        tonic::Response<tonic::codec::Streaming<salesforce_pubsub::eventbus::v1::PublishResponse>>,
        Error,
    > {
        self.pubsub
            .publish_stream(
                tokio_stream::iter(1..usize::MAX)
                    .map(move |_| request.to_owned())
                    .throttle(std::time::Duration::from_millis(10)),
            )
            .await
            .map_err(|e| Error::Tonic(Box::new(e)))
    }
}

#[cfg(test)]
mod tests {

    use std::{fs, path::PathBuf};

    use super::*;

    #[test]
    fn test_new_missing_token() {
        let service = flowgen_core::service::ServiceBuilder::new()
            .build()
            .unwrap();
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
            .credentials_path(path.clone())
            .build()
            .unwrap();
        let _ = fs::remove_file(path);
        let result = Context::new(service, client);
        assert!(matches!(result, Err(Error::MissingTokenResponse(..))));
    }
}
