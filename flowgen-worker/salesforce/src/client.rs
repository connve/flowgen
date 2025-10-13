use oauth2::basic::{BasicClient, BasicErrorResponseType, BasicTokenType};
use oauth2::reqwest::async_http_client;
use oauth2::{
    AuthUrl, ClientId, ClientSecret, EmptyExtraTokenFields, RevocationErrorResponseType,
    StandardErrorResponse, StandardRevocableToken, StandardTokenIntrospectionResponse,
    StandardTokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

/// Errors that can occur during Salesforce client operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Input/output operation failed.
    #[error("IO operation failed on path {path}: {source}")]
    IO {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to parse credentials JSON file.
    #[error(transparent)]
    ParseCredentials(#[from] serde_json::Error),
    /// Invalid URL format in credentials or configuration.
    #[error(transparent)]
    ParseUrl(#[from] url::ParseError),
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
    /// General error with descriptive message.
    #[error("Other error: {}", _0)]
    Other(String),
}
/// Used to store Salesforce Client credentials.
#[derive(Serialize, Deserialize)]
struct Credentials {
    /// Client ID from connected apps.
    client_id: String,
    /// Client Secret from connected apps.
    client_secret: String,
    /// Intance URL eg. httsp://mydomain.salesforce.com.
    instance_url: String,
    /// Tenant/org id from company info.
    tenant_id: String,
}

/// Salesforce OAuth2 client for API authentication and token management.
#[derive(Debug, Clone)]
#[allow(clippy::type_complexity)]
pub struct Client {
    /// Path to credentials file.
    credentials: PathBuf,
    /// Oauth2.0 client for getting the tokens.
    oauth2_client: Option<
        oauth2::Client<
            StandardErrorResponse<BasicErrorResponseType>,
            StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>,
            BasicTokenType,
            StandardTokenIntrospectionResponse<EmptyExtraTokenFields, BasicTokenType>,
            StandardRevocableToken,
            StandardErrorResponse<RevocationErrorResponseType>,
        >,
    >,

    /// Token result as a composite of access_token / refresh__token etc.
    pub token_result: Option<oauth2::StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>>,
    /// Intance URL eg. httsp://mydomain.salesforce.com.
    pub instance_url: Option<String>,
    /// Tenant/org id from company info.
    pub tenant_id: Option<String>,
}

impl flowgen_core::client::Client for Client {
    type Error = Error;
    /// Authorizes to Salesforce based on provided credentials.
    /// It then exchanges them for auth_token and refresh_token or returns error.
    async fn connect(mut self) -> Result<Self, Error> {
        // Load credentials from file
        let credentials_string = fs::read_to_string(&self.credentials).map_err(|e| Error::IO {
            path: self.credentials.clone(),
            source: e,
        })?;
        let credentials: Credentials =
            serde_json::from_str(&credentials_string).map_err(Error::ParseCredentials)?;

        // Build OAuth2 client
        let oauth2_client = BasicClient::new(
            ClientId::new(credentials.client_id.clone()),
            Some(ClientSecret::new(credentials.client_secret.to_owned())),
            AuthUrl::new(format!(
                "{0}/services/oauth2/authorize",
                credentials.instance_url.to_owned()
            ))
            .map_err(Error::ParseUrl)?,
            Some(
                TokenUrl::new(format!(
                    "{0}/services/oauth2/token",
                    credentials.instance_url.to_owned()
                ))
                .map_err(Error::ParseUrl)?,
            ),
        );

        // Exchange credentials for token
        let token_result = oauth2_client
            .exchange_client_credentials()
            .request_async(async_http_client)
            .await
            .map_err(|e| Error::Other(e.to_string()))?;

        self.oauth2_client = Some(oauth2_client);
        self.token_result = Some(token_result);
        self.instance_url = Some(credentials.instance_url);
        self.tenant_id = Some(credentials.tenant_id);

        Ok(self)
    }
}

#[derive(Default)]
/// Used to store Salesforce Client configuration.
pub struct Builder {
    credentials: Option<PathBuf>,
}

impl Builder {
    #[allow(clippy::new_ret_no_self)]
    /// Creates a new instance of a Builder.
    pub fn new() -> Builder {
        Builder::default()
    }

    /// Pass path to the file so that credentials can be loaded.
    pub fn credentials_path(mut self, credentials: PathBuf) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Generates a new client with the provided configuration.
    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            credentials: self
                .credentials
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials".to_string()))?,
            oauth2_client: None,
            token_result: None,
            instance_url: None,
            tenant_id: None,
        })
    }
}

#[cfg(test)]
mod tests {

    use std::env;

    use super::*;

    #[test]
    fn test_build_without_credentials() {
        let client = Builder::new().build();
        assert!(matches!(
            client,
            Err(Error::MissingRequiredAttribute(attr)) if attr == "credentials"
        ));
    }

    #[test]
    fn test_build_with_credentials() {
        let mut path = env::temp_dir();
        path.push(format!("credentials_{}.json", std::process::id()));
        let client = Builder::new().credentials_path(path).build();
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_connect_with_invalid_credentials() {
        use flowgen_core::client::Client as ClientTrait;

        let creds: &str = r#"{"client_id":"client_id"}"#;
        let mut path = env::temp_dir();
        path.push(format!("invalid_credentials_{}.json", std::process::id()));
        let _ = fs::write(path.clone(), creds);
        let client = Builder::new()
            .credentials_path(path.clone())
            .build()
            .unwrap();
        let result = client.connect().await;
        let _ = fs::remove_file(path);
        assert!(matches!(result, Err(Error::ParseCredentials(..))));
    }

    #[tokio::test]
    async fn test_connect_with_invalid_url() {
        use flowgen_core::client::Client as ClientTrait;

        let creds: &str = r#"
            {
                "client_id": "some_client_id",
                "client_secret": "some_client_secret",
                "instance_url": "mydomain.salesforce.com",
                "tenant_id": "some_tenant_id"
            }"#;
        let mut path = env::temp_dir();
        path.push(format!(
            "invalid_url_credentials_{}.json",
            std::process::id()
        ));
        let _ = fs::write(path.clone(), creds);
        let client = Builder::new()
            .credentials_path(path.clone())
            .build()
            .unwrap();
        let result = client.connect().await;
        let _ = fs::remove_file(path);
        assert!(matches!(result, Err(Error::ParseUrl(..))));
    }

    #[tokio::test]
    async fn test_connect_with_missing_file() {
        use flowgen_core::client::Client as ClientTrait;

        let mut path = env::temp_dir();
        path.push(format!("nonexistent_{}.json", std::process::id()));
        let client = Builder::new().credentials_path(path).build().unwrap();
        let result = client.connect().await;
        assert!(matches!(result, Err(Error::IO { .. })));
    }
}
