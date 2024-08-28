/// This Source Code Form is subject to the terms of the Mozilla Public
/// License, v. 2.0. If a copy of the MPL was not distributed with this
/// file, You can obtain one at https://mozilla.org/MPL/2.0/.
use oauth2::basic::{BasicClient, BasicTokenType};
use oauth2::reqwest::async_http_client;
use oauth2::{
    AuthUrl, ClientId, ClientSecret, EmptyExtraTokenFields, StandardTokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse the credentials file")]
    ParseCredentials(#[source] serde_json::Error),
    #[error("Cannot parse url")]
    ParseUrl(#[source] url::ParseError),
    #[error("Other Auth error")]
    NotCategorized(#[source] Box<dyn std::error::Error>),
}

#[derive(Debug, Serialize, Deserialize)]
/// Used to store Salesforce Client credentials.
pub struct Client {
    /// Client ID from connected apps.
    pub client_id: String,
    /// Client Secret from connected apps.
    pub client_secret: String,
    /// Intance URL eg. httsp://mydomain.salesforce.com.
    pub instance_url: String,
    /// Tenant/org id from company info.
    pub tenant_id: String,
}

impl Client {
    #[allow(clippy::new_ret_no_self)]
    /// Creates a new instance of a ClientBuilder.
    pub fn new() -> ClientBuilder {
        ClientBuilder::default()
    }
    /// Authorizes to Salesforce based on provided credentials.
    /// It then exchanges them for auth_token and refresh_token or returns error.
    pub async fn connect(
        &self,
    ) -> Result<StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>, Error> {
        let auth_client = BasicClient::new(
            ClientId::new(self.client_id.clone()),
            Some(ClientSecret::new(self.client_secret.to_owned())),
            AuthUrl::new(format!(
                "{0}/services/oauth2/authorize",
                self.instance_url.to_owned()
            ))
            .map_err(Error::ParseUrl)?,
            Some(
                TokenUrl::new(format!(
                    "{0}/services/oauth2/token",
                    self.instance_url.to_owned()
                ))
                .map_err(Error::ParseUrl)?,
            ),
        );

        let token_result = auth_client
            .exchange_client_credentials()
            .request_async(async_http_client)
            .await
            .map_err(|e| Error::NotCategorized(Box::new(e)))?;

        Ok(token_result)
    }
}

#[derive(Default)]
/// Used to store Salesforce Client configuration.
pub struct ClientBuilder {
    credentials_path: PathBuf,
}

impl ClientBuilder {
    /// Pass path to the fail so that credentials can be loaded.
    pub fn with_credentials_path(&mut self, credentials_path: PathBuf) -> &mut Self {
        self.credentials_path = credentials_path;
        self
    }

    /// Generates a new client or return error in case
    /// provided credentials path is not valid.
    pub fn build(&mut self) -> Result<Client, Error> {
        let creds = fs::read_to_string(&self.credentials_path)
            .map_err(|e| Error::OpenFile(e, self.credentials_path.clone()))?;
        let client: Client = serde_json::from_str(&creds).map_err(Error::ParseCredentials)?;
        Ok(client)
    }
}

#[cfg(test)]
mod tests {

    use std::path::PathBuf;

    use super::*;

    #[test]
    fn test_build_without_credentials() {
        let client = Client::new().build();
        assert!(matches!(client, Err(Error::OpenFile(..))));
    }

    #[test]
    fn test_build_with_invalid_credentials() {
        let creds: &str = r#"{"client_id":"client_id"}"#;
        let mut path = PathBuf::new();
        path.push("credentials.json");
        let _ = fs::write(path.clone(), creds);
        let client = Client::new().with_credentials_path(path.clone()).build();
        assert!(matches!(client, Err(Error::ParseCredentials(..))));
    }

    #[test]
    fn test_build_with_valid_credentials() {
        let creds: &str = r#"
            {
                "client_id": "some_client_id",
                "client_secret": "some_client_secret", 
                "instance_url": "some_instance_url", 
                "tenant_id": "some_tenant_id"
            }"#;
        let mut path = PathBuf::new();
        path.push("credentials.json");
        let _ = fs::write(path.clone(), creds);
        let client = Client::new().with_credentials_path(path.clone()).build();
        assert!(client.is_ok());
    }
}
