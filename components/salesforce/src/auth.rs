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
    ) -> Result<
        StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>,
        Box<dyn std::error::Error>,
    > {
        let auth_client = BasicClient::new(
            ClientId::new(self.client_id.clone()),
            Some(ClientSecret::new(self.client_secret.clone())),
            AuthUrl::new(format!(
                "{0}/services/oauth2/authorize",
                self.instance_url.clone()
            ))?,
            Some(TokenUrl::new(format!(
                "{0}/services/oauth2/token",
                self.instance_url.clone()
            ))?),
        );

        let token_result = auth_client
            .exchange_client_credentials()
            .request_async(async_http_client)
            .await?;

        Ok(token_result)
    }
}

#[derive(Default)]
/// Used to store Salesforce Client configuration.
pub struct ClientBuilder {
    credentials_path: String,
}

impl ClientBuilder {
    /// Pass path to the fail so that credentials can be loaded.
    pub fn with_credentials_path(&mut self, credentials_path: String) -> &mut Self {
        self.credentials_path = credentials_path;
        self
    }

    /// Generates a new client or return error in case
    /// provided credentials path is not valid.
    pub fn build(&mut self) -> Result<Client, Box<dyn std::error::Error>> {
        let creds = fs::read_to_string(&self.credentials_path)?;
        let client: Client = serde_json::from_str(&creds)?;
        Ok(client)
    }
}
