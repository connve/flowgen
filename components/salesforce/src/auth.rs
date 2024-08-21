use oauth2::basic::{BasicClient, BasicTokenType};
use oauth2::reqwest::async_http_client;
use oauth2::{
    AuthUrl, ClientId, ClientSecret, EmptyExtraTokenFields, StandardTokenResponse, TokenUrl,
};
use serde::{Deserialize, Serialize};
use std::fs;

#[derive(Debug, Serialize, Deserialize)]
pub struct Client {
    pub client_id: String,
    pub client_secret: String,
    pub instance_url: String,
    pub tenant_id: String,
}

impl Client {
    #[allow(clippy::new_ret_no_self)]
    pub fn new() -> ClientBuilder {
        ClientBuilder::default()
    }
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
pub struct ClientBuilder {
    credentials_path: String,
}

impl ClientBuilder {
    pub fn with_credentials_path(&mut self, credentials_path: String) -> &mut Self {
        self.credentials_path = credentials_path;
        self
    }

    pub fn build(&mut self) -> Result<Client, Box<dyn std::error::Error>> {
        let creds = fs::read_to_string(&self.credentials_path)?;
        let client: Client = serde_json::from_str(&creds)?;
        Ok(client)
    }
}
