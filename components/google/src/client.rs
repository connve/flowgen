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
use std::rc::Rc;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot open/read the credentials file at path {1}")]
    OpenFile(#[source] std::io::Error, PathBuf),
    #[error("Cannot parse the credentials file")]
    ParseCredentials(#[source] serde_json::Error),
    #[error("Cannot parse url")]
    ParseUrl(#[source] url::ParseError),
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

/// Used to store Salesforce Client credentials.
#[allow(clippy::type_complexity)]
pub struct Client {
    /// Oauth2.0 client for getting the tokens.
    scopes: Vec<String>,
    /// Token result as a composite of access_token / refresh__token etc.
    pub token_result: Option<google_cloud_auth::AccessToken>,
}

impl flowgen_core::client::Client for Client {
    type Error = Error;
    /// Authorizes to Salesforce based on provided credentials.
    /// It then exchanges them for auth_token and refresh_token or returns error.
    async fn connect(mut self) -> Result<Self, Error> {
        let credential_config = google_cloud_auth::CredentialConfigBuilder::new()
            .scopes(vec![
                "https://www.googleapis.com/auth/cloud-platform".to_string()
            ])
            .build()
            .unwrap();

        let token_result = google_cloud_auth::Credential::find_default(credential_config)
            .await
            .unwrap()
            .access_token()
            .await
            .unwrap();

        self.token_result = Some(token_result);
        Ok(self)
    }
}

#[derive(Default)]
/// Used to store Salesforce Client configuration.
pub struct Builder {
    credentials_path: PathBuf,
}

impl Builder {
    #[allow(clippy::new_ret_no_self)]
    /// Creates a new instance of a Builder.
    pub fn new() -> Builder {
        Builder::default()
    }
    /// Pass path to the fail so that credentials can be loaded.
    pub fn with_credentials_path(&mut self, credentials_path: PathBuf) -> &mut Builder {
        self.credentials_path = credentials_path;
        self
    }

    /// Generates a new client or return error in case
    /// provided credentials path is not valid.
    pub fn build(&self) -> Result<Client, Error> {
        // let credentials_string = fs::read_to_string(&self.credentials_path)
        //     .map_err(|e| Error::OpenFile(e, self.credentials_path.to_owned()))?;
        // let credentials: Credentials =
        //     serde_json::from_str(&credentials_string).map_err(Error::ParseCredentials)?;

        Ok(Client {
            scopes: vec!["https://www.googleapis.com/auth/cloud-platform".to_string()],
            token_result: None,
        })
    }
}
