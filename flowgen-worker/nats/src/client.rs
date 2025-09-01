use std::fs;
use std::path::PathBuf;

/// Default NATS Server host.
const DEFAULT_NATS_HOST: &str = "localhost:4222";

/// Used to store NATS Client credentials.
#[derive(serde::Deserialize)]
struct Credentials {
    /// nKey public key string.
    nkey: Option<String>,
    /// Optional host value, if not passed localhost:4222 will be used.
    host: Option<String>,
}

/// Errors that can occur during NATS client operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// Failed to open or read the credentials file.
    #[error(transparent)]
    OpenFile(#[from] std::io::Error),
    /// Failed to parse credentials JSON file.
    #[error(transparent)]
    ParseCredentials(#[from] serde_json::Error),
    /// Invalid URL format in credentials or configuration.
    #[error(transparent)]
    ParseUrl(#[from] url::ParseError),
    /// Failed to establish connection to server.
    #[error(transparent)]
    Connect(#[from] async_nats::ConnectError),
    /// Credentials file path was not provided during client creation.
    #[error("Credentials are not provided")]
    CredentialsNotProvided(),
    /// Required configuration attribute is missing.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

/// NATS client with optional JetStream context for reliable messaging.
#[derive(Debug)]
pub struct Client {
    /// Path to the NATS credentials file.
    credentials_path: PathBuf,
    /// JetStream context for reliable messaging operations.
    pub jetstream: Option<async_nats::jetstream::Context>,
}

impl flowgen_core::client::Client for Client {
    type Error = Error;
    /// Connect to the NATS Server with provided options.
    async fn connect(mut self) -> Result<Self, Error> {
        let credentials: Credentials = serde_json::from_str(
            &fs::read_to_string(&self.credentials_path).map_err(Error::OpenFile)?,
        )
        .map_err(Error::ParseCredentials)?;

        let mut connect_options = async_nats::ConnectOptions::new();
        if let Some(configured_nkey) = credentials.nkey {
            connect_options = async_nats::ConnectOptions::with_nkey(configured_nkey);
        }

        let mut host = DEFAULT_NATS_HOST.to_string();
        if let Some(configured_host) = credentials.host {
            host = configured_host.clone();
        }

        let nats_client = connect_options
            .connect(host)
            .await
            .map_err(Error::Connect)?;

        let jetstream = async_nats::jetstream::new(nats_client);

        self.jetstream = Some(jetstream);
        Ok(self)
    }
}

/// Builder for configuring and creating NATS clients.
#[derive(Default)]
pub struct ClientBuilder {
    /// Optional path to NATS credentials file.
    credentials_path: Option<PathBuf>,
}

impl ClientBuilder {
    /// Creates a new instance of a client builder and allows various configurartion of the client.
    pub fn new() -> Self {
        ClientBuilder::default()
    }
    /// Pass credentials file as path to the file.
    pub fn credentials_path(&mut self, credentials_path: PathBuf) -> &mut ClientBuilder {
        self.credentials_path = Some(credentials_path);
        self
    }

    /// Generates a new NATS Client or return error in case provided credentials are not valid.
    pub fn build(&self) -> Result<Client, Error> {
        Ok(Client {
            credentials_path: self
                .credentials_path
                .clone()
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials_path".to_string()))?,

            jetstream: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_client_builder_new() {
        let builder = ClientBuilder::new();
        assert!(builder.credentials_path.is_none());
    }

    #[test]
    fn test_client_builder_credentials_path() {
        let path = PathBuf::from("/path/to/nats.creds");
        let mut builder = ClientBuilder::new();
        builder.credentials_path(path.clone());
        assert_eq!(builder.credentials_path, Some(path));
    }

    #[test]
    fn test_client_builder_build_missing_credentials() {
        let builder = ClientBuilder::new();
        let result = builder.build();
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "credentials_path")
        );
    }

    #[test]
    fn test_client_builder_build_success() {
        let path = PathBuf::from("/valid/nats.creds");
        let mut builder = ClientBuilder::new();
        builder.credentials_path(path.clone());
        let result = builder.build();

        assert!(result.is_ok());
        let client = result.unwrap();
        assert_eq!(client.credentials_path, path);
        assert!(client.jetstream.is_none());
    }

    #[test]
    fn test_client_builder_method_chaining() {
        let path = PathBuf::from("/chain/test.creds");
        let mut builder = ClientBuilder::new();
        let client = builder.credentials_path(path.clone()).build().unwrap();

        assert_eq!(client.credentials_path, path);
        assert!(client.jetstream.is_none());
    }

    #[test]
    fn test_client_builder_default() {
        let builder = ClientBuilder::default();
        assert!(builder.credentials_path.is_none());
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingRequiredAttribute("test_field".to_string());
        assert!(err
            .to_string()
            .contains("Missing required attribute: test_field"));

        let err = Error::CredentialsNotProvided();
        assert!(err.to_string().contains("Credentials are not provided"));
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_NATS_HOST, "localhost:4222");
    }

    #[test]
    fn test_credentials_struct() {
        // Test that Credentials can be deserialized correctly
        let json_creds = r#"{
            "nkey": "UAABC123DEF456GHI789JKL",
            "host": "nats.example.com:4222"
        }"#;

        let creds: Result<Credentials, serde_json::Error> = serde_json::from_str(json_creds);
        assert!(creds.is_ok());

        let creds = creds.unwrap();
        assert_eq!(creds.nkey, Some("UAABC123DEF456GHI789JKL".to_string()));
        assert_eq!(creds.host, Some("nats.example.com:4222".to_string()));
    }

    #[test]
    fn test_credentials_optional_fields() {
        // Test credentials with missing optional fields
        let json_creds = r#"{}"#;

        let creds: Result<Credentials, serde_json::Error> = serde_json::from_str(json_creds);
        assert!(creds.is_ok());

        let creds = creds.unwrap();
        assert_eq!(creds.nkey, None);
        assert_eq!(creds.host, None);
    }

    #[test]
    fn test_client_structure() {
        let path = PathBuf::from("/test/nats.creds");
        let client = Client {
            credentials_path: path.clone(),
            jetstream: None,
        };

        assert_eq!(client.credentials_path, path);
        assert!(client.jetstream.is_none());
    }

    // Note: We cannot easily test the connect() method without a real NATS server
    // and valid credentials file, but the builder pattern and error handling
    // are thoroughly tested above
}
