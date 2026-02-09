use std::fs;
use std::path::PathBuf;

/// Default NATS Server URL.
pub const DEFAULT_NATS_URL: &str = "localhost:4222";

/// Authentication credentials for NATS connections.
#[derive(serde::Deserialize, Debug, Clone, PartialEq, Default)]
pub struct Credentials {
    /// NKey authentication credentials.
    pub nkey: Option<NKeyCredentials>,
}

/// NKey authentication using seed (private key).
#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct NKeyCredentials {
    /// Seed (private key) starting with 'S'.
    /// Used to sign authentication challenges.
    /// The server validates against the corresponding public key (starting with 'U').
    pub seed: String,
}

/// Errors that can occur during NATS client operations.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error reading credentials file '{path}': {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Error parsing credentials file: {source}")]
    ParseCredentials {
        #[source]
        source: serde_json::Error,
    },
    #[error("Invalid URL format: {source}")]
    ParseUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("Error connecting to '{url}': {source}")]
    Connect {
        url: String,
        #[source]
        source: async_nats::ConnectError,
    },
    #[error("No authentication credentials provided")]
    NoCredentials,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

/// NATS client with optional JetStream context for reliable messaging.
#[derive(Debug)]
pub struct Client {
    /// Path to the NATS credentials file.
    /// This file contains authentication credentials in JSON format.
    credentials_path: PathBuf,
    /// NATS server URL (e.g., "nats://localhost:4222" or "localhost:4222").
    /// If not set, defaults to "localhost:4222".
    url: Option<String>,
    /// JetStream context for reliable messaging operations.
    pub jetstream: Option<async_nats::jetstream::Context>,
}

impl flowgen_core::client::Client for Client {
    type Error = Error;

    /// Connects to the NATS server with the provided options.
    async fn connect(mut self) -> Result<Self, Error> {
        // Read and parse credentials file.
        let credentials: Credentials =
            serde_json::from_str(&fs::read_to_string(&self.credentials_path).map_err(|e| {
                Error::ReadCredentials {
                    path: self.credentials_path.clone(),
                    source: e,
                }
            })?)
            .map_err(|e| Error::ParseCredentials { source: e })?;

        // Build connect options based on credential type.
        let connect_options = if let Some(nkey_creds) = credentials.nkey {
            // For NKey authentication, the seed (private key) is passed to async_nats.
            // The seed is used to sign authentication challenges, and the server validates against the public key.
            async_nats::ConnectOptions::with_nkey(nkey_creds.seed)
        } else {
            return Err(Error::NoCredentials);
        };

        // Use provided URL or fall back to default.
        let url = self.url.as_deref().unwrap_or(DEFAULT_NATS_URL);

        // Connect to NATS server.
        let nats_client = connect_options
            .connect(url)
            .await
            .map_err(|e| Error::Connect {
                url: url.to_string(),
                source: e,
            })?;

        // Initialize JetStream context.
        let jetstream = async_nats::jetstream::new(nats_client);

        self.jetstream = Some(jetstream);
        Ok(self)
    }
}

/// Builder for configuring and creating NATS clients.
#[derive(Default)]
pub struct ClientBuilder {
    /// Path to NATS credentials file.
    credentials_path: Option<PathBuf>,
    /// NATS server URL.
    url: Option<String>,
}

impl ClientBuilder {
    /// Creates a new client builder instance for configuring NATS client options.
    pub fn new() -> Self {
        ClientBuilder::default()
    }

    /// Sets the path to the credentials file.
    ///
    /// The credentials file should be a JSON file with the following format:
    ///
    /// NKey authentication:
    /// ```json
    /// {
    ///   "nkey": {
    ///     "seed": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
    ///   }
    /// }
    /// ```
    pub fn credentials_path(&mut self, path: PathBuf) -> &mut ClientBuilder {
        self.credentials_path = Some(path);
        self
    }

    /// Sets the NATS server URL (e.g., "nats://localhost:4222" or "localhost:4222").
    /// If not set, defaults to "localhost:4222".
    pub fn url(&mut self, url: String) -> &mut ClientBuilder {
        self.url = Some(url);
        self
    }

    /// Builds a new NATS client instance.
    ///
    /// Returns an error if `credentials_path` is not provided.
    pub fn build(&self) -> Result<Client, Error> {
        Ok(Client {
            credentials_path: self
                .credentials_path
                .clone()
                .ok_or_else(|| Error::MissingBuilderAttribute("credentials_path".to_string()))?,
            url: self.url.clone(),
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
        assert!(builder.url.is_none());
    }

    #[test]
    fn test_client_builder_credentials_path() {
        let path = PathBuf::from("/path/to/nats.creds");
        let mut builder = ClientBuilder::new();
        builder.credentials_path(path.clone());
        assert_eq!(builder.credentials_path, Some(path));
    }

    #[test]
    fn test_client_builder_url() {
        let url = "nats://nats.example.com:4222".to_string();
        let mut builder = ClientBuilder::new();
        builder.url(url.clone());
        assert_eq!(builder.url, Some(url));
    }

    #[test]
    fn test_client_builder_build_missing_credentials() {
        let builder = ClientBuilder::new();
        let result = builder.build();
        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingBuilderAttribute(attr) if attr == "credentials_path")
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
        assert_eq!(client.url, None);
        assert!(client.jetstream.is_none());
    }

    #[test]
    fn test_client_builder_with_url() {
        let path = PathBuf::from("/valid/nats.creds");
        let url = "nats://nats.example.com:4222".to_string();
        let mut builder = ClientBuilder::new();
        builder.credentials_path(path.clone());
        builder.url(url.clone());
        let result = builder.build();

        assert!(result.is_ok());
        let client = result.unwrap();
        assert_eq!(client.credentials_path, path);
        assert_eq!(client.url, Some(url));
        assert!(client.jetstream.is_none());
    }

    #[test]
    fn test_client_builder_method_chaining() {
        let path = PathBuf::from("/chain/test.creds");
        let url = "nats://localhost:4222".to_string();
        let mut builder = ClientBuilder::new();
        let client = builder
            .credentials_path(path.clone())
            .url(url.clone())
            .build()
            .unwrap();

        assert_eq!(client.credentials_path, path);
        assert_eq!(client.url, Some(url));
        assert!(client.jetstream.is_none());
    }

    #[test]
    fn test_client_builder_default() {
        let builder = ClientBuilder::default();
        assert!(builder.credentials_path.is_none());
        assert!(builder.url.is_none());
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_NATS_URL, "localhost:4222");
    }

    #[test]
    fn test_credentials_default() {
        let creds = Credentials::default();
        assert!(creds.nkey.is_none());
    }

    #[test]
    fn test_credentials_nkey_deserialization() {
        let json_creds = r#"{
            "nkey": {
                "seed": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
            }
        }"#;

        let creds: Result<Credentials, serde_json::Error> = serde_json::from_str(json_creds);
        assert!(creds.is_ok());

        let creds = creds.unwrap();
        assert!(creds.nkey.is_some());

        let nkey = creds.nkey.unwrap();
        assert_eq!(
            nkey.seed,
            "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
        );
    }

    #[test]
    fn test_credentials_empty_deserialization() {
        let json_creds = r#"{}"#;

        let creds: Result<Credentials, serde_json::Error> = serde_json::from_str(json_creds);
        assert!(creds.is_ok());

        let creds = creds.unwrap();
        assert!(creds.nkey.is_none());
    }

    #[test]
    fn test_nkey_credentials_clone() {
        let nkey = NKeyCredentials {
            seed: "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY".to_string(),
        };

        let cloned = nkey.clone();
        assert_eq!(nkey, cloned);
    }

    #[test]
    fn test_client_structure() {
        let path = PathBuf::from("/test/nats.creds");
        let url = Some("nats://localhost:4222".to_string());
        let client = Client {
            credentials_path: path.clone(),
            url: url.clone(),
            jetstream: None,
        };

        assert_eq!(client.credentials_path, path);
        assert_eq!(client.url, url);
        assert!(client.jetstream.is_none());
    }
}
