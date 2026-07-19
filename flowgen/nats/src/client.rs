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
#[non_exhaustive]
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
    /// Optional path to the NATS credentials file. `None` connects
    /// anonymously — matches the server default when no
    /// `authorization` block is configured on the NATS side.
    credentials_path: Option<PathBuf>,
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
        let connect_options = match &self.credentials_path {
            Some(path) => {
                let credentials: Credentials =
                    serde_json::from_str(&fs::read_to_string(path).map_err(|e| {
                        Error::ReadCredentials {
                            path: path.clone(),
                            source: e,
                        }
                    })?)
                    .map_err(|e| Error::ParseCredentials { source: e })?;
                match credentials.nkey {
                    // NKey seed is passed to async_nats; server validates
                    // against the configured public key.
                    Some(nkey_creds) => async_nats::ConnectOptions::with_nkey(nkey_creds.seed),
                    None => return Err(Error::NoCredentials),
                }
            }
            None => async_nats::ConnectOptions::default(),
        };

        let url = self.url.as_deref().unwrap_or(DEFAULT_NATS_URL);
        let nats_client = connect_options
            .connect(url)
            .await
            .map_err(|e| Error::Connect {
                url: url.to_string(),
                source: e,
            })?;

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
    ///     "seed": "your-private-key"
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

    /// Builds a new NATS client instance. Anonymous when
    /// `credentials_path` is not set.
    pub fn build(&self) -> Result<Client, Error> {
        Ok(Client {
            credentials_path: self.credentials_path.clone(),
            url: self.url.clone(),
            jetstream: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
                "seed": "your-private-key"
            }
        }"#;

        let creds: Result<Credentials, serde_json::Error> = serde_json::from_str(json_creds);
        assert!(creds.is_ok());

        let creds = creds.unwrap();
        assert!(creds.nkey.is_some());

        let nkey = creds.nkey.unwrap();
        assert_eq!(nkey.seed, "your-private-key");
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
            seed: "your-private-key".to_string(),
        };

        let cloned = nkey.clone();
        assert_eq!(nkey, cloned);
    }
}
