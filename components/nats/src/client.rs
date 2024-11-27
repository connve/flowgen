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
    #[error("Cannot parse url")]
    NatsConnection(#[source] async_nats::ConnectError),
    #[error("Credentials are not provided")]
    CredentialsNotProvided(),
}

/// Used to store Nats Client credentials.
#[derive(serde::Deserialize)]
struct Credentials {
    /// nKey public key string.
    nkey: Option<String>,
    /// Instance url.
    host: String,
}

#[derive(Debug)]
pub struct Client {
    /// Nats client connect options
    connect_options: async_nats::ConnectOptions,
    host: String,
    pub nats_client: Option<async_nats::Client>,
}

impl flowgen_core::client::Client for Client {
    type Error = Error;
    /// Connect to the Nats Server with provided options.
    async fn connect(mut self) -> Result<Self, Error> {
        let connect_options = std::mem::take(&mut self.connect_options);
        let nats_client = connect_options
            .connect(self.host.clone())
            .await
            .map_err(Error::NatsConnection)?;
        self.nats_client = Some(nats_client);
        Ok(self)
    }
}

#[derive(Default)]
/// Configuration options of the client.
pub struct Builder {
    /// Optional path to the credentials file.
    credentials_path: Option<PathBuf>,
}

impl Builder {
    #[allow(clippy::new_ret_no_self)]
    /// Creates a new instance of a Builder.
    pub fn new() -> Builder {
        Builder::default()
    }
    /// Pass credentials file.
    pub fn with_credentials_path(&mut self, credentials_path: PathBuf) -> &mut Builder {
        self.credentials_path = Some(credentials_path);
        self
    }

    /// Generates a new Nats Client or return error in case provided credentials are not valid.
    pub fn build(&self) -> Result<Client, Error> {
        if let Some(credentials_path) = &self.credentials_path {
            let credentials_string = fs::read_to_string(credentials_path)
                .map_err(|e| Error::OpenFile(e, credentials_path.to_owned()))?;
            let credentials: Credentials =
                serde_json::from_str(&credentials_string).map_err(Error::ParseCredentials)?;

            let mut nats_connect_opts = async_nats::ConnectOptions::new();
            if let Some(nkey) = credentials.nkey {
                nats_connect_opts = async_nats::ConnectOptions::with_nkey(nkey);
            }

            Ok(Client {
                connect_options: nats_connect_opts,
                nats_client: None,
                host: credentials.host,
            })
        } else {
            Err(Error::CredentialsNotProvided())
        }
    }
}
