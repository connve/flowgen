use object_store::aws::AmazonS3Builder;
use object_store::{parse_url_opts, path::Path, ObjectStore};
use std::{collections::HashMap, path::PathBuf};
use url::Url;

/// Known credential keys extracted from an AWS credentials JSON file.
const AWS_CREDENTIAL_KEYS: &[&str] = &[
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "aws_region",
];

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Invalid URL format: {source}")]
    ParseUrl {
        #[source]
        source: url::ParseError,
    },
    #[error("Object store error: {source}")]
    ObjectStore {
        #[source]
        source: object_store::Error,
    },
    #[error("No path provided")]
    EmptyPath,
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("No context available")]
    NoContext,
}

impl Error {
    /// Wraps a standard error as an S3-specific object store error.
    fn s3(source: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::ObjectStore {
            source: object_store::Error::Generic {
                store: "S3",
                source: Box::new(source),
            },
        }
    }
}

/// Object store context containing the store instance and base path.
#[derive(Debug)]
pub struct Context {
    /// Object store implementation (S3, GCS, local filesystem, etc.).
    pub object_store: Box<dyn ObjectStore>,
    /// Base path for object operations.
    pub path: Path,
}

/// Object store client with connection details.
#[derive(Debug)]
pub struct Client {
    /// Object store URL path.
    path: PathBuf,
    /// Optional path to credentials file (GCP service account JSON or AWS credentials JSON).
    credentials_path: Option<PathBuf>,
    /// Additional connection options for the object store.
    options: Option<HashMap<String, String>>,
    /// Initialized object store context after connection.
    pub context: Option<Context>,
}

impl Client {
    /// Establishes connection to the object store.
    fn create_context(&self) -> Result<Context, Error> {
        let path = self.path.to_str().ok_or(Error::EmptyPath)?;
        let url = Url::parse(path).map_err(|source| Error::ParseUrl { source })?;
        let mut parse_opts = self.options.clone().unwrap_or_default();

        if let Some(credentials_path) = &self.credentials_path {
            match url.scheme() {
                "gs" => {
                    parse_opts.insert(
                        "google_service_account".to_string(),
                        credentials_path.to_string_lossy().to_string(),
                    );
                }
                "s3" | "s3a" => {
                    Self::load_aws_credentials(credentials_path, &mut parse_opts)?;
                }
                _ => {}
            }
        }

        match url.scheme() {
            "s3" | "s3a" => self.create_s3_context(&url, parse_opts),
            _ => {
                let (object_store, path) = parse_url_opts(&url, parse_opts)
                    .map_err(|e| Error::ObjectStore { source: e })?;
                Ok(Context { object_store, path })
            }
        }
    }

    /// Creates an S3 context using `from_env()` to pick up IRSA, EKS Pod Identity,
    /// instance profiles, and env var credentials automatically.
    fn create_s3_context(
        &self,
        url: &Url,
        opts: HashMap<String, String>,
    ) -> Result<Context, Error> {
        let path =
            Path::from_url_path(url.path()).map_err(|e| Error::ObjectStore { source: e.into() })?;

        let builder = opts.into_iter().fold(
            AmazonS3Builder::from_env()
                .with_url(url.to_string())
                .with_virtual_hosted_style_request(true),
            |builder, (key, value)| match key.to_ascii_lowercase().parse() {
                Ok(k) => builder.with_config(k, value),
                Err(_) => builder,
            },
        );

        let object_store = builder
            .build()
            .map_err(|e| Error::ObjectStore { source: e })?;

        Ok(Context {
            object_store: Box::new(object_store),
            path,
        })
    }

    /// Reads an AWS credentials JSON file and inserts known keys into the options map.
    /// Keys already present in the map (e.g. from client_options) are not overwritten.
    fn load_aws_credentials(
        path: &std::path::Path,
        opts: &mut HashMap<String, String>,
    ) -> Result<(), Error> {
        let content = std::fs::read_to_string(path).map_err(Error::s3)?;
        let creds: serde_json::Value = serde_json::from_str(&content).map_err(Error::s3)?;

        for &key in AWS_CREDENTIAL_KEYS {
            if let Some(value) = creds.get(key).and_then(|v| v.as_str()) {
                opts.entry(key.to_string())
                    .or_insert_with(|| value.to_string());
            }
        }

        Ok(())
    }

    /// Reconnects to the object store, refreshing credentials.
    ///
    /// This creates a new connection with fresh credentials, which is necessary
    /// because cloud provider tokens expire (GCS OAuth ~1 hour, AWS STS
    /// session tokens ~12 hours). Without reconnecting, operations will fail
    /// with authentication errors.
    pub async fn reconnect(&mut self) -> Result<(), Error> {
        self.context = Some(self.create_context()?);
        Ok(())
    }

    /// Checks if an error is an authentication error that should trigger a reconnect.
    pub fn is_auth_error(err: &object_store::Error) -> bool {
        matches!(err, object_store::Error::Unauthenticated { .. })
    }
}

impl flowgen_core::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Client, Error> {
        self.context = Some(self.create_context()?);
        Ok(self)
    }
}

/// Builder pattern for constructing Client instances.
#[derive(Default)]
pub struct ClientBuilder {
    /// Object store URL path.
    path: Option<PathBuf>,
    /// Optional path to credentials file (GCP service account JSON or AWS credentials JSON).
    credentials_path: Option<PathBuf>,
    /// Additional connection options for the object store.
    pub options: Option<HashMap<String, String>>,
}

impl ClientBuilder {
    pub fn new() -> ClientBuilder {
        ClientBuilder {
            ..Default::default()
        }
    }

    /// Sets the credentials file path.
    pub fn credentials_path(mut self, path: PathBuf) -> Self {
        self.credentials_path = Some(path);
        self
    }

    /// Sets the object store URL path.
    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    /// Sets additional connection options.
    pub fn options(mut self, options: HashMap<String, String>) -> Self {
        self.options = Some(options);
        self
    }
    /// Builds the Client instance, validating required fields.
    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            path: self
                .path
                .ok_or_else(|| Error::MissingBuilderAttribute("path".to_string()))?,
            credentials_path: self.credentials_path,
            options: self.options,
            context: None,
        })
    }
}
