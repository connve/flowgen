use object_store::{parse_url_opts, path::Path, ObjectStore};
use std::{collections::HashMap, path::PathBuf};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ParseUrl(#[from] url::ParseError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("no path provided")]
    EmptyPath(),
}

/// Object store context containing the store instance and base path.
pub struct Context {
    /// Object store implementation (S3, GCS, local filesystem, etc.).
    pub object_store: Box<dyn ObjectStore>,
    /// Base path for object operations.
    pub path: Path,
}

/// Object store client with connection details.
pub struct Client {
    /// Object store URL path.
    path: PathBuf,
    /// Optional path to service account credentials file.
    credentials: Option<PathBuf>,
    /// Additional connection options for the object store.
    options: Option<HashMap<String, String>>,
    /// Initialized object store context after connection.
    pub context: Option<Context>,
}

impl flowgen_core::connect::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Client, Error> {
        // Parse URL and prepare connection options.
        let path = self.path.to_str().ok_or_else(Error::EmptyPath)?;
        let url = Url::parse(path)?;
        let mut parse_opts = match &self.options {
            Some(options) => options.clone(),
            None => HashMap::new(),
        };

        // Add Google Service Account credentials if provided.
        if let Some(credentials) = &self.credentials {
            parse_opts.insert(
                "google_service_account".to_string(),
                credentials.to_string_lossy().to_string(),
            );
        }

        // Initialize object store from URL and options.
        let (object_store, path) = parse_url_opts(&url, parse_opts)?;
        let context = Context { object_store, path };
        self.context = Some(context);
        Ok(self)
    }
}

/// Builder pattern for constructing Client instances.
#[derive(Default)]
pub struct ClientBuilder {
    /// Object store URL path.
    path: Option<PathBuf>,
    /// Optional path to service account credentials file.
    credentials: Option<PathBuf>,
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
    pub fn credentials(mut self, credentials: PathBuf) -> Self {
        self.credentials = Some(credentials);
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
                .ok_or_else(|| Error::MissingRequiredAttribute("path".to_string()))?,
            credentials: self.credentials,
            options: self.options,
            context: None,
        })
    }
}
