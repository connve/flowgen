use object_store::{parse_url_opts, path::Path, ObjectStore};
use std::{collections::HashMap, path::PathBuf};
use url::Url;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    ParseUrl(#[from] url::ParseError),
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error("Missing required attribute")]
    MissingRequiredAttribute(String),
    #[error("No path provided")]
    EmptyPath(),
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
    /// Optional path to service account credentials file.
    credentials: Option<PathBuf>,
    /// Additional connection options for the object store.
    options: Option<HashMap<String, String>>,
    /// Initialized object store context after connection.
    pub context: Option<Context>,
}

impl flowgen_core::client::Client for Client {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_client_builder_new() {
        let builder = ClientBuilder::new();
        assert!(builder.path.is_none());
        assert!(builder.credentials.is_none());
        assert!(builder.options.is_none());
    }

    #[test]
    fn test_client_builder_path() {
        let path = PathBuf::from("s3://test-bucket/data/");
        let builder = ClientBuilder::new().path(path.clone());
        assert_eq!(builder.path, Some(path));
    }

    #[test]
    fn test_client_builder_credentials() {
        let credentials = PathBuf::from("/path/to/credentials.json");
        let builder = ClientBuilder::new().credentials(credentials.clone());
        assert_eq!(builder.credentials, Some(credentials));
    }

    #[test]
    fn test_client_builder_options() {
        let mut options = HashMap::new();
        options.insert("region".to_string(), "us-east-1".to_string());
        options.insert("endpoint".to_string(), "https://s3.amazonaws.com".to_string());
        
        let builder = ClientBuilder::new().options(options.clone());
        assert_eq!(builder.options, Some(options));
    }

    #[test]
    fn test_client_builder_build_missing_path() {
        let result = ClientBuilder::new().build();
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::MissingRequiredAttribute(attr) if attr == "path"));
    }

    #[test]
    fn test_client_builder_build_success() {
        let path = PathBuf::from("file:///tmp/test/");
        let credentials = PathBuf::from("/service-account.json");
        let mut options = HashMap::new();
        options.insert("project_id".to_string(), "my-project".to_string());

        let client = ClientBuilder::new()
            .path(path.clone())
            .credentials(credentials.clone())
            .options(options.clone())
            .build()
            .unwrap();

        assert_eq!(client.path, path);
        assert_eq!(client.credentials, Some(credentials));
        assert_eq!(client.options, Some(options));
        assert!(client.context.is_none());
    }

    #[test]
    fn test_client_builder_chain() {
        let path = PathBuf::from("gs://bucket/path/");
        let credentials = PathBuf::from("/creds.json");
        let mut options = HashMap::new();
        options.insert("timeout".to_string(), "30".to_string());

        let client = ClientBuilder::new()
            .path(path.clone())
            .credentials(credentials.clone())
            .options(options.clone())
            .build()
            .unwrap();

        assert_eq!(client.path, path);
        assert_eq!(client.credentials, Some(credentials));
        assert_eq!(client.options, Some(options));
    }

    #[test]
    fn test_client_builder_minimal() {
        let path = PathBuf::from("file:///data/");
        let client = ClientBuilder::new()
            .path(path.clone())
            .build()
            .unwrap();

        assert_eq!(client.path, path);
        assert!(client.credentials.is_none());
        assert!(client.options.is_none());
        assert!(client.context.is_none());
    }

    #[test]
    fn test_error_display() {
        let err = Error::MissingRequiredAttribute("test_field".to_string());
        assert!(err.to_string().contains("Missing required attribute"));

        let err = Error::EmptyPath();
        assert!(err.to_string().contains("No path provided"));
    }

    #[test]  
    fn test_client_context_structure() {
        // Test that Context has the expected fields
        // We can't easily test the actual connection without real object stores,
        // but we can verify the structure exists
        let path = PathBuf::from("file:///tmp/");
        let client = ClientBuilder::new().path(path).build().unwrap();
        
        // Context should be None before connecting
        assert!(client.context.is_none());
    }
}
