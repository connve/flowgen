use crate::config::S3Options;
use object_store::aws::AmazonS3Builder;
use object_store::{parse_url_opts, path::Path, ObjectStore};
use std::path::PathBuf;
use url::Url;

/// An AWS credentials file for `s3` URLs.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AwsCredentials {
    aws_access_key_id: String,
    aws_secret_access_key: String,
    aws_session_token: Option<String>,
}

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
    #[error("Failed to read credentials: {source}")]
    Credentials {
        #[source]
        source: flowgen_core::credentials::Error,
    },
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
    /// Options of `s3://` paths.
    s3: S3Options,
    /// Initialized object store context after connection.
    pub context: Option<Context>,
}

impl Client {
    /// Establishes connection to the object store.
    async fn create_context(&self) -> Result<Context, Error> {
        let path = self.path.to_str().ok_or(Error::EmptyPath)?;
        let url = Url::parse(path).map_err(|source| Error::ParseUrl { source })?;
        let options = match (url.scheme(), &self.credentials_path) {
            ("s3" | "s3a", _) => return self.create_s3_context(&url).await,
            ("gs", Some(credentials_path)) => vec![(
                "google_service_account_key",
                flowgen_core::credentials::read_async(credentials_path)
                    .await
                    .map_err(|source| Error::Credentials { source })?,
            )],
            _ => Vec::new(),
        };
        let (object_store, path) =
            parse_url_opts(&url, options).map_err(|e| Error::ObjectStore { source: e })?;
        Ok(Context { object_store, path })
    }

    /// Creates an S3 context using `from_env()` to pick up IRSA, EKS Pod Identity,
    /// instance profiles, and env var credentials automatically.
    async fn create_s3_context(&self, url: &Url) -> Result<Context, Error> {
        let path =
            Path::from_url_path(url.path()).map_err(|e| Error::ObjectStore { source: e.into() })?;

        let mut builder = AmazonS3Builder::from_env()
            .with_url(url.to_string())
            .with_virtual_hosted_style_request(virtual_hosted_style_request(&self.s3));
        if let Some(region) = &self.s3.region {
            builder = builder.with_region(region);
        }
        if let Some(endpoint) = &self.s3.endpoint {
            builder = builder.with_endpoint(endpoint);
        }
        if let Some(allow_http) = self.s3.allow_http {
            builder = builder.with_allow_http(allow_http);
        }
        if let Some(credentials_path) = &self.credentials_path {
            let content = flowgen_core::credentials::read_async(credentials_path)
                .await
                .map_err(|source| Error::Credentials { source })?;
            let credentials: AwsCredentials = serde_json::from_str(&content).map_err(Error::s3)?;
            builder = builder
                .with_access_key_id(credentials.aws_access_key_id)
                .with_secret_access_key(credentials.aws_secret_access_key);
            if let Some(token) = credentials.aws_session_token {
                builder = builder.with_token(token);
            }
        }

        let object_store = builder
            .build()
            .map_err(|e| Error::ObjectStore { source: e })?;

        Ok(Context {
            object_store: Box::new(object_store),
            path,
        })
    }

    /// Reconnects to the object store, refreshing credentials.
    ///
    /// This creates a new connection with fresh credentials, which is necessary
    /// because cloud provider tokens expire (GCS OAuth ~1 hour, AWS STS
    /// session tokens ~12 hours). Without reconnecting, operations will fail
    /// with authentication errors.
    pub async fn reconnect(&mut self) -> Result<(), Error> {
        self.context = Some(self.create_context().await?);
        Ok(())
    }

    /// Checks if an error is an authentication error that should trigger a reconnect.
    pub fn is_auth_error(err: &object_store::Error) -> bool {
        matches!(err, object_store::Error::Unauthenticated { .. })
    }
}

/// `virtual_hosted_style_request` if set, else `true` without an `endpoint` and `false` with one.
fn virtual_hosted_style_request(s3: &S3Options) -> bool {
    match (s3.virtual_hosted_style_request, &s3.endpoint) {
        (Some(virtual_hosted), _) => virtual_hosted,
        (None, Some(_)) => false,
        (None, None) => true,
    }
}

impl flowgen_core::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Client, Error> {
        self.context = Some(self.create_context().await?);
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
    /// Options of `s3://` paths.
    s3: S3Options,
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

    /// Sets the options of `s3://` paths.
    pub fn s3(mut self, s3: S3Options) -> Self {
        self.s3 = s3;
        self
    }

    /// Builds the Client instance, validating required fields.
    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            path: self
                .path
                .ok_or_else(|| Error::MissingBuilderAttribute("path".to_string()))?,
            credentials_path: self.credentials_path,
            s3: self.s3,
            context: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_endpoint_puts_the_bucket_in_the_path_unless_set_otherwise() {
        let endpoint = Some("http://localhost:9000".to_string());
        let options = |endpoint: Option<String>, virtual_hosted: Option<bool>| S3Options {
            endpoint,
            virtual_hosted_style_request: virtual_hosted,
            ..S3Options::default()
        };

        assert!(virtual_hosted_style_request(&options(None, None)));
        assert!(!virtual_hosted_style_request(&options(
            endpoint.clone(),
            None
        )));
        assert!(virtual_hosted_style_request(&options(endpoint, Some(true))));
        assert!(!virtual_hosted_style_request(&options(None, Some(false))));
    }
}
