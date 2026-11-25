//! Shared credential types for authenticating with external services.
//!
//! Provides a unified credentials format that can be loaded from JSON files
//! and reused across HTTP requests, MCP connections, and other integrations.
//!
//! Three auth modes are supported:
//!
//! - **Bearer** (`bearer_auth`) — a static token in `Authorization: Bearer <token>`.
//! - **Basic** (`basic_auth`) — username/password in `Authorization: Basic <base64>`.
//! - **OAuth 2.0 client credentials** (`oauth2_client_credentials`) — performs
//!   the [client credentials flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4)
//!   against a token endpoint, caches the resulting access token, and
//!   auto-refreshes when it expires.

use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::sync::Mutex;

/// Errors that can occur during credential loading.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to read credentials file at {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file at {path}: {source}")]
    ParseFile {
        path: String,
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to read the credentials for {path} from the system keychain: {source}")]
    Keychain {
        path: String,
        #[source]
        source: keyring_core::Error,
    },
    #[error(
        "The credentials at {path} are encrypted with SOPS, but {} names no age key",
        crate::sops::AGE_KEY_FILE_ENV
    )]
    NoAgeKey { path: String },
    #[error("Failed to read the age key {key}: {message}")]
    AgeKey { key: String, message: String },
    #[error("Failed to decrypt the credentials at {path}: {source}")]
    Decrypt {
        path: String,
        #[source]
        source: crate::sops::Error,
    },
    #[error("The Secret beside {path} does not hold its file")]
    NotInSecret { path: String },
}

/// `content` read from `path`, decrypted in memory when SOPS encrypted it,
/// with the age key [`crate::sops::AGE_KEY_FILE_ENV`] names.
fn decrypted(path: &Path, content: String) -> Result<String, Error> {
    match crate::sops::is_encrypted(&content) {
        true => decrypt(path, &content),
        false => Ok(content),
    }
}

/// The file `path` names inside the encrypted Secret beside it, as a
/// workspace holds credentials: `a.json` in `a.yaml`. `None` when there is
/// no such Secret.
fn from_secret(path: &Path) -> Option<Result<String, Error>> {
    let file = path.file_name()?.to_str()?;
    let content = std::fs::read_to_string(path.with_extension("yaml")).ok()?;
    if !crate::sops::is_encrypted(&content) {
        return None;
    }
    let missing = || Error::NotInSecret {
        path: path.display().to_string(),
    };
    let found = decrypt(path, &content).and_then(|document| {
        let document: serde_json::Value =
            serde_json::from_str(&document).map_err(|source| Error::ParseFile {
                path: path.display().to_string(),
                source,
            })?;
        match (
            document.pointer(&format!("/stringData/{file}")),
            document.pointer(&format!("/data/{file}")),
        ) {
            (Some(serde_json::Value::String(text)), _) => Ok(text.clone()),
            (_, Some(serde_json::Value::String(encoded))) => {
                use base64::Engine as _;
                base64::engine::general_purpose::STANDARD
                    .decode(encoded)
                    .ok()
                    .and_then(|bytes| String::from_utf8(bytes).ok())
                    .ok_or_else(missing)
            }
            _ => Err(missing()),
        }
    });
    Some(found)
}

/// Whether [`read`] finds credentials for `path`, without reading them.
pub fn exists(path: &Path) -> bool {
    if path.exists() || path.with_extension("yaml").exists() {
        return true;
    }
    match keychain_entry(path) {
        Some(entry) => entry.get_password().is_ok(),
        None => false,
    }
}

/// The variable naming the folder of the credentials added in the web UI,
/// which flows read as `{{env.FLOWGEN_CREDENTIALS}}/<name>.json`.
pub const FOLDER_ENV: &str = "FLOWGEN_CREDENTIALS";

/// `content`, encrypted with SOPS, decrypted with the age key
/// [`crate::sops::AGE_KEY_FILE_ENV`] names.
fn decrypt(path: &Path, content: &str) -> Result<String, Error> {
    let shown = || path.display().to_string();
    let key = match std::env::var_os(crate::sops::AGE_KEY_FILE_ENV) {
        Some(key) if !key.is_empty() => std::path::PathBuf::from(key),
        _ => return Err(Error::NoAgeKey { path: shown() }),
    };
    let key_error = |message: String| Error::AgeKey {
        key: key.display().to_string(),
        message,
    };
    let file = std::fs::File::open(&key).map_err(|e| key_error(e.to_string()))?;
    let identities = age::IdentityFile::from_buffer(std::io::BufReader::new(file))
        .map_err(|e| key_error(e.to_string()))?
        .into_identities()
        .map_err(|e| key_error(e.to_string()))?;
    crate::sops::decrypt(content, &identities).map_err(|source| Error::Decrypt {
        path: shown(),
        source,
    })
}

/// The service flowgen keeps credentials under in the system keychain.
pub const KEYCHAIN_SERVICE: &str = "flowgen";

/// Uses the system keychain for credentials without a file; call once at startup.
pub fn use_system_keychain() -> Result<(), keyring_core::Error> {
    #[cfg(target_os = "macos")]
    let store = apple_native_keyring_store::keychain::Store::new()?;
    #[cfg(target_os = "windows")]
    let store = windows_native_keyring_store::Store::new()?;
    #[cfg(all(unix, not(target_os = "macos")))]
    let store = zbus_secret_service_keyring_store::Store::new()?;
    keyring_core::set_default_store(store);
    Ok(())
}

/// The keychain entry of the credentials at `path`, named by its absolute path;
/// none without a keychain.
fn keychain_entry(path: &Path) -> Option<keyring_core::Entry> {
    let account = std::path::absolute(path).ok()?;
    keyring_core::Entry::new(KEYCHAIN_SERVICE, &account.to_string_lossy()).ok()
}

/// Reads the credentials at `path`: the file, the file inside the encrypted
/// Secret beside it, or, when there is neither, its entry in the system
/// keychain. Anything SOPS encrypted is decrypted in memory.
pub fn read(path: &Path) -> Result<String, Error> {
    let file_error = |source| Error::ReadFile {
        path: path.display().to_string(),
        source,
    };
    let content = match std::fs::read_to_string(path) {
        Ok(content) => Ok(content),
        Err(source) if source.kind() == std::io::ErrorKind::NotFound => {
            if let Some(found) = from_secret(path) {
                return found;
            }
            match keychain_entry(path).map(|entry| entry.get_password()) {
                Some(Ok(content)) => Ok(content),
                Some(Err(keyring_core::Error::NoEntry)) | None => Err(file_error(source)),
                Some(Err(source)) => Err(Error::Keychain {
                    path: path.display().to_string(),
                    source,
                }),
            }
        }
        Err(source) => Err(file_error(source)),
    }?;
    decrypted(path, content)
}

/// Keeps `content` as the credentials at `path` in the system keychain, where
/// [`read`] finds them while no file is there.
pub fn keep(path: &Path, content: &str) -> Result<(), Error> {
    let keychain_error = |source| Error::Keychain {
        path: path.display().to_string(),
        source,
    };
    let account = std::path::absolute(path).map_err(|source| Error::ReadFile {
        path: path.display().to_string(),
        source,
    })?;
    keyring_core::Entry::new(KEYCHAIN_SERVICE, &account.to_string_lossy())
        .and_then(|entry| entry.set_password(content))
        .map_err(keychain_error)
}

/// [`read`] on a blocking thread, as the keychain and the file system block.
pub async fn read_async(path: &Path) -> Result<String, Error> {
    let owned = path.to_path_buf();
    match tokio::task::spawn_blocking(move || read(&owned)).await {
        Ok(read) => read,
        Err(join) => Err(Error::ReadFile {
            path: path.display().to_string(),
            source: std::io::Error::other(join),
        }),
    }
}

/// Errors that can occur during OAuth 2.0 token fetch.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OAuth2Error {
    #[error("Failed to send token request to {token_url}: {source}")]
    TokenRequest {
        token_url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Token endpoint at {token_url} returned status {status}: {body}")]
    TokenResponse {
        token_url: String,
        status: u16,
        body: String,
    },
    #[error(
        "Token response from {token_url} is missing `access_token`, or body is not valid JSON"
    )]
    InvalidTokenResponse {
        token_url: String,
        #[source]
        source: serde_json::Error,
    },
}

/// HTTP authentication credentials loaded from a JSON file.
///
/// Bearer wins over Basic when both are present, matching HTTP semantics
/// (a single request carries one `Authorization` header). OAuth 2.0 client
/// credentials take precedence over both — the token is fetched and cached
/// on first use, then auto-refreshed when it expires.
///
/// The JSON file format:
///
/// ```json
/// {
///   "bearer_auth": "my-secret-token",
///   "basic_auth": {
///     "username": "user",
///     "password": "pass"
///   },
///   "oauth2_client_credentials": {
///     "token_url": "https://auth.example.com/oauth2/token",
///     "client_id": "my-client-id",
///     "client_secret": "my-client-secret",
///     "scope": "api"
///   }
/// }
/// ```
#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpCredentials {
    /// Bearer token for `Authorization: Bearer <token>` header.
    #[serde(default)]
    pub bearer_auth: Option<String>,
    /// Basic authentication credentials.
    #[serde(default)]
    pub basic_auth: Option<BasicAuth>,
    /// OAuth 2.0 client credentials flow configuration. When set, takes
    /// precedence over `bearer_auth` and `basic_auth`.
    #[serde(default)]
    pub oauth2_client_credentials: Option<OAuth2ClientCredentials>,
    /// Cached OAuth 2.0 token state. Populated lazily on first
    /// `authorization_header_async` call; not serialized or deserialized.
    #[serde(skip)]
    oauth2_cache: Mutex<Option<CachedToken>>,
}

impl PartialEq for HttpCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.bearer_auth == other.bearer_auth
            && self.basic_auth == other.basic_auth
            && self.oauth2_client_credentials == other.oauth2_client_credentials
    }
}

impl Clone for HttpCredentials {
    fn clone(&self) -> Self {
        Self {
            bearer_auth: self.bearer_auth.clone(),
            basic_auth: self.basic_auth.clone(),
            oauth2_client_credentials: self.oauth2_client_credentials.clone(),
            oauth2_cache: Mutex::new(None),
        }
    }
}

/// Basic authentication username and password.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BasicAuth {
    /// Username for basic authentication.
    pub username: String,
    /// Password for basic authentication.
    pub password: String,
}

/// OAuth 2.0 client credentials flow configuration.
///
/// Performs the [client credentials flow] to obtain an access token from
/// the token endpoint. The token is cached and auto-refreshed when it
/// expires. Takes precedence over `bearer_auth` and `basic_auth` when
/// present in the credentials file.
///
/// [client credentials flow]: https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OAuth2ClientCredentials {
    /// Token endpoint URL (e.g. `https://auth.example.com/oauth2/token`).
    pub token_url: String,
    /// OAuth 2.0 client ID.
    pub client_id: String,
    /// OAuth 2.0 client secret.
    pub client_secret: String,
    /// Optional space-delimited scope(s) to request.
    #[serde(default)]
    pub scope: Option<String>,
}

/// A cached OAuth 2.0 token with its expiry time.
#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    expires_at: std::time::Instant,
}

impl CachedToken {
    /// Returns `true` if the token is still valid, keeping
    /// [`TOKEN_EXPIRY_BUFFER_SECS`] of headroom before its stated expiry.
    fn is_valid(&self) -> bool {
        self.expires_at
            .checked_duration_since(std::time::Instant::now())
            .is_some_and(|remaining| remaining.as_secs() > TOKEN_EXPIRY_BUFFER_SECS)
    }
}

/// Response body from a standard OAuth 2.0 token endpoint.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// RFC 6749 marks this RECOMMENDED, not required, so issuers may omit it.
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Assumed token lifetime when the endpoint omits `expires_in`.
const DEFAULT_TOKEN_LIFETIME_SECS: u64 = 300;

/// Refresh this long before stated expiry, so a token never lapses mid-flight.
const TOKEN_EXPIRY_BUFFER_SECS: u64 = 60;

impl HttpCredentials {
    /// Creates credentials with a static bearer token.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            bearer_auth: Some(token.into()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        }
    }

    /// Creates credentials with basic authentication.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            bearer_auth: None,
            basic_auth: Some(BasicAuth {
                username: username.into(),
                password: password.into(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        }
    }

    /// Creates credentials with OAuth 2.0 client credentials flow.
    pub fn oauth2(
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        scope: Option<String>,
    ) -> Self {
        Self {
            bearer_auth: None,
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: token_url.into(),
                client_id: client_id.into(),
                client_secret: client_secret.into(),
                scope,
            }),
            oauth2_cache: Mutex::new(None),
        }
    }

    /// Returns the value for an outgoing `Authorization` header synchronously.
    ///
    /// Only handles static `bearer_auth` and `basic_auth`. For OAuth 2.0
    /// client credentials, use [`authorization_header_async`](Self::authorization_header_async)
    /// which can perform network I/O to fetch/refresh the token.
    ///
    /// Bearer wins over Basic when both are present, matching HTTP semantics
    /// (a single request carries one `Authorization` header).
    ///
    /// Used by transports that expose raw header injection rather than a
    /// reqwest `RequestBuilder`, so basic-auth encoding is not re-implemented
    /// per call site.
    pub fn authorization_header(&self) -> Option<String> {
        use base64::Engine;
        match (&self.bearer_auth, &self.basic_auth) {
            (Some(token), _) => Some(format!("Bearer {token}")),
            (None, Some(basic)) => {
                let raw = format!("{}:{}", basic.username, basic.password);
                let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
                Some(format!("Basic {encoded}"))
            }
            (None, None) => None,
        }
    }

    /// Returns the value for an outgoing `Authorization` header, fetching
    /// or refreshing an OAuth 2.0 token when `oauth2_client_credentials` is
    /// configured.
    ///
    /// Precedence: OAuth 2.0 client credentials > Bearer > Basic > None.
    pub async fn authorization_header_async(&self) -> Result<Option<String>, OAuth2Error> {
        match &self.oauth2_client_credentials {
            Some(oauth2) => {
                let token = self.get_or_refresh_token(oauth2).await?;
                Ok(Some(format!("Bearer {token}")))
            }
            None => Ok(self.authorization_header()),
        }
    }

    /// Returns a cached token if still valid, otherwise fetches a new one
    /// from the token endpoint.
    async fn get_or_refresh_token(
        &self,
        oauth2: &OAuth2ClientCredentials,
    ) -> Result<String, OAuth2Error> {
        // Held across the fetch so concurrent callers on a cold or expired
        // cache issue one token request between them, not one each — token
        // endpoints are commonly rate-limited.
        let mut guard = self.oauth2_cache.lock().await;

        if let Some(cached) = guard.as_ref() {
            if cached.is_valid() {
                return Ok(cached.token.clone());
            }
        }

        let token = self.fetch_token(oauth2).await?;
        *guard = Some(token.clone());

        Ok(token.token)
    }

    /// Fetches a new access token from the OAuth 2.0 token endpoint using
    /// the client credentials grant type.
    async fn fetch_token(
        &self,
        oauth2: &OAuth2ClientCredentials,
    ) -> Result<CachedToken, OAuth2Error> {
        let client = reqwest::Client::new();
        let mut form = vec![
            ("grant_type".to_string(), "client_credentials".to_string()),
            ("client_id".to_string(), oauth2.client_id.clone()),
            ("client_secret".to_string(), oauth2.client_secret.clone()),
        ];
        if let Some(scope) = &oauth2.scope {
            form.push(("scope".to_string(), scope.clone()));
        }

        let response = client
            .post(&oauth2.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|source| OAuth2Error::TokenRequest {
                token_url: oauth2.token_url.clone(),
                source,
            })?;

        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status != 200 {
            return Err(OAuth2Error::TokenResponse {
                token_url: oauth2.token_url.clone(),
                status,
                body,
            });
        }

        let token_response: TokenResponse =
            serde_json::from_str(&body).map_err(|source| OAuth2Error::InvalidTokenResponse {
                token_url: oauth2.token_url.clone(),
                source,
            })?;

        let lifetime = token_response
            .expires_in
            .unwrap_or(DEFAULT_TOKEN_LIFETIME_SECS);
        let expires_at = std::time::Instant::now() + std::time::Duration::from_secs(lifetime);

        Ok(CachedToken {
            token: token_response.access_token,
            expires_at,
        })
    }
}

/// Secrets for the web UI's OIDC login, as stored on disk.
///
/// ```json
/// {
///   "client_secret": "the-oidc-client-secret",
///   "cookie_secret": "a long random string, at least 32 bytes"
/// }
/// ```
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebCredentials {
    /// OIDC client secret, for `auth.client_secret`.
    #[serde(default)]
    pub client_secret: Option<SecretString>,
    /// Key encrypting the browser session cookie, for `web.cookie_secret`.
    #[serde(default)]
    pub cookie_secret: Option<SecretString>,
}

/// Loads and parses web UI credentials from a JSON file.
pub async fn load_web_credentials(path: &Path) -> Result<WebCredentials, Error> {
    load_credentials(path).await
}

/// Shortest API key flowgen accepts as a bearer, so a key can't be guessed.
pub const MIN_API_KEY_LEN: usize = 32;

/// Machine keys accepted by the web API, as stored on disk.
///
/// ```json
/// { "api_keys": [{ "name": "operator-agent", "key": "a long random string" }] }
/// ```
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiCredentials {
    /// Keys accepted by the web API.
    #[serde(default)]
    pub api_keys: Vec<ApiKey>,
}

/// A named machine key; the name identifies the caller in logs and audit.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ApiKey {
    /// Caller name recorded in logs and audit entries.
    pub name: String,
    /// Secret the caller presents.
    pub key: SecretString,
}

/// Loads and parses web API machine keys from a JSON file.
pub async fn load_api_credentials(path: &Path) -> Result<ApiCredentials, Error> {
    load_credentials(path).await
}

/// Loads and parses HTTP credentials from a JSON file.
pub async fn load_http_credentials(path: &Path) -> Result<HttpCredentials, Error> {
    load_credentials(path).await
}

/// Loads and parses any credentials type from a JSON file, or its keychain entry.
pub async fn load_credentials<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, Error> {
    let content = read_async(path).await?;
    serde_json::from_str(&content).map_err(|source| Error::ParseFile {
        path: path.display().to_string(),
        source,
    })
}

/// Whether `content` is a service account key, with `client_email` and `private_key`.
pub fn check_service_account(content: &str) -> Result<(), String> {
    match serde_json::from_str::<serde_json::Value>(content) {
        Ok(key) => match (key.get("client_email"), key.get("private_key")) {
            (Some(serde_json::Value::String(_)), Some(serde_json::Value::String(_))) => Ok(()),
            _ => Err("A service account key needs `client_email` and `private_key`".to_string()),
        },
        Err(error) => Err(error.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypted_credentials_are_read_decrypted_with_the_age_key() {
        let folder = tempfile::tempdir().unwrap();
        let identity = age::x25519::Identity::generate();
        let key = folder.path().join("age.key");
        std::fs::write(
            &key,
            age::secrecy::ExposeSecret::expose_secret(&identity.to_string()),
        )
        .unwrap();
        let file = folder.path().join("a.json");
        let encrypted =
            crate::sops::encrypt(r#"{"api_key": "secret"}"#, &[identity.to_public()]).unwrap();
        std::fs::write(&file, &encrypted).unwrap();
        let in_secret = folder.path().join("b.json");
        let secret = crate::sops::encrypt_secret(
            "b",
            "b.json",
            r#"{"api_key": "other"}"#,
            &[identity.to_public()],
        )
        .unwrap();
        std::fs::write(folder.path().join("b.yaml"), &secret).unwrap();
        std::env::set_var(crate::sops::AGE_KEY_FILE_ENV, &key);

        let read_file: serde_json::Value = serde_json::from_str(&read(&file).unwrap()).unwrap();
        let read_secret: serde_json::Value =
            serde_json::from_str(&read(&in_secret).unwrap()).unwrap();

        assert_eq!(read_file, serde_json::json!({"api_key": "secret"}));
        assert_eq!(read_secret, serde_json::json!({"api_key": "other"}));
        assert!(!secret.contains("other"));
    }

    /// A missing file at a new path, with the mock keychain in place.
    fn missing() -> std::path::PathBuf {
        static MOCK: std::sync::Once = std::sync::Once::new();
        MOCK.call_once(|| {
            keyring_core::set_default_store(keyring_core::mock::Store::new().unwrap())
        });
        std::env::temp_dir()
            .join(format!("flowgen_test_{}", uuid::Uuid::now_v7()))
            .join("credentials.json")
    }

    fn keep(path: &Path, content: &str) {
        super::keep(path, content).unwrap();
    }

    #[test]
    fn credentials_without_a_file_come_from_the_keychain() {
        let path = missing();
        keep(&path, r#"{"bearer_auth": "t"}"#);

        assert_eq!(read(&path).unwrap(), r#"{"bearer_auth": "t"}"#);
    }

    #[test]
    fn a_file_wins_over_the_keychain() {
        let path = missing();
        keep(&path, "keychain");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "file").unwrap();

        assert_eq!(read(&path).unwrap(), "file");
    }

    #[test]
    fn credentials_in_neither_are_a_missing_file() {
        let path = missing();

        match read(&path) {
            Err(Error::ReadFile { source, .. }) => {
                assert_eq!(source.kind(), std::io::ErrorKind::NotFound)
            }
            other => panic!("Expected a missing file, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn keychain_credentials_load_like_a_file() {
        let path = missing();
        keep(&path, r#"{"bearer_auth": "t"}"#);

        let credentials: HttpCredentials = load_credentials(&path).await.unwrap();
        assert_eq!(
            credentials.authorization_header().as_deref(),
            Some("Bearer t")
        );
    }

    #[test]
    fn a_misspelled_credentials_field_is_rejected() {
        let error = serde_json::from_str::<HttpCredentials>(r#"{"bearer_token": "t"}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown field `bearer_token`"), "{error}");
        let error = serde_json::from_str::<HttpCredentials>(
            r#"{"basic_auth": {"username": "u", "password": "p", "user": "x"}}"#,
        )
        .unwrap_err()
        .to_string();
        assert!(error.contains("unknown field `user`"), "{error}");
        let error = serde_json::from_str::<WebCredentials>(r#"{"client_secrets": "s"}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown field `client_secrets`"), "{error}");
    }

    #[test]
    fn authorization_header_bearer() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(creds.authorization_header().as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn authorization_header_basic() {
        let creds = HttpCredentials {
            bearer_auth: None,
            basic_auth: Some(BasicAuth {
                username: "Aladdin".to_string(),
                password: "open sesame".to_string(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(
            creds.authorization_header().as_deref(),
            Some("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="),
        );
    }

    #[test]
    fn authorization_header_bearer_wins() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: Some(BasicAuth {
                username: "u".to_string(),
                password: "p".to_string(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(creds.authorization_header().as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn authorization_header_none() {
        let creds = HttpCredentials::default();
        assert_eq!(creds.authorization_header(), None);
    }

    #[tokio::test]
    async fn authorization_header_async_bearer_no_oauth2() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        let result = creds.authorization_header_async().await;
        assert_eq!(result.unwrap().as_deref(), Some("Bearer tok"));
    }

    #[tokio::test]
    async fn authorization_header_async_basic_no_oauth2() {
        let creds = HttpCredentials {
            bearer_auth: None,
            basic_auth: Some(BasicAuth {
                username: "Aladdin".to_string(),
                password: "open sesame".to_string(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        let result = creds.authorization_header_async().await;
        assert_eq!(
            result.unwrap().as_deref(),
            Some("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="),
        );
    }

    #[tokio::test]
    async fn authorization_header_async_none() {
        let creds = HttpCredentials::default();
        let result = creds.authorization_header_async().await;
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn cached_token_is_valid_with_future_expiry() {
        let token = CachedToken {
            token: "abc".to_string(),
            expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
        };
        assert!(token.is_valid());
    }

    #[test]
    fn cached_token_is_invalid_when_past_expiry() {
        let token = CachedToken {
            token: "abc".to_string(),
            expires_at: std::time::Instant::now(),
        };
        assert!(!token.is_valid());
    }

    #[test]
    fn cached_token_is_invalid_when_within_buffer() {
        let token = CachedToken {
            token: "abc".to_string(),
            expires_at: std::time::Instant::now() + std::time::Duration::from_secs(30),
        };
        assert!(!token.is_valid());
    }

    #[test]
    fn http_credentials_partial_eq_ignores_cache() {
        let creds_a = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let creds_b = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(creds_a, creds_b);
    }

    #[test]
    fn http_credentials_clone_resets_cache() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let mut cloned = creds.clone();
        assert!(cloned.oauth2_cache.get_mut().is_none());
    }

    #[test]
    fn oauth2_client_credentials_deserialize() {
        let json = r#"{
            "token_url": "https://auth.example.com/oauth2/token",
            "client_id": "my-client-id",
            "client_secret": "my-client-secret",
            "scope": "api"
        }"#;
        let oauth2: OAuth2ClientCredentials = serde_json::from_str(json).unwrap();
        assert_eq!(oauth2.token_url, "https://auth.example.com/oauth2/token");
        assert_eq!(oauth2.client_id, "my-client-id");
        assert_eq!(oauth2.client_secret, "my-client-secret");
        assert_eq!(oauth2.scope.as_deref(), Some("api"));
    }

    #[test]
    fn oauth2_client_credentials_deserialize_without_scope() {
        let json = r#"{
            "token_url": "https://example.com/token",
            "client_id": "id",
            "client_secret": "secret"
        }"#;
        let oauth2: OAuth2ClientCredentials = serde_json::from_str(json).unwrap();
        assert!(oauth2.scope.is_none());
    }

    #[test]
    fn http_credentials_deserialize_with_oauth2() {
        let json = r#"{
            "oauth2_client_credentials": {
                "token_url": "https://example.com/token",
                "client_id": "id",
                "client_secret": "secret"
            }
        }"#;
        let creds: HttpCredentials = serde_json::from_str(json).unwrap();
        assert!(creds.bearer_auth.is_none());
        assert!(creds.basic_auth.is_none());
        assert!(creds.oauth2_client_credentials.is_some());
    }

    #[test]
    fn http_credentials_deserialize_with_bearer_only() {
        let json = r#"{
            "bearer_auth": "my-token"
        }"#;
        let creds: HttpCredentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.bearer_auth.as_deref(), Some("my-token"));
        assert!(creds.oauth2_client_credentials.is_none());
    }

    #[test]
    fn http_credentials_serde_roundtrip_preserves_oauth2() {
        let original = HttpCredentials {
            bearer_auth: None,
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "https://example.com/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: Some("api".to_string()),
            }),
            oauth2_cache: Mutex::new(None),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HttpCredentials = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn http_credentials_serde_skips_cache_field() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert!(!json.contains("oauth2_cache"));
    }

    #[test]
    fn bearer_constructor_sets_only_bearer() {
        let creds = HttpCredentials::bearer("my-token");
        assert_eq!(creds.bearer_auth.as_deref(), Some("my-token"));
        assert!(creds.basic_auth.is_none());
        assert!(creds.oauth2_client_credentials.is_none());
    }

    #[test]
    fn basic_constructor_sets_only_basic() {
        let creds = HttpCredentials::basic("user", "pass");
        assert!(creds.bearer_auth.is_none());
        let basic = creds.basic_auth.unwrap();
        assert_eq!(basic.username, "user");
        assert_eq!(basic.password, "pass");
        assert!(creds.oauth2_client_credentials.is_none());
    }

    #[tokio::test]
    async fn authorization_header_async_falls_back_to_bearer_when_no_oauth2() {
        let creds = HttpCredentials::bearer("static-tok");
        let result = creds.authorization_header_async().await.unwrap();
        assert_eq!(result.as_deref(), Some("Bearer static-tok"));
    }

    #[tokio::test]
    async fn authorization_header_async_skips_oauth_when_cache_pre_populated() {
        let creds = HttpCredentials {
            bearer_auth: Some("fallback".to_string()),
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "http://should-not-be-called.test/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: None,
            }),
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached-token".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let result = creds.authorization_header_async().await.unwrap();
        assert_eq!(result.as_deref(), Some("Bearer cached-token"));
    }

    #[tokio::test]
    async fn authorization_header_async_returns_oauth_error_when_token_expired_and_endpoint_unreachable(
    ) {
        let creds = HttpCredentials {
            bearer_auth: Some("fallback".to_string()),
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "http://127.0.0.1:1/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: None,
            }),
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "expired".to_string(),
                expires_at: std::time::Instant::now(),
            })),
        };
        let result = creds.authorization_header_async().await;
        assert!(result.is_err());
    }

    #[test]
    fn api_credentials_reject_unknown_fields() {
        let typo = serde_json::from_str::<ApiCredentials>(r#"{"api_key": []}"#);
        assert!(typo.is_err());
        let keys: ApiCredentials =
            serde_json::from_str(r#"{"api_keys": [{"name": "a", "key": "k"}]}"#).unwrap();
        assert_eq!(keys.api_keys[0].name, "a");
    }

    #[test]
    fn oauth2_precedence_over_bearer_in_doc() {
        let creds = HttpCredentials {
            bearer_auth: Some("static".to_string()),
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "http://example.com/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: None,
            }),
            oauth2_cache: Mutex::new(None),
        };
        assert!(creds.oauth2_client_credentials.is_some());
    }
}
