//! # Kafka Client
//!
//! Builds the Kafka client and loads SASL/SSL credentials from a JSON file.
//! The security protocol follows from which credential blocks the file
//! carries, so SASL over TLS works without spelling the protocol out.

use rskafka::client::{ClientBuilder, SaslConfig};
use rskafka::BackoffConfig;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use secrecy::{ExposeSecret, SecretString};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Default Kafka bootstrap brokers.
pub const DEFAULT_KAFKA_BROKERS: &str = "localhost:9092";
/// Default SASL mechanism.
pub const DEFAULT_SASL_MECHANISM: &str = "SCRAM-SHA-256";
/// Security protocol for SASL over TLS.
pub const SECURITY_PROTOCOL_SASL_SSL: &str = "SASL_SSL";
/// Security protocol for SASL without TLS.
pub const SECURITY_PROTOCOL_SASL_PLAINTEXT: &str = "SASL_PLAINTEXT";
/// Security protocol for TLS without SASL.
pub const SECURITY_PROTOCOL_SSL: &str = "SSL";
/// Security protocol without TLS or SASL.
pub const SECURITY_PROTOCOL_PLAINTEXT: &str = "PLAINTEXT";

#[derive(serde::Deserialize, Debug, Clone, Default)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    pub sasl: Option<SaslCredentials>,
    pub ssl: Option<SslCredentials>,
    #[serde(default)]
    pub security_protocol: Option<String>,
}

impl Credentials {
    fn security_protocol(&self) -> Option<&str> {
        if let Some(protocol) = &self.security_protocol {
            return Some(protocol);
        }
        match (&self.sasl, &self.ssl) {
            (Some(_), Some(_)) => Some(SECURITY_PROTOCOL_SASL_SSL),
            (Some(_), None) => Some(SECURITY_PROTOCOL_SASL_PLAINTEXT),
            (None, Some(_)) => Some(SECURITY_PROTOCOL_SSL),
            (None, None) => None,
        }
    }
}

#[derive(serde::Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SaslCredentials {
    pub username: String,
    pub password: SecretString,
    #[serde(default = "default_sasl_mechanism")]
    pub mechanism: String,
}

fn default_sasl_mechanism() -> String {
    DEFAULT_SASL_MECHANISM.to_string()
}

#[derive(serde::Deserialize, Debug, Clone)]
#[serde(deny_unknown_fields)]
pub struct SslCredentials {
    pub ca_location: Option<PathBuf>,
    pub certificate_location: Option<PathBuf>,
    pub key_location: Option<PathBuf>,
    #[serde(default)]
    pub key_password: Option<SecretString>,
}

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error reading credentials file '{path}': {source}")]
    ReadCredentials {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Error parsing credentials file: {source}")]
    ParseCredentials {
        #[source]
        source: serde_json::Error,
    },
    #[error("No authentication credentials provided")]
    NoCredentials,
    #[error("Unsupported security protocol '{protocol}', expected PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL")]
    UnsupportedSecurityProtocol { protocol: String },
    #[error("Security protocol {protocol} requires a `sasl` block in the credentials file")]
    MissingSaslCredentials { protocol: String },
    #[error(
        "Unsupported SASL mechanism '{mechanism}', expected PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512"
    )]
    UnsupportedSaslMechanism { mechanism: String },
    #[error("Error reading certificate '{path}': {source}")]
    ReadCertificate {
        path: PathBuf,
        #[source]
        source: rustls::pki_types::pem::Error,
    },
    #[error("Error reading private key '{path}': {source}")]
    ReadPrivateKey {
        path: PathBuf,
        #[source]
        source: rustls::pki_types::pem::Error,
    },
    #[error("Error decrypting private key '{path}': {source}")]
    DecryptPrivateKey {
        path: PathBuf,
        #[source]
        source: pkcs8::Error,
    },
    #[error(
        "Both `certificate_location` and `key_location` are required for a client certificate"
    )]
    IncompleteClientCertificate,
    #[error("No trusted root certificates found; set `ssl.ca_location`")]
    NoRootCertificates,
    #[error("CA file '{path}' contains no certificates")]
    EmptyCaFile { path: PathBuf },
    #[error("Error configuring TLS: {source}")]
    Tls {
        #[source]
        source: rustls::Error,
    },
    #[error("Error connecting to Kafka: {source}")]
    Connect {
        #[source]
        source: Box<rskafka::client::error::Error>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext,
    SaslSsl,
}

impl SecurityProtocol {
    fn parse(protocol: &str) -> Result<Self, Error> {
        match protocol.to_ascii_uppercase().as_str() {
            SECURITY_PROTOCOL_PLAINTEXT => Ok(Self::Plaintext),
            SECURITY_PROTOCOL_SSL => Ok(Self::Ssl),
            SECURITY_PROTOCOL_SASL_PLAINTEXT => Ok(Self::SaslPlaintext),
            SECURITY_PROTOCOL_SASL_SSL => Ok(Self::SaslSsl),
            _ => Err(Error::UnsupportedSecurityProtocol {
                protocol: protocol.to_string(),
            }),
        }
    }

    fn uses_tls(self) -> bool {
        matches!(self, Self::Ssl | Self::SaslSsl)
    }

    fn uses_sasl(self) -> bool {
        matches!(self, Self::SaslPlaintext | Self::SaslSsl)
    }
}

fn read_credentials(path: &PathBuf) -> Result<Credentials, Error> {
    let raw = std::fs::read_to_string(path).map_err(|source| Error::ReadCredentials {
        path: path.clone(),
        source,
    })?;
    serde_json::from_str(&raw).map_err(|source| Error::ParseCredentials { source })
}

fn sasl_config(sasl: &SaslCredentials) -> Result<SaslConfig, Error> {
    let credentials = rskafka::client::Credentials::new(
        sasl.username.clone(),
        sasl.password.expose_secret().to_string(),
    );
    match sasl.mechanism.to_ascii_uppercase().as_str() {
        "PLAIN" => Ok(SaslConfig::Plain(credentials)),
        "SCRAM-SHA-256" => Ok(SaslConfig::ScramSha256(credentials)),
        "SCRAM-SHA-512" => Ok(SaslConfig::ScramSha512(credentials)),
        _ => Err(Error::UnsupportedSaslMechanism {
            mechanism: sasl.mechanism.clone(),
        }),
    }
}

fn certificates(path: &Path) -> Result<Vec<CertificateDer<'static>>, Error> {
    let read_error = |source| Error::ReadCertificate {
        path: path.to_path_buf(),
        source,
    };
    CertificateDer::pem_file_iter(path)
        .map_err(read_error)?
        .collect::<Result<Vec<_>, _>>()
        .map_err(read_error)
}

/// PEM label of an encrypted PKCS#8 private key.
const ENCRYPTED_PRIVATE_KEY_LABEL: &str = "ENCRYPTED PRIVATE KEY";

/// Loads a PEM private key, decrypting it with `password` when it is an
/// encrypted PKCS#8 key. The password is ignored for an unencrypted key.
fn private_key(path: &Path, password: Option<&str>) -> Result<PrivateKeyDer<'static>, Error> {
    let pem = std::fs::read_to_string(path).map_err(|source| Error::ReadPrivateKey {
        path: path.to_path_buf(),
        source: rustls::pki_types::pem::Error::Io(source),
    })?;
    let decrypt_error = |source| Error::DecryptPrivateKey {
        path: path.to_path_buf(),
        source,
    };
    let password = match (password, pkcs8::der::pem::decode_label(pem.as_bytes())) {
        (Some(password), Ok(ENCRYPTED_PRIVATE_KEY_LABEL)) => password,
        _ => {
            return PrivateKeyDer::from_pem_slice(pem.as_bytes()).map_err(|source| {
                Error::ReadPrivateKey {
                    path: path.to_path_buf(),
                    source,
                }
            })
        }
    };
    let (_, document) = pkcs8::Document::from_pem(&pem)
        .map_err(|source| decrypt_error(pkcs8::Error::Asn1(source)))?;
    let key = pkcs8::EncryptedPrivateKeyInfo::try_from(document.as_bytes())
        .map_err(decrypt_error)?
        .decrypt(password)
        .map_err(decrypt_error)?;
    Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        key.as_bytes().to_vec(),
    )))
}

fn tls_config(ssl: Option<&SslCredentials>) -> Result<Arc<rustls::ClientConfig>, Error> {
    let client_auth = match ssl {
        Some(ssl) => (
            ssl.certificate_location.as_deref(),
            ssl.key_location.as_deref(),
        ),
        None => (None, None),
    };
    if let (Some(_), None) | (None, Some(_)) = client_auth {
        return Err(Error::IncompleteClientCertificate);
    }

    let mut roots = rustls::RootCertStore::empty();
    match ssl.and_then(|ssl| ssl.ca_location.as_deref()) {
        Some(ca) => {
            let ca_certificates = certificates(ca)?;
            if ca_certificates.is_empty() {
                return Err(Error::EmptyCaFile {
                    path: ca.to_path_buf(),
                });
            }
            for certificate in ca_certificates {
                roots
                    .add(certificate)
                    .map_err(|source| Error::Tls { source })?;
            }
        }
        None => {
            let (added, _) =
                roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
            if added == 0 {
                return Err(Error::NoRootCertificates);
            }
        }
    }

    let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|source| Error::Tls { source })?
    .with_root_certificates(roots);

    let config = match client_auth {
        (Some(certificate), Some(key)) => builder
            .with_client_auth_cert(
                certificates(certificate)?,
                private_key(
                    key,
                    ssl.and_then(|ssl| ssl.key_password.as_ref())
                        .map(ExposeSecret::expose_secret),
                )?,
            )
            .map_err(|source| Error::Tls { source })?,
        _ => builder.with_no_client_auth(),
    };
    Ok(Arc::new(config))
}

/// Caps a duration at what the Kafka protocol accepts.
///
/// Its timeouts are i32 milliseconds, so a longer duration would overflow
/// into a negative number nobody configured.
pub fn clamp_timeout(timeout: Duration) -> Duration {
    timeout.min(Duration::from_millis(i32::MAX as u64))
}

/// Port a broker address without one connects to.
const DEFAULT_BROKER_PORT: u16 = 9092;

/// The broker address with the default port added when it has none.
fn with_default_port(broker: &str) -> String {
    match broker.rsplit_once(':') {
        Some((_, port)) if port.parse::<u16>().is_ok() => broker.to_string(),
        _ => format!("{broker}:{DEFAULT_BROKER_PORT}"),
    }
}

/// Builds a client builder from a broker list and an optional credentials path.
///
/// `timeout` bounds connecting, each request, and the backoff between retries.
pub fn client_builder(
    credentials_path: &Option<PathBuf>,
    brokers: &str,
    timeout: Duration,
) -> Result<ClientBuilder, Error> {
    let timeout = clamp_timeout(timeout);
    let brokers = brokers
        .split(',')
        .map(str::trim)
        .filter(|broker| !broker.is_empty())
        .map(with_default_port)
        .collect();
    let mut builder = ClientBuilder::new(brokers)
        .timeout(Some(timeout))
        .connect_timeout(Some(timeout))
        .backoff_config(BackoffConfig {
            deadline: Some(timeout),
            ..Default::default()
        });

    let Some(path) = credentials_path else {
        return Ok(builder);
    };
    let credentials = read_credentials(path)?;
    let protocol_name = credentials
        .security_protocol()
        .ok_or(Error::NoCredentials)?;
    let protocol = SecurityProtocol::parse(protocol_name)?;
    if protocol.uses_tls() {
        builder = builder.tls_config(tls_config(credentials.ssl.as_ref())?);
    }
    if protocol.uses_sasl() {
        let sasl = credentials
            .sasl
            .as_ref()
            .ok_or_else(|| Error::MissingSaslCredentials {
                protocol: protocol_name.to_string(),
            })?;
        builder = builder.sasl_config(sasl_config(sasl)?);
    }
    Ok(builder)
}

pub struct Client {
    credentials_path: Option<PathBuf>,
    brokers: Option<String>,
    timeout: Duration,
    pub client: Option<rskafka::client::Client>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("credentials_path", &self.credentials_path)
            .field("brokers", &self.brokers)
            .field("connected", &self.client.is_some())
            .finish()
    }
}

impl flowgen_core::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Self, Error> {
        let brokers = match &self.brokers {
            Some(brokers) => brokers.clone(),
            None => DEFAULT_KAFKA_BROKERS.to_string(),
        };
        let client = client_builder(&self.credentials_path, &brokers, self.timeout)?
            .build()
            .await
            .map_err(|source| Error::Connect {
                source: Box::new(source),
            })?;
        self.client = Some(client);
        Ok(self)
    }
}

impl Client {
    pub fn new(
        credentials_path: Option<PathBuf>,
        brokers: Option<String>,
        timeout: Duration,
    ) -> Self {
        Self {
            credentials_path,
            brokers,
            timeout,
            client: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sasl() -> SaslCredentials {
        SaslCredentials {
            username: "u".to_string(),
            password: SecretString::from("p"),
            mechanism: default_sasl_mechanism(),
        }
    }

    fn ssl() -> SslCredentials {
        SslCredentials {
            ca_location: Some(PathBuf::from("/ca.pem")),
            certificate_location: None,
            key_location: None,
            key_password: None,
        }
    }

    fn credentials_file(contents: &str) -> (tempfile::TempDir, Option<PathBuf>) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("credentials.json");
        std::fs::write(&path, contents).unwrap();
        (dir, Some(path))
    }

    #[test]
    fn test_security_protocol_from_blocks() {
        let cases = [
            (Some(sasl()), Some(ssl()), Some(SECURITY_PROTOCOL_SASL_SSL)),
            (Some(sasl()), None, Some(SECURITY_PROTOCOL_SASL_PLAINTEXT)),
            (None, Some(ssl()), Some(SECURITY_PROTOCOL_SSL)),
            (None, None, None),
        ];

        for (sasl, ssl, expected) in cases {
            let credentials = Credentials {
                sasl,
                ssl,
                security_protocol: None,
            };
            assert_eq!(credentials.security_protocol(), expected);
        }
    }

    #[test]
    fn test_security_protocol_override_wins() {
        let credentials = Credentials {
            sasl: Some(sasl()),
            ssl: Some(ssl()),
            security_protocol: Some("SASL_PLAINTEXT".to_string()),
        };

        assert_eq!(credentials.security_protocol(), Some("SASL_PLAINTEXT"));
    }

    #[test]
    fn test_security_protocol_parses_case_insensitively() {
        assert_eq!(
            SecurityProtocol::parse("sasl_ssl").unwrap(),
            SecurityProtocol::SaslSsl
        );
        assert!(matches!(
            SecurityProtocol::parse("KERBEROS"),
            Err(Error::UnsupportedSecurityProtocol { .. })
        ));
    }

    #[test]
    fn test_sasl_mechanism_defaults() {
        let credentials: Credentials =
            serde_json::from_str(r#"{ "sasl": { "username": "u", "password": "p" } }"#).unwrap();

        assert_eq!(
            credentials.sasl.map(|s| s.mechanism),
            Some(DEFAULT_SASL_MECHANISM.to_string())
        );
    }

    #[test]
    fn test_sasl_mechanisms_map_to_client_configs() {
        let mechanism = |mechanism: &str| SaslCredentials {
            mechanism: mechanism.to_string(),
            ..sasl()
        };

        assert!(matches!(
            sasl_config(&mechanism("plain")),
            Ok(SaslConfig::Plain(_))
        ));
        assert!(matches!(
            sasl_config(&mechanism("SCRAM-SHA-256")),
            Ok(SaslConfig::ScramSha256(_))
        ));
        assert!(matches!(
            sasl_config(&mechanism("SCRAM-SHA-512")),
            Ok(SaslConfig::ScramSha512(_))
        ));
        assert!(matches!(
            sasl_config(&mechanism("GSSAPI")),
            Err(Error::UnsupportedSaslMechanism { .. })
        ));
    }

    #[test]
    fn test_clamp_timeout_leaves_usable_durations_alone() {
        let timeout = Duration::from_secs(30);
        assert_eq!(clamp_timeout(timeout), timeout);
    }

    #[test]
    fn test_clamp_timeout_caps_durations_past_i32_millis() {
        let max = Duration::from_millis(i32::MAX as u64);
        assert_eq!(clamp_timeout(Duration::from_secs(60 * 60 * 24 * 30)), max);
        assert_eq!(clamp_timeout(max), max);
    }

    #[test]
    fn test_empty_credentials_are_rejected() {
        let (_dir, path) = credentials_file("{}");

        let result = client_builder(&path, "localhost:9092", Duration::from_secs(1));

        assert!(matches!(result, Err(Error::NoCredentials)));
    }

    #[test]
    fn test_sasl_protocol_without_sasl_block_is_rejected() {
        let (_dir, path) = credentials_file(r#"{ "security_protocol": "SASL_PLAINTEXT" }"#);

        let result = client_builder(&path, "localhost:9092", Duration::from_secs(1));

        assert!(matches!(result, Err(Error::MissingSaslCredentials { .. })));
    }

    #[test]
    fn test_client_certificate_needs_both_certificate_and_key() {
        let ssl = SslCredentials {
            ca_location: None,
            certificate_location: Some(PathBuf::from("/client.pem")),
            key_location: None,
            key_password: None,
        };

        let result = tls_config(Some(&ssl));

        assert!(matches!(result, Err(Error::IncompleteClientCertificate)));
    }

    #[test]
    fn test_ssl_credentials_deserialize_password_into_secret_string() {
        let ssl: SslCredentials = serde_json::from_str(r#"{ "key_password": "secret" }"#).unwrap();
        assert_eq!(
            ssl.key_password.as_ref().map(ExposeSecret::expose_secret),
            Some("secret")
        );
    }

    #[test]
    fn test_unknown_credentials_field_is_rejected() {
        let result = serde_json::from_str::<Credentials>(r#"{ "SASL": { "username": "u" } }"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_unknown_ssl_field_is_rejected() {
        let result =
            serde_json::from_str::<Credentials>(r#"{ "ssl": { "ca_locaton": "/ca.pem" } }"#);
        assert!(result.is_err());
    }

    #[test]
    fn test_broker_without_a_port_gets_the_default_port() {
        assert_eq!(with_default_port("kafka"), "kafka:9092");
        assert_eq!(with_default_port("kafka:19092"), "kafka:19092");
        assert_eq!(with_default_port("[::1]"), "[::1]:9092");
        assert_eq!(with_default_port("[::1]:19092"), "[::1]:19092");
    }

    #[test]
    fn test_empty_ca_file_is_rejected() {
        let ca = tempfile::NamedTempFile::new().unwrap();
        let ssl = SslCredentials {
            ca_location: Some(ca.path().to_path_buf()),
            certificate_location: None,
            key_location: None,
            key_password: None,
        };

        assert!(matches!(
            tls_config(Some(&ssl)),
            Err(Error::EmptyCaFile { path }) if path == ca.path()
        ));
    }

    #[test]
    fn test_debug_output_hides_passwords() {
        let credentials: Credentials = serde_json::from_str(
            r#"{ "sasl": { "username": "u", "password": "sasl-secret" },
                 "ssl": { "key_password": "key-secret" } }"#,
        )
        .unwrap();
        let debug = format!("{credentials:?}");

        assert!(!debug.contains("sasl-secret"));
        assert!(!debug.contains("key-secret"));
    }

    fn pem_file(label: &str) -> tempfile::NamedTempFile {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            format!("-----BEGIN {label}-----\nAAECAwQ=\n-----END {label}-----\n"),
        )
        .unwrap();
        file
    }

    #[test]
    fn test_password_is_ignored_for_an_unencrypted_key() {
        let key = pem_file("PRIVATE KEY");

        assert!(matches!(
            private_key(key.path(), Some("secret")),
            Ok(PrivateKeyDer::Pkcs8(_))
        ));
    }

    #[test]
    fn test_encrypted_key_is_decrypted_with_the_password() {
        let key = pem_file(ENCRYPTED_PRIVATE_KEY_LABEL);

        assert!(matches!(
            private_key(key.path(), Some("secret")),
            Err(Error::DecryptPrivateKey { .. })
        ));
    }
}
