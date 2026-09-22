//! # Kafka Client
//!
//! Builds the librdkafka producer and admin client, and loads SASL/SSL
//! credentials from a JSON file. The security protocol follows from which
//! credential blocks the file carries, so SASL over TLS works without
//! spelling the protocol out.

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::producer::FutureProducer;
use std::path::PathBuf;
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

/// librdkafka property names.
const PROP_BOOTSTRAP_SERVERS: &str = "bootstrap.servers";
const PROP_MESSAGE_TIMEOUT_MS: &str = "message.timeout.ms";
const PROP_SECURITY_PROTOCOL: &str = "security.protocol";
const PROP_SASL_MECHANISM: &str = "sasl.mechanism";
const PROP_SASL_USERNAME: &str = "sasl.username";
const PROP_SASL_PASSWORD: &str = "sasl.password";
const PROP_SSL_CA_LOCATION: &str = "ssl.ca.location";
const PROP_SSL_CERTIFICATE_LOCATION: &str = "ssl.certificate.location";
const PROP_SSL_KEY_LOCATION: &str = "ssl.key.location";
const PROP_SSL_KEY_PASSWORD: &str = "ssl.key.password";

#[derive(serde::Deserialize, Debug, Clone, PartialEq, Default)]
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

#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct SaslCredentials {
    pub username: String,
    pub password: String,
    #[serde(default = "default_sasl_mechanism")]
    pub mechanism: String,
}

fn default_sasl_mechanism() -> String {
    DEFAULT_SASL_MECHANISM.to_string()
}

#[derive(serde::Deserialize, Debug, Clone, PartialEq)]
pub struct SslCredentials {
    pub ca_location: Option<PathBuf>,
    pub certificate_location: Option<PathBuf>,
    pub key_location: Option<PathBuf>,
    pub key_password: Option<String>,
}

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
    #[error("Error creating Kafka producer: {source}")]
    CreateProducer {
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("Error creating Kafka admin client: {source}")]
    CreateAdminClient {
        #[source]
        source: rdkafka::error::KafkaError,
    },
    #[error("No authentication credentials provided")]
    NoCredentials,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

pub struct Client {
    credentials_path: Option<PathBuf>,
    brokers: Option<String>,
    ack_timeout: Duration,
    pub producer: Option<FutureProducer>,
    pub admin_client: Option<AdminClient<DefaultClientContext>>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("credentials_path", &self.credentials_path)
            .field("brokers", &self.brokers)
            .field(
                "producer",
                &self.producer.as_ref().map(|_| "FutureProducer"),
            )
            .field(
                "admin_client",
                &self.admin_client.as_ref().map(|_| "AdminClient"),
            )
            .finish()
    }
}

/// Applies SASL/SSL credentials to a `ClientConfig`.
fn apply_credentials(config: &mut ClientConfig, path: &PathBuf) -> Result<(), Error> {
    let credentials: Credentials =
        serde_json::from_str(&std::fs::read_to_string(path).map_err(|e| {
            Error::ReadCredentials {
                path: path.clone(),
                source: e,
            }
        })?)
        .map_err(|e| Error::ParseCredentials { source: e })?;

    if let Some(sasl) = &credentials.sasl {
        config.set(PROP_SASL_MECHANISM, &sasl.mechanism);
        config.set(PROP_SASL_USERNAME, &sasl.username);
        config.set(PROP_SASL_PASSWORD, &sasl.password);
    }

    if let Some(ssl) = &credentials.ssl {
        if let Some(ca) = &ssl.ca_location {
            config.set(PROP_SSL_CA_LOCATION, ca.to_string_lossy().as_ref());
        }
        if let Some(cert) = &ssl.certificate_location {
            config.set(
                PROP_SSL_CERTIFICATE_LOCATION,
                cert.to_string_lossy().as_ref(),
            );
        }
        if let Some(key) = &ssl.key_location {
            config.set(PROP_SSL_KEY_LOCATION, key.to_string_lossy().as_ref());
        }
        if let Some(pwd) = &ssl.key_password {
            config.set(PROP_SSL_KEY_PASSWORD, pwd);
        }
    }

    // A credentials file that configures nothing would connect in plaintext,
    // which is not what pointing at one asks for.
    let protocol = credentials
        .security_protocol()
        .ok_or(Error::NoCredentials)?;
    config.set(PROP_SECURITY_PROTOCOL, protocol);

    Ok(())
}

/// Caps a duration at what librdkafka accepts.
///
/// Its timeouts are i32 milliseconds and the conversion wraps rather than
/// saturates, so a longer duration is rejected with a negative number nobody
/// configured.
pub fn clamp_timeout(timeout: Duration) -> Duration {
    let max = Duration::from_millis(i32::MAX as u64);
    match timeout > max {
        true => max,
        false => timeout,
    }
}

/// Builds a base `ClientConfig` from broker string and optional credentials path.
///
/// `ack_timeout` bounds how long librdkafka keeps retrying a message before
/// giving up on it, so it has to match the timeout the producer waits on --
/// a longer wait would expire on the client side first and never be reached.
pub fn build_base_config(
    credentials_path: &Option<PathBuf>,
    brokers: &str,
    ack_timeout: Duration,
) -> Result<ClientConfig, Error> {
    let mut config = ClientConfig::new();
    config.set(PROP_BOOTSTRAP_SERVERS, brokers);
    config.set(
        PROP_MESSAGE_TIMEOUT_MS,
        clamp_timeout(ack_timeout).as_millis().to_string(),
    );

    if let Some(path) = credentials_path {
        apply_credentials(&mut config, path)?;
    }

    Ok(config)
}

impl flowgen_core::client::Client for Client {
    type Error = Error;

    async fn connect(mut self) -> Result<Self, Error> {
        let brokers = self
            .brokers
            .clone()
            .unwrap_or_else(|| DEFAULT_KAFKA_BROKERS.to_string());

        let config = build_base_config(&self.credentials_path, &brokers, self.ack_timeout)?;

        let producer: FutureProducer = config
            .create()
            .map_err(|e| Error::CreateProducer { source: e })?;

        let admin_client: AdminClient<DefaultClientContext> = config
            .create()
            .map_err(|e| Error::CreateAdminClient { source: e })?;

        self.producer = Some(producer);
        self.admin_client = Some(admin_client);
        Ok(self)
    }
}

impl Client {
    pub fn new(
        credentials_path: Option<PathBuf>,
        brokers: Option<String>,
        ack_timeout: Duration,
    ) -> Self {
        Self {
            credentials_path,
            brokers,
            ack_timeout,
            producer: None,
            admin_client: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sasl() -> SaslCredentials {
        SaslCredentials {
            username: "u".to_string(),
            password: "p".to_string(),
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
    fn test_sasl_mechanism_defaults() {
        let credentials: Credentials =
            serde_json::from_str(r#"{ "sasl": { "username": "u", "password": "p" } }"#).unwrap();

        assert_eq!(
            credentials.sasl.map(|s| s.mechanism),
            Some(DEFAULT_SASL_MECHANISM.to_string())
        );
    }

    #[test]
    fn test_ack_timeout_is_clamped_to_i32() {
        let config = build_base_config(
            &None,
            "localhost:9092",
            Duration::from_secs(60 * 60 * 24 * 30),
        )
        .unwrap();

        assert_eq!(
            config.get(PROP_MESSAGE_TIMEOUT_MS),
            Some(i32::MAX.to_string()).as_deref()
        );
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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.json");
        std::fs::write(&path, "{}").unwrap();

        let mut config = ClientConfig::new();
        let result = apply_credentials(&mut config, &path);

        assert!(matches!(result, Err(Error::NoCredentials)));
    }

    #[test]
    fn test_unknown_credentials_field_is_rejected() {
        let result = serde_json::from_str::<Credentials>(r#"{ "SASL": { "username": "u" } }"#);
        assert!(result.is_err());
    }
}
