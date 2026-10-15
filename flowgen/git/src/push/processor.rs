//! Git push processor: commits the files of each event and pushes them.

use super::client::{Author, Expected, FileChange, Push};
use super::config::Processor as ProcessorConfig;
use crate::remote::{load_credentials, Credentials};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn};

/// Errors from the git push processor.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Push(#[from] super::client::Error),
    #[error(transparent)]
    Credentials(#[from] flowgen_core::credentials::Error),
    #[error("Failed to read the signing key {path}: {source}")]
    ReadSigningKey {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("The signing key {path} is not an OpenSSH Ed25519 private key: {source}")]
    InvalidSigningKey {
        path: PathBuf,
        #[source]
        source: ssh_key::Error,
    },
    #[error("The signing key {path} is encrypted; use one without a passphrase")]
    EncryptedSigningKey { path: PathBuf },
    #[error("The signing key {path} is {algorithm}; only Ed25519 keys are supported")]
    UnsupportedSigningKey { path: PathBuf, algorithm: String },
    #[error("Event data must hold a `files` list of {{path, content}}: {source}")]
    Input {
        #[source]
        source: serde_json::Error,
    },
    #[error("Event data is not JSON: {source}")]
    NotJson {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to serialize the pushed files: {source}")]
    Output {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to build the HTTP client: {source}")]
    HttpClient {
        #[source]
        source: reqwest::Error,
    },
    #[error("Failed to render config: {source}")]
    RenderConfig {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error sending event: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

impl Error {
    /// Whether retrying the same event cannot succeed.
    fn is_permanent(&self) -> bool {
        use super::client::Error as Push;
        match self {
            Error::Push(Push::Status { status, .. }) => {
                status.is_client_error()
                    && *status != reqwest::StatusCode::REQUEST_TIMEOUT
                    && *status != reqwest::StatusCode::TOO_MANY_REQUESTS
            }
            Error::Push(source) => matches!(
                source,
                Push::InvalidPath { .. }
                    | Push::Conflict { .. }
                    | Push::NothingToPush
                    | Push::BranchMissing { .. }
                    | Push::Url(_)
                    | Push::CommandTooLong { .. }
                    | Push::TooManyObjects { .. }
                    | Push::InvalidPktLine { .. }
                    | Push::TruncatedPktLine
                    | Push::InvalidAdvertisement { .. }
                    | Push::InvalidAdvertisedId { .. }
                    | Push::UnpackFailed { .. }
                    | Push::RefRejected { .. }
                    | Push::ServerError { .. }
                    | Push::Sign { .. }
            ),
            Error::Input { .. } | Error::NotJson { .. } | Error::RenderConfig { .. } => true,
            _ => false,
        }
    }
}

/// Reads an unencrypted OpenSSH Ed25519 private key.
async fn load_signing_key(path: &Path) -> Result<ssh_key::PrivateKey, Error> {
    let pem = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| Error::ReadSigningKey {
            path: path.to_path_buf(),
            source,
        })?;
    let pem = SecretString::from(pem);
    let key = ssh_key::PrivateKey::from_openssh(pem.expose_secret()).map_err(|source| {
        Error::InvalidSigningKey {
            path: path.to_path_buf(),
            source,
        }
    })?;
    if key.is_encrypted() {
        return Err(Error::EncryptedSigningKey {
            path: path.to_path_buf(),
        });
    }
    match key.algorithm() {
        ssh_key::Algorithm::Ed25519 => Ok(key),
        algorithm => Err(Error::UnsupportedSigningKey {
            path: path.to_path_buf(),
            algorithm: algorithm.as_str().to_string(),
        }),
    }
}

/// Deserializes a required field that may be `null`.
fn nullable<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(deserializer)
}

/// Deserializes a field that may be absent, `null`, or a value.
fn present<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<Option<String>>, D::Error> {
    nullable(deserializer).map(Some)
}

/// A file in `event.data.files`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputFile {
    path: String,
    /// Required, so that only an explicit `null` deletes the file.
    #[serde(deserialize_with = "nullable")]
    content: Option<String>,
    /// Absent: no check. `null`: the file must not exist. A string: the
    /// file must hold exactly that.
    #[serde(default, deserialize_with = "present")]
    previous: Option<Option<String>>,
}

#[derive(Deserialize)]
struct Input {
    files: Vec<InputFile>,
}

/// A pushed file in the emitted event.
#[derive(Serialize)]
struct OutputFile {
    path: String,
    content: String,
}

/// Data of the emitted event.
#[derive(Serialize)]
struct Output {
    commit: String,
    changed: bool,
    files: Vec<OutputFile>,
}

/// Event handler for git push operations.
pub struct EventHandler {
    config: Arc<ProcessorConfig>,
    credentials: Option<Credentials>,
    signing_key: Option<Arc<ssh_key::PrivateKey>>,
    http: reqwest::Client,
    pushing: tokio::sync::Mutex<()>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Handler for EventHandler {
    type Error = Error;

    fn sender(&self) -> Option<&Sender<Event>> {
        self.tx.as_ref()
    }

    fn is_retryable(&self, error: &Error) -> bool {
        !error.is_permanent()
    }

    /// Commits and pushes the event's files, emitting every file under `path`.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if let Some(error) = event.error.clone() {
            event.forward_failure(self.tx.as_ref(), error).await;
            return Ok(());
        }
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }
        let event = Arc::new(event);
        let completion_tx = event.completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let config: ProcessorConfig = self
                .config
                .render_for_event(&event)
                .map_err(|source| Error::RenderConfig { source })?;
            let data = event
                .data_as_json()
                .map_err(|source| Error::NotJson { source })?;
            let input: Input =
                serde_json::from_value(data).map_err(|source| Error::Input { source })?;

            let changes = input
                .files
                .into_iter()
                .map(|file| FileChange {
                    path: file.path,
                    content: file.content.map(String::into_bytes),
                    expected: match file.previous {
                        None => Expected::Any,
                        Some(None) => Expected::Missing,
                        Some(Some(content)) => Expected::Content(content.into_bytes()),
                    },
                })
                .collect();
            let push = Push {
                repository_url: config.repository_url,
                branch: config.branch,
                path: config.path,
                credentials: self.credentials.clone(),
                committer: config.committer.map(|committer| Author {
                    name: committer.name,
                    email: committer.email,
                }),
                signing_key: self.signing_key.clone(),
                http: self.http.clone(),
                timeout: config.timeout,
            };
            let author = Author {
                name: config.author.name,
                email: config.author.email,
            };
            let pushed = {
                let _pushing = self.pushing.lock().await;
                push.push(changes, author, config.message).await?
            };

            let mut files = Vec::with_capacity(pushed.files.len());
            for file in pushed.files {
                match String::from_utf8(file.content) {
                    Ok(content) => files.push(OutputFile {
                        path: file.path,
                        content,
                    }),
                    Err(_) => {
                        warn!(path = %file.path, "Skipping a file that is not UTF-8")
                    }
                }
            }
            let output = Output {
                commit: pushed.commit.clone(),
                changed: pushed.changed,
                files,
            };
            let data = serde_json::to_value(&output).map_err(|source| Error::Output { source })?;
            let mut e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(self.config.name.clone())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;
            match self.tx {
                None => {
                    if let Some(arc) = completion_tx.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => e.completion_tx = completion_tx.clone(),
            }
            let short_commit = match pushed.commit.get(..7) {
                Some(short) => short,
                None => pushed.commit.as_str(),
            };
            e.send_with_logging(self.tx.as_ref())
                .context("commit", short_commit)
                .context("changed", pushed.changed)
                .await
                .map_err(|source| Error::SendMessage { source })
        })
        .await
    }
}

/// Git push processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<ProcessorConfig>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let credentials = load_credentials(self.config.credentials_path.as_deref()).await?;
        let signing_key = match &self.config.signing_key_path {
            Some(path) => Some(Arc::new(load_signing_key(path).await?)),
            None => None,
        };
        let mut http = reqwest::Client::builder();
        if let Some(timeout) = self.config.timeout {
            http = http.timeout(timeout);
        }
        if let Some(connect_timeout) = self.config.connect_timeout {
            http = http.connect_timeout(connect_timeout);
        }
        let http = http
            .build()
            .map_err(|source| Error::HttpClient { source })?;
        Ok(EventHandler {
            config: Arc::clone(&self.config),
            credentials,
            signing_key,
            http,
            pushing: tokio::sync::Mutex::new(()),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let handler = tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                self.init().await.map_err(|e| {
                    error!(error = %e, "Failed to initialize git push processor");
                    tokio_retry::RetryError::transient(e)
                })
            },
        )
        .await
        .map(Arc::new)?;

        flowgen_core::task::runner::process_events(
            self.rx,
            handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for the git push processor.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<ProcessorConfig>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<ProcessorConfig>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(mut self, ctx: Arc<flowgen_core::task::context::TaskContext>) -> Self {
        self.task_context = Some(ctx);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn previous_distinguishes_absent_null_and_content() {
        let input: Input = serde_json::from_str(
            r#"{"files": [
                {"path": "a", "content": "x"},
                {"path": "b", "content": "x", "previous": null},
                {"path": "c", "content": null, "previous": "old"}
            ]}"#,
        )
        .unwrap();
        let previous: Vec<_> = input.files.iter().map(|f| f.previous.clone()).collect();
        assert_eq!(
            previous,
            vec![None, Some(None), Some(Some("old".to_string()))]
        );
        assert_eq!(input.files[2].content, None);
    }

    #[test]
    fn a_missing_content_is_rejected_and_a_null_content_deletes() {
        let missing = serde_json::from_str::<Input>(r#"{"files": [{"path": "a"}]}"#);
        assert!(missing.is_err());

        let deleted: Input =
            serde_json::from_str(r#"{"files": [{"path": "a", "content": null}]}"#).unwrap();
        assert_eq!(deleted.files[0].content, None);
    }

    /// Creates a key with `ssh-keygen`, which must be in `PATH`.
    fn keygen(dir: &Path, name: &str, args: &[&str]) -> PathBuf {
        let path = dir.join(name);
        let status = std::process::Command::new("ssh-keygen")
            .args(["-q", "-C", "a@example.com", "-f"])
            .arg(&path)
            .args(args)
            .status()
            .expect("spawn ssh-keygen");
        assert!(status.success(), "ssh-keygen {args:?} exited with {status}");
        path
    }

    #[tokio::test]
    async fn only_unencrypted_ed25519_openssh_keys_are_loaded() {
        let dir = tempfile::TempDir::new().unwrap();

        let ed25519 = keygen(dir.path(), "ed25519", &["-t", "ed25519", "-N", ""]);
        let public_key = std::fs::read_to_string(ed25519.with_extension("pub")).unwrap();
        assert_eq!(
            load_signing_key(&ed25519).await.unwrap().public_key(),
            &ssh_key::PublicKey::from_openssh(&public_key).unwrap()
        );

        let encrypted = keygen(dir.path(), "encrypted", &["-t", "ed25519", "-N", "secret"]);
        assert!(matches!(
            load_signing_key(&encrypted).await,
            Err(Error::EncryptedSigningKey { .. })
        ));

        let rsa = keygen(dir.path(), "rsa", &["-t", "rsa", "-b", "2048", "-N", ""]);
        assert!(matches!(
            load_signing_key(&rsa).await,
            Err(Error::UnsupportedSigningKey { algorithm, .. }) if algorithm == "ssh-rsa"
        ));

        let ecdsa = keygen(dir.path(), "ecdsa", &["-t", "ecdsa", "-N", ""]);
        assert!(matches!(
            load_signing_key(&ecdsa).await,
            Err(Error::InvalidSigningKey { .. })
        ));

        let garbage = dir.path().join("garbage");
        std::fs::write(&garbage, "not a key").unwrap();
        assert!(matches!(
            load_signing_key(&garbage).await,
            Err(Error::InvalidSigningKey { .. })
        ));
        assert!(matches!(
            load_signing_key(&dir.path().join("missing")).await,
            Err(Error::ReadSigningKey { .. })
        ));
    }

    #[test]
    fn malformed_server_responses_are_not_retried() {
        use super::super::client::Error as Push;
        let framing = Error::Push(Push::InvalidPktLine {
            source: gix::protocol::transport::packetline::decode::Error::DataIsEmpty,
        });
        let advertisement = Error::Push(Push::InvalidAdvertisement {
            line: "x".to_string(),
        });
        assert!(framing.is_permanent());
        assert!(Error::Push(Push::TruncatedPktLine).is_permanent());
        assert!(advertisement.is_permanent());
    }

    #[test]
    fn conflicts_rejections_and_client_errors_are_not_retried() {
        use super::super::client::Error as Push;
        let status = |code: u16| {
            Error::Push(Push::Status {
                url: "u".to_string(),
                status: reqwest::StatusCode::from_u16(code).unwrap(),
            })
        };
        let conflict = Error::Push(Push::Conflict {
            path: "a".to_string(),
        });
        let rejected = Error::Push(Push::RefRejected {
            branch: "main".to_string(),
            message: "protected branch".to_string(),
        });
        let moved = Error::Push(Push::BranchMoved {
            branch: "main".to_string(),
        });
        assert!(conflict.is_permanent());
        assert!(rejected.is_permanent());
        assert!(!moved.is_permanent());
        assert!(status(403).is_permanent());
        assert!(!status(429).is_permanent());
        assert!(!status(503).is_permanent());
    }
}
