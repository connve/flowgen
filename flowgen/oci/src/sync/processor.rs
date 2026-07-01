//! OCI sync processor — pulls a manifest from a registry and emits one event
//! per layer.
//!
//! Each layer is emitted as `{path, content, digest, artifact_digest}` so the
//! downstream bootstrap pipeline (buffer → diff → NATS KV write) can swap
//! `git_sync` for `oci_sync` with no other changes.

use super::config::{Credentials, Processor as ProcessorConfig};
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info};

/// Rewrites `/`, `:`, `@` in an OCI reference to hyphens so it fits
/// the cache key alphabet.
fn sanitize_artifact_ref(artifact: &str) -> String {
    artifact.replace(['/', ':', '@'], "-")
}

/// File event emitted for each layer in the pulled artifact. Matches the
/// shape `flowgen_git::sync` emits so bootstrap flows can target either.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FileEvent {
    /// File path inside the artifact, derived from the layer's
    /// `org.opencontainers.image.title` annotation.
    pub path: String,
    /// Layer content as UTF-8.
    pub content: String,
    /// Layer blob digest.
    pub digest: String,
    /// Whole-artifact manifest digest (same across all events for one pull).
    pub artifact_digest: String,
}

/// Errors that can occur during OCI sync processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Invalid OCI reference '{reference}': {source}")]
    InvalidReference {
        reference: String,
        #[source]
        source: oci_client::ParseError,
    },
    #[error("Failed to read credentials file '{path:?}': {source}")]
    ReadCredentials {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file '{path:?}': {source}")]
    ParseCredentials {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("OCI registry pull failed for '{reference}': {source}")]
    Pull {
        reference: String,
        #[source]
        source: oci_client::errors::OciDistributionError,
    },
    #[error("Layer content is not valid UTF-8: digest={digest}, source={source}")]
    InvalidLayerEncoding {
        digest: String,
        #[source]
        source: std::string::FromUtf8Error,
    },
    #[error("Error sending event: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
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

/// Event handler for OCI sync operations.
pub struct EventHandler {
    config: Arc<ProcessorConfig>,
    client: Client,
    reference: Reference,
    auth: RegistryAuth,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Handles a trigger event by pulling the artifact and emitting layer
    /// events.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            // Cheap change-signal via HEAD — avoids the manifest GET
            // and the config-blob GET that `pull_manifest_and_config`
            // would issue unconditionally.
            let cache = &self.task_context.cache;
            let flow_name = &self.task_context.flow.name;
            let sanitized_artifact = sanitize_artifact_ref(&self.config.artifact);
            let cache_key = format!("flow.{flow_name}.oci_digest.{sanitized_artifact}");
            let cached_digest = cache
                .get(&cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok());
            if !self.config.force_pull {
                let head_digest = self
                    .client
                    .fetch_manifest_digest(&self.reference, &self.auth)
                    .await
                    .map_err(|source| Error::Pull {
                        reference: self.config.artifact.clone(),
                        source,
                    })?;
                if cached_digest.as_deref() == Some(head_digest.as_str()) {
                    info!(
                        artifact = %self.config.artifact,
                        digest = %head_digest,
                        "OCI manifest digest unchanged since last pull, skipping layer fetch"
                    );
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        let upstream_leaf_share = self.task_context.leaf_count.max(1);
                        for _ in 0..upstream_leaf_share {
                            arc.signal_completion(None);
                        }
                    }
                    return Ok(());
                }
            }

            let (manifest, manifest_digest, _config_blob) = self
                .client
                .pull_manifest_and_config(&self.reference, &self.auth)
                .await
                .map_err(|source| Error::Pull {
                    reference: self.config.artifact.clone(),
                    source,
                })?;

            let layers = manifest.layers;

            let total = layers.len();
            for (index, layer) in layers.iter().enumerate() {
                let path = layer
                    .annotations
                    .as_ref()
                    .and_then(|ann| ann.get("org.opencontainers.image.title"))
                    .cloned()
                    .unwrap_or_else(|| format!("layer-{index}"));

                let mut buf = Vec::new();
                self.client
                    .pull_blob(&self.reference, layer, &mut buf)
                    .await
                    .map_err(|source| Error::Pull {
                        reference: self.config.artifact.clone(),
                        source,
                    })?;

                let content =
                    String::from_utf8(buf).map_err(|source| Error::InvalidLayerEncoding {
                        digest: layer.digest.clone(),
                        source,
                    })?;

                let file_event = FileEvent {
                    path,
                    content,
                    digest: layer.digest.clone(),
                    artifact_digest: manifest_digest.clone(),
                };
                let data = serde_json::to_value(&file_event)
                    .map_err(|source| Error::SerdeJson { source })?;

                let mut e = EventBuilder::new()
                    .data(EventData::Json(data))
                    .subject(self.config.name.clone())
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                // Only the last layer carries the completion signal.
                if index == total - 1 {
                    match self.tx {
                        None => {
                            if let Some(arc) = completion_tx_arc.as_ref() {
                                arc.signal_completion(e.data_as_json().ok());
                            }
                        }
                        Some(_) => {
                            e.completion_tx = completion_tx_arc.clone();
                        }
                    }
                }

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }

            // Empty artifact: still satisfy the source completion contract.
            if total == 0 {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    let upstream_leaf_share = self.task_context.leaf_count.max(1);
                    for _ in 0..upstream_leaf_share {
                        arc.signal_completion(None);
                    }
                }
            }

            // Persist only after every layer event was sent — a mid-pull
            // failure must re-emit the full batch on the next tick.
            if let Err(e) = cache
                .put(&cache_key, manifest_digest.clone().into(), None)
                .await
            {
                error!(
                    artifact = %self.config.artifact,
                    digest = %manifest_digest,
                    error = %e,
                    "Failed to persist OCI manifest digest, next tick will re-pull"
                );
            }

            Ok(())
        })
        .await
    }
}

/// Builds the registry auth from `credentials_path`, auto-detecting either
/// the flowgen-native `{username, password}` shape or a Docker
/// `config.json` with multiple registry entries. Returns anonymous if no
/// path is configured.
async fn load_auth(
    credentials_path: Option<&PathBuf>,
    registry_host: &str,
) -> Result<RegistryAuth, Error> {
    let path = match credentials_path {
        Some(p) => p,
        None => return Ok(RegistryAuth::Anonymous),
    };

    let content =
        tokio::fs::read_to_string(path)
            .await
            .map_err(|source| Error::ReadCredentials {
                path: path.clone(),
                source,
            })?;

    // Docker config has a top-level `auths` map. Flowgen-native has
    // top-level `username`. Try both; the one that parses wins.
    if let Ok(cfg) = serde_json::from_str::<DockerConfig>(&content) {
        if !cfg.auths.is_empty() {
            return Ok(pick_docker_auth(&cfg, registry_host));
        }
    }

    let creds: Credentials =
        serde_json::from_str(&content).map_err(|source| Error::ParseCredentials {
            path: path.clone(),
            source,
        })?;
    Ok(RegistryAuth::Basic(creds.username, creds.password))
}

/// Picks the auth entry whose host matches the artifact's registry. Falls
/// back to anonymous if no entry matches — public artifacts still pull
/// even when an unrelated dockerconfigjson is mounted.
fn pick_docker_auth(cfg: &DockerConfig, registry_host: &str) -> RegistryAuth {
    for (auth_host, entry) in cfg.auths.iter() {
        if registry_host_matches(auth_host, registry_host) {
            if let Some(auth_b64) = &entry.auth {
                if let Some((user, pass)) = decode_basic_auth(auth_b64) {
                    return RegistryAuth::Basic(user, pass);
                }
            }
            if let (Some(user), Some(pass)) = (&entry.username, &entry.password) {
                return RegistryAuth::Basic(user.clone(), pass.clone());
            }
        }
    }
    RegistryAuth::Anonymous
}

/// Loose host match — dockerconfigjson entries are URLs (`https://index.docker.io/v1/`)
/// or bare hosts (`ghcr.io`). We compare on the host segment alone.
fn registry_host_matches(auth_host: &str, registry_host: &str) -> bool {
    let normalized = auth_host
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let normalized = normalized.split('/').next().unwrap_or(normalized);
    normalized == registry_host
}

/// Decodes the base64-encoded `auth` field (`<user>:<pass>`) used by Docker
/// configs. Returns `None` if the value is malformed.
fn decode_basic_auth(b64: &str) -> Option<(String, String)> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let s = String::from_utf8(decoded).ok()?;
    let (user, pass) = s.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

#[derive(serde::Deserialize)]
struct DockerConfig {
    #[serde(default)]
    auths: std::collections::HashMap<String, DockerConfigAuth>,
}

#[derive(serde::Deserialize)]
struct DockerConfigAuth {
    #[serde(default)]
    auth: Option<String>,
    #[serde(default)]
    username: Option<String>,
    #[serde(default)]
    password: Option<String>,
}

/// OCI sync processor.
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
        let reference: Reference =
            self.config
                .artifact
                .parse()
                .map_err(|source| Error::InvalidReference {
                    reference: self.config.artifact.clone(),
                    source,
                })?;

        let auth = load_auth(self.config.credentials_path.as_ref(), reference.registry()).await?;

        // Loopback hosts (used by integration tests against a local
        // registry container) do not serve TLS. Anything else stays on
        // HTTPS, matching production registries.
        let registry = reference.registry();
        let protocol = if registry.starts_with("127.0.0.1")
            || registry.starts_with("localhost")
            || registry.starts_with("[::1]")
        {
            ClientProtocol::Http
        } else {
            ClientProtocol::Https
        };
        let client = Client::new(ClientConfig {
            protocol,
            ..Default::default()
        });

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            client,
            reference,
            auth,
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize OCI sync processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        let mut handlers = Vec::new();
        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let event_clone = event.clone();
                    let handle = tokio::spawn(async move {
                        let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                            match handler.handle(event_clone.clone()).await {
                                Ok(()) => Ok(()),
                                Err(e) => {
                                    let is_permanent = matches!(
                                        &e,
                                        Error::InvalidReference { .. }
                                            | Error::ParseCredentials { .. }
                                            | Error::InvalidLayerEncoding { .. }
                                    );
                                    error!(error = %e, "OCI sync failed");
                                    if is_permanent {
                                        Err(tokio_retry::RetryError::permanent(e))
                                    } else {
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            }
                        })
                        .await;

                        if let Err(err) = result {
                            error!(error = %err, "OCI sync exhausted all retry attempts");
                            let mut error_event = event_clone.clone();
                            error_event.error = Some(err.to_string());
                            if let Some(ref tx) = handler.tx {
                                tx.send(error_event).await.ok();
                            } else if let Some(arc) = event_clone.completion_tx.as_ref() {
                                arc.signal_completion_with_error(err.to_string());
                            }
                        }
                    });
                    handlers.push(handle);
                }
                None => {
                    futures_util::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for OCI sync processor.
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
    use std::path::PathBuf;

    // ── Error display ───────────────────────────────────────────────

    #[test]
    fn error_display_invalid_reference() {
        // Force a real ParseError without depending on its private constructors.
        let err: oci_client::ParseError = "not a valid ref::!!".parse::<Reference>().unwrap_err();
        let display = Error::InvalidReference {
            reference: "not a valid ref::!!".to_string(),
            source: err,
        }
        .to_string();
        assert!(display.contains("Invalid OCI reference"));
        assert!(display.contains("not a valid ref::!!"));
    }

    #[test]
    fn error_display_read_credentials() {
        let err = Error::ReadCredentials {
            path: PathBuf::from("/etc/missing.json"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        };
        assert!(err.to_string().contains("/etc/missing.json"));
        assert!(err.to_string().contains("Failed to read credentials"));
    }

    #[test]
    fn error_display_parse_credentials() {
        let serde_err = serde_json::from_str::<Credentials>("not json").unwrap_err();
        let err = Error::ParseCredentials {
            path: PathBuf::from("/creds.json"),
            source: serde_err,
        };
        assert!(err.to_string().contains("/creds.json"));
        assert!(err.to_string().contains("Failed to parse credentials"));
    }

    #[test]
    fn error_display_invalid_layer_encoding() {
        // 0xFF is invalid UTF-8.
        let bad = vec![0xFF, 0xFE];
        let utf8_err = String::from_utf8(bad).unwrap_err();
        let err = Error::InvalidLayerEncoding {
            digest: "sha256:bad".to_string(),
            source: utf8_err,
        };
        assert!(err.to_string().contains("sha256:bad"));
        assert!(err.to_string().contains("not valid UTF-8"));
    }

    #[test]
    fn error_display_missing_builder_attribute() {
        let err = Error::MissingBuilderAttribute("config".to_string());
        assert_eq!(
            err.to_string(),
            "Missing required builder attribute: config"
        );
    }

    #[test]
    fn error_display_retry_exhausted() {
        let inner = Error::MissingBuilderAttribute("config".to_string());
        let err = Error::RetryExhausted {
            source: Box::new(inner),
        };
        assert!(err
            .to_string()
            .contains("Task failed after all retry attempts"));
    }

    // ── FileEvent ──────────────────────────────────────────────────

    #[test]
    fn file_event_serialization() {
        let fe = FileEvent {
            path: "flows/main.yaml".to_string(),
            content: "name: test".to_string(),
            digest: "sha256:abc".to_string(),
            artifact_digest: "sha256:def".to_string(),
        };
        let value = serde_json::to_value(&fe).unwrap();
        assert_eq!(value["path"], "flows/main.yaml");
        assert_eq!(value["content"], "name: test");
        assert_eq!(value["digest"], "sha256:abc");
        assert_eq!(value["artifact_digest"], "sha256:def");
    }

    #[test]
    fn registry_host_matches_basic() {
        assert!(registry_host_matches("ghcr.io", "ghcr.io"));
        assert!(registry_host_matches("https://ghcr.io", "ghcr.io"));
        assert!(registry_host_matches(
            "https://index.docker.io/v1/",
            "index.docker.io"
        ));
        assert!(!registry_host_matches("ghcr.io", "registry.gitlab.com"));
    }

    #[test]
    fn decode_basic_auth_round_trip() {
        // "robot:tok123" → base64 "cm9ib3Q6dG9rMTIz"
        let (u, p) = decode_basic_auth("cm9ib3Q6dG9rMTIz").unwrap();
        assert_eq!(u, "robot");
        assert_eq!(p, "tok123");
    }

    #[test]
    fn pick_docker_auth_matches_host() {
        let cfg: DockerConfig = serde_json::from_str(
            r#"{
                "auths": {
                    "ghcr.io": { "auth": "cm9ib3Q6dG9rMTIz" },
                    "registry.gitlab.com": { "username": "alice", "password": "secret" }
                }
            }"#,
        )
        .unwrap();
        let auth = pick_docker_auth(&cfg, "ghcr.io");
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "robot" && p == "tok123"));

        let auth = pick_docker_auth(&cfg, "registry.gitlab.com");
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "alice" && p == "secret"));

        let auth = pick_docker_auth(&cfg, "unrelated.example.com");
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[test]
    fn pick_docker_auth_username_password_only() {
        // No `auth` base64 field; falls back to explicit username/password.
        let cfg: DockerConfig = serde_json::from_str(
            r#"{
                "auths": {
                    "ghcr.io": { "username": "alice", "password": "secret" }
                }
            }"#,
        )
        .unwrap();
        let auth = pick_docker_auth(&cfg, "ghcr.io");
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "alice" && p == "secret"));
    }

    #[test]
    fn pick_docker_auth_empty_entry_falls_back_to_anonymous() {
        // Entry exists but has neither `auth` nor user/pass — caller should
        // get anonymous, not a panic.
        let cfg: DockerConfig = serde_json::from_str(
            r#"{
                "auths": {
                    "ghcr.io": {}
                }
            }"#,
        )
        .unwrap();
        let auth = pick_docker_auth(&cfg, "ghcr.io");
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[test]
    fn decode_basic_auth_rejects_bad_base64() {
        assert!(decode_basic_auth("not-base64!@#").is_none());
    }

    #[test]
    fn decode_basic_auth_rejects_missing_colon() {
        // "nocolon" → base64 "bm9jb2xvbg=="
        assert!(decode_basic_auth("bm9jb2xvbg==").is_none());
    }

    #[test]
    fn file_event_clone() {
        let fe = FileEvent {
            path: "a.yaml".to_string(),
            content: "x".to_string(),
            digest: "sha256:1".to_string(),
            artifact_digest: "sha256:2".to_string(),
        };
        let cloned = fe.clone();
        assert_eq!(fe.path, cloned.path);
        assert_eq!(fe.content, cloned.content);
        assert_eq!(fe.digest, cloned.digest);
        assert_eq!(fe.artifact_digest, cloned.artifact_digest);
    }

    // ── load_auth integration ──────────────────────────────────────

    #[tokio::test]
    async fn load_auth_anonymous_when_no_path() {
        let auth = load_auth(None, "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[tokio::test]
    async fn load_auth_flowgen_native_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.json");
        tokio::fs::write(&path, r#"{"username":"u","password":"p"}"#)
            .await
            .unwrap();
        let auth = load_auth(Some(&path), "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "u" && p == "p"));
    }

    #[tokio::test]
    async fn load_auth_dockerconfigjson_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        // "robot:tok" → base64 "cm9ib3Q6dG9r"
        tokio::fs::write(&path, r#"{"auths":{"ghcr.io":{"auth":"cm9ib3Q6dG9r"}}}"#)
            .await
            .unwrap();
        let auth = load_auth(Some(&path), "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "robot" && p == "tok"));
    }

    #[tokio::test]
    async fn load_auth_dockerconfigjson_no_matching_host_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        tokio::fs::write(
            &path,
            r#"{"auths":{"registry.gitlab.com":{"auth":"YTpi"}}}"#,
        )
        .await
        .unwrap();
        let auth = load_auth(Some(&path), "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[tokio::test]
    async fn load_auth_missing_file_errors() {
        let result = load_auth(Some(&PathBuf::from("/nope/missing.json")), "ghcr.io").await;
        assert!(matches!(result, Err(Error::ReadCredentials { .. })));
    }

    #[tokio::test]
    async fn load_auth_malformed_json_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        tokio::fs::write(&path, "{").await.unwrap();
        let result = load_auth(Some(&path), "ghcr.io").await;
        assert!(matches!(result, Err(Error::ParseCredentials { .. })));
    }

    // ── ProcessorBuilder validation ─────────────────────────────────

    fn test_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test_flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    fn test_config() -> ProcessorConfig {
        ProcessorConfig {
            name: "test_sync".to_string(),
            artifact: "ghcr.io/org/flows:prod".to_string(),
            credentials_path: None,
            force_pull: false,
            depends_on: None,
            retry: None,
        }
    }

    #[tokio::test]
    async fn builder_requires_config() {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let result = ProcessorBuilder::new()
            .receiver(rx)
            .task_id(1)
            .task_type("oci_sync")
            .task_context(test_task_context())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(s)) if s == "config"
        ));
    }

    #[tokio::test]
    async fn builder_requires_receiver() {
        let result = ProcessorBuilder::new()
            .config(Arc::new(test_config()))
            .task_id(1)
            .task_type("oci_sync")
            .task_context(test_task_context())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(s)) if s == "receiver"
        ));
    }

    #[tokio::test]
    async fn builder_requires_task_context() {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let result = ProcessorBuilder::new()
            .config(Arc::new(test_config()))
            .receiver(rx)
            .task_id(1)
            .task_type("oci_sync")
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(s)) if s == "task_context"
        ));
    }

    #[tokio::test]
    async fn builder_requires_task_type() {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let result = ProcessorBuilder::new()
            .config(Arc::new(test_config()))
            .receiver(rx)
            .task_id(1)
            .task_context(test_task_context())
            .build()
            .await;
        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(s)) if s == "task_type"
        ));
    }

    #[tokio::test]
    async fn builder_succeeds_without_sender() {
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let result = ProcessorBuilder::new()
            .config(Arc::new(test_config()))
            .receiver(rx)
            .task_id(42)
            .task_type("oci_sync")
            .task_context(test_task_context())
            .build()
            .await;
        let processor = result.unwrap();
        assert!(processor.tx.is_none());
        assert_eq!(processor.task_id, 42);
    }

    #[tokio::test]
    async fn builder_succeeds_with_sender() {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let result = ProcessorBuilder::new()
            .config(Arc::new(test_config()))
            .receiver(rx)
            .sender(tx)
            .task_id(7)
            .task_type("oci_sync")
            .task_context(test_task_context())
            .build()
            .await;
        let processor = result.unwrap();
        assert!(processor.tx.is_some());
    }
}
