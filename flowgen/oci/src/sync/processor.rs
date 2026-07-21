//! OCI sync processor — pulls a manifest from a registry and emits one event
//! per layer.
//!
//! Each layer is emitted as `{path, content, digest, artifact_digest}` so the
//! downstream bootstrap pipeline (buffer → diff → NATS KV write) can swap
//! `git_sync` for `oci_sync` with no other changes.

use super::config::{Credentials, Processor as ProcessorConfig};
use flowgen_core::config::ConfigExt;
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

/// Meta attached to `EventData::Bytes` layer events so downstream tasks
/// can still route by path and digest even when the payload is binary.
#[derive(Debug, Clone, serde::Serialize)]
struct BinaryLayerMeta {
    path: String,
    digest: String,
    artifact_digest: String,
}

/// Intermediate carrier before per-layer UTF-8 fallback picks JSON or Bytes.
struct PendingFileEvent {
    path: String,
    content: Vec<u8>,
    digest: String,
    artifact_digest: String,
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
    #[error("Failed to read tar archive in layer '{digest}': {source}")]
    TarRead {
        digest: String,
        #[source]
        source: std::io::Error,
    },
    #[error("File '{path}' in layer '{digest}' exceeds max_file_size ({size} > {limit} bytes)")]
    FileTooLarge {
        digest: String,
        path: String,
        size: u64,
        limit: u64,
    },
    #[error("Artifact '{reference}' exceeds max_total_size (uncompressed > {limit} bytes)")]
    ArtifactTooLarge { reference: String, limit: u64 },
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
    #[error("Failed to render OCI sync config: {source}")]
    RenderConfig {
        #[source]
        source: flowgen_core::config::Error,
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

/// One entry extracted from a layer. Docker image layers can carry
/// whiteout markers that delete a file or an entire subtree from lower
/// layers; oras artifact layers never emit whiteouts.
enum ExtractedEntry {
    File {
        /// Tar entry path when the source layer was an archive, `None`
        /// when the raw layer bytes were the file itself (oras single-file).
        path: Option<String>,
        content: Vec<u8>,
    },
    /// Docker `.wh.<name>` marker — delete `<name>` in the same directory
    /// from lower layers when merging.
    Whiteout { path: String },
    /// Docker `.wh..wh..opq` marker — hide everything in the parent
    /// directory from lower layers when merging.
    OpaqueWhiteout { parent: String },
}

/// Layer wire format, chosen by media type. Raw layers come from oras-style
/// pushes; tar and tar+gzip come from docker/OCI images.
#[derive(Copy, Clone, PartialEq, Eq)]
enum LayerFormat {
    Raw,
    Tar,
    TarGzip,
}

fn detect_layer_format(media_type: &str) -> LayerFormat {
    let mt = media_type.to_ascii_lowercase();
    if mt.ends_with("+gzip") || mt.ends_with(".tar.gzip") || mt.ends_with(".tar+gzip") {
        LayerFormat::TarGzip
    } else if mt.ends_with(".tar") || mt.ends_with("+tar") {
        LayerFormat::Tar
    } else {
        LayerFormat::Raw
    }
}

/// Extracts one or more `ExtractedFile`s from a single layer's raw bytes,
/// dispatching on the layer's media type. Enforces per-file and cumulative
/// size caps to defuse tar bombs; skips directories, symlinks, hardlinks,
/// and docker whiteout markers.
fn extract_layer_files(
    buf: Vec<u8>,
    format: LayerFormat,
    digest: &str,
    max_file_size: u64,
    max_total_size: u64,
    total_so_far: &mut u64,
    reference: &str,
) -> Result<Vec<ExtractedEntry>, Error> {
    match format {
        LayerFormat::Raw => {
            let size = buf.len() as u64;
            if size > max_file_size {
                return Err(Error::FileTooLarge {
                    digest: digest.to_string(),
                    path: String::new(),
                    size,
                    limit: max_file_size,
                });
            }
            let new_total = total_so_far.saturating_add(size);
            if new_total > max_total_size {
                return Err(Error::ArtifactTooLarge {
                    reference: reference.to_string(),
                    limit: max_total_size,
                });
            }
            *total_so_far = new_total;
            Ok(vec![ExtractedEntry::File {
                path: None,
                content: buf,
            }])
        }
        LayerFormat::Tar => extract_tar(
            &buf[..],
            digest,
            max_file_size,
            max_total_size,
            total_so_far,
            reference,
        ),
        LayerFormat::TarGzip => {
            let decoder = flate2::read::GzDecoder::new(&buf[..]);
            extract_tar(
                decoder,
                digest,
                max_file_size,
                max_total_size,
                total_so_far,
                reference,
            )
        }
    }
}

fn extract_tar<R: std::io::Read>(
    reader: R,
    digest: &str,
    max_file_size: u64,
    max_total_size: u64,
    total_so_far: &mut u64,
    reference: &str,
) -> Result<Vec<ExtractedEntry>, Error> {
    let mut archive = tar::Archive::new(reader);
    let mut out = Vec::new();

    let entries = archive.entries().map_err(|source| Error::TarRead {
        digest: digest.to_string(),
        source,
    })?;

    for entry in entries {
        let mut entry = entry.map_err(|source| Error::TarRead {
            digest: digest.to_string(),
            source,
        })?;

        let entry_type = entry.header().entry_type();
        if !entry_type.is_file() {
            continue;
        }

        let raw_path = entry
            .path()
            .map_err(|source| Error::TarRead {
                digest: digest.to_string(),
                source,
            })?
            .to_string_lossy()
            .into_owned();

        let path = raw_path.trim_start_matches('/').to_string();
        if path.is_empty() {
            continue;
        }
        let file_name = std::path::Path::new(&path)
            .file_name()
            .map(|s| s.to_string_lossy().into_owned());
        if let Some(name) = file_name {
            // Docker overlay whiteout markers; layer-merge turns these into deletions.
            if name == ".wh..wh..opq" {
                let parent = std::path::Path::new(&path)
                    .parent()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                out.push(ExtractedEntry::OpaqueWhiteout { parent });
                continue;
            }
            if let Some(target) = name.strip_prefix(".wh.") {
                let parent = std::path::Path::new(&path)
                    .parent()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let full = if parent.is_empty() {
                    target.to_string()
                } else {
                    format!("{parent}/{target}")
                };
                out.push(ExtractedEntry::Whiteout { path: full });
                continue;
            }
        }

        let size = entry.header().size().map_err(|source| Error::TarRead {
            digest: digest.to_string(),
            source,
        })?;
        if size > max_file_size {
            return Err(Error::FileTooLarge {
                digest: digest.to_string(),
                path,
                size,
                limit: max_file_size,
            });
        }
        let new_total = total_so_far.saturating_add(size);
        if new_total > max_total_size {
            return Err(Error::ArtifactTooLarge {
                reference: reference.to_string(),
                limit: max_total_size,
            });
        }
        *total_so_far = new_total;

        let mut buf = Vec::with_capacity(size as usize);
        std::io::Read::read_to_end(&mut entry, &mut buf).map_err(|source| Error::TarRead {
            digest: digest.to_string(),
            source,
        })?;

        out.push(ExtractedEntry::File {
            path: Some(path),
            content: buf,
        });
    }

    Ok(out)
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
    #[tracing::instrument(
        skip(self, event),
        name = "task.handle",
        fields(duration_ms = tracing::field::Empty)
    )]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            // Cheap change-signal via HEAD — avoids the manifest GET
            // and the config-blob GET that `pull_manifest_and_config`
            // would issue unconditionally. HEAD returns the digest of
            // whatever manifest the registry advertises for the tag —
            // for multi-arch images that is the index digest, for
            // single-arch it's the per-platform manifest digest. We
            // cache whatever HEAD saw so the next HEAD can match it,
            // instead of caching the per-platform digest that
            // `pull_manifest_and_config` returns (which won't line up
            // with the index digest on multi-arch tags and would
            // defeat the skip).
            let cache = &self.task_context.cache;
            let flow_name = self.task_context.flow.identity();
            let sanitized_artifact = sanitize_artifact_ref(&self.config.artifact);
            let cache_key = format!("flow.{flow_name}.oci_digest.{sanitized_artifact}");
            let cached_digest = cache
                .get(&cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok());
            let head_digest = if self.config.force_pull {
                None
            } else {
                let digest = self
                    .client
                    .fetch_manifest_digest(&self.reference, &self.auth)
                    .await
                    .map_err(|source| Error::Pull {
                        reference: self.config.artifact.clone(),
                        source,
                    })?;
                if cached_digest.as_deref() == Some(digest.as_str()) {
                    info!(
                        artifact = %self.config.artifact,
                        digest = %digest,
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
                Some(digest)
            };

            let (manifest, manifest_digest, _config_blob) = self
                .client
                .pull_manifest_and_config(&self.reference, &self.auth)
                .await
                .map_err(|source| Error::Pull {
                    reference: self.config.artifact.clone(),
                    source,
                })?;

            let layers = manifest.layers;

            // Two-track collection: oras artifact layers (identified by the
            // `org.opencontainers.image.title` annotation) emit one event
            // per layer as authored. Untitled layers are Docker image
            // layers; they compose an overlay filesystem so we merge them
            // in manifest order, honouring `.wh.` whiteouts, and emit the
            // final state as one event per surviving path.
            let mut file_events: Vec<PendingFileEvent> = Vec::new();
            let mut docker_state: std::collections::BTreeMap<String, PendingFileEvent> =
                std::collections::BTreeMap::new();
            let mut total_uncompressed: u64 = 0;
            for (index, layer) in layers.iter().enumerate() {
                let layer_title = layer
                    .annotations
                    .as_ref()
                    .and_then(|ann| ann.get("org.opencontainers.image.title"))
                    .cloned();

                let mut buf = Vec::new();
                self.client
                    .pull_blob(&self.reference, layer, &mut buf)
                    .await
                    .map_err(|source| Error::Pull {
                        reference: self.config.artifact.clone(),
                        source,
                    })?;

                let format = detect_layer_format(&layer.media_type);
                let extracted = extract_layer_files(
                    buf,
                    format,
                    &layer.digest,
                    self.config.max_file_size,
                    self.config.max_total_size,
                    &mut total_uncompressed,
                    &self.config.artifact,
                )?;

                let is_oras = layer_title.is_some();

                for entry in extracted {
                    match entry {
                        ExtractedEntry::File { path, content } => {
                            let resolved_path = path.unwrap_or_else(|| {
                                layer_title
                                    .clone()
                                    .unwrap_or_else(|| format!("layer-{index}"))
                            });
                            let pending = PendingFileEvent {
                                path: resolved_path.clone(),
                                content,
                                digest: layer.digest.clone(),
                                artifact_digest: manifest_digest.clone(),
                            };
                            if is_oras {
                                file_events.push(pending);
                            } else {
                                docker_state.insert(resolved_path, pending);
                            }
                        }
                        ExtractedEntry::Whiteout { path } => {
                            docker_state.remove(&path);
                            let prefix = format!("{path}/");
                            let doomed: Vec<String> = docker_state
                                .range(prefix.clone()..)
                                .take_while(|(k, _)| k.starts_with(&prefix))
                                .map(|(k, _)| k.clone())
                                .collect();
                            for key in doomed {
                                docker_state.remove(&key);
                            }
                        }
                        ExtractedEntry::OpaqueWhiteout { parent } => {
                            let prefix = if parent.is_empty() {
                                String::new()
                            } else {
                                format!("{parent}/")
                            };
                            let doomed: Vec<String> = docker_state
                                .keys()
                                .filter(|k| k.starts_with(&prefix) && k.as_str() != parent.as_str())
                                .cloned()
                                .collect();
                            for key in doomed {
                                docker_state.remove(&key);
                            }
                        }
                    }
                }
            }

            file_events.extend(docker_state.into_values());

            let total = file_events.len();
            for (index, pending) in file_events.into_iter().enumerate() {
                let PendingFileEvent {
                    path,
                    content,
                    digest,
                    artifact_digest,
                } = pending;

                let (data, meta) = match String::from_utf8(content) {
                    Ok(text) => {
                        let file_event = FileEvent {
                            path,
                            content: text,
                            digest,
                            artifact_digest,
                        };
                        let data = EventData::Json(
                            serde_json::to_value(&file_event)
                                .map_err(|source| Error::SerdeJson { source })?,
                        );
                        (data, None)
                    }
                    Err(err) => {
                        // Binary layer: surface path + digests in meta so
                        // downstream tasks can still route by key.
                        let bytes = bytes::Bytes::from(err.into_bytes());
                        let meta_struct = BinaryLayerMeta {
                            path,
                            digest,
                            artifact_digest,
                        };
                        let meta_value = serde_json::to_value(&meta_struct)
                            .map_err(|source| Error::SerdeJson { source })?;
                        let meta_map = match meta_value {
                            serde_json::Value::Object(map) => Some(map),
                            _ => Some(serde_json::Map::new()),
                        };
                        (EventData::Bytes(bytes), meta_map)
                    }
                };

                let mut builder = EventBuilder::new()
                    .data(data)
                    .subject(self.config.name.clone())
                    .task_id(self.task_id)
                    .task_type(self.task_type);
                if let Some(meta_map) = meta {
                    builder = builder.meta(meta_map);
                }
                let mut e = builder
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

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

            if total == 0 {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    let upstream_leaf_share = self.task_context.leaf_count.max(1);
                    for _ in 0..upstream_leaf_share {
                        arc.signal_completion(None);
                    }
                }
            }

            // Persist the digest HEAD returned (falling back to the
            // manifest digest on force_pull, where HEAD was skipped).
            // Persist only after every layer event was sent — a mid-pull
            // failure must re-emit the full batch on the next tick.
            let digest_to_cache = head_digest.unwrap_or_else(|| manifest_digest.clone());
            if let Err(e) = cache
                .put(&cache_key, digest_to_cache.clone().into(), None)
                .await
            {
                error!(
                    artifact = %self.config.artifact,
                    digest = %digest_to_cache,
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
        // Render the config at init time so operator-controlled fields such
        // as `artifact` can reference environment variables via
        // `{{env.VAR_NAME}}`. Event data is intentionally not in scope here —
        // the artifact reference is static for the lifetime of the task.
        let config: ProcessorConfig = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::RenderConfig { source })?;

        let reference: Reference =
            config
                .artifact
                .parse()
                .map_err(|source| Error::InvalidReference {
                    reference: config.artifact.clone(),
                    source,
                })?;

        let auth = load_auth(config.credentials_path.as_ref(), reference.registry()).await?;

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
            config: Arc::new(config),
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
                                            | Error::FileTooLarge { .. }
                                            | Error::ArtifactTooLarge { .. }
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

    /// Test helper: extract the `File` variant of `ExtractedEntry`.
    /// Returns `None` on `Whiteout` / `OpaqueWhiteout`; callers use
    /// `assert!(file.is_some(), "...")` to surface the failure.
    fn as_file(entry: &ExtractedEntry) -> Option<(Option<&str>, &[u8])> {
        match entry {
            ExtractedEntry::File { path, content } => Some((path.as_deref(), content.as_slice())),
            ExtractedEntry::Whiteout { .. } | ExtractedEntry::OpaqueWhiteout { .. } => None,
        }
    }

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
        // Entry exists but has neither `auth` nor username/password — caller should
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
            ..Default::default()
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

    fn build_tar(files: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, content) in files {
            let mut header = tar::Header::new_gnu();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_entry_type(tar::EntryType::Regular);
            header.set_cksum();
            builder.append_data(&mut header, name, *content).unwrap();
        }
        builder.into_inner().unwrap()
    }

    fn build_tar_gzip(files: &[(&str, &[u8])]) -> Vec<u8> {
        let tar_bytes = build_tar(files);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[test]
    fn detect_layer_format_raw() {
        assert!(matches!(
            detect_layer_format("application/yaml"),
            LayerFormat::Raw
        ));
        assert!(matches!(
            detect_layer_format("application/vnd.oci.image.config.v1+json"),
            LayerFormat::Raw
        ));
    }

    #[test]
    fn detect_layer_format_tar_gzip() {
        assert!(matches!(
            detect_layer_format("application/vnd.oci.image.layer.v1.tar+gzip"),
            LayerFormat::TarGzip
        ));
        assert!(matches!(
            detect_layer_format("application/vnd.docker.image.rootfs.diff.tar.gzip"),
            LayerFormat::TarGzip
        ));
    }

    #[test]
    fn detect_layer_format_tar() {
        assert!(matches!(
            detect_layer_format("application/vnd.oci.image.layer.v1.tar"),
            LayerFormat::Tar
        ));
    }

    #[test]
    fn extract_raw_layer_returns_untouched_bytes() {
        let mut total: u64 = 0;
        let result = extract_layer_files(
            b"hello: world".to_vec(),
            LayerFormat::Raw,
            "sha256:abc",
            1024,
            1024,
            &mut total,
            "test:v1",
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        let (path, content) = as_file(&result[0]).expect("raw layer emits File entry");
        assert!(path.is_none());
        assert_eq!(content, b"hello: world");
        assert_eq!(total, 12);
    }

    #[test]
    fn extract_tar_gzip_yields_one_file_per_entry() {
        let bytes = build_tar_gzip(&[
            ("flow.yaml", b"name: a"),
            ("processors/sms.yaml", b"kind: sms"),
        ]);
        let mut total: u64 = 0;
        let result = extract_layer_files(
            bytes,
            LayerFormat::TarGzip,
            "sha256:abc",
            1024,
            1024,
            &mut total,
            "test:v1",
        )
        .unwrap();
        assert_eq!(result.len(), 2);
        let (path0, content0) = as_file(&result[0]).expect("first tar entry emits File");
        assert_eq!(path0, Some("flow.yaml"));
        assert_eq!(content0, b"name: a");
        let (path1, content1) = as_file(&result[1]).expect("second tar entry emits File");
        assert_eq!(path1, Some("processors/sms.yaml"));
        assert_eq!(content1, b"kind: sms");
    }

    #[test]
    fn extract_tar_strips_leading_slash() {
        let mut header = tar::Header::new_gnu();
        header.set_size(1);
        header.set_mode(0o644);
        header.set_entry_type(tar::EntryType::Regular);
        header.set_path("etc/flow.yaml").unwrap();
        // Overwrite the name bytes in-place with a leading slash — the safe
        // setter refuses absolute paths, but real docker layers occasionally
        // contain them.
        let name_bytes = &mut header.as_old_mut().name;
        name_bytes[0] = b'/';
        for (i, b) in b"etc/flow.yaml".iter().enumerate() {
            name_bytes[i + 1] = *b;
        }
        header.set_cksum();

        let mut builder = tar::Builder::new(Vec::new());
        builder.append(&header, &b"x"[..]).unwrap();
        let bytes = builder.into_inner().unwrap();

        let mut total: u64 = 0;
        let result = extract_layer_files(
            bytes,
            LayerFormat::Tar,
            "sha256:abc",
            1024,
            1024,
            &mut total,
            "test:v1",
        )
        .unwrap();
        let (path, _content) = as_file(&result[0]).expect("stripped-slash entry emits File");
        assert_eq!(path, Some("etc/flow.yaml"));
    }

    #[test]
    fn extract_tar_surfaces_whiteout_and_opaque_markers() {
        let bytes = build_tar(&[
            ("kept.yaml", b"kept"),
            ("dir/.wh.removed.yaml", b"anything"),
            ("dir/.wh..wh..opq", b"anything"),
        ]);
        let mut total: u64 = 0;
        let result = extract_layer_files(
            bytes,
            LayerFormat::Tar,
            "sha256:abc",
            1024,
            1024,
            &mut total,
            "test:v1",
        )
        .unwrap();
        assert_eq!(result.len(), 3);
        let (kept_path, _kept_content) = as_file(&result[0]).expect("kept.yaml is a File entry");
        assert_eq!(kept_path, Some("kept.yaml"));
        assert!(matches!(
            &result[1],
            ExtractedEntry::Whiteout { path } if path == "dir/removed.yaml"
        ));
        assert!(matches!(
            &result[2],
            ExtractedEntry::OpaqueWhiteout { parent } if parent == "dir"
        ));
    }

    #[test]
    fn extract_rejects_file_over_max_file_size() {
        let big = [b'a'; 200];
        let bytes = build_tar(&[("big.yaml", &big)]);
        let mut total: u64 = 0;
        let result = extract_layer_files(
            bytes,
            LayerFormat::Tar,
            "sha256:abc",
            100,
            10_000,
            &mut total,
            "test:v1",
        );
        assert!(matches!(result, Err(Error::FileTooLarge { .. })));
    }

    #[test]
    fn extract_rejects_when_total_over_cap() {
        let a = [b'a'; 60];
        let b = [b'b'; 60];
        let bytes = build_tar(&[("a.yaml", &a), ("b.yaml", &b)]);
        let mut total: u64 = 0;
        let result = extract_layer_files(
            bytes,
            LayerFormat::Tar,
            "sha256:abc",
            1024,
            100,
            &mut total,
            "test:v1",
        );
        assert!(matches!(result, Err(Error::ArtifactTooLarge { .. })));
    }

    #[test]
    fn extract_raw_preserves_binary_bytes() {
        let mut total: u64 = 0;
        let result = extract_layer_files(
            vec![0xff, 0xfe, 0xfd],
            LayerFormat::Raw,
            "sha256:abc",
            1024,
            1024,
            &mut total,
            "test:v1",
        )
        .unwrap();
        assert_eq!(result.len(), 1);
        let (path, content) = as_file(&result[0]).expect("raw layer emits File entry");
        assert!(path.is_none());
        assert_eq!(content, &[0xff, 0xfe, 0xfd]);
    }
}
