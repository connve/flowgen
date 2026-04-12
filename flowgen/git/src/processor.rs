//! Git sync processor that clones/pulls a Git repository and syncs
//! flows and resources to the distributed cache.

use crate::config::{GitAuth, GitAuthType, Processor as ProcessorConfig};
use flowgen_core::cache::Cache;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error, info};
use walkdir::WalkDir;

/// Supported flow file extensions.
const FLOW_EXTENSIONS: &[&str] = &["yaml", "yml"];

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Git clone failed: {0}")]
    GitClone(String),
    #[error("Git pull failed: {0}")]
    GitPull(String),
    #[error("Git HEAD commit read failed: {0}")]
    GitHead(String),
    #[error("Git command IO error: {source}")]
    GitIo {
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to read file '{path}': {source}")]
    FileRead {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse YAML in '{path}': {source}")]
    YamlParse {
        path: String,
        #[source]
        source: serde_yaml::Error,
    },
    #[error("Missing 'flow.name' in '{path}'")]
    MissingFlowName { path: String },
    #[error("Flow path not relative: '{path}'")]
    PathNotRelative { path: String },
    #[error("Cache error: {source}")]
    Cache {
        #[source]
        source: flowgen_core::cache::CacheError,
    },
    #[error("Discovery error: {source}")]
    Discovery {
        #[source]
        source: walkdir::Error,
    },
    #[error("JSON serialization error: {source}")]
    Json {
        #[source]
        source: serde_json::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    #[error("Error sending event: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Cache initialization failed: {source}")]
    CacheInit {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

/// Sync statistics.
#[derive(Debug, Default, serde::Serialize)]
struct SyncStats {
    commit: String,
    flows_added: usize,
    flows_updated: usize,
    flows_deleted: usize,
    flows_unchanged: usize,
    resources_synced: usize,
}

/// Lightweight struct for extracting flow.name without full FlowConfig.
#[derive(serde::Deserialize)]
struct FlowNameCheck {
    flow: FlowNameOnly,
}

#[derive(serde::Deserialize)]
struct FlowNameOnly {
    name: Option<String>,
}

/// Validated flow with display name and content.
struct ValidatedFlow {
    display_name: String,
    content: String,
}

/// Event handler for git sync operations.
pub struct EventHandler {
    config: Arc<ProcessorConfig>,
    metadata_cache: Arc<dyn Cache>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            let stats = self.run_sync().await?;

            info!(
                commit = %stats.commit,
                added = stats.flows_added,
                updated = stats.flows_updated,
                deleted = stats.flows_deleted,
                unchanged = stats.flows_unchanged,
                resources = stats.resources_synced,
                "Sync completed"
            );

            // Emit sync result as event.
            let result_json = serde_json::to_value(&stats)
                .map_err(|source| Error::Json { source })?;

            let mut e = EventBuilder::new()
                .data(EventData::Json(result_json))
                .subject(self.config.name.clone())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            match self.tx {
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
                None => {
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        if let Ok(mut guard) = arc.lock() {
                            if let Some(tx) = guard.take() {
                                tx.send(Ok(())).ok();
                            }
                        }
                    }
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }

    /// Runs the full sync cycle: git pull, discover, validate, sync to cache.
    async fn run_sync(&self) -> Result<SyncStats, Error> {
        let commit = clone_or_pull(&self.config).await?;
        let flows_base = self.config.clone_path.join(&self.config.flows_path);

        let flow_files = discover_flows(&flows_base)
            .map_err(|source| Error::Discovery { source })?;

        let flows = validate_flows(&flows_base, &flow_files)?;

        let mut stats = sync_flows(
            self.metadata_cache.as_ref(),
            &self.config.flows_prefix,
            &flows,
        )
        .await?;
        stats.commit = commit;

        if let Some(ref resources_path) = self.config.resources_path {
            let resources_base = self.config.clone_path.join(resources_path);
            if resources_base.exists() {
                stats.resources_synced = sync_resources(
                    self.metadata_cache.as_ref(),
                    &self.config.resources_prefix,
                    &resources_base,
                )
                .await?;
            }
        }

        // Write sync metadata.
        let metadata = serde_json::to_vec(
            &serde_json::json!({
                "last_sync_at": chrono::Utc::now().timestamp(),
                "last_commit": stats.commit,
                "flows_added": stats.flows_added,
                "flows_updated": stats.flows_updated,
                "flows_deleted": stats.flows_deleted,
                "resources_synced": stats.resources_synced,
            }),
        )
        .map_err(|source| Error::Json { source })?;

        self.metadata_cache
            .put(
                "flowgen.metadata.sync_status",
                bytes::Bytes::from(metadata),
                None,
            )
            .await
            .map_err(|source| Error::Cache { source })?;

        Ok(stats)
    }
}

// --- Git operations ---

async fn clone_or_pull(config: &ProcessorConfig) -> Result<String, Error> {
    let clone_path = &config.clone_path;

    if clone_path.join(".git").exists() {
        pull(clone_path, &config.auth).await?;
    } else {
        clone(&config.repository_url, &config.branch, clone_path, &config.auth).await?;
    }

    head_commit(clone_path, &config.auth).await
}

async fn clone(url: &str, branch: &str, path: &Path, auth: &GitAuth) -> Result<(), Error> {
    let mut cmd = Command::new("git");
    cmd.args(["clone", "--branch", branch, "--single-branch", "--depth", "1"]);
    cmd.arg(url);
    cmd.arg(path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd.output().await.map_err(|source| Error::GitIo { source })?;
    if !output.status.success() {
        return Err(Error::GitClone(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }
    Ok(())
}

async fn pull(path: &Path, auth: &GitAuth) -> Result<(), Error> {
    let mut cmd = Command::new("git");
    cmd.args(["pull", "--ff-only"]);
    cmd.current_dir(path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd.output().await.map_err(|source| Error::GitIo { source })?;
    if !output.status.success() {
        return Err(Error::GitPull(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }
    Ok(())
}

async fn head_commit(path: &Path, auth: &GitAuth) -> Result<String, Error> {
    let mut cmd = Command::new("git");
    cmd.args(["rev-parse", "HEAD"]);
    cmd.current_dir(path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd.output().await.map_err(|source| Error::GitIo { source })?;
    if !output.status.success() {
        return Err(Error::GitHead(
            String::from_utf8_lossy(&output.stderr).to_string(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn apply_auth_env(cmd: &mut Command, auth: &GitAuth) {
    match auth.auth_type {
        GitAuthType::Ssh => {
            let mut ssh_cmd = "ssh".to_string();
            if let Some(ref key) = auth.ssh_key_path {
                ssh_cmd.push_str(&format!(" -i {}", key.display()));
            }
            if let Some(ref hosts) = auth.ssh_known_hosts_path {
                ssh_cmd.push_str(&format!(" -o UserKnownHostsFile={}", hosts.display()));
                ssh_cmd.push_str(" -o StrictHostKeyChecking=yes");
            } else {
                ssh_cmd.push_str(" -o StrictHostKeyChecking=no");
            }
            cmd.env("GIT_SSH_COMMAND", ssh_cmd);
        }
        GitAuthType::Token => {
            if let Some(ref token) = auth.token {
                cmd.env("GIT_ASKPASS", "echo");
                cmd.env("GIT_PASSWORD", token);
            }
        }
        GitAuthType::None => {}
    }
}

// --- Discovery ---

fn discover_flows(base_path: &Path) -> Result<Vec<PathBuf>, walkdir::Error> {
    let mut files = Vec::new();
    for entry in WalkDir::new(base_path).follow_links(false).into_iter().filter_map(|e| e.ok()) {
        let path = entry.path();
        if path.is_file() {
            if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                if FLOW_EXTENSIONS.contains(&ext) {
                    files.push(path.to_path_buf());
                }
            }
        }
    }
    Ok(files)
}

fn discover_resources(base_path: &Path) -> Result<Vec<PathBuf>, walkdir::Error> {
    let mut files = Vec::new();
    for entry in WalkDir::new(base_path).follow_links(false).into_iter().filter_map(|e| e.ok()) {
        if entry.path().is_file() {
            files.push(entry.path().to_path_buf());
        }
    }
    Ok(files)
}

// --- Validation ---

fn validate_flows(
    base_path: &Path,
    flow_files: &[PathBuf],
) -> Result<HashMap<String, ValidatedFlow>, Error> {
    let mut flows = HashMap::new();

    for file_path in flow_files {
        let content = std::fs::read_to_string(file_path).map_err(|source| Error::FileRead {
            path: file_path.display().to_string(),
            source,
        })?;

        let parsed: FlowNameCheck =
            serde_yaml::from_str(&content).map_err(|source| Error::YamlParse {
                path: file_path.display().to_string(),
                source,
            })?;

        let display_name = parsed.flow.name.ok_or_else(|| Error::MissingFlowName {
            path: file_path.display().to_string(),
        })?;

        let rel = file_path
            .strip_prefix(base_path)
            .map_err(|_| Error::PathNotRelative {
                path: file_path.display().to_string(),
            })?;

        let relative_key = rel.with_extension("").to_string_lossy().replace('\\', "/");

        flows.insert(relative_key, ValidatedFlow { display_name, content });
    }

    Ok(flows)
}

// --- Cache sync ---

async fn sync_flows(
    cache: &dyn Cache,
    prefix: &str,
    flows: &HashMap<String, ValidatedFlow>,
) -> Result<SyncStats, Error> {
    let mut stats = SyncStats::default();

    let current_keys = cache
        .list_keys(prefix)
        .await
        .map_err(|source| Error::Cache { source })?;

    for (relative_key, validated) in flows {
        let key = format!("{prefix}.{relative_key}");

        match cache.get(&key).await.map_err(|source| Error::Cache { source })? {
            Some(existing) => {
                if String::from_utf8_lossy(&existing) == validated.content {
                    stats.flows_unchanged += 1;
                    continue;
                }
                debug!(flow = %validated.display_name, key = %relative_key, "Updating flow in cache.");
                stats.flows_updated += 1;
            }
            None => {
                debug!(flow = %validated.display_name, key = %relative_key, "Adding flow to cache.");
                stats.flows_added += 1;
            }
        }

        cache
            .put(&key, bytes::Bytes::from(validated.content.clone()), None)
            .await
            .map_err(|source| Error::Cache { source })?;
    }

    let git_keys: HashSet<String> = flows
        .keys()
        .map(|k| format!("{prefix}.{k}"))
        .collect();

    for key in &current_keys {
        if !git_keys.contains(key) {
            let stripped = key.strip_prefix(&format!("{prefix}.")).unwrap_or(key);
            debug!(key = %stripped, "Deleting flow from cache.");
            cache.delete(key).await.map_err(|source| Error::Cache { source })?;
            stats.flows_deleted += 1;
        }
    }

    Ok(stats)
}

async fn sync_resources(
    cache: &dyn Cache,
    prefix: &str,
    base_path: &Path,
) -> Result<usize, Error> {
    let files = discover_resources(base_path).map_err(|source| Error::Discovery { source })?;
    let mut count = 0;

    for file_path in &files {
        let content = std::fs::read(file_path).map_err(|source| Error::FileRead {
            path: file_path.display().to_string(),
            source,
        })?;

        let rel = file_path.strip_prefix(base_path).unwrap_or(file_path.as_path());
        let key = format!("{prefix}.{}", rel.display());

        let should_put = match cache.get(&key).await.map_err(|source| Error::Cache { source })? {
            Some(existing) => existing.as_ref() != content.as_slice(),
            None => true,
        };

        if should_put {
            cache
                .put(&key, bytes::Bytes::from(content), None)
                .await
                .map_err(|source| Error::Cache { source })?;
        }
        count += 1;
    }

    Ok(count)
}

// --- Processor / Runner ---

/// Git sync processor.
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
        // Use the task context cache as the metadata store.
        let metadata_cache = Arc::clone(&self.task_context.cache);

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            metadata_cache,
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
            retry_config.strategy(),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize git sync processor");
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

        loop {
            match self.rx.recv().await {
                Some(event) => {
                    let handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    tokio::spawn(async move {
                        let result =
                            tokio_retry::Retry::spawn(retry_strategy, || async {
                                match handler.handle(event.clone()).await {
                                    Ok(()) => Ok(()),
                                    Err(e) => {
                                        error!(error = %e, "Git sync failed, retrying");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                        if let Err(e) = result {
                            error!(error = %e, "Git sync exhausted retries");
                        }
                    });
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for `Processor`.
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
