//! Git sync processor — clones or pulls a repository and emits file contents as events.
//!
//! Each file found under the configured path is emitted as a separate event
//! with `{path, content, commit}` data. Downstream tasks decide what to do
//! with the files (parse as flows, write to cache, store in object store).

use super::config::{GitAuth, GitAuthType, Processor as ProcessorConfig};
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use std::path::Path;
use std::sync::Arc;
use tokio::process::Command;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::error;
use walkdir::WalkDir;

/// File event emitted for each file found in the repository.
#[derive(Debug, Clone, serde::Serialize)]
pub struct FileEvent {
    /// Relative path of the file within the scanned directory.
    pub path: String,
    /// File content as a string.
    pub content: String,
    /// Git commit hash at HEAD.
    pub commit: String,
}

/// Errors that can occur during git sync processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Git clone failed: {stderr}")]
    GitClone { stderr: String },
    #[error("Git pull failed: {stderr}")]
    GitPull { stderr: String },
    #[error("Git rev-parse failed: {stderr}")]
    GitRevParse { stderr: String },
    #[error("Git command failed: {source}")]
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
    #[error("File walk error: {source}")]
    WalkDir {
        #[source]
        source: walkdir::Error,
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

/// Event handler for git sync operations.
pub struct EventHandler {
    config: Arc<ProcessorConfig>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Handles a trigger event by syncing the repository and emitting file events.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async {
            // Clone or pull the repository.
            let commit = clone_or_pull(&self.config).await?;

            // Determine the scan base path.
            let scan_path = match &self.config.path {
                Some(p) => self.config.clone_path.join(p),
                None => self.config.clone_path.clone(),
            };

            // Walk files and emit one event per file.
            let entries: Vec<_> = WalkDir::new(&scan_path)
                .follow_links(false)
                .into_iter()
                .filter_map(|e| e.ok())
                .filter(|e| e.path().is_file())
                .collect();

            for (index, entry) in entries.iter().enumerate() {
                let file_path = entry.path();
                let content =
                    std::fs::read_to_string(file_path).map_err(|source| Error::FileRead {
                        path: file_path.display().to_string(),
                        source,
                    })?;

                let relative_path = file_path
                    .strip_prefix(&scan_path)
                    .unwrap_or(file_path)
                    .to_string_lossy()
                    .replace('\\', "/");

                let file_event = FileEvent {
                    path: relative_path,
                    content,
                    commit: commit.clone(),
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

                // Only the last file carries the completion signal.
                if index == entries.len() - 1 {
                    match self.tx {
                        None => {
                            // Leaf task: signal completion.
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

            // If no files were found there is nothing to forward downstream.
            // The upstream completion channel was sized for every leaf in
            // git_sync's subtree, so emit one signal per leaf to satisfy that
            // contract instead of leaving the source waiting forever.
            if entries.is_empty() {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    let upstream_leaf_share = self.task_context.leaf_count.max(1);
                    for _ in 0..upstream_leaf_share {
                        arc.signal_completion(None);
                    }
                }
            }

            Ok(())
        })
        .await
    }
}

// --- Git operations ---

/// Clone or pull the repository and return the HEAD commit hash.
async fn clone_or_pull(config: &ProcessorConfig) -> Result<String, Error> {
    let clone_path = &config.clone_path;

    if clone_path.join(".git").exists() {
        pull(clone_path, &config.auth).await?;
    } else {
        clone(
            &config.repository_url,
            &config.branch,
            clone_path,
            &config.auth,
        )
        .await?;
    }

    head_commit(clone_path, &config.auth).await
}

/// Shallow clone a repository.
async fn clone(url: &str, branch: &str, path: &Path, auth: &GitAuth) -> Result<(), Error> {
    let mut cmd = Command::new("git");
    cmd.args([
        "clone",
        "--branch",
        branch,
        "--single-branch",
        "--depth",
        "1",
    ]);
    cmd.arg(url);
    cmd.arg(path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd
        .output()
        .await
        .map_err(|source| Error::GitIo { source })?;

    if !output.status.success() {
        return Err(Error::GitClone {
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }
    Ok(())
}

/// Fast-forward pull an existing clone.
async fn pull(path: &Path, auth: &GitAuth) -> Result<(), Error> {
    let mut cmd = Command::new("git");
    cmd.args(["pull", "--ff-only"]);
    cmd.current_dir(path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd
        .output()
        .await
        .map_err(|source| Error::GitIo { source })?;

    if !output.status.success() {
        return Err(Error::GitPull {
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }
    Ok(())
}

/// Read the HEAD commit hash.
async fn head_commit(path: &Path, auth: &GitAuth) -> Result<String, Error> {
    let mut cmd = Command::new("git");
    cmd.args(["rev-parse", "HEAD"]);
    cmd.current_dir(path);
    apply_auth_env(&mut cmd, auth);

    let output = cmd
        .output()
        .await
        .map_err(|source| Error::GitIo { source })?;

    if !output.status.success() {
        return Err(Error::GitRevParse {
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

/// Apply authentication environment variables to a git command.
fn apply_auth_env(cmd: &mut Command, auth: &GitAuth) {
    match auth.auth_type {
        GitAuthType::Ssh => {
            let mut ssh_cmd = "ssh".to_string();
            if let Some(ref key) = auth.ssh_key_path {
                ssh_cmd.push_str(&format!(" -i {}", key.display()));
            }
            match &auth.ssh_known_hosts_path {
                Some(hosts) => {
                    ssh_cmd.push_str(&format!(" -o UserKnownHostsFile={}", hosts.display()));
                    ssh_cmd.push_str(" -o StrictHostKeyChecking=yes");
                }
                None => {
                    ssh_cmd.push_str(" -o StrictHostKeyChecking=no");
                }
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
        Ok(EventHandler {
            config: Arc::clone(&self.config),
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

        let event_handler = match tokio_retry::Retry::spawn(retry_config.strategy(), || async {
            match self.init().await {
                Ok(handler) => Ok(handler),
                Err(e) => {
                    error!(error = %e, "Failed to initialize git sync processor.");
                    Err(tokio_retry::RetryError::transient(e))
                }
            }
        })
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
                        let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                            match handler.handle(event.clone()).await {
                                Ok(()) => Ok(()),
                                Err(e) => {
                                    error!(error = %e, "Git sync failed.");
                                    Err(tokio_retry::RetryError::transient(e))
                                }
                            }
                        })
                        .await;

                        if let Err(e) = result {
                            error!(error = %e, "Git sync exhausted all retry attempts.");
                        }
                    });
                }
                None => return Ok(()),
            }
        }
    }
}

/// Builder for git sync processor.
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
