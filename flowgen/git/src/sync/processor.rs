//! Git sync processor — clones or pulls a repository and emits file contents as events.
//!
//! Each file found under the configured path is emitted as a separate event
//! with `{path, content, commit}` data. Downstream tasks decide what to do
//! with the files (parse as flows, write to cache, store in object store).

use super::config::{GitAuthType, Processor as ProcessorConfig};
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;
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
    #[error("Git clone failed for {url}: {source}")]
    Clone {
        url: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Git fetch failed for {url}: {source}")]
    Fetch {
        url: String,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to open existing repository at {path}: {source}")]
    OpenRepo {
        path: PathBuf,
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to read HEAD commit: {source}")]
    HeadCommit {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to checkout worktree: {source}")]
    Checkout {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("SSH authentication is not supported. Use HTTPS with a token instead.")]
    SshNotSupported,
    #[error("Invalid clone_path configuration: {source}")]
    InvalidClonePath {
        #[source]
        source: flowgen_core::validate::Error,
    },
    #[error("Git operation panicked or was cancelled: {source}")]
    JoinError {
        #[source]
        source: tokio::task::JoinError,
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
    /// Resolved local path for the repository clone. Either the explicit
    /// `clone_path` from config, or the derived default
    /// `<temp>/<flow_name>/<task_name>`.
    clone_path: PathBuf,
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
            let commit = clone_or_pull(&self.config, &self.clone_path).await?;

            // Determine the scan base path.
            let scan_path = match &self.config.path {
                Some(p) => self.clone_path.join(p),
                None => self.clone_path.clone(),
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

// --- Git operations (gix-backed) ---
//
// All gix calls are blocking and must run on a dedicated thread. We wrap the
// whole clone/pull/head sequence in a single `spawn_blocking` to avoid
// re-paying the thread-handoff cost per call.

/// Clone or pull the repository and return the HEAD commit hash.
async fn clone_or_pull(config: &ProcessorConfig, clone_path: &Path) -> Result<String, Error> {
    if matches!(config.auth.auth_type, GitAuthType::Ssh) {
        return Err(Error::SshNotSupported);
    }

    let url = build_authenticated_url(config)?;
    let clone_path = clone_path.to_path_buf();
    let branch = config.branch.clone();
    let original_url = config.repository_url.clone();

    // Capture the current tracing span so log lines emitted from the gix
    // worker thread inherit the flow/task context. Without this the
    // `pull_repo` events show up bare with no `flow=…` / `task=…` fields.
    let span = tracing::Span::current();
    task::spawn_blocking(move || {
        let _enter = span.enter();
        sync_blocking(&url, &original_url, &branch, &clone_path)
    })
    .await
    .map_err(|source| Error::JoinError { source })?
}

/// Synchronous core of the clone/pull pipeline. Runs on a blocking thread.
fn sync_blocking(
    url: &str,
    original_url: &str,
    branch: &str,
    path: &Path,
) -> Result<String, Error> {
    // gix expects the clone target's parent to exist. The derived default
    // path includes per-flow and per-task segments that are not pre-created
    // by the volume mount, so ensure the full path exists up front.
    std::fs::create_dir_all(path).map_err(|e| Error::Clone {
        url: original_url.to_string(),
        source: Box::new(e),
    })?;

    if path.join(".git").exists() {
        fetch_existing(path, original_url)?;
    } else {
        shallow_clone(url, branch, path, original_url)?;
    }
    head_commit(path)
}

/// Builds the URL used for network operations, embedding a token in the
/// userinfo segment when token auth is configured. The `original_url` is
/// preserved separately for error messages so tokens never appear in logs.
fn build_authenticated_url(config: &ProcessorConfig) -> Result<String, Error> {
    if let GitAuthType::Token = config.auth.auth_type {
        if let Some(token) = config.auth.token.as_deref() {
            // Inject the token as a basic-auth username (GitHub/GitLab convention).
            // For URLs like `https://github.com/org/repo.git`, the result is
            // `https://<token>@github.com/org/repo.git`.
            if let Some(rest) = config.repository_url.strip_prefix("https://") {
                return Ok(format!("https://{token}@{rest}"));
            }
            if let Some(rest) = config.repository_url.strip_prefix("http://") {
                return Ok(format!("http://{token}@{rest}"));
            }
        }
    }
    Ok(config.repository_url.clone())
}

/// Performs a shallow clone of a single branch.
fn shallow_clone(url: &str, branch: &str, path: &Path, log_url: &str) -> Result<(), Error> {
    let mut prepare = gix::prepare_clone(url, path)
        .map_err(|e| Error::Clone {
            url: log_url.to_string(),
            source: Box::new(e),
        })?
        .with_ref_name(Some(branch))
        .map_err(|e| Error::Clone {
            url: log_url.to_string(),
            source: Box::new(e),
        })?
        .with_shallow(gix::remote::fetch::Shallow::DepthAtRemote(
            std::num::NonZeroU32::MIN,
        ));

    let (mut checkout, _outcome) = prepare
        .fetch_then_checkout(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| Error::Clone {
            url: log_url.to_string(),
            source: Box::new(e),
        })?;

    checkout
        .main_worktree(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| Error::Checkout {
            source: Box::new(e),
        })?;

    Ok(())
}

/// Fetches the latest refs into an existing clone and resets HEAD to the
/// remote tracking branch. Mirrors `git pull --ff-only` semantics.
fn fetch_existing(path: &Path, log_url: &str) -> Result<(), Error> {
    let repo = gix::open(path).map_err(|e| Error::OpenRepo {
        path: path.to_path_buf(),
        source: Box::new(e),
    })?;

    let remote = repo
        .find_default_remote(gix::remote::Direction::Fetch)
        .ok_or_else(|| Error::Fetch {
            url: log_url.to_string(),
            source: "no default remote configured".into(),
        })?
        .map_err(|e| Error::Fetch {
            url: log_url.to_string(),
            source: Box::new(e),
        })?;

    let connection = remote
        .connect(gix::remote::Direction::Fetch)
        .map_err(|e| Error::Fetch {
            url: log_url.to_string(),
            source: Box::new(e),
        })?;

    let prepared = connection
        .prepare_fetch(gix::progress::Discard, Default::default())
        .map_err(|e| Error::Fetch {
            url: log_url.to_string(),
            source: Box::new(e),
        })?;

    prepared
        .receive(gix::progress::Discard, &gix::interrupt::IS_INTERRUPTED)
        .map_err(|e| Error::Fetch {
            url: log_url.to_string(),
            source: Box::new(e),
        })?;

    Ok(())
}

/// Reads the HEAD commit object id.
fn head_commit(path: &Path) -> Result<String, Error> {
    let repo = gix::open(path).map_err(|e| Error::OpenRepo {
        path: path.to_path_buf(),
        source: Box::new(e),
    })?;

    let head_id = repo.head_id().map_err(|e| Error::HeadCommit {
        source: Box::new(e),
    })?;

    Ok(head_id.to_string())
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
        // Reject unsupported auth at startup so the task fails fast rather
        // than once per event under retry.
        if matches!(self.config.auth.auth_type, GitAuthType::Ssh) {
            return Err(Error::SshNotSupported);
        }

        let clone_path = match &self.config.clone_path {
            Some(path) => {
                flowgen_core::validate::validate_path(
                    flowgen_core::validate::PathField("clone_path"),
                    path,
                )
                .map_err(|source| Error::InvalidClonePath { source })?;
                path.clone()
            }
            None => std::env::temp_dir()
                .join(&self.task_context.flow.name)
                .join(&self.config.name),
        };

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            clone_path,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::config::{GitAuth, GitAuthType};
    use std::path::PathBuf;
    use std::sync::Arc;

    /// Helper to build a minimal TaskContext for tests.
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

    /// Helper to construct a minimal ProcessorConfig for tests.
    fn test_config() -> ProcessorConfig {
        ProcessorConfig {
            name: "test_sync".to_string(),
            repository_url: "https://github.com/org/repo.git".to_string(),
            branch: "main".to_string(),
            path: None,
            clone_path: None,
            auth: GitAuth::default(),
            depends_on: None,
            retry: None,
        }
    }

    // ── Error Display ────────────────────────────────────────────────

    #[test]
    fn error_display_clone() {
        let err = Error::Clone {
            url: "https://github.com/org/repo.git".to_string(),
            source: "network timeout".into(),
        };
        assert_eq!(
            err.to_string(),
            "Git clone failed for https://github.com/org/repo.git: network timeout"
        );
    }

    #[test]
    fn error_display_fetch() {
        let err = Error::Fetch {
            url: "https://github.com/org/repo.git".to_string(),
            source: "auth failed".into(),
        };
        assert_eq!(
            err.to_string(),
            "Git fetch failed for https://github.com/org/repo.git: auth failed"
        );
    }

    #[test]
    fn error_display_open_repo() {
        let err = Error::OpenRepo {
            path: PathBuf::from("/tmp/repo"),
            source: "corrupt index".into(),
        };
        assert_eq!(
            err.to_string(),
            "Failed to open existing repository at /tmp/repo: corrupt index"
        );
    }

    #[test]
    fn error_display_head_commit() {
        let err = Error::HeadCommit {
            source: "detached HEAD".into(),
        };
        assert_eq!(err.to_string(), "Failed to read HEAD commit: detached HEAD");
    }

    #[test]
    fn error_display_checkout() {
        let err = Error::Checkout {
            source: "conflict".into(),
        };
        assert_eq!(err.to_string(), "Failed to checkout worktree: conflict");
    }

    #[test]
    fn error_display_ssh_not_supported() {
        assert_eq!(
            Error::SshNotSupported.to_string(),
            "SSH authentication is not supported. Use HTTPS with a token instead."
        );
    }

    #[test]
    fn error_display_file_read() {
        let err = Error::FileRead {
            path: "/tmp/file.txt".to_string(),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        };
        assert_eq!(
            err.to_string(),
            "Failed to read file '/tmp/file.txt': not found"
        );
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
        let inner = Error::SshNotSupported;
        let err = Error::RetryExhausted {
            source: Box::new(inner),
        };
        assert!(err
            .to_string()
            .contains("Task failed after all retry attempts"));
    }

    // ── Config Deserialization ───────────────────────────────────────

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "sync",
            "repository_url": "https://github.com/org/repo.git"
        }"#;
        let config: ProcessorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "sync");
        assert_eq!(config.repository_url, "https://github.com/org/repo.git");
        assert_eq!(config.branch, "main");
        assert!(config.path.is_none());
        assert!(config.clone_path.is_none());
        assert_eq!(config.auth.auth_type, GitAuthType::None);
        assert!(config.depends_on.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn config_deser_full() {
        let json = r#"{
            "name": "sync_all",
            "repository_url": "https://github.com/org/repo.git",
            "branch": "develop",
            "path": "flows/",
            "clone_path": "/data/repo",
            "auth": {
                "type": "token",
                "token": "ghp_abc123"
            },
            "depends_on": ["trigger"],
            "retry": { "max_retries": 2, "initial_interval": "500ms" }
        }"#;
        let config: ProcessorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.branch, "develop");
        assert_eq!(config.path.as_deref(), Some("flows/"));
        assert_eq!(config.clone_path, Some(PathBuf::from("/data/repo")));
        assert_eq!(config.auth.auth_type, GitAuthType::Token);
        assert_eq!(config.auth.token.as_deref(), Some("ghp_abc123"));
        assert_eq!(config.depends_on, Some(vec!["trigger".to_string()]));
        assert!(config.retry.is_some());
    }

    #[test]
    fn config_deser_ssh_auth() {
        let json = r#"{
            "name": "sync_ssh",
            "repository_url": "git@github.com:org/repo.git",
            "auth": {
                "type": "ssh",
                "ssh_key_path": "/etc/git/deploy-key",
                "ssh_known_hosts_path": "/etc/git/known_hosts"
            }
        }"#;
        let config: ProcessorConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.auth.auth_type, GitAuthType::Ssh);
        assert_eq!(
            config.auth.ssh_key_path,
            Some(PathBuf::from("/etc/git/deploy-key"))
        );
        assert_eq!(
            config.auth.ssh_known_hosts_path,
            Some(PathBuf::from("/etc/git/known_hosts"))
        );
    }

    #[test]
    fn config_deser_missing_name_fails() {
        let json = r#"{
            "repository_url": "https://github.com/org/repo.git"
        }"#;
        let result = serde_json::from_str::<ProcessorConfig>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_repository_url_fails() {
        let json = r#"{
            "name": "sync"
        }"#;
        let result = serde_json::from_str::<ProcessorConfig>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_roundtrip_serde() {
        let json = r#"{
            "name": "rt",
            "repository_url": "https://github.com/org/repo.git",
            "branch": "main",
            "path": "configs/"
        }"#;
        let config: ProcessorConfig = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: ProcessorConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    // ── GitAuth / GitAuthType ───────────────────────────────────────

    #[test]
    fn git_auth_default_is_none() {
        let auth = GitAuth::default();
        assert_eq!(auth.auth_type, GitAuthType::None);
        assert!(auth.ssh_key_path.is_none());
        assert!(auth.ssh_known_hosts_path.is_none());
        assert!(auth.token.is_none());
    }

    #[test]
    fn git_auth_type_deser_none() {
        let json = r#"{ "type": "none" }"#;
        let auth: GitAuth = serde_json::from_str(json).unwrap();
        assert_eq!(auth.auth_type, GitAuthType::None);
    }

    #[test]
    fn git_auth_type_deser_token() {
        let json = r#"{ "type": "token", "token": "abc" }"#;
        let auth: GitAuth = serde_json::from_str(json).unwrap();
        assert_eq!(auth.auth_type, GitAuthType::Token);
        assert_eq!(auth.token.as_deref(), Some("abc"));
    }

    // ── FileEvent ───────────────────────────────────────────────────

    #[test]
    fn file_event_serialization() {
        let fe = FileEvent {
            path: "flows/main.yaml".to_string(),
            content: "name: test".to_string(),
            commit: "abc123".to_string(),
        };
        let value = serde_json::to_value(&fe).unwrap();
        assert_eq!(value["path"], "flows/main.yaml");
        assert_eq!(value["content"], "name: test");
        assert_eq!(value["commit"], "abc123");
    }

    #[test]
    fn file_event_clone() {
        let fe = FileEvent {
            path: "a.txt".to_string(),
            content: "hello".to_string(),
            commit: "def456".to_string(),
        };
        let cloned = fe.clone();
        assert_eq!(fe.path, cloned.path);
        assert_eq!(fe.content, cloned.content);
        assert_eq!(fe.commit, cloned.commit);
    }

    // ── build_authenticated_url ─────────────────────────────────────

    #[test]
    fn build_url_no_auth() {
        let config = test_config();
        let url = build_authenticated_url(&config).unwrap();
        assert_eq!(url, "https://github.com/org/repo.git");
    }

    #[test]
    fn build_url_token_auth_https() {
        let mut config = test_config();
        config.auth.auth_type = GitAuthType::Token;
        config.auth.token = Some("ghp_tok123".to_string());
        let url = build_authenticated_url(&config).unwrap();
        assert_eq!(url, "https://ghp_tok123@github.com/org/repo.git");
    }

    #[test]
    fn build_url_token_auth_http() {
        let mut config = test_config();
        config.repository_url = "http://git.internal/org/repo.git".to_string();
        config.auth.auth_type = GitAuthType::Token;
        config.auth.token = Some("tok".to_string());
        let url = build_authenticated_url(&config).unwrap();
        assert_eq!(url, "http://tok@git.internal/org/repo.git");
    }

    #[test]
    fn build_url_token_auth_no_token_returns_original() {
        let mut config = test_config();
        config.auth.auth_type = GitAuthType::Token;
        // Token is None
        let url = build_authenticated_url(&config).unwrap();
        assert_eq!(url, "https://github.com/org/repo.git");
    }

    #[test]
    fn build_url_token_auth_non_http_returns_original() {
        let mut config = test_config();
        config.repository_url = "git@github.com:org/repo.git".to_string();
        config.auth.auth_type = GitAuthType::Token;
        config.auth.token = Some("tok".to_string());
        let url = build_authenticated_url(&config).unwrap();
        // Non-http(s) URL is returned as-is even with token auth
        assert_eq!(url, "git@github.com:org/repo.git");
    }

    #[test]
    fn build_url_ssh_auth_returns_original() {
        let mut config = test_config();
        config.auth.auth_type = GitAuthType::Ssh;
        // SSH auth type doesn't embed tokens, just passes through
        // (clone_or_pull will reject SSH before reaching this point)
        let url = build_authenticated_url(&config).unwrap();
        assert_eq!(url, "https://github.com/org/repo.git");
    }

    // ── Builder Validation ──────────────────────────────────────────

    #[tokio::test]
    async fn builder_succeeds_without_sender() {
        let config = Arc::new(test_config());
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .task_id(42)
            .task_type("git_sync")
            .task_context(test_task_context())
            .build()
            .await;
        // tx is optional — builder should succeed without it
        assert!(result.is_ok());
        let processor = result.unwrap();
        assert!(processor.tx.is_none());
        assert_eq!(processor.task_id, 42);
    }

    #[tokio::test]
    async fn builder_succeeds_with_sender() {
        let config = Arc::new(test_config());
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let result = ProcessorBuilder::new()
            .config(config)
            .receiver(rx)
            .sender(tx)
            .task_id(7)
            .task_type("git_sync")
            .task_context(test_task_context())
            .build()
            .await;
        assert!(result.is_ok());
        let processor = result.unwrap();
        assert!(processor.tx.is_some());
    }
}
