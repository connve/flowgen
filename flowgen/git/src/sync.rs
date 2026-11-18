//! The `git_sync` task: reads the tip of a branch and emits its files.
//!
//! Every file found under the configured path is emitted in one event,
//! `{commit, files: [{path, content}]}`. Downstream tasks decide what to do
//! with the files (parse, store, transform).

use crate::remote::{check_url, load_credentials};
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::task::TaskConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{debug, error};

pub use crate::remote::Credentials;

/// The branch a remote repository is read at without `branch`.
const DEFAULT_BRANCH: &str = "main";

/// Git sync processor configuration.
///
/// Reads the tip of a branch and emits one event with every file found
/// under the configured `path` within the repository. Downstream tasks
/// decide what to do with the files (parse, store, transform).
///
/// # Example
///
/// ```yaml
/// - git_sync:
///     name: sync_configs
///     repository: "https://git.example.com/team/configs.git"
///     branch: main
///     path: "configs/"
///     credentials_path: /etc/git/credentials.json
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct GitSync {
    /// Task name.
    pub name: String,
    /// The repository: an HTTPS URL, whose branch tip is fetched shallow into
    /// a temporary folder for each sync, or a path on this machine, read in
    /// place. Either way only committed files are emitted.
    ///
    /// Example: `repository: "https://git.example.com/team/configs.git"`
    pub repository: String,
    /// Branch to track. Defaults to `main` for a URL and to the checked-out
    /// branch for a path.
    #[serde(default)]
    pub branch: Option<String>,
    /// Folder within the repository to scan for files; all files under it are emitted.
    #[serde(default)]
    pub path: Option<String>,
    /// Path to credentials JSON file for HTTPS token authentication. Empty
    /// means none.
    #[serde(default, deserialize_with = "flowgen_core::config::optional_path")]
    pub credentials_path: Option<PathBuf>,
    /// Bypass the HEAD-commit cache and re-walk the working tree every
    /// tick. Use when downstream cache was mutated out of band and
    /// needs re-seeding.
    #[serde(default)]
    pub force_pull: bool,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to the number of CPUs.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for GitSync {}

/// Where the files come from.
pub enum Source<'a> {
    /// A git server, by URL, fetched for each sync.
    Remote(&'a str),
    /// A repository on this machine, by path.
    Local(&'a Path),
}

impl GitSync {
    /// Whether the repository or credentials hold a template, so where the
    /// files come from is known only once rendered.
    pub fn templated(&self) -> bool {
        use crate::remote::is_template;
        is_template(&self.repository)
            || self
                .credentials_path
                .as_ref()
                .is_some_and(|path| is_template(&path.to_string_lossy()))
    }

    /// The repository this task reads; a local one is neither fetched nor
    /// authenticated.
    pub fn source(&self) -> Result<Source<'_>, ConfigError> {
        match crate::remote::is_url(&self.repository) {
            _ if self.repository.is_empty() => Err(ConfigError::Repository),
            true => Ok(Source::Remote(&self.repository)),
            false if self.credentials_path.is_some() => Err(ConfigError::LocalCredentials),
            false => Ok(Source::Local(Path::new(&self.repository))),
        }
    }
}

/// An invalid git sync config.
#[derive(thiserror::Error, Debug)]
pub enum ConfigError {
    #[error("Set repository to a URL or a path")]
    Repository,
    #[error("A repository on this machine is read in place and takes no credentials_path")]
    LocalCredentials,
}

/// The repository an [`EventHandler`] reads, resolved at init.
enum Repository {
    Remote {
        url: String,
        credentials: Option<Credentials>,
    },
    Local(PathBuf),
}

impl Repository {
    /// Names the repository in logs and in the cache key of its last commit.
    fn label(&self) -> String {
        match self {
            Repository::Remote { url, .. } => url.clone(),
            Repository::Local(path) => path.display().to_string(),
        }
    }
}

/// The cache key of the last commit synced from the repository named `label`,
/// with the label as a hex SHA-1 so any URL or path fits the key alphabet.
fn cursor_key(flow_key: &str, label: &str) -> String {
    let mut hasher = gix::hash::hasher(gix::hash::Kind::Sha1);
    hasher.update(label.as_bytes());
    let hash = match hasher.try_finalize() {
        Ok(hash) => hash,
        Err(gix::hash::hasher::Error::CollisionAttack { digest }) => digest,
    };
    format!("flow.{flow_key}.git_head.{hash}")
}

/// The event `git_sync` emits: every file under `path` at one commit.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Snapshot {
    /// The commit the files are read at.
    pub commit: String,
    /// The files, by their path within `path`.
    pub files: Vec<File>,
}

/// A file of a [`Snapshot`].
#[derive(Debug, Clone, serde::Serialize)]
pub struct File {
    /// Relative path of the file within the scanned directory.
    pub path: String,
    /// File content as a string.
    pub content: String,
}

/// Errors that can occur during git sync processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to create a temporary directory: {source}")]
    TempDir {
        #[source]
        source: std::io::Error,
    },
    #[error("Path {path} is not a folder in the repository")]
    NotAFolder { path: String },
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
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Remote(#[from] crate::remote::CloneError),
    #[error(transparent)]
    Local(#[from] crate::push::client::Error),
    #[error("Branch '{branch}' does not exist in {path}")]
    BranchMissing { branch: String, path: PathBuf },
    #[error("The repository at {path} has no branch checked out; set branch or check one out")]
    DetachedHead { path: PathBuf },
    #[error(transparent)]
    Url(#[from] crate::remote::UrlError),
    #[error(transparent)]
    Credentials(#[from] flowgen_core::credentials::Error),
    #[error(transparent)]
    Blocking(#[from] crate::remote::BlockingError),
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
    #[error("Failed to render git sync config: {source}")]
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

impl Error {
    /// Whether retrying the same sync cannot succeed.
    fn is_permanent(&self) -> bool {
        match self {
            Error::Local(source) => {
                matches!(source, crate::push::client::Error::InvalidPath { .. })
            }
            Error::Config(_)
            | Error::Url(_)
            | Error::RenderConfig { .. }
            | Error::NotAFolder { .. }
            | Error::BranchMissing { .. }
            | Error::DetachedHead { .. }
            | Error::OpenRepo { .. } => true,
            _ => false,
        }
    }
}

/// Event handler for git sync operations.
pub struct EventHandler {
    config: Arc<GitSync>,
    /// The repository, with the HTTPS credentials of a remote one, which
    /// reach gix through a credential helper, so the token never embeds in
    /// the repository URL.
    repository: Repository,
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

    /// Handles a trigger event by syncing the repository and emitting file events.
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
            let label = self.repository.label();
            let (path, commit, _fetched) = match &self.repository {
                Repository::Remote { url, credentials } => {
                    let branch = match &self.config.branch {
                        Some(branch) => branch.clone(),
                        None => DEFAULT_BRANCH.to_string(),
                    };
                    let (fetched, commit) = fetch(url, branch, credentials.clone()).await?;
                    (fetched.path().to_path_buf(), commit, Some(fetched))
                }
                Repository::Local(path) => {
                    let commit = local_tip(path, self.config.branch.as_deref()).await?;
                    (path.clone(), commit, None)
                }
            };

            let cache = &self.task_context.cache;
            let cache_key = cursor_key(&self.task_context.flow.id(), &label);
            let cached_commit = cache
                .get(&cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|bytes| String::from_utf8(bytes.to_vec()).ok());
            if !self.config.force_pull && cached_commit.as_deref() == Some(commit.as_str()) {
                debug!(
                    repository = %label,
                    commit = %commit,
                    "Git HEAD unchanged since last sync, skipping file walk"
                );
                if let Some(arc) = completion_tx_arc.as_ref() {
                    let upstream_leaf_share = self.task_context.leaf_count.max(1);
                    for _ in 0..upstream_leaf_share {
                        arc.signal_completion(None);
                    }
                }
                return Ok(());
            }

            let entries = local_files(&path, &commit, self.config.path.as_deref()).await?;

            let snapshot = Snapshot {
                commit: commit.clone(),
                files: entries
                    .into_iter()
                    .map(|(path, content)| File { path, content })
                    .collect(),
            };
            let data =
                serde_json::to_value(&snapshot).map_err(|source| Error::SerdeJson { source })?;
            let mut e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(self.config.name.clone())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            let save_cursor = {
                let cache = Arc::clone(cache);
                let (cache_key, commit, label) = (cache_key, commit.clone(), label.to_string());
                async move {
                    if let Err(e) = cache.put(&cache_key, commit.clone().into(), None).await {
                        error!(
                            repository = %label,
                            commit = %commit,
                            error = %e,
                            "Failed to persist git HEAD, next tick will re-walk"
                        );
                    }
                }
            };
            let save_cursor = match completion_tx_arc.as_ref() {
                Some(arc) => {
                    arc.on_success(Box::pin(save_cursor));
                    match self.tx {
                        None => arc.signal_completion(e.data_as_json().ok()),
                        Some(_) => e.completion_tx = Some(Arc::clone(arc)),
                    }
                    None
                }
                None => Some(save_cursor),
            };

            let short_commit = match commit.get(..7) {
                Some(short) => short,
                None => commit.as_str(),
            };
            e.send_with_logging(self.tx.as_ref())
                .context("commit", short_commit)
                .await
                .map_err(|source| Error::SendMessage { source })?;
            if let Some(save_cursor) = save_cursor {
                save_cursor.await;
            }

            Ok(())
        })
        .await
    }
}

/// Shallow-fetches the tip of `branch` into a temporary bare repository and
/// returns it with the tip's commit.
async fn fetch(
    url: &str,
    branch: String,
    credentials: Option<Credentials>,
) -> Result<(tempfile::TempDir, String), Error> {
    let url = url.to_string();
    crate::remote::run_blocking(None, move |interrupt| {
        let dir = tempfile::TempDir::new().map_err(|source| Error::TempDir { source })?;
        let repo = crate::remote::shallow_fetch_bare(
            &url,
            &branch,
            dir.path(),
            credentials.as_ref(),
            interrupt,
        )?;
        let commit = repo.head_id().map_err(head_error)?.to_string();
        Ok((dir, commit))
    })
    .await?
}

/// The commit at the tip of `branch`, or of the checked-out branch, in the
/// repository at `path`.
async fn local_tip(path: &Path, branch: Option<&str>) -> Result<String, Error> {
    let path = path.to_path_buf();
    let branch = branch.map(str::to_string);
    crate::remote::run_blocking(None, move |_| {
        let repo = open_local(&path)?;
        let branch = match branch {
            Some(branch) => branch,
            None => match repo.head_name().map_err(head_error)? {
                Some(head) => head.shorten().to_string(),
                None => return Err(Error::DetachedHead { path }),
            },
        };
        let reference = format!("refs/heads/{branch}");
        match repo
            .try_find_reference(reference.as_str())
            .map_err(head_error)?
        {
            Some(mut found) => Ok(found.peel_to_id().map_err(head_error)?.to_string()),
            None => Err(Error::BranchMissing { branch, path }),
        }
    })
    .await?
}

/// The UTF-8 files under `prefix` at `commit`, by their path relative to
/// `prefix`, which must be a folder at the commit.
async fn local_files(
    path: &Path,
    commit: &str,
    prefix: Option<&str>,
) -> Result<Vec<(String, String)>, Error> {
    let path = path.to_path_buf();
    let commit = commit.to_string();
    let prefix = match prefix {
        Some(prefix) => crate::push::client::normalize_path(prefix)?.join("/"),
        None => String::new(),
    };
    crate::remote::run_blocking(None, move |_| {
        let repo = open_local(&path)?;
        let id = gix::ObjectId::from_hex(commit.as_bytes()).map_err(head_error)?;
        let tree = repo
            .find_commit(id)
            .map_err(head_error)?
            .tree()
            .map_err(head_error)?;
        let folder = match prefix.is_empty() {
            true => true,
            false => matches!(
                tree.lookup_entry_by_path(&prefix).map_err(head_error)?,
                Some(entry) if entry.mode().is_tree()
            ),
        };
        if !folder {
            return Err(Error::NotAFolder { path: prefix });
        }
        let tree = tree.id;
        let mut files = Vec::new();
        for file in crate::push::client::files_under(&repo, tree, &prefix)? {
            match String::from_utf8(file.content) {
                Ok(content) => files.push((file.path, content)),
                Err(_) => tracing::warn!(path = %file.path, "Skipping a file that is not UTF-8"),
            }
        }
        Ok(files)
    })
    .await?
}

fn head_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::HeadCommit {
        source: Box::new(source),
    }
}

fn open_local(path: &Path) -> Result<gix::Repository, Error> {
    gix::open(path).map_err(|e| Error::OpenRepo {
        path: path.to_path_buf(),
        source: Box::new(e),
    })
}

/// Git sync processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<GitSync>,
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
        let config: GitSync = self
            .config
            .render(&serde_json::Map::new())
            .map_err(|source| Error::RenderConfig { source })?;

        let repository = match config.source()? {
            Source::Local(path) => Repository::Local(path.to_path_buf()),
            Source::Remote(url) => {
                let credentials = load_credentials(config.credentials_path.as_deref()).await?;
                check_url(url, credentials.as_ref())?;
                Repository::Remote {
                    url: url.to_string(),
                    credentials,
                }
            }
        };

        Ok(EventHandler {
            config: Arc::new(config),
            repository,
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

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize git sync processor");
                        match e.is_permanent() {
                            true => Err(tokio_retry::RetryError::permanent(e)),
                            false => Err(tokio_retry::RetryError::transient(e)),
                        }
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => return Err(e),
        };

        flowgen_core::task::runner::process_events(
            self.rx,
            event_handler,
            &retry_config,
            self.config.max_in_flight,
            &self.task_context,
        )
        .await;
        Ok(())
    }
}

/// Builder for git sync processor.
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<GitSync>>,
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

    pub fn config(mut self, config: Arc<GitSync>) -> Self {
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
    use std::sync::Arc;

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

    fn test_config() -> GitSync {
        GitSync {
            name: "test_sync".to_string(),
            repository: "https://github.com/org/repo.git".to_string(),
            branch: None,
            path: None,
            credentials_path: None,
            force_pull: false,
            depends_on: None,
            retry: None,
            max_in_flight: None,
        }
    }

    #[tokio::test]
    async fn a_path_that_is_not_a_folder_at_the_commit_is_a_permanent_error() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("w");
        crate::workspace::init(&root, &[("flows/a.yaml".to_string(), "a")]).unwrap();
        let commit = local_tip(&root, None).await.unwrap();

        for path in ["missing/", "flows/a.yaml"] {
            let error = local_files(&root, &commit, Some(path)).await.unwrap_err();
            assert!(matches!(error, Error::NotAFolder { .. }), "{error}");
            assert!(error.is_permanent());
        }
        assert_eq!(
            Error::NotAFolder {
                path: "missing".to_string()
            }
            .to_string(),
            "Path missing is not a folder in the repository"
        );
        let outside = local_files(&root, &commit, Some("../flows"))
            .await
            .unwrap_err();
        assert!(outside.is_permanent(), "{outside}");
        assert_eq!(
            local_files(&root, &commit, Some("flows")).await.unwrap(),
            vec![("a.yaml".to_string(), "a".to_string())]
        );
    }

    #[test]
    fn the_cursor_key_holds_a_hash_of_the_repository() {
        let key = cursor_key("f", ".");
        let hash = key.strip_prefix("flow.f.git_head.").unwrap();
        assert_eq!(hash.len(), 40);
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()), "{key}");
        assert_ne!(key, cursor_key("f", ".."));
        assert_eq!(key, cursor_key("f", "."));
    }

    #[test]
    fn config_errors_are_not_retried_and_network_errors_are() {
        let ssh = Error::Url(crate::remote::UrlError::Ssh {
            url: "git@git.example.com:a.git".to_string(),
        });
        let clone = Error::Remote(crate::remote::CloneError::Clone {
            url: "https://git.example.com/a.git".to_string(),
            source: "timeout".into(),
        });
        assert!(Error::Config(ConfigError::Repository).is_permanent());
        assert!(ssh.is_permanent());
        assert!(!clone.is_permanent());
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
    fn error_display_ssh_url() {
        let err = Error::Url(crate::remote::UrlError::Ssh {
            url: "git@github.com:org/repo.git".to_string(),
        });
        assert!(err.to_string().contains("SSH URLs are not supported"));
        assert!(err.to_string().contains("git@github.com:org/repo.git"));
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
        let inner = Error::Url(crate::remote::UrlError::Ssh {
            url: "git@github.com:org/repo.git".to_string(),
        });
        let err = Error::RetryExhausted {
            source: Box::new(inner),
        };
        assert!(err
            .to_string()
            .contains("Task failed after all retry attempts"));
    }

    #[test]
    fn test_defaults() {
        let json = r#"{
            "name": "sync",
            "repository": "https://github.com/org/repo.git"
        }"#;
        let config: GitSync = serde_json::from_str(json).unwrap();
        assert_eq!(config.branch, None);
        assert!(config.path.is_none());
        assert!(config.credentials_path.is_none());
    }

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "sync",
            "repository": "https://github.com/org/repo.git"
        }"#;
        let config: GitSync = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "sync");
        assert_eq!(config.repository, "https://github.com/org/repo.git");
        assert_eq!(config.branch, None);
        assert!(config.path.is_none());
        assert!(config.credentials_path.is_none());
        assert!(config.depends_on.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn config_deser_full() {
        let json = r#"{
            "name": "sync_all",
            "repository": "https://github.com/org/repo.git",
            "branch": "develop",
            "path": "flows/",
            "credentials_path": "/etc/flowgen/credentials/git.json",
            "depends_on": ["trigger"],
            "retry": { "max_attempts": 2, "initial_backoff": "500ms" }
        }"#;
        let config: GitSync = serde_json::from_str(json).unwrap();
        assert_eq!(config.branch.as_deref(), Some("develop"));
        assert_eq!(config.path.as_deref(), Some("flows/"));
        assert_eq!(
            config.credentials_path,
            Some(PathBuf::from("/etc/flowgen/credentials/git.json"))
        );
        assert_eq!(config.depends_on, Some(vec!["trigger".to_string()]));
        assert!(config.retry.is_some());
    }

    #[test]
    fn config_deser_missing_name_fails() {
        let json = r#"{
            "repository": "https://github.com/org/repo.git"
        }"#;
        let result = serde_json::from_str::<GitSync>(json);
        assert!(result.is_err());
    }

    #[test]
    fn a_url_is_fetched_and_a_path_is_read_in_place() {
        let source = |fields: &str| {
            let config: GitSync =
                serde_json::from_str(&format!(r#"{{"name": "sync"{fields}}}"#)).unwrap();
            match config.source() {
                Ok(Source::Remote(url)) => format!("remote {url}"),
                Ok(Source::Local(path)) => format!("local {}", path.display()),
                Err(error) => error.to_string(),
            }
        };
        assert_eq!(source(r#", "repository": ".""#), "local .");
        assert_eq!(
            source(r#", "repository": "https://g.example.com/a.git""#),
            "remote https://g.example.com/a.git"
        );
        assert_eq!(
            source(r#", "repository": """#),
            ConfigError::Repository.to_string()
        );
        assert_eq!(
            source(r#", "repository": ".", "credentials_path": "/tmp/a""#),
            ConfigError::LocalCredentials.to_string()
        );
        assert_eq!(
            source(r#", "repository": ".", "credentials_path": """#),
            "local ."
        );
    }

    fn generic_context(
        cache: Arc<dyn flowgen_core::cache::Cache>,
    ) -> Arc<flowgen_core::task::context::TaskContext> {
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test_flow".to_string())
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    /// Runs one sync of `config` and returns the paths and contents it emits.
    async fn sync_once(
        config: GitSync,
        cache: Arc<dyn flowgen_core::cache::Cache>,
    ) -> Vec<(String, String)> {
        sync_with(config, cache, None).await
    }

    /// [`sync_once`] for a trigger carrying `completion`.
    async fn sync_with(
        config: GitSync,
        cache: Arc<dyn flowgen_core::cache::Cache>,
        completion: Option<flowgen_core::event::SharedCompletionTx>,
    ) -> Vec<(String, String)> {
        use flowgen_core::task::runner::{Handler, Runner};
        let (_, rx) = tokio::sync::mpsc::channel(1);
        let (tx, mut out) = tokio::sync::mpsc::channel(64);
        let processor = ProcessorBuilder::new()
            .config(Arc::new(config))
            .receiver(rx)
            .sender(tx)
            .task_type("git_sync")
            .task_context(generic_context(cache))
            .build()
            .await
            .unwrap();
        let handler = processor.init().await.unwrap();
        let mut trigger = EventBuilder::new()
            .data(EventData::Json(serde_json::json!({})))
            .subject("tick".to_string())
            .task_id(0)
            .task_type("generate")
            .build()
            .unwrap();
        trigger.completion_tx = completion;
        handler.handle(trigger).await.unwrap();
        drop(handler);
        drop(processor);
        let mut files = Vec::new();
        while let Ok(event) = out.try_recv() {
            let data = event.data_as_json().unwrap();
            for file in data["files"].as_array().unwrap() {
                files.push((
                    file["path"].as_str().unwrap().to_string(),
                    file["content"].as_str().unwrap().to_string(),
                ));
            }
        }
        files
    }

    #[tokio::test]
    async fn the_commit_is_remembered_only_once_the_flow_succeeded() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("w");
        crate::workspace::init(&root, &[("flows/a.yaml".to_string(), "a")]).unwrap();
        let config = GitSync {
            repository: root.display().to_string(),
            path: Some("flows/".to_string()),
            ..test_config()
        };
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        let wait = || tokio::time::sleep(std::time::Duration::from_millis(100));

        let (failed, _rx) = flowgen_core::event::new_completion_channel(1);
        assert_eq!(
            sync_with(
                config.clone(),
                Arc::clone(&cache),
                Some(Arc::clone(&failed))
            )
            .await
            .len(),
            1
        );
        failed.signal_completion_with_error("boom".to_string());
        wait().await;
        let (succeeded, _rx) = flowgen_core::event::new_completion_channel(1);
        assert_eq!(
            sync_with(
                config.clone(),
                Arc::clone(&cache),
                Some(Arc::clone(&succeeded))
            )
            .await
            .len(),
            1,
            "A failed flow syncs the commit again"
        );
        succeeded.signal_completion(None);
        wait().await;

        assert!(
            sync_once(config, cache).await.is_empty(),
            "A commit the flow handled is not synced again"
        );
    }

    #[tokio::test]
    async fn without_a_branch_a_local_repository_is_read_at_the_checked_out_one() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("w");
        crate::workspace::init(&root, &[("flows/a.yaml".to_string(), "a")]).unwrap();
        let mut repo = gix::open(&root).unwrap();
        crate::push::client::ensure_committer(
            &mut repo,
            &crate::push::client::Author {
                name: "a".to_string(),
                email: "a@example.com".to_string(),
            },
        )
        .unwrap();
        let head = repo.head_id().unwrap().detach();
        repo.reference(
            "refs/heads/master",
            head,
            gix::refs::transaction::PreviousValue::MustNotExist,
            "branch",
        )
        .unwrap();
        std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/master\n").unwrap();

        assert_eq!(local_tip(&root, None).await.unwrap(), head.to_string());
        assert!(matches!(
            local_tip(&root, Some("release")).await,
            Err(Error::BranchMissing { branch, .. }) if branch == "release"
        ));
    }

    #[tokio::test]
    async fn a_local_repository_emits_its_committed_files_once_per_commit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("w");
        crate::workspace::init(
            &root,
            &[
                ("flows/a.yaml".to_string(), "a"),
                ("flows/nested/b.yaml".to_string(), "b"),
            ],
        )
        .unwrap();
        std::fs::write(root.join("flows/a.yaml"), "not committed").unwrap();
        std::fs::write(root.join("flows/c.yaml"), "untracked").unwrap();
        let config = GitSync {
            repository: root.display().to_string(),
            path: Some("flows/".to_string()),
            ..test_config()
        };
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;

        let mut files = sync_once(config.clone(), Arc::clone(&cache)).await;
        files.sort();
        assert_eq!(
            files,
            vec![
                ("a.yaml".to_string(), "a".to_string()),
                ("nested/b.yaml".to_string(), "b".to_string())
            ]
        );
        assert!(
            sync_once(config, cache).await.is_empty(),
            "Same commit, no files"
        );
    }

    #[test]
    fn config_roundtrip_serde() {
        let json = r#"{
            "name": "rt",
            "repository": "https://github.com/org/repo.git",
            "branch": "main",
            "path": "configs/"
        }"#;
        let config: GitSync = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: GitSync = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn credentials_deser() {
        let json = r#"{ "token": "ghp_abc123" }"#;
        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(
            secrecy::ExposeSecret::expose_secret(&creds.token),
            "ghp_abc123"
        );
    }

    #[test]
    fn a_snapshot_is_its_commit_and_files() {
        let snapshot = Snapshot {
            commit: "abc123".to_string(),
            files: vec![File {
                path: "flows/main.yaml".to_string(),
                content: "name: test".to_string(),
            }],
        };
        let value = serde_json::to_value(&snapshot).unwrap();
        assert_eq!(value["commit"], "abc123");
        assert_eq!(value["files"][0]["path"], "flows/main.yaml");
        assert_eq!(value["files"][0]["content"], "name: test");
    }

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
