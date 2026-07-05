//! Integration tests for `git_sync` against a real local git repository.
//!
//! Each test builds a bare-plus-workdir repository under `tempdir()`,
//! commits real files with the system `git` CLI, points `git_sync` at
//! `file://<path>` (gix accepts local URLs the same as HTTPS), and
//! observes the emitted downstream events.
//!
//! No network, no daemon. Requires `git` in `PATH` — CI runners already
//! have it. Not marked `#[ignore]` because there is no external service
//! to depend on.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_git::sync::config::Processor as GitSyncConfig;
use flowgen_git::sync::processor::ProcessorBuilder;
use tempfile::TempDir;
use tokio::sync::mpsc;

/// Builds a real git repository at `path` with the given files and one
/// commit per call. Returns the HEAD commit hash of the resulting
/// working tree so tests can assert against it directly.
fn commit_files(path: &Path, files: &[(&str, &str)], message: &str) -> String {
    for (rel, content) in files {
        let full = path.join(rel);
        if let Some(parent) = full.parent() {
            std::fs::create_dir_all(parent).expect("create parent dir");
        }
        std::fs::write(&full, content).expect("write file");
    }
    // Stage + commit with a deterministic identity so the test is
    // reproducible across CI machines that lack a global git config.
    run_git(path, &["add", "-A"]);
    run_git(
        path,
        &[
            "-c",
            "user.email=test@flowgen.local",
            "-c",
            "user.name=flowgen test",
            "commit",
            "-m",
            message,
            "--allow-empty",
        ],
    );
    let out = git_command(path, &["rev-parse", "HEAD"])
        .output()
        .expect("git rev-parse");
    assert!(out.status.success(), "git rev-parse failed: {:?}", out);
    String::from_utf8(out.stdout)
        .expect("utf-8 commit hash")
        .trim()
        .to_string()
}

/// Initialises a fresh repository at `path` on branch `main`.
fn init_repo(path: &Path) {
    run_git(path, &["init", "-q", "-b", "main", "."]);
}

/// Builds a git subprocess pinned to `path` so it never walks up to a
/// parent `.git/`. Cargo runs tests from inside the flowgen source
/// tree, so an unfenced `Command::new("git").current_dir(path)` still
/// finds the parent repo's `.git/` and its pre-commit hook /
/// `index.lock`, causing spurious "another git process seems to be
/// running" failures. Clearing the user's and system config plus the
/// template directory also stops any global hook or init template
/// from leaking into the temp repo.
fn git_command(path: &Path, args: &[&str]) -> Command {
    let git_dir = path.join(".git");
    let mut cmd = Command::new("git");
    // Start from an empty environment and hand-pick only what git needs.
    // The outer repo's pre-commit hook exports GIT_INDEX_FILE,
    // GIT_COMMON_DIR and friends before running `cargo test`; those get
    // inherited by test binaries and win over `--git-dir` / `--work-tree`
    // for index operations, so parallel tests collide on the outer
    // repo's `index.lock`. Whitelisting env vars is safer than trying to
    // enumerate every GIT_* to scrub.
    cmd.arg(format!("--git-dir={}", git_dir.display()))
        .arg(format!("--work-tree={}", path.display()))
        .args(args)
        .current_dir(path)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", std::env::var_os("HOME").unwrap_or_default())
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_TEMPLATE_DIR", "");
    cmd
}

fn run_git(path: &Path, args: &[&str]) {
    let out = git_command(path, args).output().expect("spawn git");
    assert!(
        out.status.success(),
        "git {:?} failed: stdout={} stderr={}",
        args,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// Wraps a git-repository `TempDir` and exposes the `file://` URL the
/// git_sync task consumes.
///
/// `path` is canonicalised at construction so `GIT_DIR` / `GIT_WORK_TREE`
/// resolve to the real filesystem location. On macOS `tempdir()` hands
/// back `/var/folders/...` which is a symlink to `/private/var/folders/...`;
/// git's own path resolution then walks up looking for a repository and
/// under load races with the parent flowgen `.git/` — surfacing as
/// spurious `index.lock` conflicts. Canonicalising up front kills the
/// walkup at the source.
struct TestRepo {
    _dir: TempDir,
    path: PathBuf,
}

impl TestRepo {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().canonicalize().expect("canonicalize tempdir");
        init_repo(&path);
        Self { _dir: dir, path }
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn file_url(&self) -> String {
        format!("file://{}", self.path.display())
    }
}

fn test_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
        as Arc<dyn flowgen_core::cache::Cache>;
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("build TaskContext"),
    )
}

/// Runs `git_sync` once against the given config and collects every
/// event that flows downstream within the deadline. Shares its
/// `TaskContext` across invocations when passed in — that's how the
/// HEAD-commit cache-skip test observes the second run seeing no
/// events.
async fn run_once(
    config: Arc<GitSyncConfig>,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
) -> Vec<Event> {
    let (trigger_tx, trigger_rx) = mpsc::channel(8);
    let (downstream_tx, mut downstream_rx) = mpsc::channel(16);

    let processor = ProcessorBuilder::new()
        .config(config)
        .receiver(trigger_rx)
        .sender(downstream_tx)
        .task_id(1)
        .task_type("git_sync")
        .task_context(task_context)
        .build()
        .await
        .expect("build git_sync processor");

    let handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    let (completion_state, _completion_rx) = flowgen_core::event::new_completion_channel(1);
    trigger_tx
        .send(
            EventBuilder::new()
                .data(EventData::Json(serde_json::json!({"trigger": true})))
                .subject("tick".to_string())
                .task_id(0)
                .task_type("generate")
                .completion_tx(completion_state)
                .build()
                .expect("build trigger event"),
        )
        .await
        .expect("send trigger event");

    let mut events = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while tokio::time::Instant::now() < deadline {
        match tokio::time::timeout(Duration::from_millis(500), downstream_rx.recv()).await {
            Ok(Some(event)) => events.push(event),
            Ok(None) | Err(_) if !events.is_empty() => break,
            Err(_) => continue,
            Ok(None) => break,
        }
    }

    drop(trigger_tx);
    handle.abort();
    events
}

fn base_config(url: &str, clone_path: PathBuf) -> GitSyncConfig {
    GitSyncConfig {
        name: "sync".to_string(),
        repository_url: url.to_string(),
        branch: "main".to_string(),
        path: None,
        clone_path: Some(clone_path),
        credentials_path: None,
        force_pull: false,
        depends_on: None,
        retry: Some(flowgen_core::retry::RetryConfig {
            max_attempts: Some(1),
            ..Default::default()
        }),
    }
}

// ---------------------------------------------------------------------------
// Baseline: clone, walk, emit one FileEvent per file with the HEAD commit.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn git_sync_emits_one_event_per_file_with_head_commit() {
    let repo = TestRepo::new();
    let commit = commit_files(
        repo.path(),
        &[
            ("flows/a.yaml", "name: a\n"),
            ("flows/b.yaml", "name: b\n"),
            ("README.md", "hi\n"),
        ],
        "initial",
    );

    let clone_dir = tempfile::tempdir().expect("clone tempdir");
    let config = Arc::new(base_config(
        &repo.file_url(),
        clone_dir.path().to_path_buf(),
    ));
    let events = run_once(config, test_task_context()).await;

    assert_eq!(
        events.len(),
        3,
        "expected one event per file in the working tree, got {}",
        events.len(),
    );

    let mut paths: Vec<String> = events
        .iter()
        .map(|e| {
            e.data_as_json()
                .unwrap()
                .get("path")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        vec![
            "README.md".to_string(),
            "flows/a.yaml".to_string(),
            "flows/b.yaml".to_string(),
        ],
        "every committed file must surface with its repo-relative path",
    );

    for event in &events {
        let data = event.data_as_json().unwrap();
        assert_eq!(
            data.get("commit").unwrap().as_str().unwrap(),
            commit,
            "each file event must carry HEAD commit",
        );
        assert!(
            !data.get("content").unwrap().as_str().unwrap().is_empty(),
            "content must round-trip through the walker",
        );
    }
}

// ---------------------------------------------------------------------------
// Scoped scan: `path:` scopes the walker to a subdirectory.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn git_sync_only_emits_files_under_the_configured_path() {
    let repo = TestRepo::new();
    commit_files(
        repo.path(),
        &[
            ("flows/a.yaml", "name: a\n"),
            ("flows/nested/b.yaml", "name: b\n"),
            ("processors/other.yaml", "kind: x\n"),
            ("README.md", "hi\n"),
        ],
        "initial",
    );

    let clone_dir = tempfile::tempdir().expect("clone tempdir");
    let mut config = base_config(&repo.file_url(), clone_dir.path().to_path_buf());
    config.path = Some("flows".to_string());
    let events = run_once(Arc::new(config), test_task_context()).await;

    let mut paths: Vec<String> = events
        .iter()
        .map(|e| {
            e.data_as_json()
                .unwrap()
                .get("path")
                .unwrap()
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    paths.sort();
    assert_eq!(
        paths,
        vec!["a.yaml".to_string(), "nested/b.yaml".to_string()],
        "paths must be relative to `path:` and exclude siblings",
    );
}

// ---------------------------------------------------------------------------
// HEAD-commit cache: unchanged commit skips the file walk entirely.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn git_sync_skips_the_file_walk_when_head_matches_last_sync() {
    let repo = TestRepo::new();
    commit_files(repo.path(), &[("flows/a.yaml", "name: a\n")], "initial");

    let clone_dir = tempfile::tempdir().expect("clone tempdir");
    let config = Arc::new(base_config(
        &repo.file_url(),
        clone_dir.path().to_path_buf(),
    ));
    // Share the TaskContext so the two invocations see the same cache.
    let ctx = test_task_context();

    let first = run_once(Arc::clone(&config), Arc::clone(&ctx)).await;
    assert_eq!(first.len(), 1, "first run must emit the file event");

    let second = run_once(config, ctx).await;
    assert!(
        second.is_empty(),
        "second run at the same commit must skip the walk, got {} events",
        second.len(),
    );
}

// ---------------------------------------------------------------------------
// force_pull: bypass the HEAD cache and re-emit even when nothing changed.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn git_sync_force_pull_reemits_even_when_head_is_unchanged() {
    let repo = TestRepo::new();
    commit_files(repo.path(), &[("flows/a.yaml", "name: a\n")], "initial");

    let clone_dir = tempfile::tempdir().expect("clone tempdir");
    let mut cfg = base_config(&repo.file_url(), clone_dir.path().to_path_buf());
    cfg.force_pull = true;
    let config = Arc::new(cfg);
    let ctx = test_task_context();

    let first = run_once(Arc::clone(&config), Arc::clone(&ctx)).await;
    assert_eq!(first.len(), 1);

    let second = run_once(config, ctx).await;
    assert_eq!(
        second.len(),
        1,
        "force_pull must bypass the HEAD cache and re-emit files",
    );
}

// ---------------------------------------------------------------------------
// New commit: fetch propagates and the new commit hash lands on the events.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn git_sync_reemits_with_new_commit_when_upstream_advances() {
    let repo = TestRepo::new();
    let first_commit = commit_files(repo.path(), &[("flows/a.yaml", "name: a\n")], "initial");

    let clone_dir = tempfile::tempdir().expect("clone tempdir");
    let config = Arc::new(base_config(
        &repo.file_url(),
        clone_dir.path().to_path_buf(),
    ));
    let ctx = test_task_context();

    let first_run = run_once(Arc::clone(&config), Arc::clone(&ctx)).await;
    assert_eq!(first_run.len(), 1);
    assert_eq!(
        first_run[0]
            .data_as_json()
            .unwrap()
            .get("commit")
            .unwrap()
            .as_str()
            .unwrap(),
        first_commit,
    );

    // Amend the working tree upstream with a second commit and re-run.
    let second_commit = commit_files(
        repo.path(),
        &[("flows/a.yaml", "name: a\n"), ("flows/b.yaml", "name: b\n")],
        "second",
    );
    assert_ne!(first_commit, second_commit);

    let second_run = run_once(config, ctx).await;
    assert_eq!(
        second_run.len(),
        2,
        "second run must re-emit every file at the new commit",
    );
    for event in &second_run {
        assert_eq!(
            event
                .data_as_json()
                .unwrap()
                .get("commit")
                .unwrap()
                .as_str()
                .unwrap(),
            second_commit,
            "new events must carry the advanced commit hash",
        );
    }
}

// ---------------------------------------------------------------------------
// SSH URL: rejected up front so operators cannot misconfigure the task.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn git_sync_rejects_ssh_url_before_touching_the_network() {
    let clone_dir = tempfile::tempdir().expect("clone tempdir");
    let config = Arc::new(base_config(
        "git@github.com:org/repo.git",
        clone_dir.path().to_path_buf(),
    ));
    let events = run_once(config, test_task_context()).await;

    // The processor rejects SSH inside init, so nothing should flow
    // downstream — no partial batch, no error event.
    assert!(
        events.is_empty(),
        "SSH URLs must be rejected before any events are emitted, got {}",
        events.len(),
    );
}
