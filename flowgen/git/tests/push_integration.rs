//! Integration tests for `push` against `git http-backend` served over HTTP.
//!
//! Requires `git` in `PATH`, like the sync integration tests.

use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, Method, Response, Uri};
use flowgen_git::push::client::{Author, Error, Expected, FileChange, Push};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

fn git(dir: &Path, args: &[&str]) -> String {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("HOME", dir)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_SYSTEM", "/dev/null")
        .env("GIT_TEMPLATE_DIR", "")
        .env("GIT_AUTHOR_NAME", "a")
        .env("GIT_AUTHOR_EMAIL", "a@example.com")
        .env("GIT_COMMITTER_NAME", "a")
        .env("GIT_COMMITTER_EMAIL", "a@example.com")
        .output()
        .expect("spawn git");
    assert!(
        out.status.success(),
        "git {args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

struct Server {
    root: PathBuf,
    /// Commits to the branch right before the next push lands, once.
    race: AtomicBool,
}

/// Bridges one HTTP request to the `git http-backend` CGI program.
async fn backend(
    State(server): State<Arc<Server>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response<Body> {
    if uri.path().ends_with("git-receive-pack") && server.race.swap(false, Ordering::SeqCst) {
        commit_directly(&server.root, "other.txt", "concurrent");
    }
    let content_type = headers
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    let root = server.root.clone();
    let output = tokio::task::spawn_blocking(move || {
        let mut child = Command::new("git")
            .arg("http-backend")
            .env_clear()
            .env("PATH", std::env::var_os("PATH").unwrap_or_default())
            .env("GIT_CONFIG_GLOBAL", "/dev/null")
            .env("GIT_CONFIG_SYSTEM", "/dev/null")
            .env("GIT_PROJECT_ROOT", &root)
            .env("GIT_HTTP_EXPORT_ALL", "1")
            .env("REMOTE_USER", "flowgen")
            .env("REQUEST_METHOD", method.as_str())
            .env("PATH_INFO", uri.path())
            .env("QUERY_STRING", uri.query().unwrap_or_default())
            .env("CONTENT_TYPE", content_type)
            .env("CONTENT_LENGTH", body.len().to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn git http-backend");
        child.stdin.take().unwrap().write_all(&body).unwrap();
        child.wait_with_output().unwrap().stdout
    })
    .await
    .unwrap();

    let split = output
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("CGI headers");
    let mut response = Response::builder();
    for line in String::from_utf8_lossy(&output[..split]).lines() {
        if let Some((name, value)) = line.split_once(": ") {
            match name.eq_ignore_ascii_case("status") {
                true => {
                    response = response.status(value[..3].parse::<u16>().unwrap());
                }
                false => response = response.header(name, value),
            }
        }
    }
    response
        .body(Body::from(output[split + 4..].to_vec()))
        .unwrap()
}

/// Commits a file straight into the bare repository's `main`.
fn commit_directly(root: &Path, file: &str, content: &str) {
    let work = TempDir::new().unwrap();
    let bare = root.join("repo.git");
    git(work.path(), &["clone", "-q", bare.to_str().unwrap(), "."]);
    std::fs::write(work.path().join(file), content).unwrap();
    git(work.path(), &["add", "-A"]);
    git(work.path(), &["commit", "-q", "-m", "direct"]);
    git(work.path(), &["push", "-q", "origin", "HEAD:main"]);
}

struct Remote {
    _dir: TempDir,
    root: PathBuf,
    server: Arc<Server>,
    url: String,
}

impl Remote {
    async fn new() -> Self {
        let dir = TempDir::new().unwrap();
        let root = dir.path().canonicalize().unwrap();
        git(&root, &["init", "-q", "--bare", "-b", "main", "repo.git"]);
        git(
            &root.join("repo.git"),
            &["config", "http.receivepack", "true"],
        );
        git(
            &root.join("repo.git"),
            &["config", "receive.fsckObjects", "true"],
        );
        let server = Arc::new(Server {
            root: root.clone(),
            race: AtomicBool::new(false),
        });
        let app = axum::Router::new()
            .fallback(backend)
            .with_state(Arc::clone(&server));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/repo.git", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        Self {
            _dir: dir,
            root,
            server,
            url,
        }
    }

    fn push(&self, path: Option<&str>) -> Push {
        Push {
            repository_url: self.url.clone(),
            branch: "main".to_string(),
            path: path.map(str::to_string),
            credentials: None,
            http: reqwest::Client::new(),
        }
    }

    fn read(&self, file: &str) -> String {
        git(
            &self.root.join("repo.git"),
            &["show", &format!("main:{file}")],
        )
    }

    fn head(&self) -> String {
        git(&self.root.join("repo.git"), &["rev-parse", "main"])
    }
}

fn author() -> Author {
    Author {
        name: "Jane Doe".to_string(),
        email: "jane@example.com".to_string(),
    }
}

fn write(path: &str, content: &str) -> FileChange {
    FileChange {
        path: path.to_string(),
        content: Some(content.as_bytes().to_vec()),
        expected: Expected::Any,
    }
}

#[tokio::test]
async fn the_first_push_creates_the_branch() {
    let remote = Remote::new().await;

    let pushed = remote
        .push(Some("workspace"))
        .push(
            vec![write("flows/a.yaml", "a: 1\n")],
            author(),
            "Add a".to_string(),
        )
        .await
        .unwrap();

    assert!(pushed.changed);
    assert_eq!(pushed.commit, remote.head());
    assert_eq!(remote.read("workspace/flows/a.yaml"), "a: 1");
    assert_eq!(
        git(
            &remote.root.join("repo.git"),
            &["log", "-1", "--format=%an <%ae> %s"]
        ),
        "Jane Doe <jane@example.com> Add a"
    );
}

#[tokio::test]
async fn a_push_on_an_existing_branch_keeps_other_files_and_lists_the_workspace() {
    let remote = Remote::new().await;
    commit_directly(&remote.root, "README.md", "readme");
    let push = remote.push(Some("workspace"));
    push.push(
        vec![
            write("flows/a.yaml", "a"),
            write("resources/s.rhai", "event"),
        ],
        author(),
        "First".to_string(),
    )
    .await
    .unwrap();

    let pushed = push
        .push(
            vec![
                write("flows/b.yaml", "b"),
                FileChange {
                    path: "resources/s.rhai".to_string(),
                    content: None,
                    expected: Expected::Content(b"event".to_vec()),
                },
            ],
            author(),
            "Second".to_string(),
        )
        .await
        .unwrap();

    assert_eq!(remote.read("README.md"), "readme");
    assert_eq!(remote.read("workspace/flows/b.yaml"), "b");
    let mut paths: Vec<_> = pushed.files.iter().map(|f| f.path.as_str()).collect();
    paths.sort();
    assert_eq!(paths, vec!["flows/a.yaml", "flows/b.yaml"]);
    assert_eq!(remote.head(), pushed.commit);
}

#[tokio::test]
async fn a_change_that_alters_nothing_pushes_nothing() {
    let remote = Remote::new().await;
    let push = remote.push(None);
    let first = push
        .push(vec![write("a.yaml", "a")], author(), "First".to_string())
        .await
        .unwrap();

    let again = push
        .push(vec![write("a.yaml", "a")], author(), "Again".to_string())
        .await
        .unwrap();

    assert!(!again.changed);
    assert_eq!(again.commit, first.commit);
    assert_eq!(remote.head(), first.commit);
}

#[tokio::test]
async fn a_file_changed_on_the_branch_is_a_conflict() {
    let remote = Remote::new().await;
    commit_directly(&remote.root, "a.yaml", "theirs");

    let result = remote
        .push(None)
        .push(
            vec![FileChange {
                path: "a.yaml".to_string(),
                content: Some(b"ours".to_vec()),
                expected: Expected::Missing,
            }],
            author(),
            "Add a".to_string(),
        )
        .await;

    assert!(matches!(result, Err(Error::Conflict { path }) if path == "a.yaml"));
    assert_eq!(remote.read("a.yaml"), "theirs");
}

#[tokio::test]
async fn a_branch_that_moves_during_the_push_fails_as_moved_and_a_retry_lands() {
    let remote = Remote::new().await;
    commit_directly(&remote.root, "README.md", "readme");
    remote.server.race.store(true, Ordering::SeqCst);
    let push = remote.push(None);

    let moved = push
        .push(vec![write("a.yaml", "a")], author(), "Add a".to_string())
        .await;
    assert!(matches!(moved, Err(Error::BranchMoved { .. })), "{moved:?}");

    let pushed = push
        .push(vec![write("a.yaml", "a")], author(), "Add a".to_string())
        .await
        .unwrap();

    assert_eq!(remote.read("other.txt"), "concurrent");
    assert_eq!(remote.read("a.yaml"), "a");
    assert_eq!(remote.head(), pushed.commit);
}

#[tokio::test]
async fn identical_files_in_new_directories_are_packed_once() {
    let remote = Remote::new().await;

    remote
        .push(None)
        .push(
            vec![write("a/x.yaml", "same"), write("b/x.yaml", "same")],
            author(),
            "Twins".to_string(),
        )
        .await
        .unwrap();

    assert_eq!(remote.read("a/x.yaml"), "same");
    assert_eq!(remote.read("b/x.yaml"), "same");
}

#[tokio::test]
async fn a_change_that_already_landed_pushes_again_without_a_conflict() {
    let remote = Remote::new().await;
    let new_file = || FileChange {
        path: "a.yaml".to_string(),
        content: Some(b"a".to_vec()),
        expected: Expected::Missing,
    };
    let first = remote
        .push(None)
        .push(vec![new_file()], author(), "Add a".to_string())
        .await
        .unwrap();

    let retried = remote
        .push(None)
        .push(vec![new_file()], author(), "Add a".to_string())
        .await
        .unwrap();

    assert!(!retried.changed);
    assert_eq!(retried.commit, first.commit);
}

#[tokio::test]
async fn an_empty_path_and_a_missing_branch_are_rejected() {
    let remote = Remote::new().await;
    commit_directly(&remote.root, "README.md", "readme");

    let empty = remote
        .push(Some("workspace"))
        .push(vec![write("", "x")], author(), "Empty".to_string())
        .await;
    assert!(matches!(empty, Err(Error::InvalidPath { .. })));

    let mut other = remote.push(None);
    other.branch = "flowgen-changes".to_string();
    let missing = other
        .push(vec![write("a.yaml", "a")], author(), "Add a".to_string())
        .await;
    assert!(matches!(missing, Err(Error::BranchMissing { branch }) if branch == "flowgen-changes"));
}
