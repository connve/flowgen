//! A local workspace: a git repository holding `flows/` and `resources/`.

use flowgen_core::workspace::{FLOWS_DIR, RESOURCES_DIR, STATE_DIR};
use std::path::Path;

/// The folder git keeps a repository in.
const GIT_DIR: &str = ".git";
/// The file listing what git leaves out.
pub const GITIGNORE_FILE: &str = ".gitignore";
/// The message of the commit that creates a workspace.
pub const MESSAGE: &str = "Create the workspace";
/// The author of the first commit when this machine has no git user.
const FALLBACK_NAME: &str = "flowgen";
/// The email of [`FALLBACK_NAME`].
const FALLBACK_EMAIL: &str = "flowgen@localhost";

/// Keeps the local state folder out of the repository.
pub fn gitignore() -> String {
    format!("{STATE_DIR}/\n")
}

/// Failure to create the workspace.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to create the workspace folder {path}: {source}")]
    Folder {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to create the workspace repository: {source}")]
    Repository {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
}

fn repository(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Repository {
        source: Box::new(source),
    }
}

/// Creates the workspace at `root` unless it is a repository with a commit:
/// its folders, each of `files` (path within the workspace and content) not
/// on disk yet, the state folder in `.gitignore`, and a first commit holding
/// them with what `flows/` and `resources/` already hold. Returns whether it
/// created one.
pub fn init(root: &Path, files: &[(String, &str)]) -> Result<bool, Error> {
    for folder in [FLOWS_DIR, RESOURCES_DIR] {
        create_folder(&root.join(folder))?;
    }
    let repo = match root.join(GIT_DIR).exists() {
        true => gix::open(root).map_err(repository)?,
        false => gix::init(root).map_err(repository)?,
    };
    if !repo.head().map_err(repository)?.is_unborn() {
        return Ok(false);
    }
    for (path, content) in files {
        let file = root.join(path);
        if !file.exists() {
            write_file(&file, content)?;
        }
    }
    ignore_state(root)?;

    let mut paths = std::collections::BTreeSet::from([GITIGNORE_FILE.to_string()]);
    paths.extend(files.iter().map(|(path, _)| path.clone()));
    for folder in [FLOWS_DIR, RESOURCES_DIR] {
        paths.extend(files_in(root, folder)?);
    }
    let mut changes = Vec::with_capacity(paths.len());
    for path in paths {
        let content = std::fs::read(root.join(&path)).map_err(|source| Error::Folder {
            path: path.clone(),
            source,
        })?;
        changes.push(crate::push::client::FileChange {
            path,
            content: Some(content),
            expected: crate::push::client::Expected::Missing,
        });
    }
    local_commit(root)
        .commit_blocking(changes, None, MESSAGE)
        .map_err(repository)?;
    Ok(true)
}

/// A commit onto the checked-out branch of the repository at `root`.
fn local_commit(root: &Path) -> crate::push::client::Commit {
    crate::push::client::Commit {
        repository_path: root.to_path_buf(),
        branch: None,
        path: None,
        committer: None,
        signing_key: None,
        timeout: None,
    }
}

/// Adds the state folder to the `.gitignore` at `root`, keeping its lines.
fn ignore_state(root: &Path) -> Result<(), Error> {
    let path = root.join(GITIGNORE_FILE);
    let existing = match std::fs::read_to_string(&path) {
        Ok(existing) => existing,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(source) => {
            return Err(Error::Folder {
                path: path.display().to_string(),
                source,
            })
        }
    };
    let entry = gitignore();
    if existing.lines().any(|line| line.trim() == entry.trim()) {
        return Ok(());
    }
    let separator = match existing.is_empty() || existing.ends_with('\n') {
        true => "",
        false => "\n",
    };
    write_file(&path, &format!("{existing}{separator}{entry}"))
}

/// The files under `folder` of `root`, by their path from `root`, leaving out
/// hidden files and folders.
fn files_in(root: &Path, folder: &str) -> Result<Vec<String>, Error> {
    let mut files = Vec::new();
    let walk = walkdir::WalkDir::new(root.join(folder))
        .sort_by_file_name()
        .into_iter()
        .filter_entry(|entry| !entry.file_name().to_string_lossy().starts_with('.'));
    for entry in walk {
        let entry = entry.map_err(|source| Error::Folder {
            path: folder.to_string(),
            source: source.into(),
        })?;
        if !entry.file_type().is_file() {
            continue;
        }
        if let Ok(relative) = entry.path().strip_prefix(root) {
            let segments: Vec<String> = relative
                .components()
                .map(|segment| segment.as_os_str().to_string_lossy().into_owned())
                .collect();
            files.push(segments.join("/"));
        }
    }
    Ok(files)
}

/// The root of the working tree of the git repository holding `folder`, which
/// must exist, or `None` when no repository holds it.
pub fn repository_of(folder: &Path) -> Option<std::path::PathBuf> {
    let repo = gix::discover(folder).ok()?;
    std::fs::canonicalize(repo.workdir()?).ok()
}

/// The content of each of `paths` at the last commit of the workspace at
/// `root`, `None` for a path the commit does not hold as a UTF-8 file.
pub fn committed(root: &Path, paths: &[String]) -> Result<Vec<Option<String>>, Error> {
    let repo = gix::open(root).map_err(repository)?;
    let tree = repo.head_tree_id_or_empty().map_err(repository)?;
    let tree = repo.find_tree(tree).map_err(repository)?;
    let mut contents = Vec::with_capacity(paths.len());
    for path in paths {
        let content = match tree.lookup_entry_by_path(path).map_err(repository)? {
            Some(entry) if entry.mode().is_blob() => {
                let blob = entry.object().map_err(repository)?;
                String::from_utf8(blob.detach().data).ok()
            }
            _ => None,
        };
        contents.push(content);
    }
    Ok(contents)
}

/// Commits each of `files` (path within the workspace and content) that the
/// last commit lacks onto the checked-out branch, as the git user of this
/// machine, and returns the paths it added. Files the commit holds are kept.
pub async fn add_missing(
    root: &Path,
    files: &[(String, &str)],
    message: &str,
) -> Result<Vec<String>, Error> {
    commit_files(root, files, &[], message, false).await
}

/// Commits each of `files` (path within the workspace and content) that the
/// last commit lacks or holds with other content, and deletes each of
/// `removed` it holds, onto the checked-out branch in one commit, as the git
/// user of this machine, and returns the paths it wrote or deleted.
pub async fn update(
    root: &Path,
    files: &[(String, &str)],
    removed: &[String],
    message: &str,
) -> Result<Vec<String>, Error> {
    commit_files(root, files, removed, message, true).await
}

/// The paths of `files` that the last commit lacks or holds with other content.
pub fn outdated(root: &Path, files: &[(String, &str)]) -> Result<Vec<String>, Error> {
    let paths: Vec<String> = files.iter().map(|(path, _)| path.clone()).collect();
    let committed = committed(root, &paths)?;
    Ok(files
        .iter()
        .zip(committed)
        .filter(|((_, content), current)| current.as_deref() != Some(*content))
        .map(|((path, _), _)| path.clone())
        .collect())
}

/// Commits `files` the last commit lacks and, with `replace`, those it holds
/// with other content, deletes the `removed` it holds, and returns the paths
/// written or deleted.
async fn commit_files(
    root: &Path,
    files: &[(String, &str)],
    removed: &[String],
    message: &str,
    replace: bool,
) -> Result<Vec<String>, Error> {
    let paths: Vec<String> = files.iter().map(|(path, _)| path.clone()).collect();
    let held = committed(root, &paths)?;
    let removable = committed(root, removed)?;
    let mut written = Vec::new();
    let mut changes = Vec::new();
    for (path, current) in removed.iter().zip(removable) {
        if let Some(current) = current {
            changes.push(crate::push::client::FileChange {
                path: path.clone(),
                content: None,
                expected: crate::push::client::Expected::Content(current.into_bytes()),
            });
            written.push(path.clone());
        }
    }
    for ((path, content), current) in files.iter().zip(held) {
        let expected = match current {
            None => crate::push::client::Expected::Missing,
            Some(current) if replace && current != *content => {
                crate::push::client::Expected::Content(current.into_bytes())
            }
            Some(_) => continue,
        };
        changes.push(crate::push::client::FileChange {
            path: path.clone(),
            content: Some(content.as_bytes().to_vec()),
            expected,
        });
        written.push(path.clone());
    }
    if changes.is_empty() {
        return Ok(written);
    }
    local_commit(root)
        .commit(changes, None, message.to_string())
        .await
        .map_err(repository)?;
    Ok(written)
}

fn create_folder(path: &Path) -> Result<(), Error> {
    std::fs::create_dir_all(path).map_err(|source| Error::Folder {
        path: path.display().to_string(),
        source,
    })
}

fn write_file(path: &Path, content: &str) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        create_folder(parent)?;
    }
    std::fs::write(path, content).map_err(|source| Error::Folder {
        path: path.display().to_string(),
        source,
    })
}

/// The git user of this machine as seen by `repo`, or flowgen when none is set.
pub(crate) fn machine_user(repo: &gix::Repository) -> (String, String) {
    match repo.committer() {
        Some(Ok(user)) => (user.name.to_string(), user.email.to_string()),
        _ => (FALLBACK_NAME.to_string(), FALLBACK_EMAIL.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_workspace_is_a_clean_repository_with_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flowgen");
        let nested = format!("{FLOWS_DIR}/nested/b.yaml");
        assert!(init(&root, &[(nested.clone(), "b")]).unwrap());

        assert!(root.join(FLOWS_DIR).is_dir());
        assert!(root.join(RESOURCES_DIR).is_dir());
        assert_eq!(std::fs::read_to_string(root.join(&nested)).unwrap(), "b");
        let repo = gix::open(&root).unwrap();
        let head = repo.head_commit().unwrap();
        assert_eq!(head.message_raw_sloppy(), MESSAGE);
        assert_eq!(head.parent_ids().count(), 0);
        let tree = head.tree().unwrap();
        assert!(tree.lookup_entry_by_path(&nested).unwrap().is_some());
        let status = repo
            .status(gix::progress::Discard)
            .unwrap()
            .into_index_worktree_iter(Vec::new())
            .unwrap()
            .count();
        assert_eq!(status, 0, "Nothing is changed or untracked");
    }

    #[test]
    fn committed_files_are_read_from_the_last_commit_not_the_working_tree() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flowgen");
        let path = format!("{FLOWS_DIR}/a.yaml");
        init(&root, &[(path.clone(), "a")]).unwrap();
        std::fs::write(root.join(&path), "not committed").unwrap();

        let contents = committed(&root, &[path, format!("{FLOWS_DIR}/missing.yaml")]).unwrap();

        assert_eq!(contents, vec![Some("a".to_string()), None]);
    }

    #[tokio::test]
    async fn only_missing_files_are_added_in_one_commit() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flowgen");
        let kept = format!("{FLOWS_DIR}/a.yaml");
        let added = format!("{FLOWS_DIR}/nested/b.yaml");
        init(&root, &[(kept.clone(), "mine")]).unwrap();
        let files = [(kept.clone(), "default"), (added.clone(), "b")];

        assert_eq!(
            add_missing(&root, &files, "Add b").await.unwrap(),
            vec![added.clone()]
        );
        assert_eq!(std::fs::read_to_string(root.join(&kept)).unwrap(), "mine");
        assert_eq!(std::fs::read_to_string(root.join(&added)).unwrap(), "b");
        let repo = gix::open(&root).unwrap();
        assert_eq!(repo.head_commit().unwrap().message_raw_sloppy(), "Add b");
        assert!(add_missing(&root, &files, "Add b")
            .await
            .unwrap()
            .is_empty());
    }

    #[test]
    fn a_folder_inside_a_repository_belongs_to_that_repository() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("repo");
        init(&root, &[]).unwrap();
        let nested = root.join("workspaces/a");
        std::fs::create_dir_all(&nested).unwrap();
        let outside = dir.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();

        assert_eq!(
            repository_of(&nested),
            Some(std::fs::canonicalize(&root).unwrap())
        );
        assert_eq!(repository_of(&outside), None);
    }

    #[test]
    fn a_repository_without_commits_becomes_a_workspace() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flowgen");
        gix::init(&root).unwrap();
        let path = format!("{FLOWS_DIR}/a.yaml");

        assert_eq!(
            committed(&root, std::slice::from_ref(&path)).unwrap(),
            vec![None]
        );
        assert!(init(&root, &[(path.clone(), "a")]).unwrap());
        assert_eq!(
            committed(&root, &[path]).unwrap(),
            vec![Some("a".to_string())]
        );
    }

    #[test]
    fn a_folder_with_flows_keeps_and_commits_them() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flowgen");
        let mine = format!("{FLOWS_DIR}/nested/mine.yaml");
        let starter = format!("{FLOWS_DIR}/system/a.yaml");
        for (path, content) in [
            (mine.as_str(), "mine"),
            (starter.as_str(), "edited"),
            (GITIGNORE_FILE, "secret.txt"),
            (".hidden/b.txt", "b"),
        ] {
            write_file(&root.join(path), content).unwrap();
        }

        assert!(init(&root, &[(starter.clone(), "default")]).unwrap());

        assert_eq!(
            committed(&root, &[mine, starter, ".hidden/b.txt".to_string()]).unwrap(),
            vec![Some("mine".to_string()), Some("edited".to_string()), None]
        );
        assert_eq!(
            std::fs::read_to_string(root.join(GITIGNORE_FILE)).unwrap(),
            format!("secret.txt\n{}", gitignore())
        );
    }

    #[test]
    fn an_existing_repository_is_left_as_it_is() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flowgen");
        assert!(init(&root, &[]).unwrap());
        let first = gix::open(&root).unwrap().head_id().unwrap().detach();

        assert!(!init(&root, &[]).unwrap());
        assert_eq!(gix::open(&root).unwrap().head_id().unwrap().detach(), first);
    }
}
