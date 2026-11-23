//! `flowgen.lock`: what a workspace installed from hubs, by digest and files.
//! flowgen writes it when a change from a template is committed, based on the
//! deployed one; the deploy flow mirrors it to the system bucket.

use flowgen_client::types as api;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use tracing::warn;

/// The lock file, at the root of the workspace.
pub const LOCK_FILE: &str = "flowgen.lock";

/// The key the deploy flow mirrors [`LOCK_FILE`] to in the system bucket.
pub const LOCK_KEY: &str = "workspace.lock";

/// The contents of [`LOCK_FILE`].
#[derive(Debug, Default, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Lock {
    /// Installed hub templates by `<hub>/<slug>`.
    #[serde(default)]
    pub templates: BTreeMap<String, Locked>,
}

/// One installed template.
#[derive(Debug, PartialEq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Locked {
    /// The digest of the template's files when it was installed.
    pub digest: String,
    /// The paths of the files it installed, which an update or uninstall deletes when they go.
    pub files: Vec<String>,
}

/// The key of a template in [`Lock::templates`].
pub fn key(hub: &str, slug: &str) -> String {
    format!("{hub}/{slug}")
}

/// The SHA-256 of a template's `files`, as `sha256:<hex>`, whatever their order.
pub fn digest(files: &[api::WorkspaceFile]) -> String {
    let mut sorted: Vec<&api::WorkspaceFile> = files.iter().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    let mut hasher = Sha256::new();
    for file in sorted {
        hasher.update(file.path.as_bytes());
        hasher.update([0]);
        hasher.update(file.content.as_deref().unwrap_or_default().as_bytes());
        hasher.update([0]);
    }
    format!("sha256:{:x}", hasher.finalize())
}

impl Lock {
    /// The lock once `change` is committed: each of its templates installed at
    /// its digest with those of its files the change writes, or dropped when
    /// it is removed.
    pub fn with(mut self, change: &api::Change) -> Self {
        for source in &change.templates {
            let key = key(&source.hub, &source.slug);
            match source.removed {
                true => {
                    self.templates.remove(&key);
                }
                false => {
                    let mut files: Vec<String> = change
                        .files
                        .iter()
                        .filter(|file| file.content.is_some() && source.files.contains(&file.path))
                        .map(|file| file.path.clone())
                        .collect();
                    files.sort();
                    self.templates.insert(
                        key,
                        Locked {
                            digest: source.digest.clone(),
                            files,
                        },
                    );
                }
            }
        }
        self
    }

    /// The file as flowgen writes it.
    pub fn render(&self) -> Result<String, serde_yaml::Error> {
        Ok(format!(
            "# Written by flowgen: the hub templates this workspace installed.\n{}",
            serde_yaml::to_string(self)?
        ))
    }

    /// The lock `content` holds; empty when there is none or it is invalid.
    pub fn parse(content: Option<&str>) -> Self {
        match content.map(serde_yaml::from_str::<Lock>) {
            Some(Ok(lock)) => lock,
            Some(Err(error)) => {
                warn!(error = %error, "The deployed flowgen.lock is invalid");
                Lock::default()
            }
            None => Lock::default(),
        }
    }
}

/// The deployed flowgen.lock as written; `None` when there is none or it can't be read.
pub async fn deployed(cache: &dyn flowgen_core::cache::Cache) -> Option<String> {
    match cache.get(LOCK_KEY).await {
        Ok(Some(bytes)) => Some(String::from_utf8_lossy(&bytes).into_owned()),
        Ok(None) => None,
        Err(error) => {
            warn!(error = %error, "Failed to read the deployed flowgen.lock");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, content: &str) -> api::WorkspaceFile {
        api::WorkspaceFile {
            path: path.to_string(),
            content: Some(content.to_string()),
        }
    }

    #[test]
    fn a_digest_follows_the_files_but_not_their_order() {
        let a = digest(&[file("flows/a.yaml", "a"), file("resources/b.md", "b")]);
        let reordered = digest(&[file("resources/b.md", "b"), file("flows/a.yaml", "a")]);
        let changed = digest(&[file("flows/a.yaml", "a"), file("resources/b.md", "c")]);

        assert_eq!(a, reordered);
        assert_ne!(a, changed);
        assert!(a.starts_with("sha256:"));
    }

    /// A change from template `slug` writing two of its files and deleting a
    /// third, with an edit of a file outside it.
    fn change(slug: &str, removed: bool) -> api::Change {
        serde_json::from_value(serde_json::json!({
            "id": "0", "target": "workspace", "title": "a", "status": "pending",
            "proposedBy": "a", "createdAt": 0, "issues": [],
            "files": [
                {"path": "resources/b.md", "content": "b", "diff": ""},
                {"path": "flows/a.yaml", "content": "a", "diff": ""},
                {"path": "resources/old.md", "content": null, "diff": ""},
                {"path": "flows/other.yaml", "content": "o", "diff": ""}
            ],
            "templates": [{
                "hub": "local", "slug": slug, "digest": "sha256:1", "removed": removed,
                "files": ["flows/a.yaml", "resources/b.md", "resources/old.md"]
            }]
        }))
        .unwrap()
    }

    #[test]
    fn an_installed_template_is_written_and_read_back_with_the_files_it_writes() {
        let lock = Lock::default().with(&change("a", false));

        let read: Lock = serde_yaml::from_str(&lock.render().unwrap()).unwrap();

        assert_eq!(read, lock);
        assert_eq!(read.templates["local/a"].digest, "sha256:1");
        assert_eq!(
            read.templates["local/a"].files,
            ["flows/a.yaml", "resources/b.md"]
        );
    }

    #[test]
    fn each_template_of_a_change_is_locked_with_its_own_files() {
        let mut change = change("a", false);
        change.files.push(api::ChangeFile {
            path: "flows/c.yaml".to_string(),
            content: Some("c".to_string()),
            previous: None,
            diff: String::new(),
        });
        change.templates.push(api::TemplateSource {
            hub: "local".to_string(),
            slug: "c".to_string(),
            digest: "sha256:2".to_string(),
            removed: false,
            files: vec!["flows/c.yaml".to_string()],
        });

        let lock = Lock::default().with(&change);

        assert_eq!(
            lock.templates["local/a"].files,
            ["flows/a.yaml", "resources/b.md"]
        );
        assert_eq!(lock.templates["local/c"].files, ["flows/c.yaml"]);
    }

    #[test]
    fn a_removed_template_leaves_the_lock() {
        let lock = Lock::default()
            .with(&change("a", false))
            .with(&change("b", false))
            .with(&change("a", true));

        assert_eq!(lock.templates.keys().collect::<Vec<_>>(), ["local/b"]);
    }
}
