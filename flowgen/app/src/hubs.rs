//! Hubs: flows in [`SYSTEM_FOLDER`] named `<hub>`[`HUB_SUFFIX`] that return
//! the files of a hub's catalog, called in-process when the web UI lists
//! templates to start new flows and resources from. Nothing is stored; a
//! template becomes a change like any other, validated and approved before it
//! runs.

use crate::web::WebState;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use flowgen_client::types as api;
use flowgen_core::workspace::{FLOWS_DIR, RESOURCES_DIR};
use serde::Deserialize;
use std::sync::Arc;

/// The folder of the hub flows, with the other system flows.
pub const SYSTEM_FOLDER: &str = "system";

/// The end of a hub flow's name, after the hub's id: `connve_hub`.
pub const HUB_SUFFIX: &str = "_hub";

/// The flow of `hub`, e.g. `system/connve_hub` for `connve`.
fn flow_of(hub: &str) -> String {
    format!("{SYSTEM_FOLDER}/{hub}{HUB_SUFFIX}")
}

/// The hub `flow` is, e.g. `connve` for `system/connve_hub`.
fn hub_of(flow: &str) -> Option<&str> {
    flow.strip_prefix(SYSTEM_FOLDER)?
        .strip_prefix('/')?
        .strip_suffix(HUB_SUFFIX)
        .filter(|hub| !hub.is_empty())
}

/// The file that makes a catalog folder a template.
const TEMPLATE_FILE: &str = "template.yaml";

/// The template's description for people, shown with it.
const README_FILE: &str = "README.md";

/// Failure to list a hub's templates.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("No hub '{hub}' runs here")]
    NotFound { hub: String },
    #[error("The hub '{hub}' failed: {reason}")]
    Failed { hub: String, reason: String },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let status = match &self {
            Error::NotFound { .. } => StatusCode::NOT_FOUND,
            Error::Failed { .. } => StatusCode::BAD_GATEWAY,
        };
        (status, self.to_string()).into_response()
    }
}

/// What a hub flow returns.
#[derive(Deserialize)]
struct Returned {
    #[serde(default)]
    files: Vec<ReturnedFile>,
}

#[derive(Deserialize)]
struct ReturnedFile {
    path: String,
    /// The text of the file; a binary file has none and is left out.
    #[serde(default)]
    content: Option<String>,
}

/// A template's `template.yaml`.
#[derive(Deserialize)]
struct Manifest {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    category: Option<String>,
    #[serde(default)]
    tags: Vec<String>,
}

/// `GET /api/hubs`.
pub(crate) async fn list_hubs(State(state): State<Arc<WebState>>) -> Json<api::Hubs> {
    let flows: Vec<String> = state
        .inproc
        .flows_within(SYSTEM_FOLDER)
        .into_iter()
        .filter(|flow| hub_of(flow).is_some())
        .collect();
    let names = match state.flow_registry.read() {
        Ok(registry) => flows
            .iter()
            .map(|flow| {
                registry
                    .get(flow)
                    .and_then(|handle| handle.display_name())
                    .map(str::to_string)
            })
            .collect(),
        Err(_) => vec![None; flows.len()],
    };
    let hubs = flows
        .iter()
        .zip(names)
        .filter_map(|(flow, name)| {
            let id = hub_of(flow)?.to_string();
            Some(api::Hub {
                name: match name {
                    Some(name) => name,
                    None => id.clone(),
                },
                id,
            })
        })
        .collect();
    Json(api::Hubs { hubs })
}

/// `GET /api/hubs/{hub}/templates`.
pub(crate) async fn list_templates(
    State(state): State<Arc<WebState>>,
    Path(hub): Path<String>,
) -> Result<Json<api::HubTemplates>, Error> {
    let flow = flow_of(&hub);
    let known = !hub.is_empty() && !hub.contains('/') && state.inproc.is_registered(&flow);
    if !known {
        return Err(Error::NotFound { hub });
    }
    let failed = |reason: String| Error::Failed {
        hub: hub.clone(),
        reason,
    };
    let returned = state
        .inproc
        .call(
            &[],
            &flow,
            serde_json::Value::Object(serde_json::Map::new()),
            serde_json::Map::new(),
            None,
        )
        .await
        .map_err(|error| failed(error.to_string()))?;
    let files = match returned {
        Some(value) => {
            serde_json::from_value::<Returned>(value)
                .map_err(|error| failed(error.to_string()))?
                .files
        }
        None => Vec::new(),
    };
    let lock = match state.flows_cache.as_deref() {
        Some(cache) => crate::lock::Lock::parse(crate::lock::deployed(cache).await.as_deref()),
        None => crate::lock::Lock::default(),
    };
    let templates = installed(templates(files), &lock, &hub);
    Ok(Json(api::HubTemplates { templates }))
}

/// `GET /api/workspace/templates`: the templates the deployed `flowgen.lock` holds.
pub(crate) async fn list_installed(
    State(state): State<Arc<WebState>>,
) -> Json<api::InstalledTemplates> {
    let lock = match state.flows_cache.as_deref() {
        Some(cache) => crate::lock::Lock::parse(crate::lock::deployed(cache).await.as_deref()),
        None => crate::lock::Lock::default(),
    };
    let templates = lock
        .templates
        .into_iter()
        .filter_map(|(key, locked)| {
            let (hub, slug) = key.split_once('/')?;
            Some(api::InstalledTemplate {
                hub: hub.to_string(),
                slug: slug.to_string(),
                digest: locked.digest,
                files: locked.files,
            })
        })
        .collect();
    Json(api::InstalledTemplates { templates })
}

/// `templates` of `hub` with the digest and files `lock` holds for those installed.
fn installed(
    templates: Vec<api::HubTemplate>,
    lock: &crate::lock::Lock,
    hub: &str,
) -> Vec<api::HubTemplate> {
    templates
        .into_iter()
        .map(
            |template| match lock.templates.get(&crate::lock::key(hub, &template.slug)) {
                Some(locked) => api::HubTemplate {
                    installed_digest: Some(locked.digest.clone()),
                    installed_files: locked.files.clone(),
                    ..template
                },
                None => template,
            },
        )
        .collect()
}

/// The templates among a catalog's `files`: each top folder holding a valid
/// [`TEMPLATE_FILE`], with its `flows/` and `resources/` files, by name.
fn templates(files: Vec<ReturnedFile>) -> Vec<api::HubTemplate> {
    let mut folders: std::collections::BTreeMap<String, Vec<(String, String)>> =
        std::collections::BTreeMap::new();
    for file in files {
        if let (Some((slug, path)), Some(content)) = (file.path.split_once('/'), file.content) {
            folders
                .entry(slug.to_string())
                .or_default()
                .push((path.to_string(), content));
        }
    }
    let workspace = |path: &str| {
        path.starts_with(&format!("{FLOWS_DIR}/")) || path.starts_with(&format!("{RESOURCES_DIR}/"))
    };
    let mut templates: Vec<api::HubTemplate> = folders
        .into_iter()
        .filter_map(|(slug, files)| {
            let manifest = files.iter().find(|(path, _)| path == TEMPLATE_FILE)?;
            let manifest: Manifest = serde_yaml::from_str(&manifest.1).ok()?;
            let readme = files
                .iter()
                .find(|(path, _)| path == README_FILE)
                .map(|(_, content)| content.clone());
            let mut files: Vec<api::WorkspaceFile> = files
                .into_iter()
                .filter(|(path, _)| workspace(path))
                .map(|(path, content)| api::WorkspaceFile {
                    path,
                    content: Some(content),
                })
                .collect();
            files.sort_by(|a, b| a.path.cmp(&b.path));
            Some(api::HubTemplate {
                slug,
                name: manifest.name,
                description: manifest.description,
                category: manifest.category,
                tags: manifest.tags,
                readme,
                digest: crate::lock::digest(&files),
                installed_digest: None,
                installed_files: Vec::new(),
                files,
            })
        })
        .collect();
    templates.sort_by(|a, b| a.name.cmp(&b.name));
    templates
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, content: &str) -> ReturnedFile {
        ReturnedFile {
            path: path.to_string(),
            content: Some(content.to_string()),
        }
    }

    #[test]
    fn a_hub_is_a_system_flow_named_after_it() {
        assert_eq!(flow_of("connve"), "system/connve_hub");
        assert_eq!(hub_of("system/connve_hub"), Some("connve"));
        assert_eq!(hub_of("system/_hub"), None);
        assert_eq!(hub_of("system/commit_workspace"), None);
        assert_eq!(hub_of("other/connve_hub"), None);
    }

    #[test]
    fn each_folder_with_a_template_file_is_a_template_of_its_workspace_files() {
        let templates = templates(vec![
            file(
                "b-sync/template.yaml",
                "name: B sync\ncategory: solutions\ntags: [a]\n",
            ),
            file("b-sync/README.md", "# B"),
            file("b-sync/flows/b/sync.yaml", "flow: {}"),
            file("b-sync/resources/b.sql", "select 1"),
            file("b-sync/notes.txt", "dropped"),
            file("a-basic/template.yaml", "name: A basic\n"),
            file("a-basic/flows/a.yaml", "flow: {}"),
            file("no-manifest/flows/c.yaml", "flow: {}"),
            file("broken/template.yaml", "name: [unclosed"),
            file("README.md", "the catalog"),
        ]);

        let names: Vec<&str> = templates.iter().map(|t| t.name.as_str()).collect();
        assert_eq!(names, vec!["A basic", "B sync"]);
        let b = &templates[1];
        assert_eq!(b.slug, "b-sync");
        assert_eq!(b.category.as_deref(), Some("solutions"));
        assert_eq!(b.tags, vec!["a"]);
        assert_eq!(b.readme.as_deref(), Some("# B"));
        let paths: Vec<&str> = b.files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(paths, vec!["flows/b/sync.yaml", "resources/b.sql"]);
    }

    #[test]
    fn an_installed_template_carries_the_digest_and_files_of_the_lock() {
        let catalog = templates(vec![
            file("a/template.yaml", "name: A\n"),
            file("a/flows/a.yaml", "flow: {}"),
            file("b/template.yaml", "name: B\n"),
            file("b/flows/b.yaml", "flow: {}"),
        ]);
        let lock: crate::lock::Lock = serde_yaml::from_str(
            "templates:\n  local/a:\n    digest: sha256:1\n    files: [flows/a.yaml, resources/old.md]\n",
        )
        .unwrap();

        let listed = installed(catalog, &lock, "local");

        assert_eq!(listed[0].installed_digest.as_deref(), Some("sha256:1"));
        assert_eq!(
            listed[0].installed_files,
            ["flows/a.yaml", "resources/old.md"]
        );
        assert_eq!(listed[1].installed_digest, None);
        assert!(listed[1].installed_files.is_empty());
    }
}
