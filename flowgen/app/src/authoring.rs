//! Workspace change proposals: validated, diffed against what is deployed,
//! and published by a flow once a signed-in user approves them.

use crate::validation::flow_identity;
use crate::web::{Caller, WebState};
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use flowgen_client::types as api;
use serde::Serialize;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Key prefix for changes in the system cache.
const CHANGE_KEY_PREFIX: &str = "authoring.changes.";

/// How long a diff searches for the smallest edit before settling for a larger one.
const DIFF_TIMEOUT: Duration = Duration::from_secs(1);

/// Failure of a change request, with the status it answers.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Authoring is not configured")]
    NotConfigured,
    #[error("No change with id '{id}'")]
    NotFound { id: String },
    #[error("Invalid path '{path}': {reason}")]
    InvalidPath {
        path: String,
        #[source]
        reason: PathError,
    },
    #[error("The change lists '{path}' more than once")]
    DuplicatePath { path: String },
    #[error("A change needs a title")]
    MissingTitle,
    #[error("The change is already {status}")]
    NotPending { status: api::ChangeStatus },
    #[error("The change has invalid files")]
    Invalid,
    #[error("A change needs at least one file")]
    NoFiles,
    #[error("'{path}' is outside every authoring target")]
    NoTarget { path: String },
    #[error(
        "The files belong to the targets '{first}' and '{second}'; propose a change per target"
    )]
    MixedTargets { first: String, second: String },
    #[error("The change's target '{target}' no longer covers its files")]
    TargetGone { target: String },
    #[error("The change's files moved from target '{stored}' to '{current}'")]
    TargetChanged { stored: String, current: String },
    #[error("Only a signed-in user can approve or reject changes")]
    UserRequired,
    #[error("Only the proposer can edit this change")]
    NotProposer,
    #[error("The change was edited. Please review it again.")]
    EditedSinceReview,
    #[error("'{user}' is not allowed to approve or reject changes")]
    NotApprover { user: String },
    #[error("Publishing timed out after {timeout:?}. Please try again.")]
    PublishTimeout { timeout: Duration },
    #[error("Publishing stopped unexpectedly: {source}")]
    PublishAborted {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("Checking the files stopped unexpectedly: {source}")]
    Worker {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error(transparent)]
    Call(#[from] flowgen_core::task::inproc::registry::CallError),
    #[error("Failed to serialize the publish request: {source}")]
    Serialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to read the deployed '{path}': {source}")]
    Deployed {
        path: String,
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("The running flows cannot be read")]
    RegistryUnavailable,
    #[error("Failed to read the deployed '{path}': {source}")]
    DeployedCache {
        path: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    #[error("The change was updated. Please reload and try again.")]
    Concurrent,
    #[error("Change store unavailable: {source}")]
    Store {
        #[source]
        source: flowgen_core::cache::Error,
    },
    #[error("Stored change is unreadable: {source}")]
    Corrupt {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to encode the change: {source}")]
    Encode {
        #[source]
        source: serde_json::Error,
    },
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        let (status, fixed) = match &self {
            Error::NotConfigured | Error::NotFound { .. } => (StatusCode::NOT_FOUND, None),
            Error::InvalidPath { .. }
            | Error::DuplicatePath { .. }
            | Error::MissingTitle
            | Error::NoFiles
            | Error::NoTarget { .. }
            | Error::MixedTargets { .. } => (StatusCode::BAD_REQUEST, None),
            Error::NotPending { .. }
            | Error::Invalid
            | Error::Concurrent
            | Error::EditedSinceReview
            | Error::TargetGone { .. }
            | Error::TargetChanged { .. } => (StatusCode::CONFLICT, None),
            Error::UserRequired | Error::NotApprover { .. } | Error::NotProposer => {
                (StatusCode::FORBIDDEN, None)
            }
            Error::Store { .. } => (
                StatusCode::SERVICE_UNAVAILABLE,
                Some("Change store unavailable"),
            ),
            Error::Corrupt { .. } => (
                StatusCode::SERVICE_UNAVAILABLE,
                Some("Stored change is unreadable"),
            ),
            Error::Deployed { .. } | Error::DeployedCache { .. } | Error::RegistryUnavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                Some("Deployed content unavailable"),
            ),
            Error::PublishTimeout { .. }
            | Error::PublishAborted { .. }
            | Error::Call(_)
            | Error::Serialize { .. } => {
                (StatusCode::INTERNAL_SERVER_ERROR, Some("Publishing failed"))
            }
            Error::Encode { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Some("Failed to encode the change"),
            ),
            Error::Worker { .. } => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Some("Failed to check the files"),
            ),
        };
        match fixed {
            Some(message) => {
                warn!(error = %self, "{message}");
                (status, message).into_response()
            }
            None => (status, self.to_string()).into_response(),
        }
    }
}

/// Where a workspace file lives once deployed.
#[derive(Debug, PartialEq)]
enum Location<'a> {
    /// A flow file; `file` is its path within `flows/`.
    Flow { file: &'a str },
    /// A resource; `key` is its path within `resources/`.
    Resource { key: &'a str },
}

/// Why a path cannot hold a flow or resource.
#[derive(thiserror::Error, Debug, Clone, Copy, PartialEq)]
pub enum PathError {
    #[error("it must start with flows/ or resources/")]
    Root,
    #[error("it needs a file name")]
    NoFileName,
    #[error("it has an empty folder name")]
    EmptySegment,
    #[error("'.' and '..' are not allowed")]
    Relative,
    #[error("names can only use letters, digits, '.', '_' and '-'")]
    Characters,
}

/// Splits a workspace path into its kind; segments are limited to
/// `[A-Za-z0-9._-]` because flow identities and resource keys become cache keys.
fn locate(path: &str) -> Result<Location<'_>, PathError> {
    let (location, rest) = match path.split_once('/') {
        Some(("flows", file)) => (Location::Flow { file }, file),
        Some(("resources", key)) => (Location::Resource { key }, key),
        _ => return Err(PathError::Root),
    };
    if rest.is_empty() || rest.ends_with('/') {
        return Err(PathError::NoFileName);
    }
    for segment in rest.split('/') {
        match segment {
            "" => return Err(PathError::EmptySegment),
            "." | ".." => return Err(PathError::Relative),
            _ if !segment
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-')) =>
            {
                return Err(PathError::Characters)
            }
            _ => {}
        }
    }
    Ok(location)
}

/// Where each file of a proposal lives once deployed; the title becomes the
/// commit message, so it may not be blank.
fn check_proposal(proposal: &api::ChangeProposal) -> Result<Vec<Location<'_>>, Error> {
    if proposal.title.trim().is_empty() {
        return Err(Error::MissingTitle);
    }
    check_paths(&proposal.files)
}

/// Where each file lives once deployed; each path may appear once.
fn check_paths(files: &[api::WorkspaceFile]) -> Result<Vec<Location<'_>>, Error> {
    let mut seen = std::collections::HashSet::new();
    let mut locations = Vec::with_capacity(files.len());
    for file in files {
        let location = match locate(&file.path) {
            Ok(location) => location,
            Err(reason) => {
                return Err(Error::InvalidPath {
                    path: file.path.clone(),
                    reason,
                })
            }
        };
        if !seen.insert(file.path.as_str()) {
            return Err(Error::DuplicatePath {
                path: file.path.clone(),
            });
        }
        locations.push(location);
    }
    Ok(locations)
}

/// Validates on a blocking thread, as parsing and compiling large files takes a while.
async fn validate_blocking(
    files: Vec<api::WorkspaceFile>,
) -> Result<Vec<api::ValidationIssue>, Error> {
    tokio::task::spawn_blocking(move || validate_files(&files))
        .await
        .map_err(|source| Error::Worker { source })
}

/// Issues for every file that has content; deletions are not validated.
fn validate_files(files: &[api::WorkspaceFile]) -> Vec<api::ValidationIssue> {
    let mut issues = Vec::new();
    for file in files {
        let found = match (locate(&file.path), &file.content) {
            (Err(reason), _) => vec![crate::validation::Issue {
                location: None,
                message: Error::InvalidPath {
                    path: file.path.clone(),
                    reason,
                }
                .to_string(),
            }],
            (Ok(_), None) => Vec::new(),
            (Ok(Location::Flow { file: name }), Some(content)) => {
                crate::validation::validate_flow(name, content)
            }
            (Ok(Location::Resource { key }), Some(content)) => {
                crate::validation::validate_resource(key, content)
            }
        };
        issues.extend(found.into_iter().map(|issue| api::ValidationIssue {
            path: file.path.clone(),
            location: issue.location,
            message: issue.message,
        }));
    }
    issues
}

/// The deployed content of a workspace file, `None` when it does not exist.
///
/// A flow is read from the flows cache, which holds the synced source even
/// when the flow failed to start, and from the running flows for flows
/// loaded from the filesystem.
async fn deployed(
    state: &WebState,
    path: &str,
    location: &Location<'_>,
) -> Result<Option<String>, Error> {
    match location {
        Location::Flow { file } => {
            let identity = flow_identity(file);
            if let (Some(cache), Some(options)) =
                (&state.flows_cache, &state.app_config.flows.cache)
            {
                let synced = cache
                    .get(&format!("{}.{identity}", options.prefix))
                    .await
                    .map_err(|source| Error::DeployedCache {
                        path: path.to_string(),
                        source,
                    })?;
                if let Some(bytes) = synced {
                    return Ok(Some(String::from_utf8_lossy(&bytes).into_owned()));
                }
            }
            match state.flow_registry.read() {
                Ok(registry) => Ok(registry
                    .get(identity)
                    .map(|handle| handle.flow_yaml().to_string())),
                Err(_) => Err(Error::RegistryUnavailable),
            }
        }
        Location::Resource { key } => match &state.resource_loader {
            Some(loader) => match loader.load(key).await {
                Ok(content) => Ok(Some(content)),
                Err(flowgen_core::resource::Error::ResourceNotFound { .. }) => Ok(None),
                Err(source) => Err(Error::Deployed {
                    path: path.to_string(),
                    source,
                }),
            },
            None => Ok(None),
        },
    }
}

/// Whether two versions of a file match, ignoring trailing whitespace; `None`
/// is a missing file.
fn same_content(a: Option<&str>, b: Option<&str>) -> bool {
    match (a, b) {
        (Some(a), Some(b)) => a.trim_end() == b.trim_end(),
        (None, None) => true,
        _ => false,
    }
}

/// Published versions of each path with the change that made them, oldest
/// approval first.
type Published = std::collections::HashMap<String, Vec<(String, api::ChangeFile)>>;

fn index_published<'a>(changes: impl IntoIterator<Item = &'a api::Change>) -> Published {
    let mut decided: Vec<_> = changes
        .into_iter()
        .filter(|change| change.status == api::ChangeStatus::Published)
        .map(|change| match change.decided_at {
            Some(decided_at) => (decided_at, change),
            None => (change.created_at, change),
        })
        .collect();
    decided.sort_by_key(|(decided_at, _)| *decided_at);
    let mut published = Published::new();
    for (_, change) in decided {
        for file in &change.files {
            published
                .entry(file.path.clone())
                .or_default()
                .push((change.id.clone(), file.clone()));
        }
    }
    published
}

async fn published_versions(state: &WebState) -> Result<Published, Error> {
    let keys = state
        .conversation_cache
        .list_keys(CHANGE_KEY_PREFIX)
        .await
        .map_err(|source| Error::Store { source })?;
    let mut changes = Vec::with_capacity(keys.len());
    for key in keys {
        if let Ok((change, _)) = load(state, key.trim_start_matches(CHANGE_KEY_PREFIX)).await {
            changes.push(change);
        }
    }
    Ok(index_published(&changes))
}

/// Applies the published versions of a file to what runs: from the newest
/// version already running, each later one based on the content so far. Gives
/// what the branch holds as far as flowgen knows, and the changes applied,
/// which are not deployed yet.
fn apply_published<'a>(
    running: Option<&'a str>,
    versions: &'a [(String, api::ChangeFile)],
) -> (Option<&'a str>, Vec<&'a str>) {
    let start = match versions
        .iter()
        .rposition(|(_, version)| same_content(running, version.content.as_deref()))
    {
        Some(deployed) => deployed + 1,
        None => 0,
    };
    let mut content = running;
    let mut pending = Vec::new();
    for (id, version) in &versions[start..] {
        if same_content(content, version.previous.as_deref()) {
            content = version.content.as_deref();
            pending.push(id.as_str());
        }
    }
    (content, pending)
}

/// What `path` holds on the branch as far as flowgen knows.
async fn base_content(
    state: &WebState,
    path: &str,
    location: &Location<'_>,
    published: &Published,
) -> Result<Option<String>, Error> {
    let running = deployed(state, path, location).await?;
    let versions = match published.get(path) {
        Some(versions) => versions.as_slice(),
        None => &[],
    };
    Ok(apply_published(running.as_deref(), versions)
        .0
        .map(str::to_string))
}

/// Where a published change stands against what this instance runs; `None`
/// for other changes, for targets without `track_deployment`, and when the
/// running content cannot be read.
async fn deployment(
    state: &WebState,
    change: &api::Change,
    published: &Published,
) -> Option<api::ChangeDeployment> {
    let tracked = match (&change.status, &state.authoring) {
        (api::ChangeStatus::Published, Some(authoring)) => authoring
            .targets
            .iter()
            .any(|target| target.name == change.target && target.track_deployment),
        _ => false,
    };
    if !tracked {
        return None;
    }
    for file in &change.files {
        let location = match locate(&file.path) {
            Ok(location) => location,
            Err(_) => return None,
        };
        let versions = match published.get(&file.path) {
            Some(versions) => versions.as_slice(),
            None => &[],
        };
        match deployed(state, &file.path, &location).await {
            Ok(running)
                if apply_published(running.as_deref(), versions)
                    .1
                    .contains(&change.id.as_str()) =>
            {
                return Some(api::ChangeDeployment::Waiting)
            }
            Ok(_) => {}
            Err(error) => {
                warn!(change = %change.id, error = %error, "Failed to check the deployment of a change");
                return None;
            }
        }
    }
    Some(api::ChangeDeployment::Deployed)
}

/// The change with each file's diff filled in, on a blocking thread as large
/// files take a while; diffs are not stored.
async fn with_diffs(mut change: api::Change) -> Result<api::Change, Error> {
    tokio::task::spawn_blocking(move || {
        for file in &mut change.files {
            file.diff = unified_diff(
                &file.path,
                file.previous.as_deref(),
                file.content.as_deref(),
            );
        }
        change
    })
    .await
    .map_err(|source| Error::Worker { source })
}

/// The one target every path falls in.
fn target_for<'a, 'p>(
    authoring: &'a crate::config::AuthoringOptions,
    paths: impl IntoIterator<Item = &'p str>,
) -> Result<&'a crate::config::AuthoringTarget, Error> {
    let mut found: Option<&crate::config::AuthoringTarget> = None;
    for path in paths {
        let target = match authoring.target_of(path) {
            Some(target) => target,
            None => {
                return Err(Error::NoTarget {
                    path: path.to_string(),
                })
            }
        };
        match found {
            Some(first) if first.name != target.name => {
                return Err(Error::MixedTargets {
                    first: first.name.clone(),
                    second: target.name.clone(),
                })
            }
            _ => found = Some(target),
        }
    }
    match found {
        Some(target) => Ok(target),
        None => Err(Error::NoFiles),
    }
}

/// The target a stored change still belongs to under the current configuration,
/// so approver groups and the publish flow cannot come from a target that no
/// longer owns its files.
fn stored_target<'a>(
    authoring: &'a crate::config::AuthoringOptions,
    change: &api::Change,
) -> Result<&'a crate::config::AuthoringTarget, Error> {
    match target_for(
        authoring,
        change.files.iter().map(|file| file.path.as_str()),
    ) {
        Ok(current) if current.name == change.target => Ok(current),
        Ok(current) => Err(Error::TargetChanged {
            stored: change.target.clone(),
            current: current.name.clone(),
        }),
        Err(_) => Err(Error::TargetGone {
            target: change.target.clone(),
        }),
    }
}

/// Change ids are lowercase hex, as generated on proposal.
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// Unified diff from `previous` to `content`, `/dev/null` standing for a
/// missing side.
fn unified_diff(path: &str, previous: Option<&str>, content: Option<&str>) -> String {
    let (old_header, old) = match previous {
        Some(text) => (format!("a/{path}"), text),
        None => ("/dev/null".to_string(), ""),
    };
    let (new_header, new) = match content {
        Some(text) => (format!("b/{path}"), text),
        None => ("/dev/null".to_string(), ""),
    };
    similar::TextDiff::configure()
        .timeout(DIFF_TIMEOUT)
        .diff_lines(old, new)
        .unified_diff()
        .header(&old_header, &new_header)
        .to_string()
}

fn change_key(id: &str) -> String {
    format!("{CHANGE_KEY_PREFIX}{id}")
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn caller_label(caller: Option<&Caller>) -> String {
    match caller {
        Some(caller) => caller.label(),
        None => "anonymous".to_string(),
    }
}

/// Approving and rejecting is for people, and for members of the target's
/// `approver_groups` when set: a machine key is refused. Without `web.auth`
/// there is no caller and everyone may decide.
fn decider(
    caller: Option<&Caller>,
    authoring: &crate::config::AuthoringOptions,
    target: &crate::config::AuthoringTarget,
) -> Result<Option<flowgen_core::auth::UserContext>, Error> {
    let user = match caller {
        Some(Caller::User(user)) => user,
        Some(Caller::Key(_)) => return Err(Error::UserRequired),
        None => return Ok(None),
    };
    let groups: Vec<&str> = match user.claims.get(&authoring.groups_claim) {
        Some(serde_json::Value::Array(groups)) => groups
            .iter()
            .filter_map(serde_json::Value::as_str)
            .collect(),
        Some(serde_json::Value::String(group)) => vec![group.as_str()],
        _ => Vec::new(),
    };
    let allowed = target.approver_groups.is_empty()
        || target
            .approver_groups
            .iter()
            .any(|group| groups.contains(&group.as_str()));
    match allowed {
        true => Ok(Some(user.clone())),
        false => Err(Error::NotApprover {
            user: user.user_id.clone(),
        }),
    }
}

async fn load(state: &WebState, id: &str) -> Result<(api::Change, u64), Error> {
    if !valid_id(id) {
        return Err(Error::NotFound { id: id.to_string() });
    }
    let stored = state
        .conversation_cache
        .get_with_revision(&change_key(id))
        .await
        .map_err(|source| Error::Store { source })?;
    match stored {
        Some((bytes, revision)) => match serde_json::from_slice(&bytes) {
            Ok(change) => Ok((change, revision)),
            Err(source) => Err(Error::Corrupt { source }),
        },
        None => Err(Error::NotFound { id: id.to_string() }),
    }
}

fn encode(change: &api::Change) -> Result<bytes::Bytes, Error> {
    match serde_json::to_vec(change) {
        Ok(bytes) => Ok(bytes.into()),
        Err(source) => Err(Error::Encode { source }),
    }
}

async fn store(state: &WebState, change: &api::Change) -> Result<(), Error> {
    state
        .conversation_cache
        .put(&change_key(&change.id), encode(change)?, None)
        .await
        .map_err(|source| Error::Store { source })
}

/// Writes `change` only if nobody wrote it since `revision`, returning the new revision.
async fn transition(state: &WebState, change: &api::Change, revision: u64) -> Result<u64, Error> {
    match state
        .conversation_cache
        .update(&change_key(&change.id), encode(change)?, revision, None)
        .await
    {
        Ok(next) => Ok(next),
        Err(flowgen_core::cache::Error::RevisionMismatch { .. }) => Err(Error::Concurrent),
        Err(source) => Err(Error::Store { source }),
    }
}

fn require_authoring(state: &WebState) -> Result<&crate::config::AuthoringOptions, Error> {
    match &state.authoring {
        Some(authoring) => Ok(authoring),
        None => Err(Error::NotConfigured),
    }
}

/// `POST /api/workspace/validate`.
pub(crate) async fn validate_workspace(
    Json(body): Json<api::WorkspaceFiles>,
) -> Result<Json<api::WorkspaceValidation>, Error> {
    Ok(Json(api::WorkspaceValidation {
        issues: validate_blocking(body.files).await?,
    }))
}

/// Query of `GET /api/workspace/file`.
#[derive(serde::Deserialize)]
pub(crate) struct FileQuery {
    path: String,
}

/// `GET /api/workspace/file`: the version a change to `path` is based on.
pub(crate) async fn workspace_file(
    State(state): State<Arc<WebState>>,
    Query(query): Query<FileQuery>,
) -> Result<Json<api::WorkspaceFile>, Error> {
    require_authoring(&state)?;
    let location = match locate(&query.path) {
        Ok(location) => location,
        Err(reason) => {
            return Err(Error::InvalidPath {
                path: query.path.clone(),
                reason,
            })
        }
    };
    let published = published_versions(&state).await?;
    let content = base_content(&state, &query.path, &location, &published).await?;
    Ok(Json(api::WorkspaceFile {
        path: query.path,
        content,
    }))
}

/// `GET /api/changes`.
pub(crate) async fn list_changes(State(state): State<Arc<WebState>>) -> Result<Response, Error> {
    require_authoring(&state)?;
    let keys = state
        .conversation_cache
        .list_keys(CHANGE_KEY_PREFIX)
        .await
        .map_err(|source| Error::Store { source })?;
    let mut loaded = Vec::with_capacity(keys.len());
    for key in keys {
        match load(&state, key.trim_start_matches(CHANGE_KEY_PREFIX)).await {
            Ok((change, _)) => loaded.push(change),
            Err(error) => warn!(key = %key, error = %error, "Skipping unreadable change"),
        }
    }
    let published = index_published(&loaded);
    let mut changes = Vec::with_capacity(loaded.len());
    for change in loaded {
        changes.push(api::ChangeSummary {
            deployment: deployment(&state, &change, &published).await,
            paths: change.files.iter().map(|file| file.path.clone()).collect(),
            id: change.id,
            target: change.target,
            title: change.title,
            status: change.status,
            proposed_by: change.proposed_by,
            created_at: change.created_at,
            updated_at: change.updated_at,
            file_count: change.files.len() as i64,
        });
    }
    changes.sort_by_key(|change| std::cmp::Reverse(change.created_at));
    Ok(Json(ChangeList { changes }).into_response())
}

/// Body of `GET /api/changes`.
#[derive(Serialize)]
struct ChangeList {
    changes: Vec<api::ChangeSummary>,
}

/// `POST /api/changes`.
pub(crate) async fn propose_change(
    State(state): State<Arc<WebState>>,
    caller: Option<Extension<Caller>>,
    Json(proposal): Json<api::ChangeProposal>,
) -> Result<Response, Error> {
    let authoring = require_authoring(&state)?;
    let caller = caller.map(|Extension(caller)| caller);
    let locations = check_proposal(&proposal)?;
    let target = target_for(
        authoring,
        proposal.files.iter().map(|file| file.path.as_str()),
    )?
    .name
    .clone();
    let published = published_versions(&state).await?;
    let mut files = Vec::with_capacity(proposal.files.len());
    for (file, location) in proposal.files.iter().zip(&locations) {
        let previous = base_content(&state, &file.path, location, &published).await?;
        files.push(api::ChangeFile {
            diff: String::new(),
            path: file.path.clone(),
            content: file.content.clone(),
            previous,
        });
    }
    let issues = validate_blocking(proposal.files).await?;
    let created_at = now_ms();
    let change = api::Change {
        id: uuid::Uuid::now_v7().simple().to_string(),
        target,
        title: proposal.title,
        description: proposal.description,
        status: api::ChangeStatus::Pending,
        proposed_by: caller_label(caller.as_ref()),
        created_at,
        updated_at: Some(created_at),
        decided_by: None,
        decided_at: None,
        issues,
        files,
        result: Default::default(),
        error: None,
        deployment: None,
        stalled: None,
    };
    store(&state, &change).await?;
    Ok((StatusCode::CREATED, Json(with_diffs(change).await?)).into_response())
}

/// `GET /api/changes/{id}`.
pub(crate) async fn get_change(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Result<Json<api::Change>, Error> {
    let authoring = require_authoring(&state)?;
    let (mut change, _) = load(&state, &id).await?;
    change.stalled = Some(match stored_target(authoring, &change) {
        Ok(target) => stalled(&change, publish_wait(&state, target)),
        Err(_) => false,
    });
    change.deployment = match published_versions(&state).await {
        Ok(published) => deployment(&state, &change, &published).await,
        Err(error) => {
            warn!(change = %change.id, error = %error, "Failed to check the deployment of a change");
            None
        }
    };
    Ok(Json(with_diffs(change).await?))
}

/// Marks the change edited, always later than its last edit so a review of the
/// previous version never matches.
fn touch(change: &mut api::Change) {
    let previous = match change.updated_at {
        Some(updated_at) => updated_at,
        None => change.created_at,
    };
    change.updated_at = Some(now_ms().max(previous + 1));
}

/// A pending change the caller may edit: its proposer, or anyone without `web.auth`.
async fn load_editable(
    state: &WebState,
    caller: Option<&Caller>,
    id: &str,
) -> Result<(api::Change, u64), Error> {
    let (change, revision) = load(state, id).await?;
    if change.status != api::ChangeStatus::Pending {
        return Err(Error::NotPending {
            status: change.status,
        });
    }
    match caller {
        Some(caller) if caller.label() != change.proposed_by => Err(Error::NotProposer),
        _ => Ok((change, revision)),
    }
}

/// `PATCH /api/changes/{id}`.
pub(crate) async fn edit_change(
    State(state): State<Arc<WebState>>,
    caller: Option<Extension<Caller>>,
    Path(id): Path<String>,
    Json(edit): Json<api::ChangeEdit>,
) -> Result<Json<api::Change>, Error> {
    require_authoring(&state)?;
    let caller = caller.map(|Extension(caller)| caller);
    let (mut change, revision) = load_editable(&state, caller.as_ref(), &id).await?;
    match edit.title {
        Some(title) if title.trim().is_empty() => return Err(Error::MissingTitle),
        Some(title) => change.title = title,
        None => {}
    }
    match edit.description {
        Some(description) if description.trim().is_empty() => change.description = None,
        Some(description) => change.description = Some(description),
        None => {}
    }
    touch(&mut change);
    transition(&state, &change, revision).await?;
    Ok(Json(with_diffs(change).await?))
}

/// `POST /api/changes/{id}/files`: a file already in the change gets the new
/// content and keeps the deployed content recorded when it was first added.
pub(crate) async fn add_change_files(
    State(state): State<Arc<WebState>>,
    caller: Option<Extension<Caller>>,
    Path(id): Path<String>,
    Json(body): Json<api::WorkspaceFiles>,
) -> Result<Json<api::Change>, Error> {
    let authoring = require_authoring(&state)?;
    let caller = caller.map(|Extension(caller)| caller);
    let (mut change, revision) = load_editable(&state, caller.as_ref(), &id).await?;
    let locations = check_paths(&body.files)?;
    let target = target_for(
        authoring,
        change
            .files
            .iter()
            .map(|file| file.path.as_str())
            .chain(body.files.iter().map(|file| file.path.as_str())),
    )?;
    if target.name != change.target {
        return Err(Error::TargetChanged {
            stored: change.target.clone(),
            current: target.name.clone(),
        });
    }
    let published = published_versions(&state).await?;
    for (file, location) in body.files.iter().zip(&locations) {
        match change
            .files
            .iter_mut()
            .find(|known| known.path == file.path)
        {
            Some(known) => known.content = file.content.clone(),
            None => {
                let previous = base_content(&state, &file.path, location, &published).await?;
                change.files.push(api::ChangeFile {
                    diff: String::new(),
                    path: file.path.clone(),
                    content: file.content.clone(),
                    previous,
                });
            }
        }
    }
    let files = change
        .files
        .iter()
        .map(|file| api::WorkspaceFile {
            path: file.path.clone(),
            content: file.content.clone(),
        })
        .collect();
    change.issues = validate_blocking(files).await?;
    touch(&mut change);
    transition(&state, &change, revision).await?;
    Ok(Json(with_diffs(change).await?))
}

/// File sent to the publish flow; `previous` is always present, `null` for a new file.
#[derive(Serialize)]
struct PublishFile<'a> {
    path: &'a str,
    content: Option<&'a str>,
    previous: Option<&'a str>,
}

#[derive(Serialize)]
struct PublishAuthor {
    name: String,
    email: String,
}

/// Event data the publish flow receives.
#[derive(Serialize)]
struct Publish<'a> {
    id: &'a str,
    target: &'a str,
    title: &'a str,
    author: PublishAuthor,
    files: Vec<PublishFile<'a>>,
}

/// Commit author for a decider: name and email claims, else the user id.
fn author(user: Option<&flowgen_core::auth::UserContext>) -> PublishAuthor {
    let claim = |name: &str| match user {
        Some(user) => match user.claims.get(name) {
            Some(serde_json::Value::String(value)) if !value.trim().is_empty() => {
                Some(value.trim().to_string())
            }
            _ => None,
        },
        None => None,
    };
    let id = match user {
        Some(user) => user.user_id.clone(),
        None => "flowgen".to_string(),
    };
    PublishAuthor {
        name: match claim("name") {
            Some(name) => name,
            None => id.clone(),
        },
        email: match claim("email") {
            Some(email) => email,
            None => id,
        },
    }
}

/// Query of `POST /api/changes/{id}/approve`.
#[derive(serde::Deserialize)]
pub(crate) struct Reviewed {
    /// The change's `updatedAt` as the approver reviewed it.
    #[serde(rename = "updatedAt")]
    updated_at: Option<i64>,
}

/// `POST /api/changes/{id}/approve`.
pub(crate) async fn approve_change(
    State(state): State<Arc<WebState>>,
    caller: Option<Extension<Caller>>,
    Path(id): Path<String>,
    Query(reviewed): Query<Reviewed>,
) -> Result<Json<api::Change>, Error> {
    let authoring = require_authoring(&state)?;
    let caller = caller.map(|Extension(caller)| caller);
    let (mut change, revision) = load(&state, &id).await?;
    let target = stored_target(authoring, &change)?;
    let user = decider(caller.as_ref(), authoring, target)?;
    if !can_publish(&change, publish_wait(&state, target)) {
        return Err(Error::NotPending {
            status: change.status,
        });
    }
    match reviewed.updated_at {
        Some(updated_at) if change.updated_at != Some(updated_at) => {
            return Err(Error::EditedSinceReview)
        }
        _ => {}
    }
    if !change.issues.is_empty() {
        return Err(Error::Invalid);
    }
    change.status = api::ChangeStatus::Publishing;
    change.decided_by = Some(caller_label(caller.as_ref()));
    change.decided_at = Some(now_ms());
    change.error = None;
    let revision = transition(&state, &change, revision).await?;

    let publishing = tokio::spawn(publish(
        Arc::clone(&state),
        target.clone(),
        user,
        change,
        revision,
    ));
    match publishing.await {
        Ok(result) => Ok(Json(with_diffs(result?).await?)),
        Err(source) => Err(Error::PublishAborted { source }),
    }
}

/// Whether a change may be published: pending, failed (a retry), or stuck in
/// `publishing` for longer than `timeout`, how long publishing may take.
/// Without a `timeout` a change in `publishing` may not be published again.
fn can_publish(change: &api::Change, timeout: Option<Duration>) -> bool {
    match change.status {
        api::ChangeStatus::Pending | api::ChangeStatus::Failed => true,
        api::ChangeStatus::Publishing => {
            match (
                change.decided_at,
                timeout.map(|t| i64::try_from(t.as_millis())),
            ) {
                (Some(decided_at), Some(Ok(timeout_ms))) => now_ms() - decided_at > timeout_ms,
                (Some(_), Some(Err(_)) | None) => false,
                (None, _) => true,
            }
        }
        api::ChangeStatus::Rejected | api::ChangeStatus::Published => false,
    }
}

/// Whether publishing ran past `timeout`, so approving again retries it.
fn stalled(change: &api::Change, timeout: Option<Duration>) -> bool {
    matches!(change.status, api::ChangeStatus::Publishing) && can_publish(change, timeout)
}

/// How long publishing for `target` may take: the `ack_timeout` of its publish
/// flow's `inproc_endpoint`. `None` when the flow does not run on this pod,
/// which then cannot tell whether another pod is still publishing.
fn publish_wait(state: &WebState, target: &crate::config::AuthoringTarget) -> Option<Duration> {
    state.inproc.ack_timeout(&target.publish_flow)
}

/// Runs the publish flow for `change` and returns its result.
async fn run_publish_flow(
    state: &WebState,
    target: &crate::config::AuthoringTarget,
    user: Option<&flowgen_core::auth::UserContext>,
    change: &api::Change,
) -> Result<Option<serde_json::Value>, Error> {
    let publish = Publish {
        id: &change.id,
        target: &target.name,
        title: &change.title,
        author: author(user),
        files: change
            .files
            .iter()
            .map(|file| PublishFile {
                path: &file.path,
                content: file.content.as_deref(),
                previous: file.previous.as_deref(),
            })
            .collect(),
    };
    let data = serde_json::to_value(&publish).map_err(|source| Error::Serialize { source })?;
    let mut meta = serde_json::Map::new();
    let mut session = None;
    if let Some(user) = user {
        let auth = serde_json::to_value(user).map_err(|source| Error::Serialize { source })?;
        meta.insert(flowgen_core::auth::AUTH.to_string(), auth);
        session = Some(Arc::new(flowgen_core::auth::UserSession {
            user: user.clone(),
            token: None,
        }));
    }
    match state
        .inproc
        .call(None, &target.publish_flow, data, meta, session)
        .await
    {
        Ok(result) => Ok(result),
        Err(flowgen_core::task::inproc::registry::CallError::Timeout { timeout }) => {
            Err(Error::PublishTimeout { timeout })
        }
        Err(error) => Err(Error::Call(error)),
    }
}

/// Runs the publish flow and records its outcome over the record written at
/// `revision`, leaving a record rewritten since then alone. Spawned, so a
/// client that disconnects does not leave the change in `publishing`.
async fn publish(
    state: Arc<WebState>,
    target: crate::config::AuthoringTarget,
    user: Option<flowgen_core::auth::UserContext>,
    mut change: api::Change,
    revision: u64,
) -> Result<api::Change, Error> {
    match run_publish_flow(&state, &target, user.as_ref(), &change).await {
        Ok(result) => {
            change.status = api::ChangeStatus::Published;
            change.result = match result {
                Some(serde_json::Value::Object(fields)) => fields,
                _ => Default::default(),
            };
        }
        Err(error) => {
            warn!(change = %change.id, error = %error, "Publishing a change failed");
            change.status = api::ChangeStatus::Failed;
            change.error = Some(match error {
                Error::Call(flowgen_core::task::inproc::registry::CallError::Failed { reason }) => {
                    reason
                }
                error => error.to_string(),
            });
        }
    }
    let retry = flowgen_core::retry::RetryConfig::merge(&state.app_config.retry, &None);
    let recorded = tokio_retry::Retry::spawn(retry.strategy(), || async {
        match transition(&state, &change, revision).await {
            Ok(_) => Ok(()),
            Err(error @ Error::Store { .. }) => {
                warn!(change = %change.id, error = %error, "Failed to record the publish outcome");
                Err(tokio_retry::RetryError::transient(error))
            }
            Err(error) => Err(tokio_retry::RetryError::permanent(error)),
        }
    })
    .await;
    match recorded {
        Ok(()) => Ok(change),
        Err(Error::Concurrent) => {
            warn!(
                change = %change.id,
                "The change was rewritten while publishing, keeping the newer record"
            );
            Ok(change)
        }
        Err(error) => Err(error),
    }
}

/// `POST /api/changes/{id}/reject`.
pub(crate) async fn reject_change(
    State(state): State<Arc<WebState>>,
    caller: Option<Extension<Caller>>,
    Path(id): Path<String>,
) -> Result<Json<api::Change>, Error> {
    let authoring = require_authoring(&state)?;
    let caller = caller.map(|Extension(caller)| caller);
    let (mut change, revision) = load(&state, &id).await?;
    decider(
        caller.as_ref(),
        authoring,
        stored_target(authoring, &change)?,
    )?;
    let rejectable = matches!(
        change.status,
        api::ChangeStatus::Pending | api::ChangeStatus::Failed
    );
    if !rejectable {
        return Err(Error::NotPending {
            status: change.status,
        });
    }
    change.status = api::ChangeStatus::Rejected;
    change.decided_by = Some(caller_label(caller.as_ref()));
    change.decided_at = Some(now_ms());
    transition(&state, &change, revision).await?;
    Ok(Json(with_diffs(change).await?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_paths_are_located_under_flows_and_resources_only() {
        assert_eq!(
            locate("flows/a/b.yaml"),
            Ok(Location::Flow { file: "a/b.yaml" })
        );
        assert_eq!(
            locate("resources/scripts/s.rhai"),
            Ok(Location::Resource {
                key: "scripts/s.rhai"
            })
        );
        assert_eq!(
            locate("flows/team-a/sync_v1.2.yaml"),
            Ok(Location::Flow {
                file: "team-a/sync_v1.2.yaml"
            })
        );
        for (path, reason) in [
            ("other/a", PathError::Root),
            ("flows", PathError::Root),
            ("/flows/a", PathError::Root),
            ("flows/", PathError::NoFileName),
            ("resources/a/b/", PathError::NoFileName),
            ("flows//a", PathError::EmptySegment),
            ("flows/../a", PathError::Relative),
            ("flows/a\\b", PathError::Characters),
            ("flows/a b.yaml", PathError::Characters),
            ("resources/a:b", PathError::Characters),
            ("resources/é.txt", PathError::Characters),
            ("resources/a*", PathError::Characters),
        ] {
            assert_eq!(locate(path), Err(reason), "{path}");
        }
        assert_eq!(flow_identity("a/b.yaml"), "a/b");
        assert_eq!(flow_identity("a/b.txt"), "a/b.txt");
    }

    #[test]
    fn files_are_validated_by_kind_and_deletions_are_skipped() {
        let file = |path: &str, content: Option<&str>| api::WorkspaceFile {
            path: path.to_string(),
            content: content.map(str::to_string),
        };
        let issues = validate_files(&[
            file("flows/a.yaml", Some("flow: [")),
            file("resources/s.rhai", Some("let x = ;")),
            file("resources/q.sql", Some("select")),
            file("flows/gone.yaml", None),
            file("elsewhere.txt", Some("x")),
        ]);
        let paths: Vec<_> = issues.iter().map(|i| i.path.as_str()).collect();
        assert_eq!(
            paths,
            vec!["flows/a.yaml", "resources/s.rhai", "elsewhere.txt"]
        );
    }

    #[tokio::test]
    async fn server_errors_answer_with_a_fixed_message_and_client_errors_with_the_reason() {
        let body = |response: Response| async move {
            let status = response.status().as_u16();
            let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap();
            (status, String::from_utf8(bytes.to_vec()).unwrap())
        };
        let store = Error::Store {
            source: flowgen_core::cache::Error::GetFailed("connection to 10.0.0.1 refused".into()),
        };
        let not_found = Error::NotFound {
            id: "abc".to_string(),
        };

        assert_eq!(
            body(store.into_response()).await,
            (503, "Change store unavailable".to_string())
        );
        assert_eq!(
            body(not_found.into_response()).await,
            (404, "No change with id 'abc'".to_string())
        );
    }

    #[test]
    fn the_diff_marks_new_and_deleted_files_against_dev_null() {
        let added = unified_diff("flows/a.yaml", None, Some("a\n"));
        assert!(
            added.starts_with("--- /dev/null\n+++ b/flows/a.yaml\n"),
            "{added}"
        );
        assert!(added.contains("+a\n"));

        let changed = unified_diff("flows/a.yaml", Some("a\n"), Some("b\n"));
        assert!(changed.contains("-a\n+b\n"), "{changed}");

        let deleted = unified_diff("flows/a.yaml", Some("a\n"), None);
        assert!(deleted.contains("+++ /dev/null"), "{deleted}");
    }

    #[test]
    fn failed_and_stuck_changes_can_be_published_again() {
        let change = |status: api::ChangeStatus, decided_at: Option<i64>| api::Change {
            id: "a".to_string(),
            target: "workspace".to_string(),
            title: "t".to_string(),
            description: None,
            status,
            proposed_by: "p".to_string(),
            created_at: 0,
            updated_at: None,
            decided_by: None,
            decided_at,
            files: Vec::new(),
            issues: Vec::new(),
            result: Default::default(),
            error: None,
            deployment: None,
            stalled: None,
        };
        let timeout = Some(std::time::Duration::from_secs(60));
        assert!(can_publish(
            &change(api::ChangeStatus::Pending, None),
            timeout
        ));
        assert!(can_publish(
            &change(api::ChangeStatus::Failed, None),
            timeout
        ));
        assert!(!stalled(&change(api::ChangeStatus::Failed, None), timeout));
        assert!(!can_publish(
            &change(api::ChangeStatus::Publishing, Some(now_ms())),
            timeout
        ));
        assert!(!stalled(
            &change(api::ChangeStatus::Publishing, Some(now_ms())),
            timeout
        ));
        assert!(can_publish(
            &change(api::ChangeStatus::Publishing, Some(now_ms() - 61_000)),
            timeout
        ));
        assert!(stalled(
            &change(api::ChangeStatus::Publishing, Some(now_ms() - 61_000)),
            timeout
        ));
        assert!(!stalled(
            &change(api::ChangeStatus::Publishing, Some(now_ms() - 61_000)),
            None
        ));
        assert!(!can_publish(
            &change(api::ChangeStatus::Published, None),
            timeout
        ));
        assert!(!can_publish(
            &change(api::ChangeStatus::Rejected, None),
            timeout
        ));
        assert!(!valid_id("a>b"));
        assert!(valid_id("0199ab"));
    }

    #[test]
    fn published_versions_apply_from_the_newest_one_already_running() {
        let version = |id: &str, previous: Option<&str>, content: Option<&str>| {
            (
                id.to_string(),
                api::ChangeFile {
                    path: "resources/a.rhai".to_string(),
                    content: content.map(str::to_string),
                    previous: previous.map(str::to_string),
                    diff: String::new(),
                },
            )
        };
        let chain = [
            version("1", Some("s"), Some("t")),
            version("2", Some("t"), Some("u")),
        ];
        assert_eq!(
            apply_published(Some("s\n"), &chain),
            (Some("u"), vec!["1", "2"])
        );
        assert_eq!(apply_published(Some("t"), &chain), (Some("u"), vec!["2"]));
        assert_eq!(apply_published(Some("u"), &chain), (Some("u"), vec![]));
        assert_eq!(
            apply_published(Some("by hand"), &chain),
            (Some("by hand"), vec![])
        );

        let toggled = [
            version("1", Some("s"), Some("t")),
            version("2", Some("t"), Some("s")),
            version("3", Some("s"), Some("t")),
            version("4", Some("t"), Some("s")),
        ];
        assert_eq!(apply_published(Some("t"), &toggled), (Some("s"), vec!["4"]));
        assert_eq!(apply_published(Some("s"), &toggled), (Some("s"), vec![]));

        let lifecycle = [version("1", None, Some("a")), version("2", Some("a"), None)];
        assert_eq!(
            apply_published(None, &lifecycle[..1]),
            (Some("a"), vec!["1"])
        );
        assert_eq!(apply_published(Some("a"), &lifecycle), (None, vec!["2"]));
        assert_eq!(apply_published(None, &lifecycle), (None, vec![]));
    }

    #[test]
    fn a_change_belongs_to_exactly_one_target() {
        let target = |name: &str, paths: &[&str]| crate::config::AuthoringTarget {
            name: name.to_string(),
            paths: paths.iter().map(|p| p.to_string()).collect(),
            publish_flow: "system/publish_workspace".to_string(),
            approver_groups: Vec::new(),
            track_deployment: true,
        };
        let authoring = crate::config::AuthoringOptions {
            enabled: true,
            targets: vec![
                target("platform", &["flows/platform/", "resources/platform/"]),
                target("user", &["flows/user/", "resources/user/"]),
            ],
            groups_claim: "groups".to_string(),
        };

        let user = target_for(&authoring, ["flows/user/a.yaml", "resources/user/s.rhai"]);
        assert_eq!(user.unwrap().name, "user");
        assert!(matches!(
            target_for(&authoring, ["flows/user/a.yaml", "flows/platform/b.yaml"]),
            Err(Error::MixedTargets { .. })
        ));
        assert!(matches!(
            target_for(&authoring, ["flows/system/publish.yaml"]),
            Err(Error::NoTarget { path }) if path == "flows/system/publish.yaml"
        ));
        assert!(matches!(target_for(&authoring, []), Err(Error::NoFiles)));
    }

    #[test]
    fn a_stored_change_is_decided_only_under_the_target_that_owns_its_files_now() {
        let target = |name: &str, paths: &[&str]| crate::config::AuthoringTarget {
            name: name.to_string(),
            paths: paths.iter().map(|p| p.to_string()).collect(),
            publish_flow: "system/publish_workspace".to_string(),
            approver_groups: Vec::new(),
            track_deployment: true,
        };
        let authoring = crate::config::AuthoringOptions {
            enabled: true,
            targets: vec![
                target("workspace", &["flows/"]),
                target("platform", &["flows/platform/"]),
            ],
            groups_claim: "groups".to_string(),
        };
        let change = |target: &str, path: &str| api::Change {
            id: "a".to_string(),
            target: target.to_string(),
            title: "t".to_string(),
            description: None,
            status: api::ChangeStatus::Pending,
            proposed_by: "p".to_string(),
            created_at: 0,
            updated_at: None,
            decided_by: None,
            decided_at: None,
            files: vec![api::ChangeFile {
                path: path.to_string(),
                content: None,
                previous: None,
                diff: String::new(),
            }],
            issues: Vec::new(),
            result: Default::default(),
            error: None,
            deployment: None,
            stalled: None,
        };

        assert_eq!(
            stored_target(&authoring, &change("platform", "flows/platform/a.yaml"))
                .unwrap()
                .name,
            "platform"
        );
        assert!(matches!(
            stored_target(&authoring, &change("workspace", "flows/platform/a.yaml")),
            Err(Error::TargetChanged { stored, current }) if stored == "workspace" && current == "platform"
        ));
        assert!(matches!(
            stored_target(&authoring, &change("user", "resources/a.rhai")),
            Err(Error::TargetGone { target }) if target == "user"
        ));
    }

    #[test]
    fn machine_keys_cannot_decide_and_the_author_comes_from_claims() {
        let decide = |caller: Option<&Caller>, groups: &[&str], claim: &str| {
            let options = crate::config::AuthoringOptions {
                enabled: true,
                targets: vec![crate::config::AuthoringTarget {
                    name: "user".to_string(),
                    paths: vec!["flows/user/".to_string()],
                    publish_flow: "p".to_string(),
                    approver_groups: groups.iter().map(|g| g.to_string()).collect(),
                    track_deployment: true,
                }],
                groups_claim: claim.to_string(),
            };
            decider(caller, &options, &options.targets[0])
        };
        let key = Caller::Key("agent".to_string());
        assert!(matches!(
            decide(Some(&key), &[], "groups"),
            Err(Error::UserRequired)
        ));
        assert!(matches!(decide(None, &["a"], "groups"), Ok(None)));

        let user = flowgen_core::auth::UserContext {
            user_id: "u1".to_string(),
            claims: [
                ("name".to_string(), serde_json::json!("Jane Doe")),
                ("email".to_string(), serde_json::json!("jane@example.com")),
                (
                    "groups".to_string(),
                    serde_json::json!(["flowgen-approvers", "staff"]),
                ),
                ("role".to_string(), serde_json::json!("admin")),
            ]
            .into_iter()
            .collect(),
        };
        let signed_in = Caller::User(user.clone());
        let signed_in = Some(&signed_in);
        assert!(decide(signed_in, &[], "groups").is_ok());
        assert!(decide(signed_in, &["flowgen-approvers"], "groups").is_ok());
        assert!(decide(signed_in, &["admin"], "role").is_ok());
        assert!(matches!(
            decide(signed_in, &["finance"], "groups"),
            Err(Error::NotApprover { .. })
        ));
        assert!(matches!(
            decide(signed_in, &["flowgen-approvers"], "missing"),
            Err(Error::NotApprover { .. })
        ));
        let from_claims = author(Some(&user));
        assert_eq!(
            (from_claims.name.as_str(), from_claims.email.as_str()),
            ("Jane Doe", "jane@example.com")
        );
        let bare = author(Some(&flowgen_core::auth::UserContext {
            user_id: "u2".to_string(),
            claims: Default::default(),
        }));
        assert_eq!((bare.name.as_str(), bare.email.as_str()), ("u2", "u2"));
    }
}
