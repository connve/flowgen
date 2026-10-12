//! Commits file changes on top of a remote branch and pushes them over smart
//! HTTP (`git-receive-pack`), without a git binary.
//!
//! The commit is built with gix in a temporary bare shallow fetch; the pack and the
//! receive-pack exchange are written here because gix has no push support.

use crate::remote::{CloneError, Credentials};
use gix::objs::tree::EntryKind;
use gix::odb::pack::data::entry::Header as PackEntryHeader;
use gix::protocol::transport::packetline;
use gix::ObjectId;
use secrecy::ExposeSecret;
use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::sync::atomic::AtomicBool;

/// Failure to commit or push.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Invalid file path '{path}': must be relative and name a file, without '.', '..' or '.git' segments")]
    InvalidPath { path: String },
    #[error("'{path}' changed in the repository. Please propose the change again.")]
    Conflict { path: String },
    #[error("Nothing to push to an empty repository")]
    NothingToPush,
    #[error("Branch '{branch}' does not exist and the repository is not empty")]
    BranchMissing { branch: String },
    #[error(transparent)]
    Clone(#[from] CloneError),
    #[error(transparent)]
    Url(#[from] crate::remote::UrlError),
    #[error(transparent)]
    Blocking(#[from] crate::remote::BlockingError),
    #[error("Failed to initialize an empty repository: {source}")]
    InitRepository {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to read from the fetched repository: {source}")]
    ReadRepository {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to write the commit: {source}")]
    WriteRepository {
        #[source]
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    #[error("Failed to sign the commit: {source}")]
    Sign {
        #[source]
        source: ssh_key::Error,
    },
    #[error("Failed to encode the pack: {source}")]
    EncodePack {
        #[source]
        source: std::io::Error,
    },
    #[error("A pack holds at most {} objects, the commit needs {count}", u32::MAX)]
    TooManyObjects {
        count: usize,
        #[source]
        source: std::num::TryFromIntError,
    },
    #[error("Failed to encode the receive-pack request: {source}")]
    EncodeRequest {
        #[source]
        source: std::io::Error,
    },
    #[error("Request to {url} failed: {source}")]
    Http {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Git server at {url} answered {status}")]
    Status {
        url: String,
        status: reqwest::StatusCode,
    },
    #[error("The git server sent malformed pkt-line framing: {source}")]
    InvalidPktLine {
        #[source]
        source: packetline::decode::Error,
    },
    #[error("The git server response ends inside a pkt-line")]
    TruncatedPktLine,
    #[error("The git server sent a malformed ref advertisement line: {line}")]
    InvalidAdvertisement { line: String },
    #[error("The git server advertised an invalid object id '{id}': {source}")]
    InvalidAdvertisedId {
        id: String,
        #[source]
        source: gix::hash::decode::Error,
    },
    #[error("The ref update command does not fit in one pkt-line: {source}")]
    CommandTooLong {
        #[source]
        source: std::io::Error,
    },
    #[error("The git server did not report the update of '{branch}'")]
    MissingUpdateReport { branch: String },
    #[error("The git server failed to unpack the pushed objects: {status}")]
    UnpackFailed { status: String },
    #[error("The git server rejected the update of '{branch}': {message}")]
    RefRejected { branch: String, message: String },
    #[error("The git server answered with an error: {message}")]
    ServerError { message: String },
    #[error("Branch '{branch}' moved while the commit was being built")]
    BranchMoved { branch: String },
    #[error("Failed to create a temporary directory: {source}")]
    TempDir {
        #[source]
        source: std::io::Error,
    },
}

fn read_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::ReadRepository {
        source: Box::new(source),
    }
}

fn write_error(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::WriteRepository {
        source: Box::new(source),
    }
}

/// What a file must contain on the branch for a change to apply.
#[derive(Debug, Clone, PartialEq)]
pub enum Expected {
    /// No check: the change overwrites whatever is there.
    Any,
    /// The file must not exist.
    Missing,
    /// The file must have exactly this content.
    Content(Vec<u8>),
}

/// One file to write or delete, relative to the push's `path`.
#[derive(Debug, Clone, PartialEq)]
pub struct FileChange {
    /// Path relative to the push's `path`, with `/` separators.
    pub path: String,
    /// New content; `None` deletes the file.
    pub content: Option<Vec<u8>>,
    /// What the file must hold on the branch for the change to apply.
    pub expected: Expected,
}

/// Commit author or committer.
#[derive(Debug, Clone, PartialEq)]
pub struct Author {
    /// Name recorded on the commit.
    pub name: String,
    /// Email recorded on the commit.
    pub email: String,
}

/// A file under the push's `path` at the pushed commit.
#[derive(Debug, Clone, PartialEq)]
pub struct CommittedFile {
    /// Path relative to the push's `path`.
    pub path: String,
    /// File content at the commit.
    pub content: Vec<u8>,
}

/// Result of a push.
#[derive(Debug, Clone, PartialEq)]
pub struct Pushed {
    /// The branch tip after the push.
    pub commit: String,
    /// False when the changes left the tree as it was and nothing was pushed.
    pub changed: bool,
    /// Every file under `path` at `commit`.
    pub files: Vec<CommittedFile>,
}

/// Where to push.
#[derive(Debug, Clone)]
pub struct Push {
    /// HTTPS repository URL.
    pub repository_url: String,
    /// Branch the commit lands on.
    pub branch: String,
    /// Directory within the repository the changes are relative to.
    pub path: Option<String>,
    /// Token sent with every request to the git server.
    pub credentials: Option<Credentials>,
    /// Committer; `None` commits as the author.
    pub committer: Option<Author>,
    /// Key that signs the commit.
    pub signing_key: Option<std::sync::Arc<ssh_key::PrivateKey>>,
    /// Client for the receive-pack exchange, with the caller's timeouts.
    pub http: reqwest::Client,
    /// Time budget for fetching the branch and building the commit; `None`
    /// waits indefinitely.
    pub timeout: Option<std::time::Duration>,
}

/// What the server advertises for the push.
struct Advertisement {
    tip: Option<String>,
    has_refs: bool,
    capabilities: Vec<String>,
}

/// Refs and capabilities of a receive-pack advertisement.
struct AdvertisedRefs {
    refs: HashMap<String, String>,
    capabilities: Vec<String>,
}

/// Capabilities requested with the ref update when the server offers them:
/// messages on a side band, without progress output.
const SIDE_BAND: &str = "side-band-64k";
const QUIET: &str = "quiet";

/// A commit built locally, ready to send.
struct Built {
    commit: String,
    parent: Option<String>,
    pack: Option<Vec<u8>>,
    files: Vec<CommittedFile>,
}

impl Push {
    /// Commits `changes` on top of the branch and pushes the commit. When the
    /// branch moved meanwhile the push fails with [`Error::BranchMoved`], for
    /// the caller to retry on the new tip.
    pub async fn push(
        &self,
        changes: Vec<FileChange>,
        author: Author,
        message: String,
    ) -> Result<Pushed, Error> {
        crate::remote::check_url(&self.repository_url, self.credentials.as_ref())?;
        let prefix = match &self.path {
            Some(path) => normalize_path(path)?,
            None => Vec::new(),
        };
        let mut full_paths = Vec::with_capacity(changes.len());
        for change in changes {
            let segments = normalize_path(&change.path)?;
            if segments.is_empty() {
                return Err(Error::InvalidPath { path: change.path });
            }
            let mut full = prefix.clone();
            full.extend(segments);
            full_paths.push((full.join("/"), change));
        }

        let advertised = self.advertisement().await?;
        if advertised.tip.is_none() && advertised.has_refs {
            return Err(Error::BranchMissing {
                branch: self.branch.clone(),
            });
        }
        let tip = advertised.tip;
        let capabilities = advertised.capabilities;
        let built = {
            let push = self.clone();
            let prefix = prefix.join("/");
            crate::remote::run_blocking(self.timeout, move |interrupt| {
                push.build(
                    tip.is_some(),
                    &full_paths,
                    &author,
                    &message,
                    &prefix,
                    interrupt,
                )
            })
            .await??
        };
        let pack = match built.pack {
            Some(pack) => pack,
            None => {
                return Ok(Pushed {
                    commit: built.commit,
                    changed: false,
                    files: built.files,
                })
            }
        };
        match self
            .send(built.parent.as_deref(), &built.commit, pack, &capabilities)
            .await
        {
            Ok(()) => Ok(Pushed {
                commit: built.commit,
                changed: true,
                files: built.files,
            }),
            Err(error @ Error::RefRejected { .. }) => {
                match self.advertisement().await?.tip == built.parent {
                    true => Err(error),
                    false => Err(Error::BranchMoved {
                        branch: self.branch.clone(),
                    }),
                }
            }
            Err(error) => Err(error),
        }
    }

    fn url(&self, suffix: &str) -> String {
        format!("{}/{suffix}", self.repository_url.trim_end_matches('/'))
    }

    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let request = request.header(
            reqwest::header::USER_AGENT,
            format!("git/{}", gix::env::agent()),
        );
        match &self.credentials {
            Some(credentials) => request.basic_auth(
                credentials.username(),
                Some(credentials.token.expose_secret()),
            ),
            None => request,
        }
    }

    /// The branch tip the server advertises, and whether it has any refs at all.
    async fn advertisement(&self) -> Result<Advertisement, Error> {
        let url = self.url("info/refs?service=git-receive-pack");
        let body = self
            .fetch(self.authorized(self.http.get(&url)), &url)
            .await?;
        let advertised = advertised_refs(&body)?;
        Ok(Advertisement {
            tip: advertised
                .refs
                .get(&format!("refs/heads/{}", self.branch))
                .cloned(),
            has_refs: !advertised.refs.is_empty(),
            capabilities: advertised.capabilities,
        })
    }

    async fn send(
        &self,
        parent: Option<&str>,
        commit: &str,
        pack: Vec<u8>,
        capabilities: &[String],
    ) -> Result<(), Error> {
        let parent = match parent {
            Some(parent) => parent.to_string(),
            None => ObjectId::null(gix::hash::Kind::Sha1).to_string(),
        };
        let offered = |capability: &str| capabilities.iter().any(|c| c == capability);
        let side_band = offered(SIDE_BAND);
        let mut requested = vec!["report-status"];
        if side_band {
            requested.push(SIDE_BAND);
        }
        if offered(QUIET) {
            requested.push(QUIET);
        }
        let command = format!(
            "{parent} {commit} refs/heads/{}\0{}\n",
            self.branch,
            requested.join(" ")
        );
        let mut body = Vec::new();
        packetline::blocking_io::encode::data_to_write(command.as_bytes(), &mut body)
            .map_err(|source| Error::CommandTooLong { source })?;
        packetline::blocking_io::encode::flush_to_write(&mut body)
            .map_err(|source| Error::EncodeRequest { source })?;
        body.extend(pack);

        let url = self.url("git-receive-pack");
        let request = self
            .authorized(self.http.post(&url))
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-git-receive-pack-request",
            )
            .header(
                reqwest::header::ACCEPT,
                "application/x-git-receive-pack-result",
            )
            .body(body);
        let response = self.fetch(request, &url).await?;
        match side_band {
            true => {
                let (report, messages) = demultiplex(&response)?;
                check_report(&report, &self.branch).map_err(|error| explain(error, &messages))
            }
            false => check_report(&response, &self.branch),
        }
    }

    async fn fetch(&self, request: reqwest::RequestBuilder, url: &str) -> Result<Vec<u8>, Error> {
        let http_error = |source| Error::Http {
            url: url.to_string(),
            source,
        };
        let response = request.send().await.map_err(http_error)?;
        let status = response.status();
        if !status.is_success() {
            return Err(Error::Status {
                url: url.to_string(),
                status,
            });
        }
        Ok(response.bytes().await.map_err(http_error)?.to_vec())
    }

    /// Builds the commit in a temporary bare repository. Runs on a blocking
    /// thread and stops fetching once `interrupt` is set.
    fn build(
        &self,
        branch_exists: bool,
        changes: &[(String, FileChange)],
        author: &Author,
        message: &str,
        prefix: &str,
        interrupt: &AtomicBool,
    ) -> Result<Built, Error> {
        let dir = tempfile::TempDir::new().map_err(|source| Error::TempDir { source })?;
        let repo = match branch_exists {
            true => crate::remote::shallow_fetch_bare(
                &self.repository_url,
                &self.branch,
                dir.path(),
                self.credentials.as_ref(),
                interrupt,
            )?,
            false => gix::init_bare(dir.path()).map_err(|e| Error::InitRepository {
                source: Box::new(e),
            })?,
        };

        let parent = match branch_exists {
            true => Some(repo.head_id().map_err(read_error)?.detach()),
            false => None,
        };
        let base_tree = match parent {
            Some(parent) => repo
                .find_commit(parent)
                .map_err(read_error)?
                .tree_id()
                .map_err(read_error)?
                .detach(),
            None => ObjectId::empty_tree(repo.object_hash()),
        };

        let mut kinds = Vec::with_capacity(changes.len());
        for (path, change) in changes {
            let current = entry_at(&repo, base_tree, path)?;
            check_expected(path, change, &current)?;
            kinds.push(match current {
                Some(Current::File { kind, .. }) => kind,
                _ => EntryKind::Blob,
            });
        }

        let mut editor = repo.edit_tree(base_tree).map_err(read_error)?;
        for ((path, change), kind) in changes.iter().zip(kinds) {
            match &change.content {
                Some(content) => {
                    let blob = repo.write_blob(content).map_err(write_error)?;
                    editor
                        .upsert(path.as_str(), kind, blob)
                        .map_err(write_error)?;
                }
                None => {
                    editor.remove(path.as_str()).map_err(write_error)?;
                }
            }
        }
        let tree = editor.write().map_err(write_error)?.detach();

        if tree == base_tree {
            let commit = match parent {
                Some(parent) => parent.to_string(),
                None => return Err(Error::NothingToPush),
            };
            return Ok(Built {
                commit,
                parent: parent.map(|p| p.to_string()),
                pack: None,
                files: files_under(&repo, tree, prefix)?,
            });
        }

        let commit = write_commit(
            &repo,
            tree,
            parent,
            author,
            self.committer.as_ref(),
            message,
            self.signing_key.as_deref(),
        )?;

        let mut objects = vec![object_entry(&repo, commit)?];
        let mut known = reachable(&repo, base_tree)?;
        new_objects(&repo, tree, &mut known, &mut objects)?;

        Ok(Built {
            commit: commit.to_string(),
            parent: parent.map(|p| p.to_string()),
            pack: Some(write_pack(&objects)?),
            files: files_under(&repo, tree, prefix)?,
        })
    }
}

/// Splits a relative path into its segments, rejecting anything that could
/// leave the directory it is relative to.
fn normalize_path(path: &str) -> Result<Vec<String>, Error> {
    let segments: Vec<String> = path
        .split('/')
        .filter(|segment| !segment.is_empty())
        .map(str::to_string)
        .collect();
    let options = gix::validate::path::component::Options {
        protect_windows: false,
        protect_hfs: true,
        protect_ntfs: true,
    };
    let invalid = path.starts_with('/')
        || path.contains(['\\', '\0'])
        || segments.iter().any(|segment| {
            gix::validate::path::component(gix::bstr::BStr::new(segment), None, options).is_err()
        });
    match invalid {
        true => Err(Error::InvalidPath {
            path: path.to_string(),
        }),
        false => Ok(segments),
    }
}

/// What a path holds on the branch.
enum Current {
    File {
        kind: EntryKind,
        content: Vec<u8>,
    },
    /// A directory, symlink, or submodule.
    Other,
}

/// What `path` holds in `tree`; an ancestor that is not a directory makes
/// it [`Current::Other`], since writing below it would replace it.
fn entry_at(repo: &gix::Repository, tree: ObjectId, path: &str) -> Result<Option<Current>, Error> {
    let mut tree = read_tree(repo, tree)?;
    let mut segments = path.split('/').peekable();
    while let Some(segment) = segments.next() {
        let (kind, id) = match find_entry(&tree, segment)? {
            Some(entry) => entry,
            None => return Ok(None),
        };
        match (kind, segments.peek().is_some()) {
            (EntryKind::Tree, true) => tree = read_tree(repo, id)?,
            (EntryKind::Blob | EntryKind::BlobExecutable, false) => {
                let content = read_object(repo, id)?.detach().data;
                return Ok(Some(Current::File { kind, content }));
            }
            _ => return Ok(Some(Current::Other)),
        }
    }
    Ok(None)
}

/// The kind and id of the entry named `name` directly in `tree`.
fn find_entry(tree: &gix::Tree<'_>, name: &str) -> Result<Option<(EntryKind, ObjectId)>, Error> {
    for entry in tree.iter() {
        let entry = entry.map_err(read_error)?;
        if entry.filename() == name {
            return Ok(Some((entry.mode().kind(), entry.object_id())));
        }
    }
    Ok(None)
}

fn read_tree(repo: &gix::Repository, id: ObjectId) -> Result<gix::Tree<'_>, Error> {
    repo.find_tree(id).map_err(read_error)
}

fn read_object(repo: &gix::Repository, id: ObjectId) -> Result<gix::Object<'_>, Error> {
    repo.find_object(id).map_err(read_error)
}

/// A change applies when the branch holds what it was prepared against, or
/// already holds its result (so a push retried after it landed still succeeds).
fn check_expected(path: &str, change: &FileChange, current: &Option<Current>) -> Result<(), Error> {
    let current_content = match current {
        Some(Current::File { content, .. }) => Some(content),
        _ => None,
    };
    let already_applied = match (&change.content, current) {
        (Some(target), Some(Current::File { content, .. })) => target == content,
        (None, None) => true,
        _ => false,
    };
    let holds = already_applied
        || match &change.expected {
            Expected::Any => !matches!(current, Some(Current::Other)),
            Expected::Missing => current.is_none(),
            Expected::Content(content) => current_content == Some(content),
        };
    match holds {
        true => Ok(()),
        false => Err(Error::Conflict {
            path: path.to_string(),
        }),
    }
}

/// Writes the commit, signed with `signing_key` the way git signs with an SSH
/// key: an SSHSIG over the unsigned commit in the `gpgsig` header.
fn write_commit(
    repo: &gix::Repository,
    tree: ObjectId,
    parent: Option<ObjectId>,
    author: &Author,
    committer: Option<&Author>,
    message: &str,
    signing_key: Option<&ssh_key::PrivateKey>,
) -> Result<ObjectId, Error> {
    let time = gix::date::Time::now_utc();
    let signature = |identity: &Author| gix::actor::Signature {
        name: identity.name.as_str().into(),
        email: identity.email.as_str().into(),
        time,
    };
    let mut commit = gix::objs::Commit {
        tree,
        parents: parent.into_iter().collect(),
        author: signature(author),
        committer: signature(match committer {
            Some(committer) => committer,
            None => author,
        }),
        encoding: None,
        message: message.into(),
        extra_headers: Vec::new(),
    };
    if let Some(key) = signing_key {
        let mut unsigned = Vec::new();
        gix::objs::WriteTo::write_to(&commit, &mut unsigned).map_err(write_error)?;
        let sign_error = |source| Error::Sign { source };
        let pem = key
            .sign("git", ssh_key::HashAlg::Sha512, &unsigned)
            .map_err(sign_error)?
            .to_pem(ssh_key::LineEnding::LF)
            .map_err(sign_error)?;
        commit
            .extra_headers
            .push((gix::objs::commit::SIGNATURE_FIELD_NAME.into(), pem.into()));
    }
    Ok(repo.write_object(&commit).map_err(write_error)?.detach())
}

fn object_entry(repo: &gix::Repository, id: ObjectId) -> Result<(PackEntryHeader, Vec<u8>), Error> {
    let object = read_object(repo, id)?;
    let header = match object.kind {
        gix::object::Kind::Commit => PackEntryHeader::Commit,
        gix::object::Kind::Tree => PackEntryHeader::Tree,
        gix::object::Kind::Blob => PackEntryHeader::Blob,
        gix::object::Kind::Tag => PackEntryHeader::Tag,
    };
    Ok((header, object.detach().data))
}

/// Ids of every tree and blob reachable from `tree`.
fn reachable(repo: &gix::Repository, tree: ObjectId) -> Result<HashSet<ObjectId>, Error> {
    let mut seen = HashSet::new();
    let mut pending = vec![tree];
    while let Some(id) = pending.pop() {
        if !seen.insert(id) {
            continue;
        }
        let tree = read_tree(repo, id)?;
        for entry in tree.iter() {
            let entry = entry.map_err(read_error)?;
            match entry.mode().is_tree() {
                true => pending.push(entry.oid().to_owned()),
                false => {
                    seen.insert(entry.oid().to_owned());
                }
            }
        }
    }
    Ok(seen)
}

/// Appends every object under `tree` the server does not have yet, each once;
/// `known` grows with what is appended.
fn new_objects(
    repo: &gix::Repository,
    tree: ObjectId,
    known: &mut HashSet<ObjectId>,
    objects: &mut Vec<(PackEntryHeader, Vec<u8>)>,
) -> Result<(), Error> {
    if !known.insert(tree) {
        return Ok(());
    }
    objects.push(object_entry(repo, tree)?);
    for entry in read_tree(repo, tree)?.iter() {
        let entry = entry.map_err(read_error)?;
        let id = entry.object_id();
        match entry.mode().kind() {
            EntryKind::Tree => new_objects(repo, id, known, objects)?,
            EntryKind::Commit => {}
            EntryKind::Blob | EntryKind::BlobExecutable | EntryKind::Link => {
                if known.insert(id) {
                    objects.push(object_entry(repo, id)?);
                }
            }
        }
    }
    Ok(())
}

/// Files under `prefix` in `tree`, with paths relative to `prefix`.
fn files_under(
    repo: &gix::Repository,
    tree: ObjectId,
    prefix: &str,
) -> Result<Vec<CommittedFile>, Error> {
    let root = match prefix.is_empty() {
        true => Some(tree),
        false => {
            let lookup = read_tree(repo, tree)?
                .lookup_entry_by_path(prefix)
                .map_err(read_error)?;
            match lookup {
                Some(entry) if entry.mode().is_tree() => Some(entry.object_id()),
                _ => None,
            }
        }
    };
    let mut files = Vec::new();
    if let Some(root) = root {
        collect_files(repo, root, "", &mut files)?;
    }
    Ok(files)
}

fn collect_files(
    repo: &gix::Repository,
    tree: ObjectId,
    base: &str,
    files: &mut Vec<CommittedFile>,
) -> Result<(), Error> {
    for entry in read_tree(repo, tree)?.iter() {
        let entry = entry.map_err(read_error)?;
        let path = match base.is_empty() {
            true => entry.filename().to_string(),
            false => format!("{base}/{}", entry.filename()),
        };
        let id = entry.object_id();
        match entry.mode().kind() {
            EntryKind::Tree => collect_files(repo, id, &path, files)?,
            EntryKind::Blob | EntryKind::BlobExecutable => files.push(CommittedFile {
                path,
                content: read_object(repo, id)?.detach().data,
            }),
            EntryKind::Link | EntryKind::Commit => {}
        }
    }
    Ok(())
}

/// Encodes a pack of whole (undeltified) objects.
fn write_pack(objects: &[(PackEntryHeader, Vec<u8>)]) -> Result<Vec<u8>, Error> {
    let count = u32::try_from(objects.len()).map_err(|source| Error::TooManyObjects {
        count: objects.len(),
        source,
    })?;
    let encode = |source| Error::EncodePack { source };
    let mut pack =
        gix::odb::pack::data::header::encode(gix::odb::pack::data::Version::V2, count).to_vec();
    for (header, data) in objects {
        header
            .write_to(data.len() as u64, &mut pack)
            .map_err(encode)?;
        let mut encoder =
            flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(data).map_err(encode)?;
        pack.extend(encoder.finish().map_err(encode)?);
    }
    let mut hasher = gix::hash::hasher(gix::hash::Kind::Sha1);
    hasher.update(&pack);
    let digest = hasher
        .try_finalize()
        .map_err(|e| encode(std::io::Error::other(e)))?;
    pack.extend_from_slice(digest.as_bytes());
    Ok(pack)
}

/// The data lines of a pkt-line stream, skipping flush and delimiter packets.
fn pkt_lines(mut body: &[u8]) -> Result<Vec<&[u8]>, Error> {
    let mut lines = Vec::new();
    while !body.is_empty() {
        match packetline::decode::streaming(body) {
            Ok(packetline::decode::Stream::Complete {
                line,
                bytes_consumed,
            }) => {
                if let packetline::PacketLineRef::Data(data) = line {
                    lines.push(data);
                }
                body = &body[bytes_consumed..];
            }
            Ok(packetline::decode::Stream::Incomplete { .. }) => {
                return Err(Error::TruncatedPktLine)
            }
            Err(source) => return Err(Error::InvalidPktLine { source }),
        }
    }
    Ok(lines)
}

/// Ref name to object id from a receive-pack advertisement, and the
/// capabilities after the first ref.
fn advertised_refs(body: &[u8]) -> Result<AdvertisedRefs, Error> {
    let mut refs = HashMap::new();
    let mut capabilities = Vec::new();
    for line in pkt_lines(body)? {
        let line = match line.iter().position(|&b| b == 0) {
            Some(nul) => {
                if capabilities.is_empty() {
                    capabilities = String::from_utf8_lossy(&line[nul + 1..])
                        .split_whitespace()
                        .map(str::to_string)
                        .collect();
                }
                &line[..nul]
            }
            None => line,
        };
        let line = String::from_utf8_lossy(line);
        match line.trim_end().split_once(' ') {
            Some(("#", _)) => {}
            Some(("ERR", message)) => {
                return Err(Error::ServerError {
                    message: message.to_string(),
                })
            }
            Some((id, name)) => {
                let id = ObjectId::from_hex(id.as_bytes()).map_err(|source| {
                    Error::InvalidAdvertisedId {
                        id: id.to_string(),
                        source,
                    }
                })?;
                if !id.is_null() {
                    refs.insert(name.to_string(), id.to_string());
                }
            }
            None => {
                return Err(Error::InvalidAdvertisement {
                    line: line.trim_end().to_string(),
                })
            }
        }
    }
    Ok(AdvertisedRefs { refs, capabilities })
}

/// Splits a side-band response into the `report-status` stream (band 1) and
/// the server's messages (band 2); a message on band 3 is a fatal error.
fn demultiplex(body: &[u8]) -> Result<(Vec<u8>, String), Error> {
    let mut report = Vec::new();
    let mut messages = Vec::new();
    for line in pkt_lines(body)? {
        match line.split_first() {
            Some((1, data)) => report.extend_from_slice(data),
            Some((2, data)) => messages.extend(message_lines(data)),
            Some((3, data)) => {
                return Err(Error::ServerError {
                    message: message_lines(data).join(" "),
                })
            }
            _ => {}
        }
    }
    Ok((report, messages.join(" ")))
}

/// The non-empty lines of a side-band message.
fn message_lines(data: &[u8]) -> Vec<String> {
    String::from_utf8_lossy(data)
        .split(['\n', '\r'])
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_string)
        .collect()
}

/// A rejection with the server's own explanation in front of its short reason.
fn explain(error: Error, messages: &str) -> Error {
    match error {
        Error::RefRejected { branch, message } if !messages.is_empty() => Error::RefRejected {
            branch,
            message: format!("{messages} ({message})"),
        },
        Error::UnpackFailed { status } if !messages.is_empty() => Error::UnpackFailed {
            status: format!("{messages} ({status})"),
        },
        error => error,
    }
}

/// Checks a `report-status` response for the pushed branch.
fn check_report(body: &[u8], branch: &str) -> Result<(), Error> {
    let target = format!("refs/heads/{branch}");
    let mut updated = false;
    for line in pkt_lines(body)? {
        let line = String::from_utf8_lossy(line);
        match line.trim_end().split_once(' ') {
            Some(("unpack", "ok")) => {}
            Some(("ok", name)) => updated |= name == target,
            Some(("unpack", status)) => {
                return Err(Error::UnpackFailed {
                    status: status.to_string(),
                })
            }
            Some(("ng", rejection)) => {
                let message = match rejection.split_once(' ') {
                    Some((_, message)) => message,
                    None => rejection,
                };
                return Err(Error::RefRejected {
                    branch: branch.to_string(),
                    message: message.to_string(),
                });
            }
            Some(("ERR", message)) => {
                return Err(Error::ServerError {
                    message: message.to_string(),
                })
            }
            _ => {}
        }
    }
    match updated {
        true => Ok(()),
        false => Err(Error::MissingUpdateReport {
            branch: branch.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encodes `lines` as pkt-lines; `None` is a flush packet.
    fn stream(lines: &[Option<&[u8]>]) -> Vec<u8> {
        let mut body = Vec::new();
        for line in lines {
            match line {
                Some(data) => packetline::blocking_io::encode::data_to_write(data, &mut body),
                None => packetline::blocking_io::encode::flush_to_write(&mut body),
            }
            .unwrap();
        }
        body
    }

    fn change(content: Option<&str>, expected: Expected) -> FileChange {
        FileChange {
            path: "a".to_string(),
            content: content.map(|c| c.as_bytes().to_vec()),
            expected,
        }
    }

    fn file(content: &str) -> Option<Current> {
        Some(Current::File {
            kind: EntryKind::Blob,
            content: content.as_bytes().to_vec(),
        })
    }

    #[test]
    fn expectations_hold_on_the_prepared_state_or_on_the_result() {
        let new_file = change(Some("new"), Expected::Missing);
        assert!(check_expected("a", &new_file, &None).is_ok());
        assert!(check_expected("a", &new_file, &file("new")).is_ok());
        assert!(check_expected("a", &new_file, &file("other")).is_err());

        let edit = change(Some("new"), Expected::Content(b"old".to_vec()));
        assert!(check_expected("a", &edit, &file("old")).is_ok());
        assert!(check_expected("a", &edit, &file("new")).is_ok());
        assert!(check_expected("a", &edit, &file("other")).is_err());

        let delete = change(None, Expected::Content(b"old".to_vec()));
        assert!(check_expected("a", &delete, &None).is_ok());

        let over_directory = change(Some("new"), Expected::Any);
        assert!(check_expected("a", &over_directory, &Some(Current::Other)).is_err());
    }

    #[test]
    fn server_err_lines_are_server_errors() {
        let advertisement = stream(&[Some(b"ERR access denied\n")]);
        assert!(
            matches!(advertised_refs(&advertisement), Err(Error::ServerError { message }) if message == "access denied")
        );
        let report = stream(&[Some(b"unpack ok\n"), Some(b"ERR hook declined\n"), None]);
        assert!(
            matches!(check_report(&report, "main"), Err(Error::ServerError { message }) if message == "hook declined")
        );
        let unpack = stream(&[Some(b"unpack index-pack failed\n"), None]);
        assert!(
            matches!(check_report(&unpack, "main"), Err(Error::UnpackFailed { status }) if status == "index-pack failed")
        );
        let silent = stream(&[Some(b"unpack ok\n"), None]);
        assert!(matches!(
            check_report(&silent, "main"),
            Err(Error::MissingUpdateReport { .. })
        ));
    }

    #[test]
    fn pkt_lines_skip_flush_packets_and_reject_bad_framing() {
        let body = stream(&[Some(b"hello\n"), None, Some(b"world\n")]);
        assert_eq!(
            pkt_lines(&body).unwrap(),
            vec![&b"hello\n"[..], &b"world\n"[..]]
        );
        assert!(pkt_lines(b"000ahi").is_err());
        assert!(pkt_lines(b"zzzz").is_err());
    }

    #[test]
    fn advertisement_lists_branches_and_skips_an_empty_repository_marker() {
        let id = "1111111111111111111111111111111111111111";
        let line = format!("{id} refs/heads/main\0report-status side-band-64k quiet\n");
        let body = stream(&[
            Some(b"# service=git-receive-pack\n"),
            None,
            Some(line.as_bytes()),
            None,
        ]);
        let advertised = advertised_refs(&body).unwrap();
        assert_eq!(
            advertised.refs.get("refs/heads/main"),
            Some(&id.to_string())
        );
        assert_eq!(
            advertised.capabilities,
            vec!["report-status", "side-band-64k", "quiet"]
        );

        let null = ObjectId::null(gix::hash::Kind::Sha1);
        let line = format!("{null} capabilities^{{}}\0report-status\n");
        let empty = stream(&[
            Some(b"# service=git-receive-pack\n"),
            None,
            Some(line.as_bytes()),
            None,
        ]);
        assert!(advertised_refs(&empty).unwrap().refs.is_empty());
    }

    #[test]
    fn side_band_messages_explain_a_rejection() {
        let report = stream(&[
            Some(b"unpack ok\n"),
            Some(b"ng refs/heads/main Internal Server Error\n"),
            None,
        ]);
        let band = |number: u8, data: &[u8]| [&[number][..], data].concat();
        let body = stream(&[
            Some(&band(
                2,
                b"error: GH013: Repository rule violations found.\n",
            )),
            Some(&band(
                2,
                b"\r- Changes must be made through a pull request.\n",
            )),
            Some(&band(1, &report)),
            None,
        ]);
        let (report, messages) = demultiplex(&body).unwrap();
        assert_eq!(
            messages,
            "error: GH013: Repository rule violations found. - Changes must be made through a pull request."
        );
        match check_report(&report, "main").map_err(|error| explain(error, &messages)) {
            Err(Error::RefRejected { message, .. }) => {
                assert_eq!(message, format!("{messages} (Internal Server Error)"))
            }
            other => panic!("{other:?}"),
        }

        let fatal = stream(&[Some(&band(3, b"repository is archived\n")), None]);
        assert!(matches!(
            demultiplex(&fatal),
            Err(Error::ServerError { message }) if message == "repository is archived"
        ));
        assert!(matches!(
            explain(
                Error::BranchMoved {
                    branch: "main".to_string()
                },
                "x"
            ),
            Error::BranchMoved { .. }
        ));
    }

    #[test]
    fn report_status_accepts_an_updated_branch_and_surfaces_rejections() {
        let ok = stream(&[Some(b"unpack ok\n"), Some(b"ok refs/heads/main\n"), None]);
        assert!(check_report(&ok, "main").is_ok());

        let rejected = stream(&[
            Some(b"unpack ok\n"),
            Some(b"ng refs/heads/main fetch first\n"),
            None,
        ]);
        match check_report(&rejected, "main") {
            Err(Error::RefRejected { branch, message }) => {
                assert_eq!((branch.as_str(), message.as_str()), ("main", "fetch first"))
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn paths_outside_the_directory_are_rejected() {
        for path in [
            "/a",
            "../a",
            "a/../b",
            "./a",
            "a\\b",
            ".git/config",
            ".GIT/config",
            "a/.git /b",
            "a\0b",
        ] {
            assert!(normalize_path(path).is_err(), "{path}");
        }
        assert_eq!(
            normalize_path("flows/a.yaml").unwrap(),
            vec!["flows", "a.yaml"]
        );
    }

    #[test]
    fn names_that_ntfs_or_hfs_resolve_to_dot_git_are_rejected() {
        for path in [
            ".git.",
            "a/.git./b",
            ".git::$INDEX_ALLOCATION/config",
            "git~1/config",
            "GIT~1/config",
            ".g\u{200c}it/config",
            "a/.\u{feff}GIT/b",
        ] {
            assert!(normalize_path(path).is_err(), "{path:?}");
        }
        for path in [".github/ci.yaml", ".gitignore", "git/a.yaml", "a.git/b"] {
            assert!(normalize_path(path).is_ok(), "{path:?}");
        }
    }

    #[test]
    fn a_signed_commit_verifies_against_the_signing_key() {
        let key = ssh_key::PrivateKey::from(ssh_key::private::Ed25519Keypair::from_seed(&[7; 32]));
        let author = Author {
            name: "a".to_string(),
            email: "a@example.com".to_string(),
        };
        let committer = Author {
            name: "b".to_string(),
            email: "b@example.com".to_string(),
        };
        let dir = tempfile::TempDir::new().unwrap();
        let repo = gix::init_bare(dir.path()).unwrap();
        let tree = ObjectId::empty_tree(repo.object_hash());
        let id = write_commit(
            &repo,
            tree,
            None,
            &author,
            Some(&committer),
            "m",
            Some(&key),
        )
        .unwrap();

        let commit = repo.find_commit(id).unwrap();
        assert_eq!(commit.author().unwrap().email, "a@example.com");
        assert_eq!(commit.committer().unwrap().email, "b@example.com");
        let (signature, signed) =
            gix::objs::CommitRefIter::signature(&commit.data, repo.object_hash())
                .unwrap()
                .unwrap();
        let signature = ssh_key::SshSig::from_pem(&*signature).unwrap();
        let signed = signed.to_bstring();
        key.public_key().verify("git", &signed, &signature).unwrap();
        assert!(key
            .public_key()
            .verify("git", b"other", &signature)
            .is_err());

        let unsigned = write_commit(&repo, tree, None, &author, None, "m", None).unwrap();
        let unsigned = repo.find_commit(unsigned).unwrap();
        assert_eq!(unsigned.committer().unwrap().email, "a@example.com");
        assert!(
            gix::objs::CommitRefIter::signature(&unsigned.data, repo.object_hash())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_written_pack_is_readable_by_gix() {
        let dir = tempfile::TempDir::new().unwrap();
        let repo = gix::init_bare(dir.path()).unwrap();
        let blob = repo.write_blob(b"content").unwrap().detach();
        let mut editor = repo
            .edit_tree(ObjectId::empty_tree(repo.object_hash()))
            .unwrap();
        editor
            .upsert("flows/a.yaml", EntryKind::Blob, blob)
            .unwrap();
        let tree = editor.write().unwrap().detach();
        let mut objects = Vec::new();
        new_objects(&repo, tree, &mut HashSet::new(), &mut objects).unwrap();
        assert_eq!(objects.len(), 3);

        let pack = write_pack(&objects).unwrap();
        let target = tempfile::TempDir::new().unwrap();
        let other = gix::init_bare(target.path()).unwrap();
        let pack_dir = target.path().join("objects/pack");
        let outcome = gix::odb::pack::Bundle::write_to_directory(
            &mut std::io::BufReader::new(&pack[..]),
            Some(&pack_dir),
            &mut gix::progress::Discard,
            &gix::interrupt::IS_INTERRUPTED,
            None::<gix::objs::find::Never>,
            Default::default(),
        )
        .unwrap();
        assert_eq!(outcome.index.num_objects, 3);
        drop(other);
        let other = gix::open(target.path()).unwrap();
        let found = other.find_object(blob).unwrap();
        assert_eq!(found.data, b"content");
    }
}
