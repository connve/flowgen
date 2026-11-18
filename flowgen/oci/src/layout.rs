//! Artifacts in an OCI Image Layout on this machine: a folder of
//! content-addressed blobs and an `index.json` naming the tagged manifests,
//! written and read without a registry.

use oci_client::manifest::{ImageIndexEntry, OciImageIndex, OCI_IMAGE_INDEX_MEDIA_TYPE};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

/// The scheme of a layout reference: `oci-layout://<folder>` for a
/// repository, `oci-layout://<folder>:<tag>` for an artifact.
pub const SCHEME: &str = "oci-layout://";

const LAYOUT_FILE: &str = "oci-layout";
const LAYOUT_CONTENT: &str = r#"{"imageLayoutVersion":"1.0.0"}"#;
const INDEX_FILE: &str = "index.json";
/// The index is written here first and renamed over [`INDEX_FILE`], so a
/// reader never sees half of it.
const INDEX_DRAFT_FILE: &str = "index.json.draft";
/// A blob is written next to its path with this extension first, then renamed.
const DRAFT_EXTENSION: &str = "draft";
const BLOBS_DIR: &str = "blobs";
const ALGORITHM: &str = "sha256";
/// The annotation that names a manifest's tag in the index.
const REF_NAME: &str = "org.opencontainers.image.ref.name";

/// Failure to read or write a layout.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Invalid layout reference '{reference}': expected oci-layout://<folder>:<tag>")]
    InvalidReference { reference: String },
    #[error("Invalid tag '{tag}': 1 to 128 letters, digits, '_', '.' or '-', not starting with '.' or '-'")]
    InvalidTag { tag: String },
    #[error("Failed to access the layout at {path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("The index of the layout at {path} is not valid: {source}")]
    Index {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("Unsupported digest '{digest}': only sha256 is read")]
    UnsupportedDigest { digest: String },
    #[error("Blob {digest} does not match its digest")]
    Corrupt { digest: String },
}

impl Error {
    /// Whether trying again cannot succeed.
    pub fn is_permanent(&self) -> bool {
        !matches!(self, Error::Io { .. })
    }
}

/// The folder and tag of `oci-layout://<folder>:<tag>`, or `None` when
/// `reference` is not a layout reference.
pub fn artifact(reference: &str) -> Option<Result<(PathBuf, String), Error>> {
    let rest = reference.strip_prefix(SCHEME)?;
    let invalid = || Error::InvalidReference {
        reference: reference.to_string(),
    };
    let (folder, tag) = match rest.rsplit_once(':') {
        Some((folder, tag)) if !folder.is_empty() && !tag.contains('/') => (folder, tag),
        _ => return Some(Err(invalid())),
    };
    Some(check_tag(tag).map(|()| (PathBuf::from(folder), tag.to_string())))
}

/// The folder of `oci-layout://<folder>`, or `None` when `repository` is not
/// a layout reference.
pub fn repository(repository: &str) -> Option<Result<PathBuf, Error>> {
    let folder = repository.strip_prefix(SCHEME)?;
    match folder.is_empty() {
        true => Some(Err(Error::InvalidReference {
            reference: repository.to_string(),
        })),
        false => Some(Ok(PathBuf::from(folder))),
    }
}

/// The tag rule of the OCI distribution spec.
pub fn check_tag(tag: &str) -> Result<(), Error> {
    let valid = (1..=128).contains(&tag.len())
        && !tag.starts_with(['.', '-'])
        && tag
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
    match valid {
        true => Ok(()),
        false => Err(Error::InvalidTag {
            tag: tag.to_string(),
        }),
    }
}

/// An OCI Image Layout folder. Its methods block on the filesystem.
#[derive(Debug, Clone)]
pub struct Layout {
    root: PathBuf,
}

impl Layout {
    pub fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn io(&self, path: &Path) -> impl FnOnce(std::io::Error) -> Error {
        let path = path.to_path_buf();
        move |source| Error::Io { path, source }
    }

    fn blob_path(&self, digest: &str) -> Result<PathBuf, Error> {
        match digest.split_once(':') {
            Some((ALGORITHM, hex))
                if !hex.is_empty() && hex.chars().all(|c| c.is_ascii_hexdigit()) =>
            {
                Ok(self.root.join(BLOBS_DIR).join(ALGORITHM).join(hex))
            }
            _ => Err(Error::UnsupportedDigest {
                digest: digest.to_string(),
            }),
        }
    }

    /// Stores `content` under its digest, once, and returns the digest. The
    /// blob is written to a draft renamed into place, so an interrupted write
    /// leaves no partial blob, and a stored blob that does not match its digest
    /// is written again.
    pub fn write_blob(&self, content: &[u8]) -> Result<String, Error> {
        let digest = digest(content);
        let path = self.blob_path(&digest)?;
        if let Ok(stored) = std::fs::read(&path) {
            if self::digest(&stored) == digest {
                return Ok(digest);
            }
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(self.io(parent))?;
        }
        let layout = self.root.join(LAYOUT_FILE);
        if !layout.exists() {
            std::fs::write(&layout, LAYOUT_CONTENT).map_err(self.io(&layout))?;
        }
        let draft = path.with_extension(DRAFT_EXTENSION);
        std::fs::write(&draft, content).map_err(self.io(&draft))?;
        std::fs::rename(&draft, &path).map_err(self.io(&path))?;
        Ok(digest)
    }

    /// The blob with `digest`, checked against it.
    pub fn read_blob(&self, digest: &str) -> Result<Vec<u8>, Error> {
        let path = self.blob_path(digest)?;
        let content = std::fs::read(&path).map_err(self.io(&path))?;
        match self::digest(&content) == digest {
            true => Ok(content),
            false => Err(Error::Corrupt {
                digest: digest.to_string(),
            }),
        }
    }

    fn index(&self) -> Result<OciImageIndex, Error> {
        let path = self.root.join(INDEX_FILE);
        match std::fs::read(&path) {
            Ok(content) => serde_json::from_slice(&content).map_err(|source| Error::Index {
                path: path.clone(),
                source,
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(OciImageIndex {
                schema_version: 2,
                media_type: Some(OCI_IMAGE_INDEX_MEDIA_TYPE.to_string()),
                manifests: Vec::new(),
                artifact_type: None,
                annotations: None,
            }),
            Err(source) => Err(Error::Io { path, source }),
        }
    }

    /// Stores `manifest` and points each of `tags` at it, moving a tag that
    /// pointed elsewhere.
    pub fn tag(&self, manifest: &[u8], media_type: &str, tags: &[String]) -> Result<String, Error> {
        for tag in tags {
            check_tag(tag)?;
        }
        let digest = self.write_blob(manifest)?;
        let mut index = self.index()?;
        index
            .manifests
            .retain(|entry| !tags.iter().any(|tag| ref_name(entry) == Some(tag.as_str())));
        for tag in tags {
            index.manifests.push(ImageIndexEntry {
                media_type: media_type.to_string(),
                digest: digest.clone(),
                size: manifest.len() as i64,
                platform: None,
                annotations: Some(BTreeMap::from([(REF_NAME.to_string(), tag.clone())])),
                artifact_type: None,
            });
        }
        let content = serde_json::to_vec_pretty(&index).map_err(|source| Error::Index {
            path: self.root.join(INDEX_FILE),
            source,
        })?;
        let draft = self.root.join(INDEX_DRAFT_FILE);
        std::fs::write(&draft, content).map_err(self.io(&draft))?;
        let index_path = self.root.join(INDEX_FILE);
        std::fs::rename(&draft, &index_path).map_err(self.io(&index_path))?;
        Ok(digest)
    }

    /// The digest of the manifest `tag` points at, if any.
    pub fn resolve(&self, tag: &str) -> Result<Option<String>, Error> {
        Ok(self
            .index()?
            .manifests
            .into_iter()
            .find(|entry| ref_name(entry) == Some(tag))
            .map(|entry| entry.digest))
    }
}

fn ref_name(entry: &ImageIndexEntry) -> Option<&str> {
    entry
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(REF_NAME))
        .map(String::as_str)
}

/// The OCI digest of `content`.
pub fn digest(content: &[u8]) -> String {
    format!("{ALGORITHM}:{:x}", Sha256::digest(content))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MANIFEST: &str = "application/vnd.oci.image.manifest.v1+json";

    fn tags(tags: &[&str]) -> Vec<String> {
        tags.iter().map(|tag| tag.to_string()).collect()
    }

    #[test]
    fn references_name_a_folder_and_a_tag() {
        let (folder, tag) = artifact("oci-layout://.flowgen/releases:latest")
            .unwrap()
            .unwrap();
        assert_eq!(
            (folder, tag.as_str()),
            (PathBuf::from(".flowgen/releases"), "latest")
        );
        assert!(artifact("registry.example.com/a:latest").is_none());
        assert!(matches!(
            artifact("oci-layout://.flowgen/releases"),
            Some(Err(Error::InvalidReference { .. }))
        ));
        assert!(matches!(
            artifact("oci-layout://a:-bad"),
            Some(Err(Error::InvalidTag { .. }))
        ));
        assert_eq!(
            repository("oci-layout:///abs/releases").unwrap().unwrap(),
            PathBuf::from("/abs/releases")
        );
        assert!(repository("registry.example.com/a").is_none());
    }

    #[test]
    fn a_tag_points_at_the_last_manifest_tagged_with_it() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path().join("releases"));
        assert_eq!(layout.resolve("latest").unwrap(), None);

        let first = layout
            .tag(b"one", MANIFEST, &tags(&["a1", "latest"]))
            .unwrap();
        let second = layout
            .tag(b"two", MANIFEST, &tags(&["b2", "latest"]))
            .unwrap();

        assert_eq!(layout.resolve("a1").unwrap(), Some(first.clone()));
        assert_eq!(layout.resolve("b2").unwrap(), Some(second.clone()));
        assert_eq!(layout.resolve("latest").unwrap(), Some(second.clone()));
        assert_eq!(layout.read_blob(&first).unwrap(), b"one");
        let on_disk: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("releases/oci-layout")).unwrap())
                .unwrap();
        assert_eq!(on_disk["imageLayoutVersion"], "1.0.0");
        assert!(!dir.path().join("releases").join(INDEX_DRAFT_FILE).exists());
    }

    #[test]
    fn a_blob_that_no_longer_matches_its_digest_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path().to_path_buf());
        let digest = layout.write_blob(b"content").unwrap();
        std::fs::write(layout.blob_path(&digest).unwrap(), b"changed").unwrap();

        assert!(matches!(
            layout.read_blob(&digest),
            Err(Error::Corrupt { .. })
        ));
        assert!(matches!(
            layout.read_blob("sha256:../../etc"),
            Err(Error::UnsupportedDigest { .. })
        ));
    }

    #[test]
    fn a_damaged_blob_is_written_again() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path().to_path_buf());
        let digest = layout.write_blob(b"content").unwrap();
        let path = layout.blob_path(&digest).unwrap();
        std::fs::write(&path, b"cont").unwrap();

        assert_eq!(layout.write_blob(b"content").unwrap(), digest);

        assert_eq!(layout.read_blob(&digest).unwrap(), b"content");
        assert!(!path.with_extension(DRAFT_EXTENSION).exists());
    }

    #[test]
    fn invalid_tags_are_refused_before_anything_is_written() {
        let dir = tempfile::tempdir().unwrap();
        let layout = Layout::new(dir.path().join("releases"));

        let result = layout.tag(b"one", MANIFEST, &tags(&["ok", "not valid!"]));

        assert!(matches!(result, Err(Error::InvalidTag { .. })));
        assert!(!dir.path().join("releases").exists());
    }
}
