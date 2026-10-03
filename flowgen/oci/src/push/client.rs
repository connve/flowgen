//! Pushes a set of files to a registry as a single-layer OCI artifact: one
//! tar+gzip layer holding the files at their paths.

use bytes::Bytes;
use oci_client::manifest::{
    OciDescriptor, OciImageManifest, IMAGE_LAYER_GZIP_MEDIA_TYPE, OCI_IMAGE_MEDIA_TYPE,
};
use oci_client::Reference;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;

/// Media type of the OCI 1.1 empty descriptor, the config of an artifact
/// that has no configuration.
pub const EMPTY_MEDIA_TYPE: &str = "application/vnd.oci.empty.v1+json";

/// Content of the OCI 1.1 empty descriptor.
const EMPTY_CONTENT: &[u8] = b"{}";

/// Artifact type of every pushed manifest.
pub const ARTIFACT_TYPE: &str = "application/vnd.connve.flowgen.files.v1";

/// Failure to push an artifact.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("At least one tag is required")]
    NoTags,
    #[error("Invalid OCI reference '{reference}': {source}")]
    InvalidReference {
        reference: String,
        #[source]
        source: oci_client::ParseError,
    },
    #[error("Invalid file path '{path}': must be a non-empty relative path without '..' segments")]
    InvalidPath { path: String },
    #[error("File path '{path}' appears more than once")]
    DuplicatePath { path: String },
    #[error(transparent)]
    Registry(#[from] crate::registry::Error),
    #[error("Failed to build the artifact layer: {source}")]
    Layer {
        #[source]
        source: std::io::Error,
    },
    #[error("Artifact layer build did not complete: {source}")]
    LayerTask {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("Failed to serialize the artifact manifest: {source}")]
    Manifest {
        #[source]
        source: serde_json::Error,
    },
    #[error("OCI registry push failed for '{reference}': {source}")]
    Push {
        reference: String,
        #[source]
        source: oci_client::errors::OciDistributionError,
    },
}

impl Error {
    /// Whether retrying the same push cannot succeed.
    pub fn is_permanent(&self) -> bool {
        match self {
            Error::Registry(source) => source.is_permanent(),
            Error::NoTags
            | Error::InvalidReference { .. }
            | Error::InvalidPath { .. }
            | Error::DuplicatePath { .. }
            | Error::Layer { .. }
            | Error::Manifest { .. } => true,
            Error::LayerTask { .. } | Error::Push { .. } => false,
        }
    }
}

/// A file to place in the artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactFile {
    /// Relative path of the file inside the artifact, `/`-separated.
    pub path: String,
    /// File content.
    pub content: Vec<u8>,
}

/// Result of a push.
#[derive(Debug, Clone, PartialEq)]
pub struct Pushed {
    /// Manifest digest, the same under every tag.
    pub digest: String,
    /// Every reference pushed, `<repository>:<tag>`.
    pub references: Vec<String>,
}

/// Where to push.
#[derive(Debug, Clone)]
pub struct Push {
    /// Registry and repository without a tag, e.g. `registry.example.com/team/configs`.
    pub repository: String,
    /// Registry credentials: `{username, password}` or a Docker `config.json`.
    pub credentials_path: Option<PathBuf>,
}

impl Push {
    /// Pushes `files` as one artifact under every tag in `tags`: the blobs
    /// once, then the same manifest per tag. Tags and paths are checked
    /// before anything is sent, so a bad input never leaves a partial release.
    pub async fn push(&self, files: Vec<ArtifactFile>, tags: &[String]) -> Result<Pushed, Error> {
        let references = tags
            .iter()
            .map(|tag| self.reference(tag))
            .collect::<Result<Vec<Reference>, Error>>()?;
        let first = match references.first() {
            Some(first) => first,
            None => return Err(Error::NoTags),
        };
        let files = prepare(files)?;
        let auth =
            crate::registry::load_auth(self.credentials_path.as_deref(), first.registry()).await?;
        let layer = tokio::task::spawn_blocking(move || layer(files))
            .await
            .map_err(|source| Error::LayerTask { source })?
            .map_err(|source| Error::Layer { source })?;
        let layer = Bytes::from(layer);
        let layer_descriptor = descriptor(IMAGE_LAYER_GZIP_MEDIA_TYPE, &layer);
        let config_descriptor = descriptor(EMPTY_MEDIA_TYPE, EMPTY_CONTENT);
        let manifest = manifest(config_descriptor.clone(), layer_descriptor.clone())?;
        let digest = sha256_digest(&manifest);

        let client = crate::registry::client(first);
        client
            .store_auth_if_needed(first.resolve_registry(), &auth)
            .await;
        let blobs = [
            (layer, layer_descriptor.digest),
            (Bytes::from_static(EMPTY_CONTENT), config_descriptor.digest),
        ];
        for (blob, blob_digest) in blobs {
            client
                .push_blob(first, blob, &blob_digest)
                .await
                .map_err(|source| Error::Push {
                    reference: first.to_string(),
                    source,
                })?;
        }
        for reference in &references {
            client
                .push_manifest_raw(
                    reference,
                    manifest.clone(),
                    http::HeaderValue::from_static(OCI_IMAGE_MEDIA_TYPE),
                )
                .await
                .map_err(|source| Error::Push {
                    reference: reference.to_string(),
                    source,
                })?;
        }
        Ok(Pushed {
            digest,
            references: references.iter().map(Reference::to_string).collect(),
        })
    }

    fn reference(&self, tag: &str) -> Result<Reference, Error> {
        let reference = format!("{}:{tag}", self.repository.trim_end_matches('/'));
        reference
            .parse()
            .map_err(|source| Error::InvalidReference { reference, source })
    }
}

/// Files keyed by normalized path, rejecting paths that cannot be extracted
/// safely and paths that name the same file twice.
fn prepare(files: Vec<ArtifactFile>) -> Result<BTreeMap<String, Vec<u8>>, Error> {
    let mut prepared = BTreeMap::new();
    for ArtifactFile { path, content } in files {
        let path = normalize_path(&path)?;
        if prepared.contains_key(&path) {
            return Err(Error::DuplicatePath { path });
        }
        prepared.insert(path, content);
    }
    Ok(prepared)
}

/// `path` with empty and `.` segments dropped, the form the tar entry gets.
fn normalize_path(path: &str) -> Result<String, Error> {
    let segments: Vec<&str> = path
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect();
    let invalid = path.starts_with('/')
        || path.contains('\0')
        || segments.is_empty()
        || segments.contains(&"..");
    match invalid {
        true => Err(Error::InvalidPath {
            path: path.to_string(),
        }),
        false => Ok(segments.join("/")),
    }
}

/// A gzipped tarball of `files` in path order with fixed metadata, so the
/// same files always give the same bytes.
fn layer(files: BTreeMap<String, Vec<u8>>) -> Result<Vec<u8>, std::io::Error> {
    let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    let mut builder = tar::Builder::new(encoder);
    for (path, content) in files {
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o644);
        header.set_uid(0);
        header.set_gid(0);
        header.set_mtime(0);
        header.set_entry_type(tar::EntryType::Regular);
        builder.append_data(&mut header, &path, content.as_slice())?;
    }
    builder.into_inner()?.finish()
}

/// The exact manifest bytes pushed under every tag.
fn manifest(config: OciDescriptor, layer: OciDescriptor) -> Result<Bytes, Error> {
    let manifest = OciImageManifest {
        media_type: Some(OCI_IMAGE_MEDIA_TYPE.to_string()),
        artifact_type: Some(ARTIFACT_TYPE.to_string()),
        config,
        layers: vec![layer],
        ..Default::default()
    };
    serde_json::to_vec(&manifest)
        .map(Bytes::from)
        .map_err(|source| Error::Manifest { source })
}

fn descriptor(media_type: &str, content: &[u8]) -> OciDescriptor {
    OciDescriptor {
        media_type: media_type.to_string(),
        digest: sha256_digest(content),
        size: content.len() as i64,
        ..Default::default()
    }
}

fn sha256_digest(content: &[u8]) -> String {
    format!("sha256:{:x}", Sha256::digest(content))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Read;

    fn file(path: &str, content: &str) -> ArtifactFile {
        ArtifactFile {
            path: path.to_string(),
            content: content.as_bytes().to_vec(),
        }
    }

    fn unreachable_push() -> Push {
        Push {
            repository: "127.0.0.1:9/a".to_string(),
            credentials_path: None,
        }
    }

    fn tags(tags: &[&str]) -> Vec<String> {
        tags.iter().map(|tag| tag.to_string()).collect()
    }

    #[test]
    fn the_layer_holds_every_file_and_is_reproducible() {
        let files = vec![file("nested/b.txt", "b"), file("a.txt", "a")];
        let reversed = vec![files[1].clone(), files[0].clone()];
        let bytes = layer(prepare(files).unwrap()).unwrap();
        assert_eq!(bytes, layer(prepare(reversed).unwrap()).unwrap());

        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(&bytes[..]));
        let mut entries = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let header = entry.header();
            assert_eq!(header.mode().unwrap(), 0o644);
            assert_eq!(header.mtime().unwrap(), 0);
            assert_eq!(header.uid().unwrap(), 0);
            assert_eq!(header.gid().unwrap(), 0);
            let mut content = String::new();
            entry.read_to_string(&mut content).unwrap();
            entries.push((entry.path().unwrap().display().to_string(), content));
        }
        assert_eq!(
            entries,
            vec![
                ("a.txt".to_string(), "a".to_string()),
                ("nested/b.txt".to_string(), "b".to_string()),
            ]
        );
    }

    #[test]
    fn paths_are_normalized_to_the_form_stored_in_the_layer() {
        let prepared = prepare(vec![file("./nested//b.txt", "b")]).unwrap();
        assert_eq!(prepared.keys().collect::<Vec<_>>(), vec!["nested/b.txt"]);
    }

    #[test]
    fn empty_absolute_and_parent_paths_are_rejected_as_permanent() {
        for path in [
            "",
            ".",
            "/a.txt",
            "../a.txt",
            "nested/../../a.txt",
            "a\0.txt",
        ] {
            let error = prepare(vec![file(path, "a")]).unwrap_err();
            assert!(
                matches!(&error, Error::InvalidPath { path: p } if p == path),
                "{path:?}: {error}"
            );
            assert!(error.is_permanent());
        }
    }

    #[test]
    fn two_files_with_the_same_normalized_path_are_rejected_as_permanent() {
        let error = prepare(vec![
            file("nested/b.txt", "1"),
            file("./nested//b.txt", "2"),
        ])
        .unwrap_err();
        assert!(matches!(&error, Error::DuplicatePath { path } if path == "nested/b.txt"));
        assert!(error.is_permanent());
    }

    #[test]
    fn the_manifest_uses_the_empty_config_and_the_files_artifact_type() {
        let layer_descriptor = descriptor(IMAGE_LAYER_GZIP_MEDIA_TYPE, b"layer");
        let bytes = manifest(
            descriptor(EMPTY_MEDIA_TYPE, EMPTY_CONTENT),
            layer_descriptor.clone(),
        )
        .unwrap();
        let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(value["schemaVersion"], 2);
        assert_eq!(value["mediaType"], OCI_IMAGE_MEDIA_TYPE);
        assert_eq!(value["artifactType"], ARTIFACT_TYPE);
        assert_eq!(value["config"]["mediaType"], EMPTY_MEDIA_TYPE);
        assert_eq!(
            value["config"]["digest"],
            "sha256:44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a"
        );
        assert_eq!(value["config"]["size"], 2);
        assert_eq!(
            value["layers"][0]["digest"],
            layer_descriptor.digest.as_str()
        );
        assert_eq!(value["layers"][0]["size"], 5);
    }

    #[tokio::test]
    async fn a_push_without_tags_is_rejected() {
        let result = unreachable_push().push(vec![], &[]).await;
        assert!(matches!(result, Err(Error::NoTags)));
    }

    #[tokio::test]
    async fn a_bad_second_tag_fails_before_anything_is_sent() {
        let result = unreachable_push()
            .push(vec![file("a.txt", "a")], &tags(&["ok", "not valid!"]))
            .await;
        let error = result.unwrap_err();
        assert!(
            matches!(&error, Error::InvalidReference { reference, .. } if reference.ends_with("not valid!")),
            "{error}"
        );
        assert!(error.is_permanent());
    }

    #[tokio::test]
    async fn invalid_and_duplicate_paths_fail_before_anything_is_sent() {
        let invalid = unreachable_push()
            .push(vec![file("../a.txt", "a")], &tags(&["ok"]))
            .await;
        assert!(matches!(invalid, Err(Error::InvalidPath { .. })));

        let duplicate = unreachable_push()
            .push(
                vec![file("a.txt", "1"), file("./a.txt", "2")],
                &tags(&["ok"]),
            )
            .await;
        assert!(matches!(duplicate, Err(Error::DuplicatePath { .. })));
    }

    #[tokio::test]
    async fn unparsable_credentials_are_a_permanent_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.json");
        tokio::fs::write(&path, "{").await.unwrap();
        let push = Push {
            repository: "127.0.0.1:9/a".to_string(),
            credentials_path: Some(path),
        };
        let error = push
            .push(vec![file("a.txt", "a")], &tags(&["ok"]))
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::Registry(crate::registry::Error::ParseCredentials { .. })
        ));
        assert!(error.is_permanent());
    }

    #[test]
    fn layer_build_failures_are_permanent_and_registry_failures_are_not() {
        let layer_error = Error::Layer {
            source: std::io::Error::other("a"),
        };
        assert!(layer_error.is_permanent());
        let push_error = Error::Push {
            reference: "a".to_string(),
            source: oci_client::errors::OciDistributionError::RegistryNoLocationError,
        };
        assert!(!push_error.is_permanent());
    }
}
