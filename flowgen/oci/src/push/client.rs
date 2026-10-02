//! Pushes a set of files to a registry as a single-layer OCI artifact: one
//! tar+gzip layer holding the files at their paths.

use oci_client::client::{Config, ImageLayer};
use oci_client::manifest::IMAGE_LAYER_GZIP_MEDIA_TYPE;
use oci_client::Reference;
use std::io::Write;
use std::path::PathBuf;

/// Failure to push an artifact.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("At least one tag is required")]
    NoTags,
    #[error("Invalid OCI reference '{reference}': {source}")]
    InvalidReference {
        reference: String,
        #[source]
        source: oci_client::ParseError,
    },
    #[error(transparent)]
    Auth(Box<crate::sync::processor::Error>),
    #[error("Failed to build the artifact layer: {source}")]
    Layer {
        #[source]
        source: std::io::Error,
    },
    #[error("OCI registry push failed for '{reference}': {source}")]
    Push {
        reference: String,
        #[source]
        source: oci_client::errors::OciDistributionError,
    },
}

/// A file to place in the artifact.
#[derive(Debug, Clone, PartialEq)]
pub struct ArtifactFile {
    pub path: String,
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
    /// Pushes `files` under every tag in `tags`.
    pub async fn push(&self, files: &[ArtifactFile], tags: &[String]) -> Result<Pushed, Error> {
        let first = tags.first().ok_or(Error::NoTags)?;
        let first = self.reference(first)?;
        let auth =
            crate::sync::processor::load_auth(self.credentials_path.as_ref(), first.registry())
                .await
                .map_err(|source| Error::Auth(Box::new(source)))?;
        let client = crate::sync::processor::registry_client(&first);
        let layer = layer(files).map_err(|source| Error::Layer { source })?;

        let mut references = Vec::with_capacity(tags.len());
        let mut digest = None;
        for tag in tags {
            let reference = self.reference(tag)?;
            let push_error = |source| Error::Push {
                reference: reference.to_string(),
                source,
            };
            client
                .push(
                    &reference,
                    &[ImageLayer::new(
                        layer.clone(),
                        IMAGE_LAYER_GZIP_MEDIA_TYPE.to_string(),
                        None,
                    )],
                    Config::oci_v1(b"{}".to_vec(), None),
                    &auth,
                    None,
                )
                .await
                .map_err(push_error)?;
            if digest.is_none() {
                digest = Some(
                    client
                        .fetch_manifest_digest(&reference, &auth)
                        .await
                        .map_err(push_error)?,
                );
            }
            references.push(reference.to_string());
        }
        let digest = match digest {
            Some(digest) => digest,
            None => return Err(Error::NoTags),
        };
        Ok(Pushed { digest, references })
    }

    fn reference(&self, tag: &str) -> Result<Reference, Error> {
        let reference = format!("{}:{tag}", self.repository.trim_end_matches('/'));
        reference
            .parse()
            .map_err(|source| Error::InvalidReference { reference, source })
    }
}

/// A gzipped tarball of `files`, sorted by path with fixed metadata so the
/// same files always give the same bytes.
fn layer(files: &[ArtifactFile]) -> Result<Vec<u8>, std::io::Error> {
    let mut sorted: Vec<&ArtifactFile> = files.iter().collect();
    sorted.sort_by(|a, b| a.path.cmp(&b.path));
    let mut builder = tar::Builder::new(Vec::new());
    for file in sorted {
        let mut header = tar::Header::new_gnu();
        header.set_size(file.content.len() as u64);
        header.set_mode(0o644);
        header.set_mtime(0);
        header.set_entry_type(tar::EntryType::Regular);
        builder.append_data(&mut header, &file.path, file.content.as_slice())?;
    }
    let tarball = builder.into_inner()?;
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&tarball)?;
    encoder.finish()
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

    #[test]
    fn the_layer_holds_every_file_and_is_reproducible() {
        let files = [file("resources/s.rhai", "event"), file("flows/a.yaml", "a")];
        let bytes = layer(&files).unwrap();
        assert_eq!(bytes, layer(&[files[1].clone(), files[0].clone()]).unwrap());

        let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(&bytes[..]));
        let mut entries = Vec::new();
        for entry in archive.entries().unwrap() {
            let mut entry = entry.unwrap();
            let mut content = String::new();
            entry.read_to_string(&mut content).unwrap();
            entries.push((entry.path().unwrap().display().to_string(), content));
        }
        assert_eq!(
            entries,
            vec![
                ("flows/a.yaml".to_string(), "a".to_string()),
                ("resources/s.rhai".to_string(), "event".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn a_push_without_tags_is_rejected() {
        let push = Push {
            repository: "registry.example.com/a".to_string(),
            credentials_path: None,
        };
        assert!(matches!(push.push(&[], &[]).await, Err(Error::NoTags)));
    }
}
