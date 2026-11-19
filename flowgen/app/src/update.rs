//! `flowgen update` brings this binary to the newest release: through
//! Homebrew when it installed flowgen, else by replacing the binary with the
//! release's once its checksum matches. A container is updated by its image.

use crate::releases::{is_newer, newest_release, CURRENT_VERSION};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Where release archives are downloaded from, by tag.
const RELEASES_DOWNLOAD: &str = "https://github.com/connve/flowgen/releases/download";

/// The binary inside a release archive.
const BINARY: &str = "flowgen";

/// How long downloading a release archive may take.
const DOWNLOAD_TIMEOUT: Duration = Duration::from_secs(300);

/// How flowgen was installed, which decides how it updates.
#[derive(Debug, PartialEq)]
pub enum Install {
    /// By Homebrew, which updates it.
    Homebrew,
    /// In a container, updated by its image.
    Container,
    /// As a file, replaced in place.
    Binary(PathBuf),
}

/// What an update did.
#[derive(Debug, PartialEq)]
pub enum Outcome {
    /// This is the newest release.
    UpToDate,
    /// Homebrew updated flowgen, or found nothing to update.
    Homebrew,
    /// A container runs an older release than `latest`.
    Container { latest: String },
    /// The binary was replaced with release `version`.
    Updated { version: String },
}

/// Failure to update flowgen.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to find this binary: {source}")]
    CurrentExe {
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to check the newest release: {source}")]
    Check {
        #[source]
        source: reqwest::Error,
    },
    #[error("No flowgen release is built for {os} on {arch}")]
    Unsupported {
        os: &'static str,
        arch: &'static str,
    },
    #[error("Failed to download {url}: {source}")]
    Download {
        url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("The download of {archive} does not match its checksum")]
    Checksum { archive: String },
    #[error("Failed to read {archive}: {source}")]
    Archive {
        archive: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{archive} holds no flowgen binary")]
    MissingBinary { archive: String },
    #[error(
        "Failed to write {path}: {source}; run it as a user who can, or install flowgen again"
    )]
    Write {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to run {command}: {source}")]
    Command {
        command: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{command} failed")]
    CommandFailed { command: String },
}

/// How this flowgen was installed.
pub fn install() -> Result<Install, Error> {
    if in_container() {
        return Ok(Install::Container);
    }
    let exe = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|source| Error::CurrentExe { source })?;
    match installed_by_homebrew(&exe) {
        true => Ok(Install::Homebrew),
        false => Ok(Install::Binary(exe)),
    }
}

/// Files a container runtime leaves in its containers: Docker's and Podman's.
const CONTAINER_MARKERS: [&str; 2] = ["/.dockerenv", "/run/.containerenv"];

/// Whether flowgen runs in a container, on Kubernetes, Docker or Podman.
pub fn in_container() -> bool {
    std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
        || CONTAINER_MARKERS
            .iter()
            .any(|marker| Path::new(marker).exists())
}

/// Whether `exe` sits in a Homebrew Cellar, where its formulae install.
fn installed_by_homebrew(exe: &Path) -> bool {
    exe.components()
        .any(|component| component.as_os_str() == "Cellar")
}

/// Updates flowgen the way `install` calls for.
pub async fn run(install: &Install) -> Result<Outcome, Error> {
    if let Install::Homebrew = install {
        brew(&["update"]).await?;
        brew(&["upgrade", BINARY]).await?;
        return Ok(Outcome::Homebrew);
    }
    let client = crate::releases::client().map_err(|source| Error::Check { source })?;
    let url = crate::config::UpdateCheckOptions::default().url;
    let latest = newest_release(&client, &url)
        .await
        .map_err(|source| Error::Check { source })?;
    if !is_newer(&latest, CURRENT_VERSION) {
        return Ok(Outcome::UpToDate);
    }
    match install {
        Install::Binary(exe) => {
            replace(exe, &download(&latest).await?)?;
            Ok(Outcome::Updated { version: latest })
        }
        _ => Ok(Outcome::Container { latest }),
    }
}

/// Runs `brew` with `args`, its output shown as it goes.
async fn brew(args: &[&str]) -> Result<(), Error> {
    let command = format!("brew {}", args.join(" "));
    let status = tokio::process::Command::new("brew")
        .args(args)
        .status()
        .await
        .map_err(|source| Error::Command {
            command: command.clone(),
            source,
        })?;
    match status.success() {
        true => Ok(()),
        false => Err(Error::CommandFailed { command }),
    }
}

/// The release archive of `version` for this platform.
fn archive_name(version: &str) -> Result<String, Error> {
    let (os, arch) = (std::env::consts::OS, std::env::consts::ARCH);
    let unsupported = || Error::Unsupported { os, arch };
    let os = match os {
        "macos" => "darwin",
        "linux" => "linux",
        _ => return Err(unsupported()),
    };
    let arch = match arch {
        "aarch64" => "arm64",
        "x86_64" => "amd64",
        _ => return Err(unsupported()),
    };
    Ok(format!("{BINARY}-{os}-{arch}-{version}.tar.gz"))
}

/// The flowgen binary of release `version`, its archive checked against the
/// checksum published with it.
async fn download(version: &str) -> Result<Vec<u8>, Error> {
    let archive = archive_name(version)?;
    let client = reqwest::Client::builder()
        .timeout(DOWNLOAD_TIMEOUT)
        .user_agent(format!("{BINARY}/{CURRENT_VERSION}"))
        .build()
        .map_err(|source| Error::Check { source })?;
    let url = format!("{RELEASES_DOWNLOAD}/v{version}/{archive}");
    let bytes = fetch(&client, &url).await?;
    let checksum = fetch(&client, &format!("{url}.sha256")).await?;
    verify(&bytes, &checksum, &archive)?;
    extract(&bytes, &archive)
}

async fn fetch(client: &reqwest::Client, url: &str) -> Result<Vec<u8>, Error> {
    let failed = |source| Error::Download {
        url: url.to_string(),
        source,
    };
    let response = client
        .get(url)
        .send()
        .await
        .and_then(reqwest::Response::error_for_status)
        .map_err(failed)?;
    let bytes = response.bytes().await.map_err(failed)?;
    Ok(bytes.to_vec())
}

/// Checks `bytes` against `checksum`, a SHA-256 in hex as `sha256sum` writes it.
fn verify(bytes: &[u8], checksum: &[u8], archive: &str) -> Result<(), Error> {
    let checksum = String::from_utf8_lossy(checksum);
    let actual = format!("{:x}", Sha256::digest(bytes));
    match checksum.split_whitespace().next() {
        Some(expected) if expected.eq_ignore_ascii_case(&actual) => Ok(()),
        _ => Err(Error::Checksum {
            archive: archive.to_string(),
        }),
    }
}

/// The flowgen binary in the tar.gz `archive`.
fn extract(bytes: &[u8], archive: &str) -> Result<Vec<u8>, Error> {
    let read = |source| Error::Archive {
        archive: archive.to_string(),
        source,
    };
    let mut entries = tar::Archive::new(flate2::read::GzDecoder::new(bytes));
    for entry in entries.entries().map_err(read)? {
        let mut entry = entry.map_err(read)?;
        if entry.path().map_err(read)?.as_os_str() == BINARY {
            let mut binary = Vec::new();
            entry.read_to_end(&mut binary).map_err(read)?;
            return Ok(binary);
        }
    }
    Err(Error::MissingBinary {
        archive: archive.to_string(),
    })
}

/// Replaces `exe` with `binary`: written next to it, then renamed over it,
/// so `exe` is whole at every moment.
fn replace(exe: &Path, binary: &[u8]) -> Result<(), Error> {
    let staged = exe.with_file_name(format!(".{BINARY}-update"));
    let failed = |path: &Path, source| Error::Write {
        path: path.display().to_string(),
        source,
    };
    std::fs::write(&staged, binary).map_err(|source| failed(&staged, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o755))
            .map_err(|source| failed(&staged, source))?;
    }
    std::fs::rename(&staged, exe).map_err(|source| {
        // The rename's error is the one to report; a staged file left
        // behind is overwritten by the next update.
        let _ = std::fs::remove_file(&staged);
        failed(exe, source)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn archive_of(name: &str, content: &[u8]) -> Vec<u8> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        let mut builder = tar::Builder::new(encoder);
        let mut header = tar::Header::new_gnu();
        header.set_size(content.len() as u64);
        header.set_mode(0o755);
        builder.append_data(&mut header, name, content).unwrap();
        builder.into_inner().unwrap().finish().unwrap()
    }

    #[test]
    fn a_binary_in_a_homebrew_cellar_was_installed_by_homebrew() {
        assert!(installed_by_homebrew(Path::new(
            "/opt/homebrew/Cellar/flowgen/0.157.0/bin/flowgen"
        )));
        assert!(installed_by_homebrew(Path::new(
            "/home/linuxbrew/.linuxbrew/Cellar/flowgen/0.157.0/bin/flowgen"
        )));
        assert!(!installed_by_homebrew(Path::new(
            "/Users/someone/.local/bin/flowgen"
        )));
    }

    #[test]
    fn the_archive_is_named_after_this_platform_and_the_version() {
        let name = archive_name("0.158.0").unwrap();
        assert!(name.starts_with("flowgen-"));
        assert!(name.ends_with("-0.158.0.tar.gz"));
    }

    #[test]
    fn a_download_matching_its_checksum_passes_and_any_other_fails() {
        let bytes = b"release";
        let checksum = format!("{:x}\n", Sha256::digest(bytes));

        assert!(verify(bytes, checksum.as_bytes(), "a.tar.gz").is_ok());
        assert!(matches!(
            verify(b"tampered", checksum.as_bytes(), "a.tar.gz"),
            Err(Error::Checksum { .. })
        ));
        assert!(matches!(
            verify(bytes, b"", "a.tar.gz"),
            Err(Error::Checksum { .. })
        ));
    }

    #[test]
    fn the_binary_is_read_from_the_archive_and_its_absence_is_an_error() {
        let archive = archive_of("flowgen", b"binary");

        assert_eq!(extract(&archive, "a.tar.gz").unwrap(), b"binary");
        assert!(matches!(
            extract(&archive_of("other", b"x"), "a.tar.gz"),
            Err(Error::MissingBinary { .. })
        ));
    }

    #[test]
    fn replacing_the_binary_leaves_the_new_one_executable_and_nothing_staged() {
        let folder = tempfile::tempdir().unwrap();
        let exe = folder.path().join("flowgen");
        std::fs::write(&exe, b"old").unwrap();

        replace(&exe, b"new").unwrap();

        assert_eq!(std::fs::read(&exe).unwrap(), b"new");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&exe).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o755);
        }
        assert_eq!(std::fs::read_dir(folder.path()).unwrap().count(), 1);
    }
}
