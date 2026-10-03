//! Registry access shared by `oci_sync` and `oci_push`: credentials loading
//! and client setup.

use oci_client::client::{ClientConfig, ClientProtocol};
use oci_client::secrets::RegistryAuth;
use oci_client::{Client, Reference};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Failure to load registry credentials.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to read credentials file '{path:?}': {source}")]
    ReadCredentials {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file '{path:?}': {source}")]
    ParseCredentials {
        path: PathBuf,
        #[source]
        source: serde_json::Error,
    },
}

impl Error {
    /// Whether loading the same file again cannot succeed.
    pub fn is_permanent(&self) -> bool {
        matches!(self, Error::ParseCredentials { .. })
    }
}

/// Registry credentials loaded from the credentials JSON file.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Credentials {
    /// Registry username. For GHCR with a Personal Access Token, this is
    /// the GitHub username; with a GitHub Actions token, it's the actor.
    pub username: String,
    /// Registry password or token.
    pub password: String,
}

/// Builds the registry auth from `credentials_path`, auto-detecting either
/// a `{username, password}` JSON file or a Docker
/// `config.json` with multiple registry entries. Returns anonymous if no
/// path is configured.
pub async fn load_auth(
    credentials_path: Option<&Path>,
    registry_host: &str,
) -> Result<RegistryAuth, Error> {
    let path = match credentials_path {
        Some(p) => p,
        None => return Ok(RegistryAuth::Anonymous),
    };

    let content =
        tokio::fs::read_to_string(path)
            .await
            .map_err(|source| Error::ReadCredentials {
                path: path.to_path_buf(),
                source,
            })?;

    if let Ok(cfg) = serde_json::from_str::<DockerConfig>(&content) {
        if !cfg.auths.is_empty() {
            return Ok(pick_docker_auth(&cfg, registry_host));
        }
    }

    let creds: Credentials =
        serde_json::from_str(&content).map_err(|source| Error::ParseCredentials {
            path: path.to_path_buf(),
            source,
        })?;
    Ok(RegistryAuth::Basic(creds.username, creds.password))
}

/// Registry client for `reference`. Loopback hosts (local registries in
/// tests) do not serve TLS; anything else stays on HTTPS.
pub fn client(reference: &Reference) -> Client {
    let protocol = match is_loopback(reference.registry()) {
        true => ClientProtocol::Http,
        false => ClientProtocol::Https,
    };
    Client::new(ClientConfig {
        protocol,
        ..Default::default()
    })
}

/// Whether `registry` (a host with an optional port) is this machine.
fn is_loopback(registry: &str) -> bool {
    let host = match registry.rsplit_once(':') {
        Some((host, port)) if !host.ends_with(':') && port.bytes().all(|b| b.is_ascii_digit()) => {
            host
        }
        _ => registry,
    };
    matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

/// Picks the auth entry whose host matches the artifact's registry. Falls
/// back to anonymous if no entry matches, so public artifacts still pull
/// when an unrelated dockerconfigjson is mounted.
fn pick_docker_auth(cfg: &DockerConfig, registry_host: &str) -> RegistryAuth {
    for (auth_host, entry) in cfg.auths.iter() {
        if registry_host_matches(auth_host, registry_host) {
            if let Some(auth_b64) = &entry.auth {
                if let Some((user, pass)) = decode_basic_auth(auth_b64) {
                    return RegistryAuth::Basic(user, pass);
                }
            }
            if let (Some(user), Some(pass)) = (&entry.username, &entry.password) {
                return RegistryAuth::Basic(user.clone(), pass.clone());
            }
        }
    }
    RegistryAuth::Anonymous
}

/// Loose host match: dockerconfigjson entries are URLs (`https://index.docker.io/v1/`)
/// or bare hosts (`ghcr.io`), so only the host segment is compared.
fn registry_host_matches(auth_host: &str, registry_host: &str) -> bool {
    let normalized = auth_host
        .trim_start_matches("https://")
        .trim_start_matches("http://");
    let normalized = normalized.split('/').next().unwrap_or(normalized);
    normalized == registry_host
}

/// Decodes the base64-encoded `auth` field (`<user>:<pass>`) used by Docker
/// configs. Returns `None` if the value is malformed.
fn decode_basic_auth(b64: &str) -> Option<(String, String)> {
    use base64::Engine;
    let decoded = base64::engine::general_purpose::STANDARD.decode(b64).ok()?;
    let s = String::from_utf8(decoded).ok()?;
    let (user, pass) = s.split_once(':')?;
    Some((user.to_string(), pass.to_string()))
}

/// Docker `config.json`.
#[derive(serde::Deserialize)]
struct DockerConfig {
    /// Entries keyed by registry host or URL.
    #[serde(default)]
    auths: std::collections::HashMap<String, DockerConfigAuth>,
}

/// One registry entry of a Docker `config.json`.
#[derive(serde::Deserialize)]
struct DockerConfigAuth {
    /// Base64 of `<user>:<pass>`.
    #[serde(default)]
    auth: Option<String>,
    /// Username, when `auth` is absent.
    #[serde(default)]
    username: Option<String>,
    /// Password, when `auth` is absent.
    #[serde(default)]
    password: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_display_read_credentials() {
        let err = Error::ReadCredentials {
            path: PathBuf::from("/etc/missing.json"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        };
        assert!(err.to_string().contains("/etc/missing.json"));
        assert!(err.to_string().contains("Failed to read credentials"));
        assert!(!err.is_permanent());
    }

    #[test]
    fn error_display_parse_credentials() {
        let serde_err = serde_json::from_str::<Credentials>("not json").unwrap_err();
        let err = Error::ParseCredentials {
            path: PathBuf::from("/creds.json"),
            source: serde_err,
        };
        assert!(err.to_string().contains("/creds.json"));
        assert!(err.to_string().contains("Failed to parse credentials"));
        assert!(err.is_permanent());
    }

    #[test]
    fn credentials_deser() {
        let json = r#"{ "username": "robot", "password": "tok123" }"#;
        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.username, "robot");
        assert_eq!(creds.password, "tok123");
    }

    #[test]
    fn credentials_default_values() {
        let creds = Credentials::default();
        assert_eq!(creds.username, "");
        assert_eq!(creds.password, "");
    }

    #[test]
    fn only_exact_loopback_hosts_use_plain_http() {
        for local in [
            "localhost",
            "localhost:5000",
            "127.0.0.1:5000",
            "[::1]",
            "[::1]:5000",
        ] {
            assert!(is_loopback(local), "{local}");
        }
        for remote in ["localhost.example.com", "127.0.0.1.nip.io:5000", "ghcr.io"] {
            assert!(!is_loopback(remote), "{remote}");
        }
    }

    #[test]
    fn registry_host_matches_basic() {
        assert!(registry_host_matches("ghcr.io", "ghcr.io"));
        assert!(registry_host_matches("https://ghcr.io", "ghcr.io"));
        assert!(registry_host_matches(
            "https://index.docker.io/v1/",
            "index.docker.io"
        ));
        assert!(!registry_host_matches("ghcr.io", "registry.gitlab.com"));
    }

    #[test]
    fn decode_basic_auth_round_trip() {
        let (u, p) = decode_basic_auth("cm9ib3Q6dG9rMTIz").unwrap();
        assert_eq!(u, "robot");
        assert_eq!(p, "tok123");
    }

    #[test]
    fn pick_docker_auth_matches_host() {
        let cfg: DockerConfig = serde_json::from_str(
            r#"{
                "auths": {
                    "ghcr.io": { "auth": "cm9ib3Q6dG9rMTIz" },
                    "registry.gitlab.com": { "username": "alice", "password": "secret" }
                }
            }"#,
        )
        .unwrap();
        let auth = pick_docker_auth(&cfg, "ghcr.io");
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "robot" && p == "tok123"));

        let auth = pick_docker_auth(&cfg, "registry.gitlab.com");
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "alice" && p == "secret"));

        let auth = pick_docker_auth(&cfg, "unrelated.example.com");
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[test]
    fn pick_docker_auth_username_password_only() {
        let cfg: DockerConfig = serde_json::from_str(
            r#"{
                "auths": {
                    "ghcr.io": { "username": "alice", "password": "secret" }
                }
            }"#,
        )
        .unwrap();
        let auth = pick_docker_auth(&cfg, "ghcr.io");
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "alice" && p == "secret"));
    }

    #[test]
    fn pick_docker_auth_empty_entry_falls_back_to_anonymous() {
        let cfg: DockerConfig = serde_json::from_str(
            r#"{
                "auths": {
                    "ghcr.io": {}
                }
            }"#,
        )
        .unwrap();
        let auth = pick_docker_auth(&cfg, "ghcr.io");
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[test]
    fn decode_basic_auth_rejects_bad_base64() {
        assert!(decode_basic_auth("not-base64!@#").is_none());
    }

    #[test]
    fn decode_basic_auth_rejects_missing_colon() {
        assert!(decode_basic_auth("bm9jb2xvbg==").is_none());
    }

    #[tokio::test]
    async fn load_auth_anonymous_when_no_path() {
        let auth = load_auth(None, "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[tokio::test]
    async fn load_auth_flowgen_native_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("creds.json");
        tokio::fs::write(&path, r#"{"username":"u","password":"p"}"#)
            .await
            .unwrap();
        let auth = load_auth(Some(&path), "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "u" && p == "p"));
    }

    #[tokio::test]
    async fn load_auth_dockerconfigjson_format() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        tokio::fs::write(&path, r#"{"auths":{"ghcr.io":{"auth":"cm9ib3Q6dG9r"}}}"#)
            .await
            .unwrap();
        let auth = load_auth(Some(&path), "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Basic(u, p) if u == "robot" && p == "tok"));
    }

    #[tokio::test]
    async fn load_auth_dockerconfigjson_no_matching_host_falls_back() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("config.json");
        tokio::fs::write(
            &path,
            r#"{"auths":{"registry.gitlab.com":{"auth":"YTpi"}}}"#,
        )
        .await
        .unwrap();
        let auth = load_auth(Some(&path), "ghcr.io").await.unwrap();
        assert!(matches!(auth, RegistryAuth::Anonymous));
    }

    #[tokio::test]
    async fn load_auth_missing_file_errors() {
        let result = load_auth(Some(Path::new("/nope/missing.json")), "ghcr.io").await;
        assert!(matches!(result, Err(Error::ReadCredentials { .. })));
    }

    #[tokio::test]
    async fn load_auth_malformed_json_errors() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.json");
        tokio::fs::write(&path, "{").await.unwrap();
        let result = load_auth(Some(&path), "ghcr.io").await;
        assert!(matches!(result, Err(Error::ParseCredentials { .. })));
    }
}
