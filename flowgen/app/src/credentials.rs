//! Credentials added in the web UI: encrypted with SOPS for the configured age
//! recipients, committed under `credentials/`, and decrypted into the folder
//! flows read them from where this instance holds an age key.

use crate::config::CredentialsOptions;
use std::path::Path;
use std::str::FromStr;

/// Failure to read or create the credentials keys.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to read the age key {path}: {source}")]
    ReadKey {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to write the age key {path}: {source}")]
    WriteKey {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("The age key {path} holds no X25519 identity")]
    InvalidKey { path: String },
    #[error("'{recipient}' is not an age public key")]
    InvalidRecipient { recipient: String },
    #[error("Credentials have no one to encrypt for: set `credentials.recipients` or `credentials.identity_path`")]
    NoRecipients,
}

/// The ConfigMap in `credentials/` listing the credentials a deployment
/// mounts, which flowgen rewrites whenever a change adds or removes one.
pub const LIST_FILE: &str = "flowgen-credentials.yaml";

/// The name of the ConfigMap in [`LIST_FILE`].
const LIST_NAME: &str = "flowgen-credentials";

/// Whether `name` can name credentials: a Kubernetes Secret name of lowercase
/// letters, digits and `-`, starting and ending with a letter or digit.
pub fn is_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.starts_with('-')
        && !name.ends_with('-')
}

/// The name of the credentials in `file` within `credentials/`: `a` for `a.yaml`.
fn name_of(file: &str) -> Option<&str> {
    file.strip_suffix(".yaml").filter(|name| is_name(name))
}

/// Issues of `file` under `credentials/`: a Secret named after the file,
/// encrypted with SOPS, holding one file `<name>.json`. One Secret per
/// credentials and one file per Secret keep the mounted files apart.
pub fn validate(file: &str, content: &str) -> Vec<crate::validation::Issue> {
    let issue = |location: Option<String>, message: &str| crate::validation::Issue {
        location,
        message: message.to_string(),
    };
    if file == LIST_FILE {
        return vec![issue(
            None,
            "Written by flowgen when credentials are added or removed",
        )];
    }
    let Some(name) = name_of(file) else {
        return vec![issue(
            None,
            "Credentials are credentials/<name>.yaml, the name in lowercase letters, digits and '-'",
        )];
    };
    let plain = match flowgen_core::sops::plaintext_paths(content, flowgen_core::sops::SECRET_REGEX) {
        Ok(plain) => plain,
        Err(_) => {
            return vec![issue(
                None,
                "Credentials must be a Secret encrypted with SOPS; add them in Connections or with sops",
            )]
        }
    };
    let mut issues: Vec<_> = plain
        .into_iter()
        .map(|path| issue(Some(path), "The value is not encrypted"))
        .collect();
    let document: serde_yaml::Value = match serde_yaml::from_str(content) {
        Ok(document) => document,
        Err(_) => return issues,
    };
    let expected = format!("{name}.json");
    if document.get("kind").and_then(serde_yaml::Value::as_str) != Some("Secret") {
        issues.push(issue(
            Some("kind".to_string()),
            "Credentials must be a Secret",
        ));
    }
    let named = document
        .get("metadata")
        .and_then(|metadata| metadata.get("name"))
        .and_then(serde_yaml::Value::as_str);
    if named != Some(name) {
        issues.push(issue(
            Some("metadata.name".to_string()),
            "The Secret must be named after its file",
        ));
    }
    let mut files = Vec::new();
    for section in ["stringData", "data"] {
        if let Some(serde_yaml::Value::Mapping(entries)) = document.get(section) {
            files.extend(entries.keys().filter_map(serde_yaml::Value::as_str));
        }
    }
    if files != [expected.as_str()] {
        issues.push(issue(
            None,
            "The Secret must hold exactly one file, <name>.json, named after it",
        ));
    }
    issues
}

/// The names of the credentials `folder` holds: the Secrets of a workspace,
/// or the files of a mounted folder.
fn names_in(folder: &Path) -> std::collections::BTreeSet<String> {
    let Ok(entries) = std::fs::read_dir(folder) else {
        return Default::default();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let file = entry.file_name().to_string_lossy().into_owned();
            let name = file
                .strip_suffix(".yaml")
                .or_else(|| file.strip_suffix(".json"))?;
            match file != LIST_FILE && is_name(name) {
                true => Some(name.to_string()),
                false => None,
            }
        })
        .collect()
}

/// The ConfigMap listing `names`, which a deployment reads its mounted
/// credentials from; `None` for no credentials.
fn list(names: &std::collections::BTreeSet<String>) -> Option<String> {
    if names.is_empty() {
        return None;
    }
    let items: String = names
        .iter()
        .map(|name| format!("        - {name}\n"))
        .collect();
    Some(format!(
        "# Written by flowgen: the credentials the deployment mounts, as chart values.\n\
         apiVersion: v1\n\
         kind: ConfigMap\n\
         metadata:\n  name: {LIST_NAME}\n  labels:\n    reconcile.fluxcd.io/watch: Enabled\n\
         data:\n  values.yaml: |\n    flowgen:\n      credentials:\n{items}"
    ))
}

/// The list file after a change that adds or removes credentials, `None` for
/// no list: the credentials in the folder, with the `committed` paths not
/// deployed yet and then the change's `files` applied, each path with whether
/// it keeps a file there.
pub fn list_update(
    options: &CredentialsOptions,
    committed: &[(&str, bool)],
    files: &[(&str, bool)],
) -> Option<Option<String>> {
    let mut before = names_in(&options.path);
    apply(&mut before, committed);
    let mut after = before.clone();
    apply(&mut after, files);
    match before == after {
        true => None,
        false => Some(list(&after)),
    }
}

/// Adds the credentials `files` keep to `names` and removes the ones they delete.
fn apply(names: &mut std::collections::BTreeSet<String>, files: &[(&str, bool)]) {
    let prefix = format!("{}/", flowgen_core::workspace::CREDENTIALS_DIR);
    for (path, kept) in files {
        let Some(name) = path.strip_prefix(&prefix).and_then(name_of) else {
            continue;
        };
        match kept {
            true => names.insert(name.to_string()),
            false => names.remove(name),
        };
    }
}

/// Writes `content` to `path`, readable only by its owner.
pub(crate) fn write_private(path: &Path, content: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
    options.open(path)?.write_all(content)
}

/// The age key at `path`, created when missing.
pub fn identity(path: &Path) -> Result<age::x25519::Identity, Error> {
    let shown = || path.display().to_string();
    match std::fs::read_to_string(path) {
        Ok(text) => text
            .lines()
            .map(str::trim)
            .find(|line| !line.is_empty() && !line.starts_with('#'))
            .and_then(|line| age::x25519::Identity::from_str(line).ok())
            .ok_or_else(|| Error::InvalidKey { path: shown() }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let identity = age::x25519::Identity::generate();
            let text = format!(
                "# public key: {}\n{}\n",
                identity.to_public(),
                age::secrecy::ExposeSecret::expose_secret(&identity.to_string())
            );
            write_private(path, text.as_bytes()).map_err(|source| Error::WriteKey {
                path: shown(),
                source,
            })?;
            Ok(identity)
        }
        Err(source) => Err(Error::ReadKey {
            path: shown(),
            source,
        }),
    }
}

/// Who credentials are encrypted for: the configured recipients and the
/// public key of this instance's own age key.
pub fn recipients(options: &CredentialsOptions) -> Result<Vec<age::x25519::Recipient>, Error> {
    let mut recipients = Vec::with_capacity(options.recipients.len() + 1);
    for recipient in &options.recipients {
        recipients.push(age::x25519::Recipient::from_str(recipient).map_err(|_| {
            Error::InvalidRecipient {
                recipient: recipient.clone(),
            }
        })?);
    }
    if let Some(path) = &options.identity_path {
        let own = identity(path)?.to_public();
        if !recipients
            .iter()
            .any(|recipient| recipient.to_string() == own.to_string())
        {
            recipients.push(own);
        }
    }
    match recipients.is_empty() {
        true => Err(Error::NoRecipients),
        false => Ok(recipients),
    }
}

/// Failure of a request to encrypt credentials, with the status it answers.
#[derive(thiserror::Error, Debug)]
pub enum RequestError {
    #[error("Credentials are not configured")]
    NotConfigured,
    #[error("'{name}' cannot name credentials: use lowercase letters, digits and '-'")]
    InvalidName { name: String },
    #[error("Credentials must be a JSON object")]
    NotJson,
    #[error("No connector '{connector}'")]
    UnknownConnector { connector: String },
    #[error("{connector} credentials need one of its forms")]
    NoForm { connector: &'static str },
    #[error("The credentials don't fit {form}: {message}")]
    Invalid { form: &'static str, message: String },
    #[error(transparent)]
    Keys(#[from] Error),
    #[error("Failed to encrypt the credentials: {0}")]
    Encrypt(#[from] flowgen_core::sops::Error),
}

impl axum::response::IntoResponse for RequestError {
    fn into_response(self) -> axum::response::Response {
        use axum::http::StatusCode;
        let status = match &self {
            RequestError::NotConfigured => StatusCode::NOT_FOUND,
            RequestError::InvalidName { .. }
            | RequestError::NotJson
            | RequestError::UnknownConnector { .. }
            | RequestError::NoForm { .. }
            | RequestError::Invalid { .. } => StatusCode::BAD_REQUEST,
            RequestError::Keys(_) | RequestError::Encrypt(_) => {
                tracing::warn!(error = %self, "Failed to encrypt credentials");
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to encrypt the credentials",
                )
                    .into_response();
            }
        };
        (status, self.to_string()).into_response()
    }
}

/// Checks the credentials against the connector form that made them, as its tasks read them.
fn check(input: &flowgen_client::types::CredentialInput) -> Result<(), RequestError> {
    let connector = match crate::connections::connector(&input.connector) {
        Some(connector) => connector,
        None => {
            return Err(RequestError::UnknownConnector {
                connector: input.connector.clone(),
            })
        }
    };
    let form = match input.form.map(usize::try_from) {
        Some(Ok(index)) => connector.credentials.get(index),
        _ => None,
    };
    match (form, connector.credentials.is_empty()) {
        (Some(form), _) => match (form.check)(&input.content) {
            Ok(()) => Ok(()),
            Err(message) => Err(RequestError::Invalid {
                form: form.label,
                message,
            }),
        },
        (None, true) => Ok(()),
        (None, false) => Err(RequestError::NoForm {
            connector: connector.label,
        }),
    }
}

/// `POST /api/credentials/encrypt`: the credentials as the encrypted Secret
/// `credentials/<name>.yaml`, to propose as a change; nothing is kept.
pub(crate) async fn encrypt(
    axum::extract::State(state): axum::extract::State<std::sync::Arc<crate::web::WebState>>,
    axum::Json(input): axum::Json<flowgen_client::types::CredentialInput>,
) -> Result<axum::Json<flowgen_client::types::WorkspaceFile>, RequestError> {
    let options = match &state.app_config.credentials {
        Some(options) => options,
        None => return Err(RequestError::NotConfigured),
    };
    if !is_name(&input.name) {
        return Err(RequestError::InvalidName { name: input.name });
    }
    if serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&input.content).is_err() {
        return Err(RequestError::NotJson);
    }
    check(&input)?;
    let recipients = recipients(options)?;
    let file = format!("{}.json", input.name);
    let content =
        flowgen_core::sops::encrypt_secret(&input.name, &file, &input.content, &recipients)?;
    Ok(axum::Json(flowgen_client::types::WorkspaceFile {
        path: format!(
            "{}/{}.yaml",
            flowgen_core::workspace::CREDENTIALS_DIR,
            input.name
        ),
        content: Some(content),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(
        connector: &str,
        form: Option<u64>,
        content: &str,
    ) -> flowgen_client::types::CredentialInput {
        flowgen_client::types::CredentialInput {
            name: "a".to_string(),
            content: content.to_string(),
            connector: connector.to_string(),
            form,
        }
    }

    #[test]
    fn credentials_are_checked_against_the_form_that_made_them() {
        assert!(check(&input("http", Some(0), r#"{"bearer_auth": "t"}"#)).is_ok());
        assert!(matches!(
            check(&input("http", Some(0), r#"{"bearer_auth": 1}"#)),
            Err(RequestError::Invalid {
                form: "Bearer token",
                ..
            })
        ));
        assert!(matches!(
            check(&input("http", None, r#"{"bearer_auth": "t"}"#)),
            Err(RequestError::NoForm { .. })
        ));
        assert!(matches!(
            check(&input("unknown", Some(0), "{}")),
            Err(RequestError::UnknownConnector { .. })
        ));
        assert!(check(&input("kv", None, r#"{"any": "thing"}"#)).is_ok());
    }

    fn secret(name: &str, file: &str, recipient: &age::x25519::Recipient) -> String {
        flowgen_core::sops::encrypt_secret(
            name,
            file,
            r#"{"api_key": "a"}"#,
            std::slice::from_ref(recipient),
        )
        .unwrap()
    }

    #[test]
    fn credentials_are_one_encrypted_secret_named_after_its_file() {
        let recipient = age::x25519::Identity::generate().to_public();
        let valid = secret("a", "a.json", &recipient);
        let mut leaked: serde_json::Value = serde_json::from_str(&valid).unwrap();
        leaked["stringData"]["b.json"] = "secret".into();
        let locations = |file: &str, content: &str| -> Vec<Option<String>> {
            validate(file, content)
                .into_iter()
                .map(|issue| issue.location)
                .collect()
        };

        assert!(validate("a.yaml", &valid).is_empty());
        assert_eq!(
            locations("a.yaml", &leaked.to_string()),
            [Some("stringData.b.json".to_string()), None]
        );
        assert_eq!(
            locations("b.yaml", &valid),
            [Some("metadata.name".to_string()), None]
        );
        assert_eq!(
            locations("a.yaml", &secret("a", "other.json", &recipient)),
            [None]
        );
        assert_eq!(locations("a.yaml", r#"{"api_key": "secret"}"#), [None]);
        assert_eq!(locations("A.yaml", &valid), [None]);
        assert_eq!(locations(LIST_FILE, &valid), [None]);
    }

    #[test]
    fn adding_or_removing_credentials_rewrites_the_list() {
        let folder = tempfile::tempdir().unwrap();
        std::fs::write(folder.path().join("a.yaml"), "").unwrap();
        std::fs::write(folder.path().join(LIST_FILE), "").unwrap();
        let options = CredentialsOptions {
            recipients: Vec::new(),
            identity_path: None,
            path: folder.path().to_path_buf(),
        };

        let added = list_update(&options, &[], &[("credentials/b.yaml", true)]);
        let removed = list_update(&options, &[], &[("credentials/a.yaml", false)]);
        let rotated = list_update(&options, &[], &[("credentials/a.yaml", true)]);
        let after_committed = list_update(
            &options,
            &[("credentials/c.yaml", true)],
            &[("credentials/b.yaml", true)],
        );

        assert!(added
            .unwrap()
            .unwrap()
            .contains("        - a\n        - b\n"));
        assert_eq!(removed.unwrap(), None);
        assert!(rotated.is_none());
        assert!(after_committed
            .unwrap()
            .unwrap()
            .contains("        - a\n        - b\n        - c\n"));
    }

    #[test]
    fn a_missing_key_is_created_private_and_then_read_back() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("state").join("age.key");

        let created = identity(&path).unwrap();
        let read = identity(&path).unwrap();

        assert_eq!(
            created.to_public().to_string(),
            read.to_public().to_string()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
    }

    #[test]
    fn the_own_key_is_a_recipient_once_and_a_bad_one_is_rejected() {
        let folder = tempfile::tempdir().unwrap();
        let path = folder.path().join("age.key");
        let own = identity(&path).unwrap().to_public().to_string();
        let options = CredentialsOptions {
            recipients: vec![own.clone()],
            identity_path: Some(path),
            path: folder.path().join("credentials"),
        };

        let found: Vec<String> = recipients(&options)
            .unwrap()
            .iter()
            .map(ToString::to_string)
            .collect();
        let bad = CredentialsOptions {
            recipients: vec!["age1nope".to_string()],
            ..options
        };

        assert_eq!(found, [own]);
        assert!(matches!(
            recipients(&bad),
            Err(Error::InvalidRecipient { .. })
        ));
    }
}
