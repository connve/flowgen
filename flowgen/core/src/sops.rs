//! Files encrypted the way SOPS encrypts them with age keys, so the `sops`
//! CLI and Flux decrypt what flowgen writes, and flowgen decrypts what they
//! write. Every value is encrypted except under keys ending in
//! [`UNENCRYPTED_SUFFIX`]; keys stay readable.

use aes_gcm::aead::consts::U32;
use aes_gcm::aead::rand_core::RngCore;
use aes_gcm::aead::{Aead, KeyInit, OsRng, Payload};
use aes_gcm::aes::Aes256;
use aes_gcm::AesGcm;
use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde_yaml::{Mapping, Value};
use sha2::{Digest, Sha512};
use std::collections::BTreeMap;
use std::io::{Read, Write};

/// Values under a key ending in this stay unencrypted, as in SOPS.
pub const UNENCRYPTED_SUFFIX: &str = "_unencrypted";

/// The variable naming the age key file, as the `sops` CLI reads it.
pub const AGE_KEY_FILE_ENV: &str = "SOPS_AGE_KEY_FILE";

/// The SOPS version recorded in the files flowgen writes.
const VERSION: &str = "3.13.2";

/// The top-level key holding the SOPS metadata.
const METADATA_KEY: &str = "sops";

/// What starts an encrypted value.
const ENCRYPTED_PREFIX: &str = "ENC[AES256_GCM,";

/// The age keys a file can be decrypted with, as `age::IdentityFile` reads them.
pub type Identities = [Box<dyn age::Identity + Send + Sync>];

/// AES-256-GCM with the 32-byte nonce SOPS uses.
type Cipher = AesGcm<Aes256, U32>;

const NONCE_LEN: usize = 32;
const TAG_LEN: usize = 16;
const DATA_KEY_LEN: usize = 32;

/// Failure to encrypt or decrypt a SOPS file.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("The file is not JSON or YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("Failed to write the file: {0}")]
    Json(#[from] serde_json::Error),
    #[error("The file has no SOPS metadata")]
    NotEncrypted,
    #[error("The SOPS metadata has no {field}")]
    MissingMetadata { field: &'static str },
    #[error("No age key can decrypt the file")]
    NoIdentity,
    #[error("Failed to encrypt for age: {0}")]
    Age(String),
    #[error("No age recipient to encrypt the file for")]
    NoRecipients,
    #[error("The value at '{path}' is not a valid SOPS value")]
    InvalidValue { path: String },
    #[error("The value at '{path}' does not decrypt; the file was changed or moved")]
    Tampered { path: String },
    #[error("The file's MAC does not match its values")]
    MacMismatch,
    #[error("A key under '{path}' is not a string")]
    NonStringKey { path: String },
}

fn cipher(data_key: &[u8]) -> Cipher {
    Cipher::new(aes_gcm::Key::<Cipher>::from_slice(data_key))
}

/// A leaf as SOPS turns it into bytes for encryption and the MAC.
fn plaintext(value: &Value) -> Option<(Vec<u8>, &'static str)> {
    match value {
        Value::String(text) => Some((text.clone().into_bytes(), "str")),
        Value::Bool(true) => Some((b"True".to_vec(), "bool")),
        Value::Bool(false) => Some((b"False".to_vec(), "bool")),
        Value::Number(number) => match (number.as_i64(), number.as_u64(), number.as_f64()) {
            (Some(integer), _, _) => Some((integer.to_string().into_bytes(), "int")),
            (None, Some(integer), _) => Some((integer.to_string().into_bytes(), "int")),
            (None, None, Some(float)) => Some((float.to_string().into_bytes(), "float")),
            (None, None, None) => None,
        },
        _ => None,
    }
}

fn aad(path: &[String]) -> String {
    let mut aad = path.join(":");
    aad.push(':');
    aad
}

fn encrypt_value(
    data_key: &[u8],
    plaintext: &[u8],
    kind: &str,
    aad: &str,
) -> Result<String, Error> {
    let mut nonce = [0u8; NONCE_LEN];
    OsRng.fill_bytes(&mut nonce);
    let sealed = cipher(data_key)
        .encrypt(
            aes_gcm::Nonce::<U32>::from_slice(&nonce),
            Payload {
                msg: plaintext,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| Error::Age("AES-GCM encryption failed".to_string()))?;
    let (data, tag) = sealed.split_at(sealed.len() - TAG_LEN);
    Ok(format!(
        "{ENCRYPTED_PREFIX}data:{},iv:{},tag:{},type:{kind}]",
        STANDARD.encode(data),
        STANDARD.encode(nonce),
        STANDARD.encode(tag)
    ))
}

/// The fields of an `ENC[...]` value.
struct Sealed {
    data: Vec<u8>,
    nonce: Vec<u8>,
    tag: Vec<u8>,
    kind: String,
}

/// The fields of an `ENC[...]` value at `path`, or `None` for a plain value.
fn sealed(text: &str, path: &str) -> Result<Option<Sealed>, Error> {
    let Some(fields) = text
        .strip_prefix(ENCRYPTED_PREFIX)
        .and_then(|rest| rest.strip_suffix(']'))
    else {
        return Ok(None);
    };
    let invalid = || Error::InvalidValue {
        path: path.to_string(),
    };
    let mut parts = BTreeMap::new();
    for field in fields.split(',') {
        let (name, value) = field.split_once(':').ok_or_else(invalid)?;
        parts.insert(name, value);
    }
    let decode = |name: &str| -> Result<Vec<u8>, Error> {
        let value = parts.get(name).ok_or_else(invalid)?;
        STANDARD.decode(value).map_err(|_| invalid())
    };
    let (data, nonce, tag) = (decode("data")?, decode("iv")?, decode("tag")?);
    if nonce.len() != NONCE_LEN || tag.len() != TAG_LEN {
        return Err(invalid());
    }
    let kind = parts.get("type").ok_or_else(invalid)?.to_string();
    Ok(Some(Sealed {
        data,
        nonce,
        tag,
        kind,
    }))
}

/// The plaintext and type of an `ENC[...]` value, or `None` for a plain value.
fn decrypt_value(
    data_key: &[u8],
    text: &str,
    aad: &str,
) -> Result<Option<(Vec<u8>, String)>, Error> {
    let Some(Sealed {
        mut data,
        nonce,
        tag,
        kind,
    }) = sealed(text, aad)?
    else {
        return Ok(None);
    };
    data.extend_from_slice(&tag);
    let opened = cipher(data_key)
        .decrypt(
            aes_gcm::Nonce::<U32>::from_slice(&nonce),
            Payload {
                msg: &data,
                aad: aad.as_bytes(),
            },
        )
        .map_err(|_| Error::Tampered {
            path: aad.to_string(),
        })?;
    Ok(Some((opened, kind)))
}

/// A decrypted value back in its type.
fn typed(bytes: Vec<u8>, kind: &str, path: &str) -> Result<Value, Error> {
    let invalid = || Error::InvalidValue {
        path: path.to_string(),
    };
    let text = String::from_utf8(bytes).map_err(|_| invalid())?;
    match kind {
        "str" | "bytes" | "comment" => Ok(Value::String(text)),
        "bool" => Ok(Value::Bool(text == "True")),
        "int" | "float" => serde_yaml::from_str(&text).map_err(|_| invalid()),
        _ => Err(invalid()),
    }
}

/// What [`walk`] calls on each scalar, with its key path.
type Leaf<'a> = dyn FnMut(&[String], &mut Value) -> Result<(), Error> + 'a;

/// Calls `leaf` on every scalar in document order with its key path; list
/// items share their list's path, as in SOPS.
fn walk(value: &mut Value, path: &mut Vec<String>, leaf: &mut Leaf<'_>) -> Result<(), Error> {
    match value {
        Value::Mapping(mapping) => {
            for (key, value) in mapping.iter_mut() {
                let Value::String(key) = key else {
                    return Err(Error::NonStringKey {
                        path: path.join("."),
                    });
                };
                path.push(key.clone());
                walk(value, path, leaf)?;
                path.pop();
            }
            Ok(())
        }
        Value::Sequence(items) => {
            for item in items {
                walk(item, path, leaf)?;
            }
            Ok(())
        }
        Value::Tagged(tagged) => walk(&mut tagged.value, path, leaf),
        Value::Null => Ok(()),
        scalar => leaf(path, scalar),
    }
}

/// Which values SOPS encrypts, as the file's metadata records it.
enum Rule {
    /// Every value except under keys ending in [`UNENCRYPTED_SUFFIX`].
    UnencryptedSuffix,
    /// Only values under keys the pattern matches.
    EncryptedRegex(regex::Regex),
}

impl Rule {
    /// Values under keys `pattern` matches.
    fn encrypted_regex(pattern: &str) -> Result<Self, Error> {
        regex::Regex::new(pattern)
            .map(Rule::EncryptedRegex)
            .map_err(|_| Error::MissingMetadata {
                field: "valid encrypted_regex",
            })
    }

    fn encrypts(&self, path: &[String]) -> bool {
        match self {
            Rule::UnencryptedSuffix => !path.iter().any(|key| key.ends_with(UNENCRYPTED_SUFFIX)),
            Rule::EncryptedRegex(pattern) => path.iter().any(|key| pattern.is_match(key)),
        }
    }

    fn record(&self, metadata: &mut Mapping) {
        match self {
            Rule::UnencryptedSuffix => {
                metadata.insert("unencrypted_suffix".into(), UNENCRYPTED_SUFFIX.into())
            }
            Rule::EncryptedRegex(pattern) => {
                metadata.insert("encrypted_regex".into(), pattern.as_str().into())
            }
        };
    }
}

/// What SOPS encrypts in a Kubernetes Secret: the values under `data` and `stringData`.
pub const SECRET_REGEX: &str = "^(data|stringData)$";

fn mac_hex(mac: Sha512) -> String {
    mac.finalize()
        .iter()
        .map(|byte| format!("{byte:02X}"))
        .collect()
}

/// Splits a SOPS file into its document and metadata; `None` without metadata.
fn split(text: &str) -> Result<Option<(Value, Mapping)>, Error> {
    let mut document: Value = serde_yaml::from_str(text)?;
    let metadata = match &mut document {
        Value::Mapping(mapping) => match mapping.remove(METADATA_KEY) {
            Some(Value::Mapping(metadata)) => metadata,
            _ => return Ok(None),
        },
        _ => return Ok(None),
    };
    Ok(Some((document, metadata)))
}

/// Whether `text` is a SOPS-encrypted file.
pub fn is_encrypted(text: &str) -> bool {
    matches!(split(text), Ok(Some((_, metadata))) if metadata.contains_key("mac"))
}

/// `content`, a JSON or YAML document, with every value encrypted for
/// `recipients`, as `sops encrypt` does by default.
pub fn encrypt(content: &str, recipients: &[age::x25519::Recipient]) -> Result<String, Error> {
    seal(
        serde_yaml::from_str(content)?,
        &Rule::UnencryptedSuffix,
        recipients,
    )
}

/// A Kubernetes Secret named `name` holding `content` as the file `file`,
/// with its values encrypted for `recipients`, as Flux decrypts Secrets.
pub fn encrypt_secret(
    name: &str,
    file: &str,
    content: &str,
    recipients: &[age::x25519::Recipient],
) -> Result<String, Error> {
    let mut metadata = Mapping::new();
    metadata.insert("name".into(), name.into());
    let mut string_data = Mapping::new();
    string_data.insert(file.into(), content.into());
    let mut secret = Mapping::new();
    secret.insert("apiVersion".into(), "v1".into());
    secret.insert("kind".into(), "Secret".into());
    secret.insert("metadata".into(), Value::Mapping(metadata));
    secret.insert("type".into(), "Opaque".into());
    secret.insert("stringData".into(), Value::Mapping(string_data));
    seal(
        Value::Mapping(secret),
        &Rule::encrypted_regex(SECRET_REGEX)?,
        recipients,
    )
}

/// `document` with the values `rule` covers encrypted for `recipients`,
/// written as JSON, which YAML readers take as it is: plain YAML would leave
/// `lastmodified` unquoted, which Go's YAML reads as a time.
fn seal(
    mut document: Value,
    rule: &Rule,
    recipients: &[age::x25519::Recipient],
) -> Result<String, Error> {
    if recipients.is_empty() {
        return Err(Error::NoRecipients);
    }
    let mut data_key = [0u8; DATA_KEY_LEN];
    OsRng.fill_bytes(&mut data_key);

    let mut mac = Sha512::new();
    walk(&mut document, &mut Vec::new(), &mut |path, value| {
        let Some((bytes, kind)) = plaintext(value) else {
            return Ok(());
        };
        mac.update(&bytes);
        if rule.encrypts(path) && !bytes.is_empty() {
            *value = Value::String(encrypt_value(&data_key, &bytes, kind, &aad(path))?);
        }
        Ok(())
    })?;

    let last_modified = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
    let mac = encrypt_value(&data_key, mac_hex(mac).as_bytes(), "str", &last_modified)?;

    let mut age_entries = Vec::with_capacity(recipients.len());
    for recipient in recipients {
        let mut entry = Mapping::new();
        entry.insert("enc".into(), wrap_data_key(&data_key, recipient)?.into());
        entry.insert("recipient".into(), recipient.to_string().into());
        age_entries.push(Value::Mapping(entry));
    }
    let mut metadata = Mapping::new();
    metadata.insert("age".into(), Value::Sequence(age_entries));
    metadata.insert("lastmodified".into(), last_modified.into());
    metadata.insert("mac".into(), mac.into());
    rule.record(&mut metadata);
    metadata.insert("version".into(), VERSION.into());
    match &mut document {
        Value::Mapping(mapping) => {
            mapping.insert(METADATA_KEY.into(), Value::Mapping(metadata));
        }
        _ => return Err(Error::NotEncrypted),
    }
    let mut text = serde_json::to_string_pretty(&document)?;
    text.push('\n');
    Ok(text)
}

fn wrap_data_key(data_key: &[u8], recipient: &age::x25519::Recipient) -> Result<String, Error> {
    let age_error = |error: &dyn std::fmt::Display| Error::Age(error.to_string());
    let encryptor =
        age::Encryptor::with_recipients(std::iter::once(recipient as &dyn age::Recipient))
            .map_err(|e| age_error(&e))?;
    let mut armored = Vec::new();
    let output =
        age::armor::ArmoredWriter::wrap_output(&mut armored, age::armor::Format::AsciiArmor)
            .map_err(|e| age_error(&e))?;
    let mut writer = encryptor.wrap_output(output).map_err(|e| age_error(&e))?;
    writer.write_all(data_key).map_err(|e| age_error(&e))?;
    writer
        .finish()
        .and_then(|output| output.finish())
        .map_err(|e| age_error(&e))?;
    String::from_utf8(armored).map_err(|e| age_error(&e))
}

fn unwrap_data_key(metadata: &Mapping, identities: &Identities) -> Result<Vec<u8>, Error> {
    let entries = match metadata.get("age") {
        Some(Value::Sequence(entries)) => entries,
        _ => return Err(Error::MissingMetadata { field: "age key" }),
    };
    for entry in entries {
        let Some(Value::String(enc)) = entry.get("enc") else {
            continue;
        };
        let Ok(decryptor) = age::Decryptor::new(age::armor::ArmoredReader::new(enc.as_bytes()))
        else {
            continue;
        };
        let Ok(mut reader) = decryptor.decrypt(
            identities
                .iter()
                .map(|identity| identity.as_ref() as &dyn age::Identity),
        ) else {
            continue;
        };
        let mut data_key = Vec::with_capacity(DATA_KEY_LEN);
        if reader.read_to_end(&mut data_key).is_ok() && data_key.len() == DATA_KEY_LEN {
            return Ok(data_key);
        }
    }
    Err(Error::NoIdentity)
}

/// The decrypted document of a SOPS file, as JSON. Fails when no identity
/// decrypts it, or a value or the MAC does not match.
pub fn decrypt(text: &str, identities: &Identities) -> Result<String, Error> {
    let Some((mut document, metadata)) = split(text)? else {
        return Err(Error::NotEncrypted);
    };
    let data_key = unwrap_data_key(&metadata, identities)?;

    let mut mac = Sha512::new();
    walk(&mut document, &mut Vec::new(), &mut |path, value| {
        let aad = aad(path);
        let decrypted = match value {
            Value::String(text) => decrypt_value(&data_key, text, &aad)?,
            _ => None,
        };
        if let Some((bytes, kind)) = decrypted {
            *value = typed(bytes, &kind, &aad)?;
        }
        if let Some((bytes, _)) = plaintext(value) {
            mac.update(&bytes);
        }
        Ok(())
    })?;

    let (Some(Value::String(stored)), Some(Value::String(last_modified))) =
        (metadata.get("mac"), metadata.get("lastmodified"))
    else {
        return Err(Error::MissingMetadata { field: "mac" });
    };
    match decrypt_value(&data_key, stored, last_modified)? {
        Some((expected, _)) if expected == mac_hex(mac).as_bytes() => {}
        _ => return Err(Error::MacMismatch),
    }
    Ok(serde_json::to_string(&document)?)
}

/// The paths of the non-empty values under keys `encrypted_regex` matches
/// that are not well-formed encrypted values, whatever rule the file records;
/// fails when the file is not JSON or YAML or has no SOPS metadata.
pub fn plaintext_paths(text: &str, encrypted_regex: &str) -> Result<Vec<String>, Error> {
    let Some((mut document, _)) = split(text)? else {
        return Err(Error::NotEncrypted);
    };
    let rule = Rule::encrypted_regex(encrypted_regex)?;
    let mut found = Vec::new();
    walk(&mut document, &mut Vec::new(), &mut |path, value| {
        let encrypted = match value {
            Value::String(text) => text.is_empty() || matches!(sealed(text, ""), Ok(Some(_))),
            _ => false,
        };
        if !encrypted && rule.encrypts(path) {
            found.push(path.join("."));
        }
        Ok(())
    })?;
    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_identities() -> Vec<Box<dyn age::Identity + Send + Sync>> {
        age::IdentityFile::from_buffer(include_str!("../tests/fixtures/sops/key.txt").as_bytes())
            .unwrap()
            .into_identities()
            .unwrap()
    }

    fn identities(identity: age::x25519::Identity) -> Vec<Box<dyn age::Identity + Send + Sync>> {
        vec![Box::new(identity)]
    }

    #[test]
    fn a_file_the_sops_cli_wrote_decrypts_with_its_types() {
        let text = include_str!("../tests/fixtures/sops/a.enc.json");

        let decrypted: serde_json::Value =
            serde_json::from_str(&decrypt(text, &fixture_identities()).unwrap()).unwrap();

        assert!(is_encrypted(text));
        assert_eq!(
            decrypted,
            serde_json::json!({
                "api_key": "a",
                "port": 5432,
                "ratio": 1.5,
                "tls": true,
                "empty": "",
                "nested": {"token": "b", "hosts": ["c", "d"]},
                "region_unencrypted": "eu"
            })
        );
    }

    #[test]
    fn an_encrypted_file_hides_its_values_and_decrypts_with_its_key() {
        let identity = age::x25519::Identity::generate();
        let content = r#"{"api_key": "secret", "port": 5432, "region_unencrypted": "eu"}"#;

        let encrypted = encrypt(content, &[identity.to_public()]).unwrap();
        let decrypted: serde_json::Value =
            serde_json::from_str(&decrypt(&encrypted, &identities(identity)).unwrap()).unwrap();

        assert!(!encrypted.contains("secret"));
        assert!(encrypted.contains("\"region_unencrypted\": \"eu\""));
        assert!(plaintext_paths(&encrypted, "^(api_key|port)$")
            .unwrap()
            .is_empty());
        assert_eq!(
            decrypted,
            serde_json::from_str::<serde_json::Value>(content).unwrap()
        );
    }

    #[test]
    fn another_key_or_a_changed_file_does_not_decrypt() {
        let identity = age::x25519::Identity::generate();
        let encrypted = encrypt(
            r#"{"api_key": "a", "region_unencrypted": "eu"}"#,
            &[identity.to_public()],
        )
        .unwrap();
        let moved = encrypted.replace(
            "\"region_unencrypted\": \"eu\"",
            "\"region_unencrypted\": \"us\"",
        );

        assert!(matches!(
            decrypt(&encrypted, &identities(age::x25519::Identity::generate())),
            Err(Error::NoIdentity)
        ));
        assert!(matches!(
            decrypt(&moved, &identities(identity)),
            Err(Error::MacMismatch)
        ));
    }

    #[test]
    fn plain_or_malformed_values_in_an_encrypted_secret_are_found_whatever_its_rule() {
        let identity = age::x25519::Identity::generate();
        let encrypted = encrypt_secret(
            "a",
            "a.json",
            r#"{"api_key": "a"}"#,
            &[identity.to_public()],
        )
        .unwrap();
        let mut leaked: serde_json::Value = serde_json::from_str(&encrypted).unwrap();
        leaked["data"]["b.json"] = "secret".into();
        leaked["data"]["c.json"] = "ENC[AES256_GCM,data:c2VjcmV0,type:str]".into();
        leaked["sops"]["encrypted_regex"] = "^stringData$".into();

        assert!(plaintext_paths(&encrypted, SECRET_REGEX)
            .unwrap()
            .is_empty());
        assert_eq!(
            plaintext_paths(&leaked.to_string(), SECRET_REGEX).unwrap(),
            ["data.b.json", "data.c.json"]
        );
        assert!(matches!(
            plaintext_paths(r#"{"api_key": "a"}"#, SECRET_REGEX),
            Err(Error::NotEncrypted)
        ));
        assert!(!is_encrypted(r#"{"api_key": "a"}"#));
    }

    /// Checks what flowgen writes against the `sops` binary on this machine.
    #[test]
    #[ignore = "requires the sops binary"]
    fn the_sops_cli_decrypts_a_file_flowgen_wrote() {
        let identity = age::x25519::Identity::generate();
        let encrypted = encrypt(
            r#"{"api_key": "secret", "port": 5432, "tls": true}"#,
            &[identity.to_public()],
        )
        .unwrap();
        let folder = tempfile::tempdir().unwrap();
        let file = folder.path().join("a.json");
        std::fs::write(&file, encrypted).unwrap();
        let output = std::process::Command::new("sops")
            .arg("decrypt")
            .arg(&file)
            .env(
                "SOPS_AGE_KEY",
                age::secrecy::ExposeSecret::expose_secret(&identity.to_string()),
            )
            .output()
            .unwrap();

        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let decrypted: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
        assert_eq!(
            decrypted,
            serde_json::json!({"api_key": "secret", "port": 5432, "tls": true})
        );
    }
}
