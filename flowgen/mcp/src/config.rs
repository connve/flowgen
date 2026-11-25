//! Configuration shared by the MCP server and the `mcp_tool` task.

use flowgen_core::credentials::MIN_API_KEY_LEN;
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Deserializer};

/// Authentication credentials for MCP API key validation.
///
/// ```json
/// { "api_keys": ["a random string of at least 32 characters"] }
/// ```
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Credentials {
    /// Keys accepted as `Authorization: Bearer <key>`, each at least
    /// [`MIN_API_KEY_LEN`] characters.
    #[serde(deserialize_with = "long_keys")]
    pub api_keys: Vec<SecretString>,
}

fn long_keys<'de, D>(deserializer: D) -> Result<Vec<SecretString>, D::Error>
where
    D: Deserializer<'de>,
{
    let keys = Vec::<SecretString>::deserialize(deserializer)?;
    if keys
        .iter()
        .any(|key| key.expose_secret().len() < MIN_API_KEY_LEN)
    {
        return Err(serde::de::Error::custom(format!(
            "every API key must be at least {MIN_API_KEY_LEN} characters"
        )));
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credentials_read_their_keys() {
        let key = "k".repeat(MIN_API_KEY_LEN);
        let credentials: Credentials =
            serde_json::from_str(&format!(r#"{{"api_keys": ["{key}"]}}"#)).unwrap();

        assert_eq!(credentials.api_keys[0].expose_secret(), key);
    }

    #[test]
    fn a_short_key_is_rejected() {
        let error = serde_json::from_str::<Credentials>(r#"{"api_keys": ["short"]}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("at least 32 characters"), "{error}");
    }

    #[test]
    fn a_misspelled_credentials_field_is_rejected() {
        let error = serde_json::from_str::<Credentials>(r#"{"api_key": ["k"]}"#)
            .unwrap_err()
            .to_string();
        assert!(error.contains("unknown field `api_key`"), "{error}");
    }
}
