//! Flow identity and its two projections.
//!
//! A flow is identified by its path relative to `flows.path` (filesystem
//! load) or the cache key suffix (cache load), with folder separators
//! preserved. That human form is what operators read in logs, the UI, REST
//! paths, and resource URIs. It is not, however, guaranteed to be valid as a
//! key across cache backends — it may contain path separators — so anything
//! used as a cache or lease key, or hashed for peer ownership, uses the
//! key-safe projection instead. [`FlowIdentity`] owns both forms so callers
//! pick the right one at the type level rather than passing bare strings.

use std::fmt;

/// A flow's identity: the path-shaped source of truth, with both a
/// human-readable form ([`as_str`](Self::as_str), via [`fmt::Display`]) and a
/// key-safe form ([`as_key`](Self::as_key)).
#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct FlowIdentity(String);

impl FlowIdentity {
    /// Wraps a path-shaped identity string.
    pub fn new(path: impl Into<String>) -> Self {
        FlowIdentity(path.into())
    }

    /// Returns the human-readable identity — folder separators preserved.
    /// Used for the registry key, the tracing `flow=` field, REST `path`, and
    /// resource URIs. Not a key-safe form; use [`as_key`](Self::as_key) for
    /// cache and lease keys.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the key-safe id — see [`encode_key`]. Use for any cache or
    /// lease key, or as a peer-ownership hash input.
    pub fn as_key(&self) -> String {
        encode_key(&self.0)
    }
}

impl fmt::Display for FlowIdentity {
    /// Displays the human-readable form, so `flow = %identity` log fields and
    /// error messages read as the path the operator authored.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Encodes a flow identity into a key-safe id.
///
/// base64url without padding, so the result is restricted to `[A-Za-z0-9_-]`
/// — a conservative alphabet accepted as a key by every supported cache
/// backend, and by `validate_name`. The encoding is bijective, so distinct
/// identities never collide (unlike folding separators to another character).
/// This is the single place the identity-to-key encoding happens; every key
/// builder routes through it so the human and key forms cannot drift apart.
pub fn encode_key(identity: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    #[test]
    fn as_key_produces_a_key_safe_token() {
        let id = FlowIdentity::new("salesforce/cdc/pubsubapi_account_subscriber");
        let key = id.as_key();
        assert!(key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
        let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(&key)
            .unwrap();
        assert_eq!(decoded, b"salesforce/cdc/pubsubapi_account_subscriber");
    }

    #[test]
    fn as_key_is_bijective_across_folders() {
        assert_ne!(
            FlowIdentity::new("nats/generate").as_key(),
            FlowIdentity::new("demo/generate").as_key()
        );
        assert_ne!(
            FlowIdentity::new("a/b").as_key(),
            FlowIdentity::new("a_b").as_key()
        );
    }

    #[test]
    fn as_key_survives_symlinked_config_mount_paths() {
        let key = FlowIdentity::new("..2026_07_23_10_30_00.123456789/salesforce/cdc/foo").as_key();
        assert!(key
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn display_is_the_human_form() {
        let id = FlowIdentity::new("demo/foo");
        assert_eq!(id.to_string(), "demo/foo");
        assert_eq!(id.as_str(), "demo/foo");
    }
}
