//! The `salesforce_restapi_search` task: SOSL searches through the REST API.

use crate::processor::{to_json, Call, Emitter, Error, Operation, Output};
use flowgen_core::config::ConfigExt;
use flowgen_core::task::TaskConfig;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// SOSL reserved characters that must be backslash-escaped inside a phrase
/// search (`FIND {"..."}`). Hyphens, ampersands, and the like otherwise abort
/// the parser with `MALFORMED_SEARCH`.
const SOSL_RESERVED: &[char] = &[
    '\\', '+', '-', '&', '|', '!', '(', ')', '{', '}', '[', ']', '^', '"', '~', '*', '?', ':', '\'',
];

/// Configuration for Salesforce REST API SOSL search operations.
///
/// # Examples
///
/// ```yaml
/// salesforce_restapi_search:
///   name: find_accounts
///   credentials_path: /path/to/salesforce_creds.json
///   query: "FIND {{{event.data.search_term}}} IN ALL FIELDS RETURNING Account(Id, Name)"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Search {
    /// Unique task identifier.
    pub name: String,
    /// Path to Salesforce authentication credentials.
    pub credentials_path: PathBuf,
    /// SOSL query string (supports Handlebars templating).
    pub query: String,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Events handled at once; defaults to 64.
    #[serde(default)]
    pub max_in_flight: Option<std::num::NonZeroUsize>,
}

impl ConfigExt for Search {}

#[async_trait::async_trait]
impl Operation for Search {
    type Client = salesforce_core::restapi::Client;

    fn client(auth: &salesforce_core::client::Client) -> Result<Self::Client, Error> {
        crate::processor::rest_client(auth)
    }

    fn credentials_path(&self) -> &Path {
        &self.credentials_path
    }

    async fn call(
        &self,
        client: &Self::Client,
        _call: &Call<'_>,
        emitter: &mut Emitter<'_>,
    ) -> Result<(), Error> {
        let response = client
            .search(&escape_sosl_phrases(&self.query))
            .await
            .map_err(|source| Error::Search {
                source: Box::new(source),
            })?;
        emitter.emit(Output::json(to_json(&response)?)).await
    }
}

/// Auto-escapes phrase-search content in a rendered SOSL query so user input
/// containing reserved characters doesn't crash the Salesforce parser.
///
/// Finds every `{"..."}` clause, then within the phrase escapes any [`SOSL_RESERVED`]
/// char that isn't already backslash-escaped. Outside of phrase clauses the
/// query is left untouched so the static parts (`RETURNING Account(...)`, etc.)
/// keep working.
fn escape_sosl_phrases(query: &str) -> String {
    let mut out = String::with_capacity(query.len());
    let mut rest = query;
    while let Some(open) = rest.find("{\"") {
        out.push_str(&rest[..open]);
        let after_open = &rest[open + 2..];
        match after_open.find("\"}") {
            Some(close) => {
                out.push_str("{\"");
                let phrase = &after_open[..close];
                let mut chars = phrase.chars();
                while let Some(ch) = chars.next() {
                    if ch == '\\' {
                        out.push('\\');
                        if let Some(next) = chars.next() {
                            out.push(next);
                        }
                    } else {
                        if SOSL_RESERVED.contains(&ch) {
                            out.push('\\');
                        }
                        out.push(ch);
                    }
                }
                out.push_str("\"}");
                rest = &after_open[close + 2..];
            }
            None => {
                out.push_str(&rest[open..]);
                return out;
            }
        }
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_phrase_with_hyphen() {
        let q = r#"FIND {"acm-p"} IN ALL FIELDS RETURNING Account(Id, Name)"#;
        let escaped = escape_sosl_phrases(q);
        assert_eq!(
            escaped,
            r#"FIND {"acm\-p"} IN ALL FIELDS RETURNING Account(Id, Name)"#
        );
    }

    #[test]
    fn escape_leaves_outside_phrase_untouched() {
        let q = r#"FIND {"abc"} IN ALL FIELDS RETURNING Account(Id, Name)"#;
        assert_eq!(escape_sosl_phrases(q), q);
    }

    #[test]
    fn escape_multiple_reserved_chars() {
        let q = r#"FIND {"a&b|c?d!e"} RETURNING Account"#;
        assert_eq!(
            escape_sosl_phrases(q),
            r#"FIND {"a\&b\|c\?d\!e"} RETURNING Account"#
        );
    }

    #[test]
    fn escape_does_not_double_escape() {
        let q = r#"FIND {"foo\-bar"} RETURNING Account"#;
        assert_eq!(escape_sosl_phrases(q), q);
    }

    #[test]
    fn escape_preserves_utf8_outside_phrase() {
        let q = r#"FIND {"café"} RETURNING Account(Naïve)"#;
        assert_eq!(
            escape_sosl_phrases(q),
            r#"FIND {"café"} RETURNING Account(Naïve)"#
        );
    }

    #[test]
    fn escape_empty_phrase() {
        let q = r#"FIND {""} RETURNING Account"#;
        assert_eq!(escape_sosl_phrases(q), q);
    }

    #[test]
    fn escape_unterminated_phrase_is_passthrough() {
        let q = r#"FIND {"unterminated"#;
        assert_eq!(escape_sosl_phrases(q), q);
    }

    #[test]
    fn escape_brace_search_without_quotes_untouched() {
        let q = r#"FIND {Acme} RETURNING Account"#;
        assert_eq!(escape_sosl_phrases(q), q);
    }

    #[test]
    fn config_deser_minimal() {
        let json = r#"{
            "name": "find_accounts",
            "credentials_path": "/path/to/creds.json",
            "query": "FIND {Acme} IN ALL FIELDS RETURNING Account(Id, Name)"
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "find_accounts");
        assert_eq!(
            config.credentials_path,
            std::path::PathBuf::from("/path/to/creds.json")
        );
        assert_eq!(
            config.query,
            "FIND {Acme} IN ALL FIELDS RETURNING Account(Id, Name)"
        );
        assert!(config.depends_on.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn config_deser_with_depends_on() {
        let json = r#"{
            "name": "search_contacts",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "depends_on": ["upstream_task"]
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let deps = config.depends_on.unwrap();
        assert_eq!(deps.len(), 1);
        assert_eq!(deps[0], "upstream_task");
    }

    #[test]
    fn config_deser_with_retry() {
        let json = r#"{
            "name": "retryable_search",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "retry": { "max_attempts": 5, "initial_backoff": "2s" }
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        assert!(config.retry.is_some());
    }

    #[test]
    fn config_deser_missing_name_fails() {
        let json = r#"{
            "credentials_path": "/creds.json",
            "query": "FIND {test}"
        }"#;
        let result = serde_json::from_str::<Search>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_credentials_path_fails() {
        let json = r#"{
            "name": "test",
            "query": "FIND {test}"
        }"#;
        let result = serde_json::from_str::<Search>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_query_fails() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/creds.json"
        }"#;
        let result = serde_json::from_str::<Search>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_roundtrip_serde() {
        let json = r#"{
            "name": "roundtrip",
            "credentials_path": "/creds.json",
            "query": "FIND {Acme} RETURNING Account"
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: Search = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    #[test]
    fn config_deser_with_handlebars_template_in_query() {
        let json = r#"{
            "name": "templated_search",
            "credentials_path": "/creds.json",
            "query": "FIND {{{event.data.search_term}}} IN ALL FIELDS RETURNING Account(Id, Name)"
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        assert!(config.query.contains("{{event.data.search_term}}"));
    }

    #[test]
    fn config_deser_with_multiple_depends_on() {
        let json = r#"{
            "name": "multi_dep",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "depends_on": ["task_a", "task_b", "task_c"]
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let deps = config.depends_on.unwrap();
        assert_eq!(deps.len(), 3);
        assert_eq!(deps[2], "task_c");
    }

    #[test]
    fn config_deser_empty_depends_on_is_empty_vec() {
        let json = r#"{
            "name": "empty_dep",
            "credentials_path": "/creds.json",
            "query": "FIND {test}",
            "depends_on": []
        }"#;
        let config: Search = serde_json::from_str(json).unwrap();
        let deps = config.depends_on.unwrap();
        assert!(deps.is_empty());
    }
}
