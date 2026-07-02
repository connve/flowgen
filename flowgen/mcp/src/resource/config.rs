//! MCP resource task configuration.

use crate::completion::Completion;
use flowgen_core::{config::ConfigExt, resource::Source};
use serde::{Deserialize, Serialize};

/// MCP resource task configuration.
///
/// # Example YAML
/// ```yaml
/// - mcp_resource:
///     name: sf_account_schema
///     description: "Salesforce Account SObject field schema."
///     mime_type: application/json
///     content:
///       resource: "schemas/sf-account.json"
/// ```
///
/// # Templated URI example
/// ```yaml
/// - mcp_resource:
///     name: account_summary
///     uri_template: "flowgen://account/{id}"
///     description: "Per-account summary; bind {id} to Salesforce ID."
///     mime_type: text/markdown
///     content: |
///       # Account {{id}}
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Processor {
    /// Task name; also the trailing segment of the auto-generated URI
    /// `<scheme>://<flow_name>/<name>` when neither `uri` nor
    /// `uri_template` is set.
    pub name: String,
    /// Explicit resource URI. Mutually exclusive with `uri_template`;
    /// bypasses the auto-generated `<scheme>://<flow_name>/<name>` form.
    #[serde(default)]
    pub uri: Option<String>,
    /// RFC 6570 URI template with `{param}` placeholders. When set, the
    /// task registers as a resource template; `resources/read` matches
    /// concrete URIs, binds params, and renders `content` as a
    /// Handlebars template against them.
    #[serde(default)]
    pub uri_template: Option<String>,
    /// Human-readable description shown to MCP clients.
    pub description: String,
    /// Standard MIME type.
    #[serde(default = "default_mime_type")]
    pub mime_type: String,
    /// Inline string or `resource:` reference. For templates, rendered
    /// against URI-template param bindings on each read.
    pub content: Source,
    /// Optional per-parameter metadata for URI-template placeholders.
    /// Applies only when `uri_template` is set; each entry attaches
    /// completion sources to `{param}` bindings served by
    /// `completion/complete`.
    #[serde(default)]
    pub parameters: Vec<Parameter>,
    /// Standard task wiring; resource tasks rarely depend on anything.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Overrides the app-level retry config.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

/// URI-template parameter descriptor.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Parameter {
    /// Parameter identifier — must match a `{name}` placeholder in
    /// `uri_template`.
    pub name: String,
    /// Optional completion source served by `completion/complete`.
    #[serde(default)]
    pub completion: Option<Completion>,
}

fn default_mime_type() -> String {
    "text/plain".to_string()
}

impl ConfigExt for Processor {}

impl Processor {
    /// True when this task registers as a URI template rather than a
    /// concrete resource.
    pub fn is_template(&self) -> bool {
        self.uri_template.is_some()
    }

    /// Registration key for the MCP dispatch table.
    ///
    /// Templates use the raw pattern; concrete resources use the
    /// explicit `uri` when set, otherwise the auto-generated
    /// `<scheme>://<flow_name>/<name>` form.
    pub fn registration_key(&self, scheme: &str, flow_name: &str) -> String {
        match (&self.uri_template, &self.uri) {
            (Some(template), _) => template.clone(),
            (None, Some(uri)) => uri.clone(),
            (None, None) => format!("{scheme}://{flow_name}/{}", self.name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str, content: &str) -> Processor {
        Processor {
            name: name.to_string(),
            uri: None,
            uri_template: None,
            description: "d".to_string(),
            mime_type: default_mime_type(),
            content: Source::Inline(content.to_string()),
            parameters: vec![],
            depends_on: None,
            retry: None,
        }
    }

    #[test]
    fn registration_key_auto_generated() {
        let p = fixture("schema", "body");
        assert_eq!(
            p.registration_key("flowgen", "sales"),
            "flowgen://sales/schema"
        );
        assert!(!p.is_template());
    }

    #[test]
    fn registration_key_prefers_explicit_uri() {
        let mut p = fixture("api_docs", "body");
        p.uri = Some("https://docs.company.com/api.md".to_string());
        assert_eq!(
            p.registration_key("flowgen", "docs"),
            "https://docs.company.com/api.md",
        );
    }

    #[test]
    fn registration_key_uses_template_pattern() {
        let mut p = fixture("account", "Account {{id}}");
        p.uri_template = Some("flowgen://account/{id}".to_string());
        assert_eq!(
            p.registration_key("flowgen", "sales"),
            "flowgen://account/{id}",
        );
        assert!(p.is_template());
    }

    #[test]
    fn custom_scheme_flows_through_to_auto_uri() {
        let p = fixture("schema", "body");
        assert_eq!(p.registration_key("acme", "sales"), "acme://sales/schema");
    }

    #[test]
    fn default_mime_type_is_text_plain() {
        let p = fixture("n", "c");
        assert_eq!(p.mime_type, "text/plain");
    }

    #[test]
    fn rejects_unknown_fields() {
        let json = r#"{"name":"n","description":"d","content":"c","xyz":1}"#;
        assert!(serde_json::from_str::<Processor>(json).is_err());
    }

    #[test]
    fn parameter_with_inline_completion() {
        let json = r#"{
            "name":"n","description":"d","content":"c",
            "uri_template":"flowgen://account/{id}",
            "parameters":[
                {"name":"id","completion":{"values":["a","b"]}}
            ]
        }"#;
        let p: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(p.parameters.len(), 1);
        assert_eq!(p.parameters[0].name, "id");
        match &p.parameters[0].completion {
            Some(Completion::Values { values }) => {
                assert_eq!(values, &vec!["a".to_string(), "b".to_string()]);
            }
            other => panic!("expected Values, got {other:?}"),
        }
    }

    #[test]
    fn parameter_with_resource_completion() {
        let json = r#"{
            "name":"n","description":"d","content":"c",
            "uri_template":"flowgen://account/{id}",
            "parameters":[
                {"name":"id","completion":{"resource":"completions/accounts.txt"}}
            ]
        }"#;
        let p: Processor = serde_json::from_str(json).unwrap();
        match &p.parameters[0].completion {
            Some(Completion::Resource { resource }) => {
                assert_eq!(resource, "completions/accounts.txt");
            }
            other => panic!("expected Resource, got {other:?}"),
        }
    }
}
