//! MCP prompt task configuration.

use crate::completion::Completion;
use flowgen_core::{config::ConfigExt, resource::Source};
use serde::{Deserialize, Serialize};

/// MCP prompt task configuration.
///
/// # Single-message example
/// ```yaml
/// - mcp_prompt:
///     name: explain_query
///     description: "Explain what a BigQuery SQL query does."
///     arguments:
///       - name: query
///         description: "The SQL query text."
///         required: true
///     template: |
///       Explain what this query does:
///       ```sql
///       {{arguments.query}}
///       ```
/// ```
///
/// # Multi-message example (few-shot prompt)
/// ```yaml
/// - mcp_prompt:
///     name: classify_intent
///     description: "Classify user intent as sales/support/other."
///     arguments:
///       - name: message
///         description: "User message to classify."
///         required: true
///     messages:
///       - role: user
///         content: "How much does the enterprise plan cost?"
///       - role: assistant
///         content: "sales"
///       - role: user
///         content: "{{arguments.message}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Processor {
    /// Prompt identifier. Becomes the slash command in MCP clients.
    pub name: String,
    /// Human-readable description shown to MCP clients in the prompt picker.
    pub description: String,
    /// Argument definitions passed to the prompt.
    #[serde(default)]
    pub arguments: Vec<Argument>,
    /// Single-message shorthand: renders to one `user`-role message.
    /// Mutually exclusive with `messages`.
    #[serde(default)]
    pub template: Option<Source>,
    /// Multi-message form: list of role-tagged messages, each rendered
    /// as a Handlebars template. Mutually exclusive with `template`.
    #[serde(default)]
    pub messages: Option<Vec<Message>>,
    /// Standard task wiring; prompt tasks rarely depend on anything.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Overrides the app-level retry config.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

/// A prompt argument definition.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Argument {
    /// Argument identifier, referenced in the template as
    /// `{{arguments.<name>}}`.
    pub name: String,
    /// Human-readable description shown when the client prompts the user
    /// for a value.
    pub description: String,
    /// When true, the client must collect a value before invoking
    /// `prompts/get`.
    #[serde(default)]
    pub required: bool,
    /// Value substituted when the client omits an optional argument;
    /// empty string when not set.
    #[serde(default)]
    pub default: Option<String>,
    /// Optional completion source served by `completion/complete`.
    #[serde(default)]
    pub completion: Option<Completion>,
}

/// A message in a multi-message prompt template.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    /// Message role — MCP spec defines `user` and `assistant`.
    pub role: Role,
    /// Handlebars template body, rendered against `{{arguments.*}}`.
    pub content: Source,
}

/// Message role.
#[derive(PartialEq, Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Assistant,
}

impl ConfigExt for Processor {}

impl Processor {
    /// Returns the messages this prompt renders to, either the
    /// single-message shorthand promoted to a user turn, or the explicit
    /// multi-message form. Returns an error when neither is set, or both.
    pub fn resolved_messages(&self) -> Result<Vec<Message>, ShapeError> {
        match (&self.template, &self.messages) {
            (Some(_), Some(_)) => Err(ShapeError::TemplateAndMessages),
            (None, None) => Err(ShapeError::NeitherTemplateNorMessages),
            (Some(template), None) => Ok(vec![Message {
                role: Role::User,
                content: template.clone(),
            }]),
            (None, Some(messages)) => match messages.is_empty() {
                true => Err(ShapeError::EmptyMessages),
                false => Ok(messages.clone()),
            },
        }
    }
}

/// Error returned when the prompt config is malformed (bad shape rather
/// than bad content).
#[derive(thiserror::Error, Debug, PartialEq)]
#[non_exhaustive]
pub enum ShapeError {
    #[error("mcp_prompt must set exactly one of `template` or `messages`, not both")]
    TemplateAndMessages,
    #[error("mcp_prompt must set either `template` or `messages`")]
    NeitherTemplateNorMessages,
    #[error("mcp_prompt `messages` list must not be empty")]
    EmptyMessages,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Processor {
        Processor {
            name: name.to_string(),
            description: "d".to_string(),
            arguments: vec![],
            template: None,
            messages: None,
            depends_on: None,
            retry: None,
        }
    }

    #[test]
    fn template_promotes_to_single_user_message() {
        let mut p = fixture("p");
        p.template = Some(Source::Inline("Hello {{arguments.name}}".to_string()));
        let msgs = p.resolved_messages().unwrap();
        assert_eq!(msgs.len(), 1);
        assert_eq!(msgs[0].role, Role::User);
    }

    #[test]
    fn messages_pass_through() {
        let mut p = fixture("p");
        p.messages = Some(vec![
            Message {
                role: Role::User,
                content: Source::Inline("hi".to_string()),
            },
            Message {
                role: Role::Assistant,
                content: Source::Inline("hello".to_string()),
            },
        ]);
        let msgs = p.resolved_messages().unwrap();
        assert_eq!(msgs.len(), 2);
        assert_eq!(msgs[1].role, Role::Assistant);
    }

    #[test]
    fn both_template_and_messages_is_error() {
        let mut p = fixture("p");
        p.template = Some(Source::Inline("x".to_string()));
        p.messages = Some(vec![Message {
            role: Role::User,
            content: Source::Inline("y".to_string()),
        }]);
        assert_eq!(
            p.resolved_messages().unwrap_err(),
            ShapeError::TemplateAndMessages
        );
    }

    #[test]
    fn neither_template_nor_messages_is_error() {
        let p = fixture("p");
        assert_eq!(
            p.resolved_messages().unwrap_err(),
            ShapeError::NeitherTemplateNorMessages
        );
    }

    #[test]
    fn empty_messages_is_error() {
        let mut p = fixture("p");
        p.messages = Some(vec![]);
        assert_eq!(
            p.resolved_messages().unwrap_err(),
            ShapeError::EmptyMessages
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        let json = r#"{"name":"p","description":"d","template":"x","xyz":1}"#;
        assert!(serde_json::from_str::<Processor>(json).is_err());
    }

    #[test]
    fn argument_default_deserializes() {
        let json = r#"{
            "name":"p","description":"d",
            "arguments":[{"name":"tone","description":"tone","default":"formal"}],
            "template":"x"
        }"#;
        let p: Processor = serde_json::from_str(json).unwrap();
        assert_eq!(p.arguments[0].default.as_deref(), Some("formal"));
        assert!(!p.arguments[0].required);
    }

    #[test]
    fn completion_values_deserialize() {
        let json = r#"{
            "name":"p","description":"d",
            "arguments":[{
                "name":"language","description":"lang",
                "completion":{"values":["python","rust"]}
            }],
            "template":"x"
        }"#;
        let p: Processor = serde_json::from_str(json).unwrap();
        match &p.arguments[0].completion {
            Some(Completion::Values { values }) => {
                assert_eq!(values, &vec!["python".to_string(), "rust".to_string()]);
            }
            other => panic!("expected Values variant, got {other:?}"),
        }
    }

    #[test]
    fn completion_resource_deserializes() {
        let json = r#"{
            "name":"p","description":"d",
            "arguments":[{
                "name":"account","description":"id",
                "completion":{"resource":"completions/accounts.txt"}
            }],
            "template":"x"
        }"#;
        let p: Processor = serde_json::from_str(json).unwrap();
        match &p.arguments[0].completion {
            Some(Completion::Resource { resource }) => {
                assert_eq!(resource, "completions/accounts.txt");
            }
            other => panic!("expected Resource variant, got {other:?}"),
        }
    }
}
