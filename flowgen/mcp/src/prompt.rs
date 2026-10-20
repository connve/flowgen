//! The `mcp_prompt` task: registers a prompt (slash-command template) with
//! the MCP server. Each `prompts/get` call binds user-supplied arguments and
//! renders the template as Handlebars against `{{arguments.*}}`.

use crate::completion::Completion;
use flowgen_core::{
    config::ConfigExt,
    resource::Source,
    task::{context::TaskContext, runner::Runner, TaskConfig},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

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
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Prompt {
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
    /// HTTP headers a request must carry, with matching values, for this
    /// prompt to be visible on `prompts/list` and usable at all. All
    /// declared headers must match (AND). An empty map (the default) means
    /// every caller can see and use it.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
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

impl ConfigExt for Prompt {}

impl Prompt {
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

/// Errors that can occur during MCP prompt processor operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Invalid mcp_prompt configuration: {source}")]
    Shape {
        #[source]
        source: ShapeError,
    },
    #[error("Failed to register MCP prompt: {source}")]
    Registration {
        #[source]
        source: crate::server::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Cannot resolve completion resource without a resource loader")]
    CompletionResourceLoaderMissing,
    #[error("Failed to load completion resource: {source}")]
    ResolveCompletion {
        #[source]
        source: flowgen_core::resource::Error,
    },
}

/// Event handler carrying the rendered config to `run()`.
#[derive(Clone, Debug)]
pub struct EventHandler {
    config: Arc<Prompt>,
}

/// MCP prompt processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Prompt>,
    task_id: usize,
    task_context: Arc<TaskContext>,
    task_type: &'static str,
    mcp_server: Arc<crate::server::McpServer>,
}

#[async_trait::async_trait]
impl Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        // `template` and `messages` are deferred to per-get rendering.
        let mut for_render = (*self.config).clone();
        let raw_template = for_render.template.take();
        let raw_messages = for_render.messages.take();
        let mut rendered = for_render
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;
        rendered.template = raw_template;
        rendered.messages = raw_messages;
        Ok(EventHandler {
            config: Arc::new(rendered),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize MCP prompt processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await?;

        let cfg = &event_handler.config;
        let messages_cfg = cfg
            .resolved_messages()
            .map_err(|source| Error::Shape { source })?;

        let mut arguments = Vec::with_capacity(cfg.arguments.len());
        for a in &cfg.arguments {
            let completion_values = match &a.completion {
                None => None,
                Some(Completion::Values { values }) => Some(values.clone()),
                Some(Completion::Resource { resource }) => {
                    let loader = self
                        .task_context
                        .resource_loader
                        .as_ref()
                        .ok_or(Error::CompletionResourceLoaderMissing)?;
                    let content = loader
                        .load(resource)
                        .await
                        .map_err(|source| Error::ResolveCompletion { source })?;
                    Some(crate::completion::parse_completion_lines(&content))
                }
            };
            arguments.push(crate::server::PromptArgument {
                name: a.name.clone(),
                description: a.description.clone(),
                required: a.required,
                default: a.default.clone(),
                completion_values,
            });
        }

        let messages = messages_cfg
            .into_iter()
            .map(|m| crate::server::PromptMessage {
                role: match m.role {
                    Role::User => crate::server::PromptRole::User,
                    Role::Assistant => crate::server::PromptRole::Assistant,
                },
                content: m.content,
            })
            .collect();

        let registration = crate::server::PromptRegistration {
            flow_name: self.task_context.flow.identity().to_string(),
            name: cfg.name.clone(),
            description: cfg.description.clone(),
            arguments,
            messages,
            resource_loader: self.task_context.resource_loader.clone(),
            headers: cfg.headers.clone(),
        };

        crate::server::register_prompt(self.mcp_server.as_ref(), cfg.name.clone(), registration)
            .map_err(|source| Error::Registration { source })?;

        info!(prompt = %cfg.name, "MCP prompt registered");
        Ok(())
    }
}

/// Builder for [`Processor`].
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Prompt>>,
    task_id: usize,
    task_context: Option<Arc<TaskContext>>,
    task_type: Option<&'static str>,
    mcp_server: Option<Arc<crate::server::McpServer>>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<Prompt>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(mut self, task_context: Arc<TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub fn mcp_server(mut self, server: Arc<crate::server::McpServer>) -> Self {
        self.mcp_server = Some(server);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
            mcp_server: self
                .mcp_server
                .ok_or_else(|| Error::MissingBuilderAttribute("mcp_server".to_string()))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(name: &str) -> Prompt {
        Prompt {
            name: name.to_string(),
            description: "d".to_string(),
            arguments: vec![],
            template: None,
            messages: None,
            headers: std::collections::HashMap::new(),
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
        assert!(serde_json::from_str::<Prompt>(json).is_err());
    }

    #[test]
    fn argument_default_deserializes() {
        let json = r#"{
            "name":"p","description":"d",
            "arguments":[{"name":"tone","description":"tone","default":"formal"}],
            "template":"x"
        }"#;
        let p: Prompt = serde_json::from_str(json).unwrap();
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
        let p: Prompt = serde_json::from_str(json).unwrap();
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
        let p: Prompt = serde_json::from_str(json).unwrap();
        match &p.arguments[0].completion {
            Some(Completion::Resource { resource }) => {
                assert_eq!(resource, "completions/accounts.txt");
            }
            other => panic!("expected Resource variant, got {other:?}"),
        }
    }
}
