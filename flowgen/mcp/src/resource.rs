//! The `mcp_resource` task: registers a concrete resource (content resolved
//! at init) or a resource template (content deferred to `resources/read`)
//! with the MCP server. Emits no events; the pipeline sender is never used.

use crate::completion::Completion;
use flowgen_core::{
    config::ConfigExt,
    resource::Source,
    task::{context::TaskContext, runner::Runner, TaskConfig},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{error, info};

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
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Resource {
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
    /// HTTP headers a request must carry, with matching values, for this
    /// resource to be visible on `resources/list` and readable at all. All
    /// declared headers must match (AND). An empty map (the default) means
    /// every caller can see and read it.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
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

impl ConfigExt for Resource {}

impl Resource {
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

/// Errors that can occur during MCP resource processor operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to render configuration template: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Failed to resolve resource content: {source}")]
    ResolveContent {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Failed to register MCP resource: {source}")]
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
    #[error("`parameters` is only allowed when `uri_template` is set")]
    ParametersOnConcreteResource,
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
    config: Arc<Resource>,
}

/// MCP resource processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Resource>,
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
        // `content` is deferred to per-read rendering.
        let mut for_render = (*self.config).clone();
        let raw_content = std::mem::replace(&mut for_render.content, Source::Inline(String::new()));
        let mut rendered = for_render
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;
        rendered.content = raw_content;
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
                        error!(error = %e, "Failed to initialize MCP resource processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await?;

        let cfg = &event_handler.config;
        let flow_name = self.task_context.flow.identity();
        let scheme = &self.mcp_server.extras().resource_uri_scheme;
        let key = cfg.registration_key(scheme, flow_name);

        // Concrete resources resolve once at init; templates defer render
        // to read time so each `{param}` binding produces distinct content.
        let body = match cfg.is_template() {
            true => crate::server::ResourceBody::Template(cfg.content.clone()),
            false => {
                let resolved = cfg
                    .content
                    .render(
                        self.task_context.resource_loader.as_ref(),
                        &serde_json::json!({}),
                    )
                    .await
                    .map_err(|source| Error::ResolveContent { source })?;
                crate::server::ResourceBody::Concrete(resolved)
            }
        };

        // `parameters` only bind against `{placeholder}` in `uri_template`.
        if !cfg.is_template() && !cfg.parameters.is_empty() {
            return Err(Error::ParametersOnConcreteResource);
        }

        let mut parameters = Vec::with_capacity(cfg.parameters.len());
        for p in &cfg.parameters {
            let completion_values = match &p.completion {
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
            parameters.push(crate::server::TemplateParameter {
                name: p.name.clone(),
                completion_values,
            });
        }

        let registration = crate::server::ResourceRegistration {
            flow_name: flow_name.to_string(),
            uri: key.clone(),
            name: cfg.name.clone(),
            description: cfg.description.clone(),
            mime_type: cfg.mime_type.clone(),
            body,
            parameters,
            resource_loader: self.task_context.resource_loader.clone(),
            headers: cfg.headers.clone(),
        };

        match cfg.is_template() {
            true => crate::server::register_resource_template(
                self.mcp_server.as_ref(),
                key.clone(),
                registration,
            ),
            false => crate::server::register_resource(
                self.mcp_server.as_ref(),
                key.clone(),
                registration,
            ),
        }
        .map_err(|source| Error::Registration { source })?;

        info!(uri = %key, template = cfg.is_template(), "MCP resource registered");
        Ok(())
    }
}

/// Builder for [`Processor`].
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Resource>>,
    task_id: usize,
    task_context: Option<Arc<TaskContext>>,
    task_type: Option<&'static str>,
    mcp_server: Option<Arc<crate::server::McpServer>>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<Resource>) -> Self {
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

    fn fixture(name: &str, content: &str) -> Resource {
        Resource {
            name: name.to_string(),
            uri: None,
            uri_template: None,
            description: "d".to_string(),
            mime_type: default_mime_type(),
            content: Source::Inline(content.to_string()),
            parameters: vec![],
            headers: std::collections::HashMap::new(),
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
        assert!(serde_json::from_str::<Resource>(json).is_err());
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
        let p: Resource = serde_json::from_str(json).unwrap();
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
        let p: Resource = serde_json::from_str(json).unwrap();
        match &p.parameters[0].completion {
            Some(Completion::Resource { resource }) => {
                assert_eq!(resource, "completions/accounts.txt");
            }
            other => panic!("expected Resource, got {other:?}"),
        }
    }
}
