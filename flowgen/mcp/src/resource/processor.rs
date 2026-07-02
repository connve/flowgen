//! MCP resource task processor.
//!
//! Registers a concrete resource (content resolved at init) or a resource
//! template (content deferred to `resources/read`) with the MCP server.
//! Emits no events; the pipeline sender is never used.

use flowgen_core::{
    config::ConfigExt,
    task::{context::TaskContext, runner::Runner},
};
use std::sync::Arc;
use tracing::{error, info};

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
        source: super::super::server::Error,
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
    config: Arc<super::config::Processor>,
}

/// MCP resource processor.
#[derive(Debug)]
pub struct Processor {
    config: Arc<super::config::Processor>,
    task_id: usize,
    task_context: Arc<TaskContext>,
    task_type: &'static str,
    mcp_server: Arc<super::super::server::McpServer>,
}

#[async_trait::async_trait]
impl Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;
        Ok(EventHandler {
            config: Arc::new(init_config),
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
        let flow_name = &self.task_context.flow.name;
        let scheme = &self.mcp_server.extras().resource_uri_scheme;
        let key = cfg.registration_key(scheme, flow_name);

        // Concrete resources resolve once at init; templates defer render
        // to read time so each `{param}` binding produces distinct content.
        let body = match cfg.is_template() {
            true => super::super::server::ResourceBody::Template(cfg.content.clone()),
            false => {
                let resolved = cfg
                    .content
                    .resolve(self.task_context.resource_loader.as_ref())
                    .await
                    .map_err(|source| Error::ResolveContent { source })?;
                super::super::server::ResourceBody::Concrete(resolved)
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
                Some(crate::completion::Completion::Values { values }) => Some(values.clone()),
                Some(crate::completion::Completion::Resource { resource }) => {
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
            parameters.push(super::super::server::TemplateParameter {
                name: p.name.clone(),
                completion_values,
            });
        }

        let registration = super::super::server::ResourceRegistration {
            flow_name: flow_name.clone(),
            uri: key.clone(),
            name: cfg.name.clone(),
            description: cfg.description.clone(),
            mime_type: cfg.mime_type.clone(),
            body,
            parameters,
            resource_loader: self.task_context.resource_loader.clone(),
        };

        match cfg.is_template() {
            true => super::super::server::register_resource_template(
                self.mcp_server.as_ref(),
                key.clone(),
                registration,
            ),
            false => super::super::server::register_resource(
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
    config: Option<Arc<super::config::Processor>>,
    task_id: usize,
    task_context: Option<Arc<TaskContext>>,
    task_type: Option<&'static str>,
    mcp_server: Option<Arc<super::super::server::McpServer>>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
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

    pub fn mcp_server(mut self, server: Arc<super::super::server::McpServer>) -> Self {
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
