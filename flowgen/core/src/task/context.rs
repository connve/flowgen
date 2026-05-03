//! Task execution context providing metadata and runtime configuration.
//!
//! Contains task and flow identification, runtime feature flags, and other shared
//! context that tasks need for proper execution, logging, and coordination.

use serde_json::{Map, Value};

/// Errors that can occur during TaskContext operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
}

/// Flow identification and metadata.
#[derive(Clone, Debug)]
pub struct FlowOptions {
    /// Flow name.
    pub name: String,
    /// Optional labels for flow metadata.
    pub labels: Option<Map<String, Value>>,
}

/// Context information for task execution shared across all tasks.
#[derive(Clone)]
pub struct TaskContext {
    /// Flow identification and metadata.
    pub flow: FlowOptions,
    /// Task manager for centralized task lifecycle management.
    pub task_manager: std::sync::Arc<crate::task::manager::TaskManager>,
    /// Shared cache for task operations.
    pub cache: std::sync::Arc<dyn crate::cache::Cache>,
    /// Optional shared HTTP server for webhook tasks.
    pub http_server: Option<std::sync::Arc<dyn crate::http_server::HttpServer>>,
    /// Optional shared MCP server for exposing flows as MCP tools.
    pub mcp_server: Option<std::sync::Arc<dyn crate::mcp_server::McpServer>>,
    /// Optional shared response registry for streaming progress back to source tasks.
    pub response_registry: Option<std::sync::Arc<crate::registry::ResponseRegistry>>,
    /// Optional resource loader for loading external assets (SQL files, templates, etc.).
    pub resource_loader: Option<crate::resource::ResourceLoader>,
    /// Optional app-level retry configuration (can be overridden per task).
    pub retry: Option<crate::retry::RetryConfig>,
    /// Cancellation token for graceful shutdown coordination.
    pub cancellation_token: tokio_util::sync::CancellationToken,
    /// Number of leaf tasks in this flow's directed acyclic graph.
    ///
    /// Source tasks use this when constructing the completion channel so the
    /// upstream message is acknowledged only after every leaf has signalled.
    /// Defaults to one for linear flows where a single terminal task signals
    /// completion.
    pub leaf_count: usize,
}

impl std::fmt::Debug for TaskContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskContext")
            .field("flow", &self.flow)
            .field("task_manager", &"<TaskManager>")
            .field("cache", &"<Cache>")
            .field(
                "http_server",
                &self.http_server.as_ref().map(|_| "<HttpServer>"),
            )
            .field(
                "mcp_server",
                &self.mcp_server.as_ref().map(|_| "<McpServer>"),
            )
            .field(
                "response_registry",
                &self
                    .response_registry
                    .as_ref()
                    .map(|_| "<ResponseRegistry>"),
            )
            .field("resource_loader", &self.resource_loader)
            .field("retry", &self.retry)
            .field("cancellation_token", &"<CancellationToken>")
            .finish()
    }
}

/// Builder for constructing TaskContext instances.
#[derive(Default)]
pub struct TaskContextBuilder {
    /// Unique flow name.
    flow_name: Option<String>,
    /// Optional labels for flow metadata.
    flow_labels: Option<Map<String, Value>>,
    /// Task manager for centralized task lifecycle management.
    task_manager: Option<std::sync::Arc<crate::task::manager::TaskManager>>,
    /// Shared cache for task operations.
    cache: Option<std::sync::Arc<dyn crate::cache::Cache>>,
    /// Optional shared HTTP server for webhook tasks.
    http_server: Option<std::sync::Arc<dyn crate::http_server::HttpServer>>,
    /// Optional shared MCP server for exposing flows as MCP tools.
    mcp_server: Option<std::sync::Arc<dyn crate::mcp_server::McpServer>>,
    /// Optional shared response registry for streaming progress.
    response_registry: Option<std::sync::Arc<crate::registry::ResponseRegistry>>,
    /// Resource loader for loading external assets.
    resource_loader: Option<crate::resource::ResourceLoader>,
    /// Optional app-level retry configuration.
    retry: Option<crate::retry::RetryConfig>,
    /// Cancellation token for graceful shutdown coordination.
    cancellation_token: Option<tokio_util::sync::CancellationToken>,
    /// Number of leaf tasks in the flow. Defaults to one when not set.
    leaf_count: Option<usize>,
}

impl TaskContextBuilder {
    /// Creates a new TaskContextBuilder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the unique flow name.
    ///
    /// # Arguments
    /// * `name` - The unique name for this flow.
    pub fn flow_name(mut self, name: String) -> Self {
        self.flow_name = Some(name);
        self
    }

    /// Sets the optional flow labels for metadata.
    ///
    /// # Arguments
    /// * `labels` - Optional labels map for metadata and logging
    pub fn flow_labels(mut self, labels: Option<Map<String, Value>>) -> Self {
        self.flow_labels = labels;
        self
    }

    /// Sets the task manager for centralized task lifecycle management.
    ///
    /// # Arguments
    /// * `task_manager` - Task manager instance
    pub fn task_manager(
        mut self,
        task_manager: std::sync::Arc<crate::task::manager::TaskManager>,
    ) -> Self {
        self.task_manager = Some(task_manager);
        self
    }

    /// Sets the cache for task operations.
    ///
    /// # Arguments
    /// * `cache` - Cache instance
    pub fn cache(mut self, cache: std::sync::Arc<dyn crate::cache::Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Sets the optional HTTP server for webhook tasks.
    ///
    /// # Arguments
    /// * `http_server` - Optional HTTP server instance
    pub fn http_server(
        mut self,
        http_server: Option<std::sync::Arc<dyn crate::http_server::HttpServer>>,
    ) -> Self {
        self.http_server = http_server;
        self
    }

    /// Sets the optional MCP server for exposing flows as MCP tools.
    ///
    /// # Arguments
    /// * `mcp_server` - Optional MCP server instance
    pub fn mcp_server(
        mut self,
        mcp_server: Option<std::sync::Arc<dyn crate::mcp_server::McpServer>>,
    ) -> Self {
        self.mcp_server = mcp_server;
        self
    }

    /// Sets the shared response registry for streaming progress.
    pub fn response_registry(
        mut self,
        registry: std::sync::Arc<crate::registry::ResponseRegistry>,
    ) -> Self {
        self.response_registry = Some(registry);
        self
    }

    /// Sets the resource loader for loading external assets.
    ///
    /// # Arguments
    /// * `resource_loader` - Optional resource loader instance
    pub fn resource_loader(
        mut self,
        resource_loader: Option<crate::resource::ResourceLoader>,
    ) -> Self {
        self.resource_loader = resource_loader;
        self
    }

    /// Sets the app-level retry configuration.
    ///
    /// # Arguments
    /// * `retry` - Retry configuration
    pub fn retry(mut self, retry: crate::retry::RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Sets the cancellation token for graceful shutdown.
    ///
    /// # Arguments
    /// * `cancellation_token` - Cancellation token for coordinating graceful shutdown
    pub fn cancellation_token(
        mut self,
        cancellation_token: tokio_util::sync::CancellationToken,
    ) -> Self {
        self.cancellation_token = Some(cancellation_token);
        self
    }

    /// Sets the number of leaf tasks in the flow's directed acyclic graph.
    ///
    /// The flow builder computes this from the wired task topology and passes
    /// it here so source tasks can construct a completion channel that waits
    /// for every leaf to signal before acknowledging the upstream message.
    pub fn leaf_count(mut self, leaf_count: usize) -> Self {
        self.leaf_count = Some(leaf_count);
        self
    }

    /// Builds the TaskContext instance.
    ///
    /// # Errors
    /// Returns `Error::MissingBuilderAttribute` if required fields are not set.
    pub fn build(self) -> Result<TaskContext, Error> {
        Ok(TaskContext {
            flow: FlowOptions {
                name: self
                    .flow_name
                    .ok_or_else(|| Error::MissingBuilderAttribute("flow_name".to_string()))?,
                labels: self.flow_labels,
            },
            task_manager: self
                .task_manager
                .ok_or_else(|| Error::MissingBuilderAttribute("task_manager".to_string()))?,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingBuilderAttribute("cache".to_string()))?,
            http_server: self.http_server,
            mcp_server: self.mcp_server,
            response_registry: self.response_registry,
            resource_loader: self.resource_loader,
            retry: self.retry,
            cancellation_token: self.cancellation_token.unwrap_or_default(),
            leaf_count: self.leaf_count.unwrap_or(1),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_task_context_builder_new() {
        let builder = TaskContextBuilder::new();
        assert!(builder.flow_name.is_none());
        assert!(builder.flow_labels.is_none());
    }

    #[test]
    fn test_task_context_builder_build_success() {
        let mut labels = Map::new();
        labels.insert("name".to_string(), Value::String("Test Flow".to_string()));
        labels.insert("environment".to_string(), Value::String("test".to_string()));

        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let context = TaskContextBuilder::new()
            .flow_name("test-flow".to_string())
            .flow_labels(Some(labels.clone()))
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .unwrap();

        assert_eq!(context.flow.name, "test-flow");
        assert_eq!(context.flow.labels, Some(labels));
    }

    #[test]
    fn test_task_context_builder_missing_flow_name() {
        let mut labels = Map::new();
        labels.insert("name".to_string(), Value::String("Test".to_string()));

        let result = TaskContextBuilder::new().flow_labels(Some(labels)).build();

        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[test]
    fn test_task_context_builder_defaults() {
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let context = TaskContextBuilder::new()
            .flow_name("default-test".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .unwrap();

        assert_eq!(context.flow.name, "default-test");
        assert!(context.flow.labels.is_none());
    }

    #[test]
    fn test_task_context_builder_chain() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Chained Builder Test".to_string()),
        );
        labels.insert("type".to_string(), Value::String("test".to_string()));

        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let context = TaskContextBuilder::new()
            .flow_name("chain-test".to_string())
            .flow_labels(Some(labels.clone()))
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .unwrap();

        assert_eq!(context.flow.name, "chain-test");
        assert_eq!(context.flow.labels, Some(labels));
    }

    #[test]
    fn test_task_context_clone() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );

        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let context = TaskContextBuilder::new()
            .flow_name("clone-test".to_string())
            .flow_labels(Some(labels.clone()))
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .unwrap();

        let cloned = context.clone();
        assert_eq!(context.flow.name, cloned.flow.name);
        assert_eq!(context.flow.labels, cloned.flow.labels);
    }
}
