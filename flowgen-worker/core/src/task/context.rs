//! Task execution context providing metadata and runtime configuration.
//!
//! Contains task and flow identification, runtime feature flags, and other shared
//! context that tasks need for proper execution, logging, and coordination.

use serde_json::{Map, Value};

/// Errors that can occur during TaskContext operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Required builder attribute was not provided.
    #[error("Missing required attribute: {}", _0)]
    MissingRequiredAttribute(String),
}

/// Context information for task execution shared across all tasks.
#[derive(Clone, Debug)]
pub struct TaskContext {
    /// Unique flow identifier.
    pub flow_name: String,
    /// Optional labels for flow metadata and logging.
    pub flow_labels: Option<Map<String, Value>>,
    /// Whether Kubernetes features are enabled for this flow.
    pub k8s_enabled: bool,
    /// Kubernetes namespace for resources created by this flow.
    pub k8s_namespace: Option<String>,
    /// Whether metrics collection is enabled for this flow.
    pub metrics_enabled: bool,
}

/// Builder for constructing TaskContext instances.
#[derive(Default)]
pub struct TaskContextBuilder {
    /// Unique flow name.
    flow_name: Option<String>,
    /// Optional labels for flow metadata.
    flow_labels: Option<Map<String, Value>>,
    /// Whether Kubernetes features are enabled.
    k8s_enabled: bool,
    /// Kubernetes namespace for resources.
    k8s_namespace: Option<String>,
    /// Whether metrics collection is enabled.
    metrics_enabled: bool,
}

impl TaskContextBuilder {
    /// Creates a new TaskContextBuilder with default values.
    pub fn new() -> Self {
        Self {
            k8s_enabled: false,
            metrics_enabled: true,
            ..Default::default()
        }
    }

    /// Sets the unique flow name.
    ///
    /// # Arguments
    /// * `id` - The unique identifier for this flow
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

    /// Sets whether Kubernetes features are enabled.
    ///
    /// # Arguments
    /// * `enabled` - Whether to enable Kubernetes coordination features
    pub fn k8s_enabled(mut self, enabled: bool) -> Self {
        self.k8s_enabled = enabled;
        self
    }

    /// Sets the Kubernetes namespace for resources.
    ///
    /// # Arguments
    /// * `namespace` - Optional namespace for Kubernetes resources
    pub fn k8s_namespace(mut self, namespace: Option<String>) -> Self {
        self.k8s_namespace = namespace;
        self
    }

    /// Sets whether metrics collection is enabled.
    ///
    /// # Arguments
    /// * `enabled` - Whether to enable metrics collection
    pub fn metrics_enabled(mut self, enabled: bool) -> Self {
        self.metrics_enabled = enabled;
        self
    }

    /// Builds the TaskContext instance.
    ///
    /// # Errors
    /// Returns `Error::MissingRequiredAttribute` if required fields are not set.
    pub fn build(self) -> Result<TaskContext, Error> {
        Ok(TaskContext {
            flow_name: self
                .flow_name
                .ok_or_else(|| Error::MissingRequiredAttribute("flow_name".to_string()))?,
            flow_labels: self.flow_labels,
            k8s_enabled: self.k8s_enabled,
            k8s_namespace: self.k8s_namespace,
            metrics_enabled: self.metrics_enabled,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_context_builder_new() {
        let builder = TaskContextBuilder::new();
        assert!(builder.flow_name.is_none());
        assert!(builder.flow_labels.is_none());
        assert!(!builder.k8s_enabled);
        assert!(builder.k8s_namespace.is_none());
        assert!(builder.metrics_enabled);
    }

    #[test]
    fn test_task_context_builder_build_success() {
        let mut labels = Map::new();
        labels.insert("name".to_string(), Value::String("Test Flow".to_string()));
        labels.insert("environment".to_string(), Value::String("test".to_string()));

        let context = TaskContextBuilder::new()
            .flow_name("test-flow".to_string())
            .flow_labels(Some(labels.clone()))
            .k8s_enabled(true)
            .k8s_namespace(Some("flowgen".to_string()))
            .metrics_enabled(false)
            .build()
            .unwrap();

        assert_eq!(context.flow_name, "test-flow");
        assert_eq!(context.flow_labels, Some(labels));
        assert!(context.k8s_enabled);
        assert_eq!(context.k8s_namespace, Some("flowgen".to_string()));
        assert!(!context.metrics_enabled);
    }

    #[test]
    fn test_task_context_builder_missing_flow_name() {
        let mut labels = Map::new();
        labels.insert("name".to_string(), Value::String("Test".to_string()));

        let result = TaskContextBuilder::new().flow_labels(Some(labels)).build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: flow_name"));
    }

    #[test]
    fn test_task_context_builder_defaults() {
        let context = TaskContextBuilder::new()
            .flow_name("default-test".to_string())
            .build()
            .unwrap();

        assert_eq!(context.flow_name, "default-test");
        assert!(context.flow_labels.is_none());
        assert!(!context.k8s_enabled);
        assert!(context.k8s_namespace.is_none());
        assert!(context.metrics_enabled);
    }

    #[test]
    fn test_task_context_builder_chain() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Chained Builder Test".to_string()),
        );
        labels.insert("type".to_string(), Value::String("test".to_string()));

        let context = TaskContextBuilder::new()
            .flow_name("chain-test".to_string())
            .flow_labels(Some(labels.clone()))
            .k8s_enabled(true)
            .k8s_namespace(Some("test-namespace".to_string()))
            .metrics_enabled(true)
            .build()
            .unwrap();

        assert_eq!(context.flow_name, "chain-test");
        assert_eq!(context.flow_labels, Some(labels));
        assert!(context.k8s_enabled);
        assert_eq!(context.k8s_namespace, Some("test-namespace".to_string()));
        assert!(context.metrics_enabled);
    }

    #[test]
    fn test_task_context_clone() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );

        let context = TaskContextBuilder::new()
            .flow_name("clone-test".to_string())
            .flow_labels(Some(labels.clone()))
            .build()
            .unwrap();

        let cloned = context.clone();
        assert_eq!(context.flow_name, cloned.flow_name);
        assert_eq!(context.flow_labels, cloned.flow_labels);
        assert_eq!(context.k8s_enabled, cloned.k8s_enabled);
    }
}
