//! Task execution context providing metadata and runtime configuration.
//!
//! Contains task and flow identification, runtime feature flags, and other shared
//! context that tasks need for proper execution, logging, and coordination.

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
    pub flow_id: String,
    /// Optional human-readable flow label for logging.
    pub flow_label: Option<String>,
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
    /// Unique flow identifier (required for build).
    flow_id: Option<String>,
    /// Optional human-readable flow label.
    flow_label: Option<String>,
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

    /// Sets the unique flow identifier.
    ///
    /// # Arguments
    /// * `id` - The unique identifier for this flow
    pub fn flow_id(mut self, id: String) -> Self {
        self.flow_id = Some(id);
        self
    }

    /// Sets the optional human-readable flow label.
    ///
    /// # Arguments
    /// * `label` - Optional label for logging and display purposes
    pub fn flow_label(mut self, label: Option<String>) -> Self {
        self.flow_label = label;
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
            flow_id: self
                .flow_id
                .ok_or_else(|| Error::MissingRequiredAttribute("flow_id".to_string()))?,
            flow_label: self.flow_label,
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
        assert!(builder.flow_id.is_none());
        assert!(builder.flow_label.is_none());
        assert!(!builder.k8s_enabled);
        assert!(builder.k8s_namespace.is_none());
        assert!(builder.metrics_enabled);
    }

    #[test]
    fn test_task_context_builder_build_success() {
        let context = TaskContextBuilder::new()
            .flow_id("test-flow".to_string())
            .flow_label(Some("Test Flow".to_string()))
            .k8s_enabled(true)
            .k8s_namespace(Some("flowgen".to_string()))
            .metrics_enabled(false)
            .build()
            .unwrap();

        assert_eq!(context.flow_id, "test-flow");
        assert_eq!(context.flow_label, Some("Test Flow".to_string()));
        assert!(context.k8s_enabled);
        assert_eq!(context.k8s_namespace, Some("flowgen".to_string()));
        assert!(!context.metrics_enabled);
    }

    #[test]
    fn test_task_context_builder_missing_flow_id() {
        let result = TaskContextBuilder::new()
            .flow_label(Some("Test".to_string()))
            .build();

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Missing required attribute: flow_id"));
    }

    #[test]
    fn test_task_context_builder_defaults() {
        let context = TaskContextBuilder::new()
            .flow_id("default-test".to_string())
            .build()
            .unwrap();

        assert_eq!(context.flow_id, "default-test");
        assert!(context.flow_label.is_none());
        assert!(!context.k8s_enabled);
        assert!(context.k8s_namespace.is_none());
        assert!(context.metrics_enabled);
    }

    #[test]
    fn test_task_context_builder_chain() {
        let context = TaskContextBuilder::new()
            .flow_id("chain-test".to_string())
            .flow_label(Some("Chained Builder Test".to_string()))
            .k8s_enabled(true)
            .k8s_namespace(Some("test-namespace".to_string()))
            .metrics_enabled(true)
            .build()
            .unwrap();

        assert_eq!(context.flow_id, "chain-test");
        assert_eq!(context.flow_label, Some("Chained Builder Test".to_string()));
        assert!(context.k8s_enabled);
        assert_eq!(context.k8s_namespace, Some("test-namespace".to_string()));
        assert!(context.metrics_enabled);
    }

    #[test]
    fn test_task_context_clone() {
        let context = TaskContextBuilder::new()
            .flow_id("clone-test".to_string())
            .flow_label(Some("Clone Test".to_string()))
            .build()
            .unwrap();

        let cloned = context.clone();
        assert_eq!(context.flow_id, cloned.flow_id);
        assert_eq!(context.flow_label, cloned.flow_label);
        assert_eq!(context.k8s_enabled, cloned.k8s_enabled);
    }
}