//! Configuration structures for flowgen application and flows.
//!
//! Provides configuration structures for the main application and individual
//! flows. Supports deserialization from TOML files and environment variables.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Top-level configuration for an individual flow.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowConfig {
    /// Flow definition containing name and tasks.
    pub flow: Flow,
}

/// Flow definition with name and task list.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Flow {
    /// Unique name for this flow.
    pub name: String,
    /// Optional label for logging.
    pub labels: Option<Map<String, Value>>,
    /// List of tasks to execute in this flow.
    pub tasks: Vec<Task>,
}

/// Available task types in the flowgen ecosystem.
///
/// Each variant corresponds to a specific processor type from the
/// various flowgen worker crates. Task configurations are embedded
/// within each variant.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum Task {
    /// Data conversion task.
    convert(flowgen_core::convert::config::Processor),
    /// Object store reader task.
    object_store_reader(flowgen_object_store::config::Reader),
    /// Object store writer task.
    object_store_writer(flowgen_object_store::config::Writer),
    /// Data generation task.
    generate(flowgen_core::generate::config::Subscriber),
    /// HTTP request task.
    http_request(flowgen_http::config::Processor),
    /// HTTP webhook handler task.
    http_webhook(flowgen_http::config::Processor),
    /// NATS JetStream subscriber task.
    nats_jetstream_subscriber(flowgen_nats::jetstream::config::Subscriber),
    /// NATS JetStream publisher task.
    nats_jetstream_publisher(flowgen_nats::jetstream::config::Publisher),
    /// Salesforce Pub/Sub subscriber task.
    salesforce_pubsub_subscriber(flowgen_salesforce::pubsub::config::Subscriber),
    /// Salesforce Pub/Sub publisher task.
    salesforce_pubsub_publisher(flowgen_salesforce::pubsub::config::Publisher),
}

/// Main application configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct AppConfig {
    /// Optional cache configuration.
    pub cache: Option<CacheOptions>,
    /// Flow discovery options.
    pub flows: FlowOptions,
    /// Optional HTTP server configuration.
    pub http: Option<HttpOptions>,
    /// Optional host coordination configuration.
    pub host: Option<HostOptions>,
}

/// Cache configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CacheOptions {
    /// Whether caching is enabled.
    pub enabled: bool,
    /// Path to cache credentials file.
    pub credentials_path: PathBuf,
}

/// Flow loading configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowOptions {
    /// Directory pattern for discovering flow configuration files.
    pub dir: Option<PathBuf>,
}

/// HTTP server configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct HttpOptions {
    /// Optional HTTP server port number (defaults to 3000).
    pub port: Option<u16>,
}

/// Host type for coordination.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HostType {
    /// Kubernetes host.
    K8s,
}

/// Host coordination configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct HostOptions {
    /// Host type for coordination.
    pub host_type: HostType,
    /// Optional namespace for Kubernetes resources.
    pub namespace: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::path::PathBuf;

    #[test]
    fn test_flow_config_creation() {
        let flow_config = FlowConfig {
            flow: Flow {
                name: "test_flow".to_string(),
                labels: None,
                tasks: vec![],
            },
        };

        assert_eq!(flow_config.flow.name, "test_flow");
        assert!(flow_config.flow.labels.is_none());
        assert!(flow_config.flow.tasks.is_empty());
    }

    #[test]
    fn test_flow_config_serialization() {
        let mut labels = Map::new();
        labels.insert("environment".to_string(), Value::String("test".to_string()));

        let flow_config = FlowConfig {
            flow: Flow {
                name: "serialize_test".to_string(),
                labels: Some(labels),
                tasks: vec![],
            },
        };

        let serialized = serde_json::to_string(&flow_config).unwrap();
        let deserialized: FlowConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow_config, deserialized);
    }

    #[test]
    fn test_flow_creation() {
        let mut labels = Map::new();
        labels.insert("type".to_string(), Value::String("test".to_string()));

        let flow = Flow {
            name: "test_flow".to_string(),
            labels: Some(labels.clone()),
            tasks: vec![],
        };

        assert_eq!(flow.name, "test_flow");
        assert_eq!(flow.labels, Some(labels));
        assert!(flow.tasks.is_empty());
    }

    #[test]
    fn test_flow_with_tasks() {
        let convert_config = flowgen_core::convert::config::Processor::default();
        let task = Task::convert(convert_config);

        let flow = Flow {
            name: "flow_with_tasks".to_string(),
            labels: None,
            tasks: vec![task],
        };

        assert_eq!(flow.name, "flow_with_tasks");
        assert!(flow.labels.is_none());
        assert_eq!(flow.tasks.len(), 1);
        assert!(matches!(flow.tasks[0], Task::convert(_)));
    }

    #[test]
    fn test_flow_serialization() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Serializable Flow".to_string()),
        );

        let flow = Flow {
            name: "serialize_flow".to_string(),
            labels: Some(labels),
            tasks: vec![],
        };

        let serialized = serde_json::to_string(&flow).unwrap();
        let deserialized: Flow = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow, deserialized);
    }

    #[test]
    fn test_flow_clone() {
        let flow = Flow {
            name: "clone_test".to_string(),
            labels: None,
            tasks: vec![],
        };

        let cloned = flow.clone();
        assert_eq!(flow, cloned);
    }

    #[test]
    fn test_task_variants() {
        let convert_task = Task::convert(flowgen_core::convert::config::Processor::default());
        let generate_task = Task::generate(flowgen_core::generate::config::Subscriber::default());

        assert!(matches!(convert_task, Task::convert(_)));
        assert!(matches!(generate_task, Task::generate(_)));
    }

    #[test]
    fn test_app_config_creation() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: true,
                credentials_path: PathBuf::from("/test/cache"),
            }),
            flows: FlowOptions {
                dir: Some(PathBuf::from("/test/flows/*")),
            },
            http: None,
            host: None,
        };

        assert!(app_config.cache.is_some());
        assert!(app_config.cache.as_ref().unwrap().enabled);
        assert!(app_config.flows.dir.is_some());
        assert!(app_config.http.is_none());
        assert!(app_config.host.is_none());
    }

    #[test]
    fn test_app_config_without_cache() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                dir: Some(PathBuf::from("/flows/*")),
            },
            http: None,
            host: None,
        };

        assert!(app_config.cache.is_none());
        assert!(app_config.flows.dir.is_some());
    }

    #[test]
    fn test_app_config_serialization() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: false,
                credentials_path: PathBuf::from("/serialize/cache"),
            }),
            flows: FlowOptions {
                dir: Some(PathBuf::from("/serialize/flows/*")),
            },
            http: None,
            host: None,
        };

        let serialized = serde_json::to_string(&app_config).unwrap();
        let deserialized: AppConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(app_config, deserialized);
    }

    #[test]
    fn test_app_config_clone() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: true,
                credentials_path: PathBuf::from("/clone/cache"),
            }),
            flows: FlowOptions { dir: None },
            http: None,
            host: None,
        };

        let cloned = app_config.clone();
        assert_eq!(app_config, cloned);
    }

    #[test]
    fn test_cache_options_creation() {
        let cache_options = CacheOptions {
            enabled: true,
            credentials_path: PathBuf::from("/test/credentials"),
        };

        assert!(cache_options.enabled);
        assert_eq!(
            cache_options.credentials_path,
            PathBuf::from("/test/credentials")
        );
    }

    #[test]
    fn test_cache_options_disabled() {
        let cache_options = CacheOptions {
            enabled: false,
            credentials_path: PathBuf::from("/disabled/cache"),
        };

        assert!(!cache_options.enabled);
        assert_eq!(
            cache_options.credentials_path,
            PathBuf::from("/disabled/cache")
        );
    }

    #[test]
    fn test_cache_options_serialization() {
        let cache_options = CacheOptions {
            enabled: true,
            credentials_path: PathBuf::from("/serialize/credentials"),
        };

        let serialized = serde_json::to_string(&cache_options).unwrap();
        let deserialized: CacheOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cache_options, deserialized);
    }

    #[test]
    fn test_flow_options_with_dir() {
        let flow_options = FlowOptions {
            dir: Some(PathBuf::from("/test/flows/*.toml")),
        };

        assert!(flow_options.dir.is_some());
        assert_eq!(
            flow_options.dir.unwrap(),
            PathBuf::from("/test/flows/*.toml")
        );
    }

    #[test]
    fn test_flow_options_without_dir() {
        let flow_options = FlowOptions { dir: None };

        assert!(flow_options.dir.is_none());
    }

    #[test]
    fn test_flow_options_serialization() {
        let flow_options = FlowOptions {
            dir: Some(PathBuf::from("/serialize/flows/*.toml")),
        };

        let serialized = serde_json::to_string(&flow_options).unwrap();
        let deserialized: FlowOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow_options, deserialized);
    }

    #[test]
    fn test_complex_flow_config() {
        let convert_config = flowgen_core::convert::config::Processor::default();
        let generate_config = flowgen_core::generate::config::Subscriber::default();

        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Complex Multi-Task Flow".to_string()),
        );
        labels.insert("complexity".to_string(), Value::String("high".to_string()));

        let flow_config = FlowConfig {
            flow: Flow {
                name: "complex_flow".to_string(),
                labels: Some(labels.clone()),
                tasks: vec![
                    Task::convert(convert_config),
                    Task::generate(generate_config),
                ],
            },
        };

        assert_eq!(flow_config.flow.name, "complex_flow");
        assert_eq!(flow_config.flow.labels, Some(labels));
        assert_eq!(flow_config.flow.tasks.len(), 2);
        assert!(matches!(flow_config.flow.tasks[0], Task::convert(_)));
        assert!(matches!(flow_config.flow.tasks[1], Task::generate(_)));
    }

    #[test]
    fn test_http_options_creation() {
        let http_options = HttpOptions { port: Some(8080) };

        assert_eq!(http_options.port, Some(8080));
    }

    #[test]
    fn test_http_options_without_port() {
        let http_options = HttpOptions { port: None };

        assert!(http_options.port.is_none());
    }

    #[test]
    fn test_app_config_with_http_options() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions { dir: None },
            http: Some(HttpOptions { port: Some(8080) }),
            host: None,
        };

        assert!(app_config.http.is_some());
        assert_eq!(app_config.http.as_ref().unwrap().port, Some(8080));
    }
}
