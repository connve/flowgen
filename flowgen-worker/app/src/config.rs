//! Configuration structures for flowgen application and flows.
//!
//! Provides configuration structures for the main application and individual
//! flows. Supports deserialization from TOML files and environment variables.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Default NATS server URL function for serde.
fn default_nats_url() -> String {
    flowgen_nats::client::DEFAULT_NATS_URL.to_string()
}

/// Default cache database name.
pub const DEFAULT_CACHE_DB_NAME: &str = "flowgen_cache";

/// Supported flow configuration file extensions for recursive discovery.
pub const FLOW_CONFIG_EXTENSIONS: &[&str] = &["yaml", "yml", "json"];

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
    pub tasks: Vec<TaskType>,
    /// Whether this flow requires leader election (defaults to false if not specified).
    pub require_leader_election: Option<bool>,
}

/// Available task types in the flowgen ecosystem.
///
/// Each variant corresponds to a specific processor type from the
/// various flowgen worker crates. Task configurations are embedded
/// within each variant.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum TaskType {
    /// Data conversion task.
    convert(flowgen_core::task::convert::config::Processor),
    /// Iterate over arrays task.
    iterate(flowgen_core::task::iterate::config::Processor),
    /// Log output task.
    log(flowgen_core::task::log::config::Processor),
    /// Script execution task.
    script(flowgen_core::task::script::config::Processor),
    /// Buffer task for accumulating events into batches.
    buffer(flowgen_core::task::buffer::config::Processor),
    /// Object store read task.
    object_store_read(flowgen_object_store::config::ReadProcessor),
    /// Object store write task.
    object_store_write(flowgen_object_store::config::WriteProcessor),
    /// Object store list task.
    object_store_list(flowgen_object_store::config::ListProcessor),
    /// Object store move task.
    object_store_move(flowgen_object_store::config::MoveProcessor),
    /// Data generation task.
    generate(flowgen_core::task::generate::config::Subscriber),
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
    /// Salesforce Bulk API query job operations (create, get, delete, abort, get_results).
    salesforce_bulkapi_query_job(flowgen_salesforce::bulkapi::config::QueryJob),
    /// GCP BigQuery query task.
    gcp_bigquery_query(flowgen_gcp::bigquery::config::Query),
    /// GCP BigQuery Storage Read API task.
    gcp_bigquery_storage_read(flowgen_gcp::bigquery::config::StorageRead),
    /// GCP BigQuery unified job operations (create, get, cancel, delete).
    gcp_bigquery_job(flowgen_gcp::bigquery::config::Job),
}

impl TaskType {
    /// Returns the task type as a static string for event categorization.
    pub const fn as_str(&self) -> &'static str {
        match self {
            TaskType::convert(_) => "convert",
            TaskType::iterate(_) => "iterate",
            TaskType::log(_) => "log",
            TaskType::script(_) => "script",
            TaskType::buffer(_) => "buffer",
            TaskType::object_store_read(_) => "object_store_read",
            TaskType::object_store_write(_) => "object_store_write",
            TaskType::object_store_list(_) => "object_store_list",
            TaskType::object_store_move(_) => "object_store_move",
            TaskType::generate(_) => "generate",
            TaskType::http_request(_) => "http_request",
            TaskType::http_webhook(_) => "http_webhook",
            TaskType::nats_jetstream_subscriber(_) => "nats_jetstream_subscriber",
            TaskType::nats_jetstream_publisher(_) => "nats_jetstream_publisher",
            TaskType::salesforce_pubsub_subscriber(_) => "salesforce_pubsub_subscriber",
            TaskType::salesforce_pubsub_publisher(_) => "salesforce_pubsub_publisher",
            TaskType::salesforce_bulkapi_query_job(_) => "salesforce_bulkapi_query_job",
            TaskType::gcp_bigquery_query(_) => "gcp_bigquery_query",
            TaskType::gcp_bigquery_storage_read(_) => "gcp_bigquery_storage_read",
            TaskType::gcp_bigquery_job(_) => "gcp_bigquery_job",
        }
    }
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Main application configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct AppConfig {
    /// Optional cache configuration (shared across components).
    pub cache: Option<CacheOptions>,
    /// Flow discovery options (shared across components).
    pub flows: FlowOptions,
    /// Optional resource loading configuration (shared across components).
    pub resources: Option<ResourceOptions>,
    /// Optional worker component configuration.
    pub worker: Option<WorkerConfig>,
}

/// Worker component configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct WorkerConfig {
    /// Optional HTTP server configuration for webhooks, health checks, and metrics.
    pub http_server: Option<HttpServerOptions>,
    /// Optional host coordination configuration.
    pub host: Option<HostOptions>,
    /// Optional app-level retry configuration (can be overridden per task).
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Optional event channel buffer size (defaults to 10M if not specified).
    /// Controls memory allocation for the broadcast channel used for inter-task communication.
    /// Approximate memory usage: buffer_size * 128 bytes (e.g., 10M = ~1.2 GB).
    pub event_buffer_size: Option<usize>,
}

/// Cache type for storage backend.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    /// NATS JetStream Key-Value store.
    Nats,
}

/// Cache configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CacheOptions {
    /// Whether caching is enabled.
    pub enabled: bool,
    /// Cache backend type.
    #[serde(rename = "type")]
    pub cache_type: CacheType,
    /// Path to cache credentials file.
    pub credentials_path: PathBuf,
    /// NATS server URL (e.g., "nats://localhost:4222"). Defaults to "localhost:4222".
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// Cache database name (defaults to DEFAULT_CACHE_DB if not provided).
    pub db_name: Option<String>,
}

/// Flow loading configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowOptions {
    /// Path for discovering flow configuration files.
    /// Can be either:
    /// - A base directory path (e.g., "/flows") - will recursively discover all .yaml, .yml, and .json files
    /// - A glob pattern (e.g., "/flows/*.yaml") - for backward compatibility, uses pattern directly
    pub path: Option<PathBuf>,
}

/// Resource loading configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, Default)]
pub struct ResourceOptions {
    /// Base path for loading external resource files (SQL, JSON, HTML, etc.).
    /// Resource keys are resolved relative to this path.
    /// Example: with path="local/resources/", key="queries/orders.sql" resolves to "local/resources/queries/orders.sql".
    #[serde(default)]
    pub path: PathBuf,
}

/// HTTP server configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct HttpServerOptions {
    /// Whether HTTP server is enabled.
    pub enabled: bool,
    /// Optional HTTP server port number (defaults to 3000).
    pub port: Option<u16>,
    /// Optional path prefix for all routes (e.g., "/workers").
    pub routes_prefix: Option<String>,
}

/// Host coordination configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct HostOptions {
    /// Whether host coordination is enabled.
    pub enabled: bool,
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
                require_leader_election: None,
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
                require_leader_election: None,
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
            require_leader_election: None,
        };

        assert_eq!(flow.name, "test_flow");
        assert_eq!(flow.labels, Some(labels));
        assert!(flow.tasks.is_empty());
    }

    #[test]
    fn test_flow_with_tasks() {
        let convert_config = flowgen_core::task::convert::config::Processor::default();
        let task = TaskType::convert(convert_config);

        let flow = Flow {
            name: "flow_with_tasks".to_string(),
            labels: None,
            tasks: vec![task],
            require_leader_election: None,
        };

        assert_eq!(flow.name, "flow_with_tasks");
        assert!(flow.labels.is_none());
        assert_eq!(flow.tasks.len(), 1);
        assert!(matches!(flow.tasks[0], TaskType::convert(_)));
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
            require_leader_election: None,
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
            require_leader_election: None,
        };

        let cloned = flow.clone();
        assert_eq!(flow, cloned);
    }

    #[test]
    fn test_task_variants() {
        let convert_task =
            TaskType::convert(flowgen_core::task::convert::config::Processor::default());
        let generate_task =
            TaskType::generate(flowgen_core::task::generate::config::Subscriber::default());

        assert!(matches!(convert_task, TaskType::convert(_)));
        assert!(matches!(generate_task, TaskType::generate(_)));
    }

    #[test]
    fn test_app_config_creation() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: true,
                cache_type: CacheType::Nats,
                credentials_path: PathBuf::from("/test/cache"),
                url: "localhost:4222".to_string(),
                db_name: None,
            }),
            flows: FlowOptions {
                path: Some(PathBuf::from("/test/flows/*")),
            },
            resources: None,
            worker: Some(WorkerConfig {
                http_server: None,
                host: None,
                retry: None,
                event_buffer_size: None,
            }),
        };

        assert!(app_config.cache.is_some());
        assert!(app_config.cache.as_ref().unwrap().enabled);
        assert!(app_config.flows.path.is_some());
        assert!(app_config.worker.as_ref().unwrap().http_server.is_none());
        assert!(app_config.worker.as_ref().unwrap().host.is_none());
        assert!(app_config.worker.as_ref().unwrap().retry.is_none());
        assert!(app_config
            .worker
            .as_ref()
            .unwrap()
            .event_buffer_size
            .is_none());
        assert!(app_config.resources.is_none());
    }

    #[test]
    fn test_app_config_without_cache() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                path: Some(PathBuf::from("/flows/*")),
            },
            resources: None,
            worker: Some(WorkerConfig {
                http_server: None,
                host: None,
                retry: None,
                event_buffer_size: None,
            }),
        };

        assert!(app_config.cache.is_none());
        assert!(app_config.flows.path.is_some());
    }

    #[test]
    fn test_app_config_serialization() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: false,
                cache_type: CacheType::Nats,
                credentials_path: PathBuf::from("/serialize/cache"),
                url: "localhost:4222".to_string(),
                db_name: Some("test_db".to_string()),
            }),
            flows: FlowOptions {
                path: Some(PathBuf::from("/serialize/flows/*")),
            },
            resources: None,
            worker: Some(WorkerConfig {
                http_server: None,
                host: None,
                retry: None,
                event_buffer_size: None,
            }),
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
                cache_type: CacheType::Nats,
                credentials_path: PathBuf::from("/clone/cache"),
                url: "localhost:4222".to_string(),
                db_name: None,
            }),
            flows: FlowOptions { path: None },
            resources: None,
            worker: Some(WorkerConfig {
                http_server: None,
                host: None,
                retry: None,
                event_buffer_size: None,
            }),
        };

        let cloned = app_config.clone();
        assert_eq!(app_config, cloned);
    }

    #[test]
    fn test_cache_options_creation() {
        let cache_options = CacheOptions {
            enabled: true,
            cache_type: CacheType::Nats,
            credentials_path: PathBuf::from("/test/credentials_path"),
            url: "localhost:4222".to_string(),
            db_name: None,
        };

        assert!(cache_options.enabled);
        assert_eq!(
            cache_options.credentials_path,
            PathBuf::from("/test/credentials_path")
        );
    }

    #[test]
    fn test_cache_options_disabled() {
        let cache_options = CacheOptions {
            enabled: false,
            cache_type: CacheType::Nats,
            credentials_path: PathBuf::from("/disabled/cache"),
            url: "localhost:4222".to_string(),
            db_name: Some("custom_db".to_string()),
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
            cache_type: CacheType::Nats,
            credentials_path: PathBuf::from("/serialize/credentials_path"),
            url: "localhost:4222".to_string(),
            db_name: None,
        };

        let serialized = serde_json::to_string(&cache_options).unwrap();
        let deserialized: CacheOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cache_options, deserialized);
    }

    #[test]
    fn test_flow_options_with_path() {
        let flow_options = FlowOptions {
            path: Some(PathBuf::from("/test/flows/*.toml")),
        };

        assert!(flow_options.path.is_some());
        assert_eq!(
            flow_options.path.unwrap(),
            PathBuf::from("/test/flows/*.toml")
        );
    }

    #[test]
    fn test_flow_options_without_path() {
        let flow_options = FlowOptions { path: None };

        assert!(flow_options.path.is_none());
    }

    #[test]
    fn test_flow_options_serialization() {
        let flow_options = FlowOptions {
            path: Some(PathBuf::from("/serialize/flows/*.toml")),
        };

        let serialized = serde_json::to_string(&flow_options).unwrap();
        let deserialized: FlowOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow_options, deserialized);
    }

    #[test]
    fn test_complex_flow_config() {
        let convert_config = flowgen_core::task::convert::config::Processor::default();
        let generate_config = flowgen_core::task::generate::config::Subscriber::default();

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
                    TaskType::convert(convert_config),
                    TaskType::generate(generate_config),
                ],
                require_leader_election: None,
            },
        };

        assert_eq!(flow_config.flow.name, "complex_flow");
        assert_eq!(flow_config.flow.labels, Some(labels));
        assert_eq!(flow_config.flow.tasks.len(), 2);
        assert!(matches!(flow_config.flow.tasks[0], TaskType::convert(_)));
        assert!(matches!(flow_config.flow.tasks[1], TaskType::generate(_)));
    }

    #[test]
    fn test_http_server_options_creation() {
        let http_server_options = HttpServerOptions {
            enabled: true,
            port: Some(8080),
            routes_prefix: None,
        };

        assert!(http_server_options.enabled);
        assert_eq!(http_server_options.port, Some(8080));
    }

    #[test]
    fn test_http_server_options_without_port() {
        let http_server_options = HttpServerOptions {
            enabled: false,
            port: None,
            routes_prefix: None,
        };

        assert!(!http_server_options.enabled);
        assert!(http_server_options.port.is_none());
    }

    #[test]
    fn test_app_config_with_http_server_options() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions { path: None },
            resources: None,
            worker: Some(WorkerConfig {
                http_server: Some(HttpServerOptions {
                    enabled: true,
                    port: Some(8080),
                    routes_prefix: Some("/workers".to_string()),
                }),
                host: None,
                retry: None,
                event_buffer_size: None,
            }),
        };

        assert!(app_config.worker.as_ref().unwrap().http_server.is_some());
        let http_server = app_config
            .worker
            .as_ref()
            .unwrap()
            .http_server
            .as_ref()
            .unwrap();
        assert!(http_server.enabled);
        assert_eq!(http_server.port, Some(8080));
        assert_eq!(http_server.routes_prefix, Some("/workers".to_string()));
    }
}
