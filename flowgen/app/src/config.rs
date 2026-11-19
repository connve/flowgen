//! Configuration structures for flowgen application and flows.
//!
//! Provides configuration structures for the main application and individual
//! flows. Supports deserialization from TOML files and environment variables.

use flowgen_core::task::context::{Services, TaskContext};
use flowgen_core::task::meta::{Connection, Connector};
use flowgen_core::task::{Task, TaskConfig, TaskError, TaskIo, TaskMeta};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinSet;
use tracing::Instrument;

/// Environment overrides for any config key, layered over the config file.
///
/// `APP_` prefixes the key and `__` separates each level, so
/// `APP_WEB__AUTH__CLIENT_SECRET` sets `web.auth.client_secret`. The two
/// separators differ because field names contain single underscores.
pub fn env_source() -> config::Environment {
    config::Environment::with_prefix("APP")
        .prefix_separator("_")
        .separator("__")
}

/// Default NATS server URL function for serde.
fn default_nats_url() -> String {
    flowgen_nats::client::DEFAULT_NATS_URL.to_string()
}

/// The address every server listens on unless `host` says otherwise.
pub const DEFAULT_HOST: &str = "0.0.0.0";

fn default_redb_path() -> PathBuf {
    PathBuf::from(flowgen_core::workspace::STATE_DIR).join(flowgen_core::workspace::CACHE_FILE)
}

pub fn default_host() -> String {
    DEFAULT_HOST.to_string()
}

/// Default cache database name.
pub const DEFAULT_CACHE_DB_NAME: &str = "flowgen_cache";

/// The key prefix of flows in the system bucket.
pub const FLOWS_CACHE_PREFIX: &str = "flows";

/// The variable every run sets to the version of flowgen, so a flow can pick
/// what matches it, such as a hub's catalog for this version.
pub const VERSION_ENV: &str = "FLOWGEN_VERSION";

/// The variable a run sets, unless set already, to the address of its web server.
pub const WEB_URL_ENV: &str = "FLOWGEN_URL";

/// The variable a run sets, unless set already, to the address of its MCP server.
pub const MCP_URL_ENV: &str = "FLOWGEN_MCP_URL";

/// The variable a run sets, unless set already, to the address of its AI gateway.
pub const AI_GATEWAY_URL_ENV: &str = "FLOWGEN_AI_GATEWAY_URL";

/// The variable a run sets, unless set already, to the `kv_store` URL of its cache.
pub const CACHE_URL_ENV: &str = "FLOWGEN_CACHE_URL";

/// The variable a run sets, unless set already, to its cache's credentials
/// file, empty when it has none.
pub const CACHE_CREDENTIALS_ENV: &str = "FLOWGEN_CACHE_CREDENTIALS";

/// The variable a run sets, unless set already, to its system bucket.
pub const SYSTEM_BUCKET_ENV: &str = "FLOWGEN_SYSTEM_BUCKET";

/// The variable a run sets, unless set already, to the OCI repository the
/// workspace is released to and deployed from.
pub const RELEASES_ENV: &str = "FLOWGEN_RELEASES";

/// Where workspace releases go without `FLOWGEN_RELEASES`: an OCI Image
/// Layout next to the local cache.
pub const DEFAULT_RELEASES: &str = "oci-layout://.flowgen/releases";

/// The variable a run sets, unless set already, to the folder flows read
/// credentials from: `credentials.path`, else `credentials`.
pub const CREDENTIALS_ENV: &str = flowgen_core::credentials::FOLDER_ENV;

/// The key prefix of resources in the system bucket.
pub const RESOURCES_CACHE_PREFIX: &str = "resources";

/// Supported flow configuration file extensions for recursive discovery.
pub const FLOW_CONFIG_EXTENSIONS: &[&str] = &["yaml", "yml", "json"];

pub use flowgen_core::identity::FlowIdentity;

/// Deserialization surface for a flow YAML file.
///
/// Public API is [`FlowConfig`] — `FlowConfigRaw` exists only because
/// serde deserializes the flow body without an identity, which the loader
/// supplies from the path. The loader wraps this into a `FlowConfig` via
/// [`FlowConfig::from_path`].
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlowConfigRaw {
    /// Flow definition (tasks and optional labels).
    pub flow: Flow,
}

impl FlowConfigRaw {
    /// Parses a flow file: JSON when `key` ends in `.json`, YAML otherwise.
    pub fn parse(key: &str, content: &str) -> Result<Self, config::ConfigError> {
        let format = match key.ends_with(".json") {
            true => config::FileFormat::Json,
            false => config::FileFormat::Yaml,
        };
        config::Config::builder()
            .add_source(config::File::from_str(content, format))
            .build()?
            .try_deserialize()
    }
}

/// Top-level configuration for an individual flow.
///
/// Construct via [`FlowConfig::from_path`]. Deserialize the YAML into
/// [`FlowConfigRaw`] first, then wrap it with the identity from the path.
#[derive(PartialEq, Clone, Debug, Serialize)]
pub struct FlowConfig {
    /// Flow definition (tasks and optional labels).
    pub flow: Flow,
    /// Verbatim YAML source the loader read this flow from. Kept for the
    /// web API so the UI can render the file as authored — round-tripping
    /// through `serde_yaml` introduces enum tags and `null` sentinels for
    /// `Option::None` fields, which is not what an operator wants to see.
    #[serde(skip)]
    pub raw_source: Option<String>,
    /// Flow identity, established at construction. Never mutated after.
    #[serde(skip)]
    identity: FlowIdentity,
}

impl FlowConfig {
    /// Builds a `FlowConfig` from a path-shaped identity. `path` is the
    /// identity string (relative to `flows.path` with extension stripped for
    /// a filesystem load, or the KV key suffix for a cache load, or the
    /// caller-supplied path for a programmatic flow). Validates task names.
    pub fn from_path(
        raw: FlowConfigRaw,
        path: String,
        raw_source: Option<String>,
    ) -> Result<Self, flowgen_core::validate::Error> {
        let config = Self {
            flow: raw.flow,
            raw_source,
            identity: FlowIdentity::new(path),
        };
        config.validate_tasks()?;
        Ok(config)
    }

    /// Returns the flow identity — the path-shaped source of truth. Used as
    /// the registry key, the tracing `flow=` field, REST `path`, and resource
    /// URIs. For cache and lease keys, project via [`FlowIdentity::as_key`].
    pub fn identity(&self) -> &str {
        self.identity.as_str()
    }

    /// Returns the typed identity, exposing both the human string
    /// ([`FlowIdentity::as_str`]) and the key-safe id
    /// ([`FlowIdentity::as_key`]).
    pub fn identity_ref(&self) -> &FlowIdentity {
        &self.identity
    }

    /// Returns the human-facing display name: `flow.name` when set,
    /// otherwise the basename of the identity path. `name` no longer affects
    /// identity — it is a pure display override. Used for UI lists and log
    /// fields, never for keying.
    pub fn display_name(&self) -> &str {
        match self.flow.name.as_deref() {
            Some(name) => name,
            None => match self.identity.as_str().rsplit('/').next() {
                Some(basename) => basename,
                None => self.identity.as_str(),
            },
        }
    }

    fn validate_tasks(&self) -> Result<(), flowgen_core::validate::Error> {
        use flowgen_core::validate::{validate_name, NameField};
        for task in &self.flow.tasks {
            validate_name(NameField::Task, task.name())?;
        }
        Ok(())
    }
}

/// Default value for parallel_instances.
fn default_parallel_instances() -> usize {
    1
}

/// Flow definition with task list. Identity always comes from the flow's
/// path (see [`FlowIdentity`]); `name` is a pure display override and does
/// not affect identity.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Flow {
    /// Optional display-name override for the UI and log fields. Does not
    /// affect identity — see [`FlowConfig::display_name`]. Accepted for
    /// backward compatibility with configs that still declare it; a flow
    /// with no `name` derives its display name from the path basename.
    #[serde(default)]
    pub name: Option<String>,
    /// Optional label for logging.
    pub labels: Option<Map<String, Value>>,
    /// List of tasks to execute in this flow.
    pub tasks: Vec<TaskType>,
    /// Whether this flow requires leader election (defaults to false if not specified).
    pub require_leader_election: Option<bool>,
    /// Number of parallel flow instances to run within the same process.
    ///
    /// Spawns multiple independent flow pipelines for higher throughput.
    /// Best used with leader election enabled. Defaults to 1.
    #[serde(default = "default_parallel_instances")]
    pub parallel_instances: usize,
    /// How long a stop lets the events in flight finish; the app's `shutdown_timeout` when unset.
    #[serde(default, with = "humantime_serde")]
    pub shutdown_timeout: Option<Duration>,
}

/// How long a stopping flow lets its events in flight finish, when the config does not say.
pub fn default_shutdown_timeout() -> Duration {
    Duration::from_secs(20)
}

macro_rules! task_types {
    ($($(#[$doc:meta])* $tag:ident($config:ty),)+) => {
        /// Available task types in the flowgen ecosystem.
        ///
        /// Each variant corresponds to a specific processor type from the
        /// various flowgen worker crates. Task configurations are embedded
        /// within each variant.
        #[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
        // YAML configs reference task types by their lowercase tag (e.g. `convert:`,
        // `script:`). The serde-driven variant naming therefore has to use the same
        // shape — Rust's CamelCase convention does not apply to this public API.
        #[expect(non_camel_case_types, reason = "variant names are YAML tags")]
        pub enum TaskType {
            $($(#[$doc])* $tag($config),)+
        }

        impl TaskType {
            /// Every task type's YAML tag with its connector.
            pub const TASK_CONNECTORS: &'static [(&'static str, Connector)] =
                &[$((stringify!($tag), <$config as TaskMeta>::CONNECTOR),)+];

            /// Returns the task type as a static string for event categorization.
            pub const fn as_str(&self) -> &'static str {
                match self {
                    $(TaskType::$tag(_) => stringify!($tag),)+
                }
            }

            /// Returns the task name from its configuration.
            pub fn name(&self) -> &str {
                match self {
                    $(TaskType::$tag(config) => config.name(),)+
                }
            }

            /// The tasks this one takes events from; `None` means the previous one.
            pub fn depends_on(&self) -> Option<&[String]> {
                match self {
                    $(TaskType::$tag(config) => config.depends_on(),)+
                }
            }

            /// The external systems the task connects to, declared by its crate.
            pub fn connections(&self) -> Vec<Connection> {
                match self {
                    $(TaskType::$tag(config) => config.connections(),)+
                }
            }

            /// Checks the task's config, as its crate defines it.
            pub fn validate(&self) -> Result<(), TaskError> {
                match self {
                    $(TaskType::$tag(config) => TaskMeta::validate(config),)+
                }
            }

            /// Checks that the shared servers the task needs are started.
            pub fn check(&self, services: &Services) -> Result<(), TaskError> {
                match self {
                    $(TaskType::$tag(config) => config.check(services),)+
                }
            }

            /// Whether the task mostly uses the CPU, declared by its crate.
            pub const fn cpu_intensive(&self) -> bool {
                match self {
                    $(TaskType::$tag(_) => <$config as TaskMeta>::CPU_INTENSIVE,)+
                }
            }

            /// Whether this task takes and sends events. Registration-only
            /// tasks have no channels and no implicit parent.
            pub const fn has_pipeline_io(&self) -> bool {
                match self {
                    $(TaskType::$tag(_) => <$config as TaskMeta>::PIPELINE_IO,)+
                }
            }

            /// Whether this task starts events and takes none, declared by its crate.
            pub const fn is_source(&self) -> bool {
                match self {
                    $(TaskType::$tag(_) => <$config as TaskMeta>::SOURCE,)+
                }
            }

            /// Whether this task registers its flow with a shared server, so the
            /// flow must run on every pod and is never leader elected.
            pub const fn is_blocking(&self) -> bool {
                match self {
                    $(TaskType::$tag(_) => <$config as TaskMeta>::REGISTERS,)+
                }
            }

            /// Whether this task starts without the startup jitter.
            pub const fn starts_at_once(&self) -> bool {
                match self {
                    $(TaskType::$tag(_) => <$config as TaskMeta>::STARTS_AT_ONCE,)+
                }
            }

            /// Whether this source finishes its events in flight after it stops reading.
            pub const fn drains(&self) -> bool {
                match self {
                    $(TaskType::$tag(_) => <$config as TaskMeta>::DRAINS,)+
                }
            }

            /// Whether this task handles events whose upstream task failed.
            pub fn handles_failed(&self) -> bool {
                match self {
                    $(TaskType::$tag(config) => config.handles_failed(),)+
                }
            }

            /// Spawns the task into `tasks`, in the current span.
            pub fn spawn(
                self,
                io: TaskIo,
                context: Arc<TaskContext>,
                tasks: &mut JoinSet<Result<(), crate::flow::Error>>,
            ) {
                let span = tracing::Span::current();
                let task = self.name().to_string();
                match self {
                    $(TaskType::$tag(config) => {
                        let stop = context.stop_reading.clone();
                        tasks.spawn(
                            async move {
                                let result = config.run(io, context).await;
                                crate::flow::task_result(
                                    task,
                                    stringify!($tag),
                                    <$config as TaskMeta>::RUNS_UNTIL_STOPPED && !stop.is_cancelled(),
                                    result,
                                )
                            }
                            .instrument(span),
                        );
                    })+
                }
            }
        }
    };
}

task_types! {
    /// Data conversion task.
    convert(flowgen_core::task::convert::Convert),
    /// Iterate over arrays task.
    iterate(flowgen_core::task::iterate::Iterate),
    /// Log output task.
    log(flowgen_core::task::log::Log),
    /// Script execution task.
    script(flowgen_core::task::script::Script),
    /// Buffer task for accumulating events into batches.
    buffer(flowgen_core::task::buffer::Buffer),
    /// Object store operations (read, write, list, move).
    object_store(flowgen_object_store::config::Processor),
    /// Data generation task.
    generate(flowgen_core::task::generate::Generate),
    /// HTTP request task.
    http_request(flowgen_http::request::Request),
    /// HTTP endpoint task: exposes a route on the worker HTTP server.
    http_endpoint(flowgen_http::endpoint::Endpoint),
    /// HTML scrape task for extracting structured data using CSS selectors.
    html_scrape(flowgen_html::scrape::Scrape),
    /// Consumes messages from a NATS JetStream stream.
    #[serde(alias = "nats_jetstream_subscriber")]
    nats_jetstream_subscribe(flowgen_nats::jetstream::subscribe::Subscribe),
    /// Publishes events to NATS JetStream.
    #[serde(alias = "nats_jetstream_publisher")]
    nats_jetstream_publish(flowgen_nats::jetstream::publish::Publish),
    /// Subscribes to a Salesforce Pub/Sub API topic.
    #[serde(alias = "salesforce_pubsubapi_subscriber")]
    salesforce_pubsubapi_subscribe(flowgen_salesforce::pubsubapi::subscribe::Subscribe),
    /// Publishes events to a Salesforce Pub/Sub API topic.
    #[serde(alias = "salesforce_pubsubapi_publisher")]
    salesforce_pubsubapi_publish(flowgen_salesforce::pubsubapi::publish::Publish),
    /// Salesforce Bulk API query job operations (create, get, delete, abort, get_results).
    salesforce_bulkapi_query_job(flowgen_salesforce::bulkapi::query_job::QueryJob),
    /// Salesforce REST API SObject operations (create, get, update, upsert, delete).
    salesforce_restapi_sobject(flowgen_salesforce::restapi::sobject::SObject),
    /// Salesforce REST API Composite operations for bulk record operations.
    salesforce_restapi_composite(flowgen_salesforce::restapi::composite::Composite),
    /// Salesforce REST API SOSL search operations.
    salesforce_restapi_search(flowgen_salesforce::restapi::search::Search),
    /// Salesforce REST API SOQL queries, following every page of results.
    salesforce_restapi_query(flowgen_salesforce::restapi::query::Query),
    /// Salesforce SOAP API merge operation for combining duplicate records.
    salesforce_soapapi_merge(flowgen_salesforce::soapapi::merge::Merge),
    /// Salesforce Tooling API operations for metadata management.
    salesforce_toolingapi(flowgen_salesforce::toolingapi::Tooling),
    /// GCP BigQuery query task.
    gcp_bigquery_query(flowgen_gcp::bigquery::query::Query),
    /// GCP BigQuery Storage Read API task.
    gcp_bigquery_storage_read(flowgen_gcp::bigquery::storage_read::StorageRead),
    /// GCP BigQuery unified job operations (create, get, cancel, delete).
    gcp_bigquery_job(flowgen_gcp::bigquery::job::Job),
    /// GCP BigQuery Storage Write API task for high-throughput streaming inserts.
    gcp_bigquery_storage_write(flowgen_gcp::bigquery::storage_write::StorageWrite),
    /// Microsoft SQL Server query task.
    mssql_query(flowgen_mssql::query::Query),
    /// PostgreSQL query task.
    postgres_query(flowgen_postgres::query::Query),
    /// PostgreSQL write task, with COPY.
    postgres_write(flowgen_postgres::write::Write),
    /// PostgreSQL change data capture task.
    postgres_cdc(flowgen_postgres::cdc::Cdc),
    /// Key-value store operations (get, put, list, delete) on a redb file or NATS.
    kv_store(flowgen_kv_store::store::KvStore),
    /// Starts the flow with a key's value whenever it changes, on a redb file or NATS.
    kv_watch(flowgen_kv_store::watch::KvWatch),
    /// AI completion task for generating responses using LLMs.
    ai_completion(flowgen_ai_agent::completion::Completion),
    /// MCP tool task for exposing flows as MCP tools callable by LLMs.
    mcp_tool(flowgen_mcp::tool::Tool),
    /// MCP resource task for exposing read-only content to MCP clients.
    mcp_resource(flowgen_mcp::resource::Resource),
    /// MCP prompt task for exposing slash-command templates to MCP clients.
    mcp_prompt(flowgen_mcp::prompt::Prompt),
    /// LLM proxy task — registers a flow as a backend on the AI gateway server.
    llm_proxy(flowgen_ai_agent::llm_proxy::LlmProxy),
    /// Git sync task for syncing flows and resources from a Git repository to the cache.
    git_sync(flowgen_git::sync::GitSync),
    /// OCI sync task for pulling flow artifacts from an OCI registry into the cache.
    oci_sync(flowgen_oci::sync::OciSync),
    /// Git push task for committing files and pushing them to a Git repository.
    git_push(flowgen_git::push::GitPush),
    /// OCI push task for releasing files to an OCI registry as an artifact.
    oci_push(flowgen_oci::push::OciPush),
    /// Braze user data export task.
    braze_export_user_data(flowgen_braze::export_user_data::ExportUserData),
    /// Braze messaging task.
    braze_messaging(flowgen_braze::messaging::Messaging),
    /// Klaviyo email templates task.
    klaviyo_templates(flowgen_klaviyo::templates::Templates),
    /// Klaviyo campaigns task.
    klaviyo_campaigns(flowgen_klaviyo::campaigns::Campaigns),
    /// Klaviyo lists task.
    klaviyo_lists(flowgen_klaviyo::lists::Lists),
    /// Klaviyo events task.
    klaviyo_events(flowgen_klaviyo::events::Events),
    /// Klaviyo profiles task.
    klaviyo_profiles(flowgen_klaviyo::profiles::Profiles),
    /// MongoDB collection read/write task.
    mongodb_collection(flowgen_mongodb::collection::Collection),
    /// MongoDB change data capture task.
    mongodb_cdc(flowgen_mongodb::cdc::Cdc),
    /// Kafka produce task.
    kafka_produce(flowgen_kafka::produce::Produce),
    /// Kafka subscribe task.
    kafka_subscribe(flowgen_kafka::subscribe::Subscribe),
    /// Source that makes the flow callable in-process at its identity.
    inproc_endpoint(flowgen_core::task::inproc::endpoint::Endpoint),
    /// Calls a flow with an `inproc_endpoint` and emits its result.
    inproc_request(flowgen_core::task::inproc::request::Request),
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Main application configuration.
///
/// Flowgen ships as a single binary — there is no separate worker/server
/// split — so component sections live at the top level. Load it with
/// [`AppConfig::load`], which rejects unknown keys of the file but not of `APP_*`.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct AppConfig {
    /// Cache configuration (shared across components); see [`AppConfig::cache_options`].
    pub cache: Option<CacheOptions>,
    /// Flow discovery options (shared across components).
    pub flows: FlowOptions,
    /// Optional resource loading configuration (shared across components).
    pub resources: Option<ResourceOptions>,
    /// Optional HTTP server configuration for webhooks, health checks, and metrics.
    pub http_server: Option<HttpServerOptions>,
    /// Optional MCP server configuration for exposing flows as MCP tools.
    pub mcp_server: Option<McpServerOptions>,
    /// Optional AI gateway configuration for OpenAI-compatible chat completion endpoints.
    /// Runs on its own port so the surface can later migrate to gRPC or WebSocket
    /// independently of the webhook HTTP server.
    pub ai_gateway: Option<AiGatewayOptions>,
    /// Optional web UI configuration for inspecting loaded flows.
    pub web: Option<WebOptions>,
    /// Kubernetes-facing liveness/readiness listener. Independent of the
    /// API listeners so probes keep working regardless of which surfaces
    /// (http_server / mcp_server / ai_gateway / web) are enabled.
    /// Defaults to enabled on port 8081 if omitted.
    #[serde(default)]
    pub health: HealthOptions,
    /// Optional app-level retry configuration (can be overridden per task).
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Optional OpenTelemetry configuration for metrics and tracing.
    pub telemetry: Option<TelemetryOptions>,
    /// How long a stopping flow lets its events in flight finish (can be overridden per flow).
    #[serde(default = "default_shutdown_timeout", with = "humantime_serde")]
    pub shutdown_timeout: Duration,
    /// The address every server listens on. Defaults to `0.0.0.0`.
    #[serde(default = "default_host")]
    pub host: String,
    /// Token and amount limits of every `ai_completion`; unlimited when omitted.
    pub ai_budget: Option<flowgen_ai_agent::budget::BudgetConfig>,
    /// Credentials added in the web UI, committed encrypted under `credentials/`.
    /// Omit to manage credentials outside flowgen.
    pub credentials: Option<CredentialsOptions>,
}

/// Where credentials added in the web UI go: each file encrypted with SOPS
/// for the `recipients` and committed under `credentials/`. This instance
/// decrypts them in memory when it has `identity_path`; in a cluster Flux
/// decrypts them into a mounted Secret instead.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialsOptions {
    /// age public keys each credential is encrypted for, such as the key
    /// the cluster decrypts with and the keys of the people who may read them.
    #[serde(default)]
    pub recipients: Vec<String>,
    /// age key file this instance decrypts credentials with, set as
    /// `SOPS_AGE_KEY_FILE` for the run; its public key is a recipient too.
    /// Omit where the cluster decrypts them.
    #[serde(default)]
    pub identity_path: Option<PathBuf>,
    /// The folder flows read credentials from, which `FLOWGEN_CREDENTIALS`
    /// names: where they are decrypted to, or where the cluster mounts them.
    pub path: PathBuf,
}

/// Rejected configuration, reported before anything starts.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ValidationError {
    #[error(transparent)]
    AiBudget {
        #[from]
        source: flowgen_ai_agent::budget::Error,
    },
    #[error("`web.agents.{field}` must be longer than 0s, e.g. \"3s\"")]
    ZeroAgentsDuration { field: &'static str },
    #[error(
        "`web.agents.cache_prefix` {prefix:?} must be segments of letters, digits, `_` and `-` \
         joined by single dots, e.g. \"agents\" or \"team.agents\""
    )]
    AgentsCachePrefix { prefix: String },
    #[error(
        "`web.frame_ancestors` entry {source_expression:?} must be one CSP source without spaces, \
         `;` or `,`, e.g. \"'self'\" or \"https://console.example.com\""
    )]
    FrameAncestor { source_expression: String },
    #[error("`flows.system_exclude` entry {identity:?} is not a system flow; they are {known}")]
    UnknownSystemFlow { identity: String, known: String },
}

impl AppConfig {
    /// What flows read about this run, by variable: the addresses of its web
    /// server, MCP server and AI gateway on this machine, its cache and system
    /// bucket, where workspace releases go, and the folder flows read
    /// credentials from.
    pub fn run_variables(&self) -> [(&'static str, String); 8] {
        let host = match self.host.as_str() {
            DEFAULT_HOST => crate::local::LOCAL_HOST,
            host => host,
        };
        let url =
            |port: u16, path: &str| format!("http://{host}:{port}{}", path.trim_end_matches('/'));
        let web = match &self.web {
            Some(web) => web.clone(),
            None => WebOptions::default(),
        };
        let mcp = match &self.mcp_server {
            Some(mcp) => mcp.clone(),
            None => McpServerOptions::default(),
        };
        let ai_gateway = match &self.ai_gateway {
            Some(ai_gateway) => ai_gateway.clone(),
            None => AiGatewayOptions::default(),
        };
        let cache = self.cache_options();
        let cache_url = match cache.cache_type {
            CacheType::Redb => format!("redb://{}", cache.path.display()),
            CacheType::Nats => cache.url.clone(),
        };
        let cache_credentials = match &cache.credentials_path {
            Some(path) => path.display().to_string(),
            None => String::new(),
        };
        let credentials = match &self.credentials {
            Some(credentials) => credentials.path.display().to_string(),
            None => flowgen_core::workspace::CREDENTIALS_DIR.to_string(),
        };
        [
            (WEB_URL_ENV, url(web.port, &web.path)),
            (MCP_URL_ENV, url(mcp.port, &mcp.path)),
            (AI_GATEWAY_URL_ENV, url(ai_gateway.port, &ai_gateway.path)),
            (CACHE_URL_ENV, cache_url),
            (CACHE_CREDENTIALS_ENV, cache_credentials),
            (SYSTEM_BUCKET_ENV, cache.system.db_name),
            (RELEASES_ENV, DEFAULT_RELEASES.to_string()),
            (CREDENTIALS_ENV, credentials),
        ]
    }

    /// The `cache` section, or the default `redb` file when there is none.
    pub fn cache_options(&self) -> CacheOptions {
        match &self.cache {
            Some(cache) => cache.clone(),
            None => CacheOptions::default(),
        }
    }

    /// Reads `file` with `env` layered over it; a key of `file` that no setting knows fails.
    pub fn load(
        file: impl config::Source + Send + Sync + 'static,
        env: config::Environment,
    ) -> Result<Self, config::ConfigError> {
        let file = config::Config::builder().add_source(file).build()?;
        let in_file: std::collections::HashSet<String> =
            config::Source::collect(&file)?.into_keys().collect();
        let merged = config::Config::builder()
            .add_source(file)
            .add_source(env)
            .build()?;
        let mut unknown = Vec::new();
        let app_config = serde_ignored::deserialize(merged, |path| {
            let path = path.to_string();
            let section = match path.split_once('.') {
                Some((section, _)) => section,
                None => path.as_str(),
            };
            if in_file.contains(section) {
                unknown.push(path);
            }
        })?;
        match unknown.is_empty() {
            true => Ok(app_config),
            false => Err(config::ConfigError::Message(format!(
                "Unknown field(s) in the config file: {}",
                unknown.join(", ")
            ))),
        }
    }

    /// Rejects configuration that parses but would run with the wrong meaning.
    pub fn validate(&self) -> Result<(), ValidationError> {
        if let Some(budget) = self.ai_budget.as_ref() {
            budget.validate()?;
        }
        if !self.flows.system_exclude.is_empty() {
            let known = crate::local::system_flow_identities();
            if let Some(identity) = self
                .flows
                .system_exclude
                .iter()
                .find(|identity| !known.contains(identity))
            {
                return Err(ValidationError::UnknownSystemFlow {
                    identity: identity.clone(),
                    known: known.join(", "),
                });
            }
        }
        let Some(web) = self.web.as_ref() else {
            return Ok(());
        };
        if let Some(source) = web.frame_ancestors.iter().find(|source| {
            source.is_empty()
                || source
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control() || c == ';' || c == ',')
        }) {
            return Err(ValidationError::FrameAncestor {
                source_expression: source.clone(),
            });
        }
        match web.agents.as_ref() {
            Some(agents) => agents.validate(),
            None => Ok(()),
        }
    }
}

/// Cache type for storage backend.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    /// NATS JetStream Key-Value store.
    Nats,
    /// A local file, for a single process without NATS.
    #[default]
    Redb,
}

/// Cache configuration options. Without a `cache` section, flowgen uses the
/// defaults: a `redb` file at `.flowgen/state.redb`.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CacheOptions {
    /// Cache backend type. Defaults to `redb`.
    #[serde(rename = "type", default)]
    pub cache_type: CacheType,
    /// Optional path to cache credentials file.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// NATS server URL (e.g., "nats://localhost:4222"). Defaults to "localhost:4222".
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// The file of a `redb` cache. Defaults to `.flowgen/state.redb`.
    #[serde(default = "default_redb_path")]
    pub path: PathBuf,
    /// Runtime bucket, reachable from scripts through `ctx.cache`.
    #[serde(default)]
    pub runtime: BucketOptions,
    /// System bucket holding leader-election leases and peer registration.
    /// Kept apart from `runtime` so scripts cannot reach coordination state.
    #[serde(default)]
    pub system: SystemBucketOptions,
    /// Values a NATS bucket keeps per key, set when it is created. Defaults to 64.
    pub history: Option<i64>,
    /// TTL for KV delete and purge tombstone markers.
    /// Accepts human-readable durations (e.g., "1h", "30m", "1d").
    /// Must be set to some duration for per-key TTLs on cache entries to work —
    /// this flag enables per-message TTL on the underlying JetStream stream.
    /// Defaults to 1 hour.
    #[serde(default, with = "humantime_serde")]
    pub tombstone_ttl: Option<std::time::Duration>,
}

impl Default for CacheOptions {
    fn default() -> Self {
        Self {
            cache_type: CacheType::default(),
            credentials_path: None,
            url: default_nats_url(),
            path: default_redb_path(),
            runtime: BucketOptions::default(),
            system: SystemBucketOptions::default(),
            history: None,
            tombstone_ttl: None,
        }
    }
}

/// Runtime bucket options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BucketOptions {
    /// Bucket name (defaults to "flowgen_cache").
    #[serde(default = "default_runtime_db_name")]
    pub db_name: String,
}

impl Default for BucketOptions {
    fn default() -> Self {
        Self {
            db_name: default_runtime_db_name(),
        }
    }
}

/// System bucket options.
///
/// Both buckets sit on the connection `CacheOptions` describes, so this
/// inherits `url`, `credentials_path`, and `type`. With the cache disabled
/// the bucket falls back to an in-memory store, which cannot coordinate
/// across pods — see the warning `App` emits for leader-elected flows.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SystemBucketOptions {
    /// Bucket name (defaults to "flowgen_system").
    #[serde(default = "default_system_db_name")]
    pub db_name: String,
}

impl Default for SystemBucketOptions {
    fn default() -> Self {
        Self {
            db_name: default_system_db_name(),
        }
    }
}

/// Default bucket name for the runtime cache.
fn default_runtime_db_name() -> String {
    DEFAULT_CACHE_DB_NAME.to_string()
}

/// Flow loading configuration.
///
/// Filesystem and cache sources are independent: the worker loads from each
/// configured source and merges the results. Configure both for hybrid mode
/// (a small set of bootstrap flows on disk plus user flows in the cache).
/// On a name collision the filesystem entry wins so a locally mounted flow
/// cannot be silently overridden by a stale cache entry.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlowOptions {
    /// Path for discovering flow configuration files.
    /// Can be either:
    /// - A base directory path (e.g., "/flows") - will recursively discover all .yaml, .yml, and .json files
    /// - A glob pattern (e.g., "/flows/*.yaml") - for backward compatibility, uses pattern directly
    pub path: Option<PathBuf>,
    /// Optional cache-based flow loading configuration.
    /// When enabled, flows are loaded from the distributed cache instead of the filesystem.
    #[serde(default)]
    pub cache: Option<FlowCacheOptions>,
    /// Runs this version's system flows under `system/`: committing,
    /// releasing and deploying the workspace, Flowgen Agent and its tools,
    /// and the hubs. A flow at `path` with the same identity takes their place.
    #[serde(default)]
    pub system: bool,
    /// System flows left out, by identity, such as `system/release_workspace`
    /// when the repository's CI releases the workspace.
    #[serde(default)]
    pub system_exclude: Vec<String>,
}

/// Default system bucket name.
fn default_system_db_name() -> String {
    "flowgen_system".to_string()
}

/// Cache-based flow loading options. Flows are read from the system bucket,
/// under [`FLOWS_CACHE_PREFIX`].
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct FlowCacheOptions {
    /// Whether to load flows from the cache instead of the filesystem.
    pub enabled: bool,
}

/// Resource loading configuration.
///
/// Filesystem and cache sources are independent: configure either, both, or
/// neither. When both are configured the loader tries the filesystem first
/// and falls back to the cache on a miss, so a locally mounted resource
/// always takes precedence over a cached copy with the same key.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ResourceOptions {
    /// Optional base path for loading external resource files (SQL, JSON, HTML, etc.).
    /// Resource keys are resolved relative to this path.
    /// Example: with path="local/resources/", key="queries/orders.sql" resolves to "local/resources/queries/orders.sql".
    /// Omit to skip filesystem loading.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Optional cache-based resource loading configuration.
    /// When enabled, resources are loaded from the distributed cache.
    /// Combine with `path` for hybrid loading (filesystem first, cache fallback).
    #[serde(default)]
    pub cache: Option<ResourceCacheOptions>,
}

/// Cache-based resource loading options. Resources are read from the system
/// bucket, under [`RESOURCES_CACHE_PREFIX`].
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResourceCacheOptions {
    /// Whether to load resources from the cache instead of the filesystem.
    pub enabled: bool,
}

/// Default MCP server port.
fn default_mcp_port() -> u16 {
    3001
}

/// Default MCP endpoint path.
fn default_mcp_path() -> String {
    "/mcp/v1".to_string()
}

/// MCP server configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct McpServerOptions {
    /// Whether MCP server is enabled. Must be explicitly enabled by the admin
    /// for `mcp_tool` tasks to be registered.
    pub enabled: bool,
    /// MCP server port number (defaults to 3001).
    #[serde(default = "default_mcp_port")]
    pub port: u16,
    /// MCP endpoint path (defaults to "/mcp").
    #[serde(default = "default_mcp_path")]
    pub path: String,
    /// Optional path to global credentials file for API key authentication.
    /// Individual `mcp_tool` tasks can override this with their own `credentials_path`.
    pub credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider configuration for user identity resolution.
    /// Shared across all `mcp_tool` flows on this worker.
    pub auth: Option<flowgen_core::auth::AuthConfig>,
    /// URI scheme used when auto-generating `mcp_resource` URIs of the form
    /// `<scheme>://<flow_name>/<name>`. White-label deployments override
    /// this (e.g. `acme`) so URIs emitted to LLM clients carry the
    /// deployment's brand instead of `flowgen`.
    #[serde(default = "default_resource_uri_scheme")]
    pub resource_uri_scheme: String,
}

impl Default for McpServerOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_mcp_port(),
            path: default_mcp_path(),
            credentials_path: None,
            auth: None,
            resource_uri_scheme: default_resource_uri_scheme(),
        }
    }
}

/// Default scheme for auto-generated MCP resource URIs.
fn default_resource_uri_scheme() -> String {
    "flowgen".to_string()
}

/// Default AI gateway port.
fn default_ai_gateway_port() -> u16 {
    3002
}

/// Default AI gateway path prefix.
///
/// `/v1` matches the de-facto OpenAI-compatible convention used by vLLM, Ollama,
/// LiteLLM, OpenRouter, and the OpenAI SDKs. Clients configure `base_url=http://host/v1`
/// and reach `POST /v1/chat/completions` and `GET /v1/models` directly.
fn default_ai_gateway_path() -> String {
    "/v1".to_string()
}

/// Default max request body size for the AI gateway (128 MiB). Sized for
/// modern 1 M-token context windows plus tool schemas and history —
/// roughly 4 bytes per token after JSON overhead means a full 1 M-token
/// prompt lands around 4 MB; the 128 MiB ceiling absorbs multi-turn
/// histories and worst-case tool-payload growth without needing per-flow
/// overrides.
fn default_ai_gateway_max_body_bytes() -> usize {
    128 * 1024 * 1024
}

/// AI gateway server configuration options.
///
/// Exposes registered AI gateway flows as a single OpenAI-compatible endpoint.
/// Routing across gateways is driven by the `model` field of the request body
/// (`model: "<gateway-name>/<downstream-model>"`), so OpenAI clients work with
/// stock `base_url` configuration and no per-gateway URL changes.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AiGatewayOptions {
    /// Whether the AI gateway server is enabled. Must be explicitly enabled by
    /// the admin for `llm_proxy` tasks to be registered.
    pub enabled: bool,
    /// AI gateway port number. Defaults to 3002.
    #[serde(default = "default_ai_gateway_port")]
    pub port: u16,
    /// Path prefix for AI gateway routes. Defaults to "/v1".
    /// The chat completion endpoint is served at `<path>/chat/completions` and
    /// the model list at `<path>/models`.
    #[serde(default = "default_ai_gateway_path")]
    pub path: String,
    /// Optional path to global credentials file for API key authentication.
    /// Individual `llm_proxy` tasks can override this with their own `credentials_path`.
    pub credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider configuration for user identity resolution.
    /// Shared across all AI gateway flows on this worker.
    pub auth: Option<flowgen_core::auth::AuthConfig>,
    /// Maximum request body size in bytes. Defaults to 128 MiB, which
    /// covers 1 M-token prompts with tool schemas and multi-turn
    /// histories. Requests over this limit are rejected with
    /// `413 Payload Too Large`.
    #[serde(default = "default_ai_gateway_max_body_bytes")]
    pub max_body_bytes: usize,
}

impl Default for AiGatewayOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_ai_gateway_port(),
            path: default_ai_gateway_path(),
            credentials_path: None,
            auth: None,
            max_body_bytes: default_ai_gateway_max_body_bytes(),
        }
    }
}

/// Default web UI port.
fn default_web_port() -> u16 {
    8080
}

/// Default web UI path prefix.
fn default_web_path() -> String {
    crate::web::DEFAULT_WEB_PATH.to_string()
}

/// Web UI configuration options.
///
/// Serves a lightweight, read-only dashboard for inspecting loaded flows.
/// The UI is built into the binary as static assets.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WebOptions {
    /// Whether the web UI is enabled.
    pub enabled: bool,
    /// Port for the web server. Defaults to 8080.
    #[serde(default = "default_web_port")]
    pub port: u16,
    /// Path prefix for the web UI. Defaults to "/flowgen".
    #[serde(default = "default_web_path")]
    pub path: String,
    /// Base URL of the AI gateway the Agents tab replies through, e.g.
    /// `http://127.0.0.1:3002/v1`. Omit to target the same-process AI gateway
    /// on loopback. Set when the gateway runs in a separate process or pod.
    #[serde(default)]
    pub ai_gateway_url: Option<String>,
    /// The built-in Agents chat. Omit to hide the Agents tab and disable
    /// `/api/agents`.
    #[serde(default)]
    pub agents: Option<AgentsOptions>,
    /// HTTP headers sent with every request the web server makes to the AI
    /// gateway for the Agents tab, such as a key the gateway requires.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// OIDC login for the web UI (Okta, Zitadel, Auth0, or any
    /// standard-compliant IdP). Omit to leave the web UI unauthenticated.
    #[serde(default)]
    pub auth: Option<crate::login::LoginConfig>,
    /// Key used to encrypt the browser session cookie (holds the IdP's
    /// tokens — flowgen keeps no server-side session store, see
    /// `crate::login`). At least 32 bytes. Unset, flowgen generates the key
    /// and keeps it in the system bucket; `auth.credentials_path` can carry
    /// a `cookie_secret` instead.
    #[serde(default, serialize_with = "serialize_redacted_cookie_secret")]
    pub cookie_secret: Option<secrecy::SecretString>,
    /// Whether the login cookies carry the `Secure` attribute, which browsers
    /// require HTTPS to send. Defaults to `true`; set to `false` only for a
    /// plain-HTTP deployment (local testing, or a proxy that terminates TLS
    /// and forwards HTTP — in which case the proxy, not flowgen, should be
    /// trusted to keep the connection private).
    #[serde(default = "default_cookie_secure")]
    pub cookie_secure: bool,
    /// JSON file with machine keys (`{"api_keys": [{"name", "key"}]}`) that
    /// `/api/*` accepts as `Authorization: Bearer <key>` alongside the login
    /// session. Only used when `auth` is set.
    #[serde(default)]
    pub api_credentials_path: Option<PathBuf>,
    /// Change proposals and their publishing. Omit to disable `/api/changes`.
    #[serde(default)]
    pub authoring: Option<AuthoringOptions>,
    /// Who may show the web UI in a frame, as CSP `frame-ancestors` sources,
    /// e.g. `["'self'", "https://console.example.com"]`. Defaults to `'self'`.
    #[serde(default = "default_frame_ancestors")]
    pub frame_ancestors: Vec<String>,
    /// Checking for a newer flowgen release, shown in the web UI.
    #[serde(default)]
    pub update_check: UpdateCheckOptions,
}

/// Checking for a newer flowgen release, shown in the web UI.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct UpdateCheckOptions {
    /// Whether to check. Defaults to `true`.
    #[serde(default = "default_update_check_enabled")]
    pub enabled: bool,
    /// The newest release, as a releases API `latest` response with `tag_name`.
    #[serde(default = "default_update_check_url")]
    pub url: String,
    /// How often to check. Defaults to an hour.
    #[serde(default = "default_update_check_interval", with = "humantime_serde")]
    pub interval: Duration,
}

impl Default for UpdateCheckOptions {
    fn default() -> Self {
        Self {
            enabled: default_update_check_enabled(),
            url: default_update_check_url(),
            interval: default_update_check_interval(),
        }
    }
}

fn default_update_check_enabled() -> bool {
    true
}

fn default_update_check_url() -> String {
    "https://api.github.com/repos/connve/flowgen/releases/latest".to_string()
}

fn default_update_check_interval() -> Duration {
    Duration::from_secs(60 * 60)
}

impl Default for WebOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_web_port(),
            path: default_web_path(),
            ai_gateway_url: None,
            agents: None,
            headers: Default::default(),
            auth: None,
            cookie_secret: None,
            cookie_secure: default_cookie_secure(),
            api_credentials_path: None,
            authoring: None,
            frame_ancestors: default_frame_ancestors(),
            update_check: UpdateCheckOptions::default(),
        }
    }
}

pub(crate) fn default_frame_ancestors() -> Vec<String> {
    vec!["'self'".to_string()]
}

fn default_cookie_secure() -> bool {
    true
}

impl PartialEq for WebOptions {
    /// Compares `cookie_secret` by presence only, matching `JwtConfig`'s
    /// convention — `SecretString` deliberately has no `PartialEq`.
    fn eq(&self, other: &Self) -> bool {
        self.enabled == other.enabled
            && self.port == other.port
            && self.path == other.path
            && self.ai_gateway_url == other.ai_gateway_url
            && self.agents == other.agents
            && self.headers == other.headers
            && self.auth == other.auth
            && self.cookie_secret.is_some() == other.cookie_secret.is_some()
            && self.cookie_secure == other.cookie_secure
            && self.api_credentials_path == other.api_credentials_path
            && self.authoring == other.authoring
            && self.frame_ancestors == other.frame_ancestors
            && self.update_check == other.update_check
    }
}

/// Proposing workspace changes from the web API and publishing approved ones.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoringOptions {
    /// Whether change proposals are enabled.
    pub enabled: bool,
    /// Where changes can be proposed. Defaults to one `workspace` target
    /// covering `flows/`, `resources/` and `credentials/`.
    #[serde(default = "default_targets")]
    pub targets: Vec<AuthoringTarget>,
    /// Claim of the signed-in user that lists their groups. Defaults to `groups`.
    #[serde(default = "default_groups_claim")]
    pub groups_claim: String,
}

/// Workspace paths changed, approved, and published together.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoringTarget {
    /// Name shown on its changes, e.g. `user`.
    pub name: String,
    /// Workspace folders it covers, e.g. `flows/user/`. A path covered by several
    /// targets belongs to the one with the most specific folder, and a change's
    /// files must all belong to one target.
    pub paths: Vec<String>,
    /// Identity of the flow, starting with an `inproc_endpoint`, that commits
    /// an approved change. It receives `{id, target, title, author, files}`.
    /// Defaults to `system/commit_workspace`.
    #[serde(default = "default_commit_flow")]
    pub commit_flow: String,
    /// Identity provider groups whose members may approve and reject. Empty
    /// allows every signed-in user.
    #[serde(default)]
    pub approver_groups: Vec<String>,
    /// Whether a committed change reports when this instance runs it. Turn off
    /// when the target is not deployed here automatically. Defaults to `true`.
    #[serde(default = "default_track_deployment")]
    pub track_deployment: bool,
}

impl AuthoringOptions {
    /// The target owning the workspace `path`: of the targets covering it, the
    /// one with the most specific path, so `flows/system/` wins over `flows/`.
    pub fn target_of(&self, path: &str) -> Option<&AuthoringTarget> {
        self.targets
            .iter()
            .filter_map(|target| target.matched_len(path).map(|len| (len, target)))
            .max_by_key(|(len, _)| *len)
            .map(|(_, target)| target)
    }
}

impl AuthoringTarget {
    /// Length of the longest of its paths that `path` falls under.
    fn matched_len(&self, path: &str) -> Option<usize> {
        self.paths
            .iter()
            .map(|folder| folder.trim_end_matches('/'))
            .filter(|folder| flowgen_core::identity::within(path, folder))
            .map(str::len)
            .max()
    }
}

impl Default for AuthoringOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            targets: default_targets(),
            groups_claim: default_groups_claim(),
        }
    }
}

fn default_targets() -> Vec<AuthoringTarget> {
    vec![AuthoringTarget {
        name: "workspace".to_string(),
        paths: vec![
            "flows/".to_string(),
            "resources/".to_string(),
            "credentials/".to_string(),
        ],
        commit_flow: default_commit_flow(),
        approver_groups: Vec::new(),
        track_deployment: default_track_deployment(),
    }]
}

fn default_commit_flow() -> String {
    "system/commit_workspace".to_string()
}

fn default_track_deployment() -> bool {
    true
}

fn default_groups_claim() -> String {
    "groups".to_string()
}

fn serialize_redacted_cookie_secret<S>(
    secret: &Option<secrecy::SecretString>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match secret {
        Some(_) => s.serialize_some("***"),
        None => s.serialize_none(),
    }
}

/// Settings for the built-in Agents chat surface.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AgentsOptions {
    /// Whether the Agents chat is enabled.
    pub enabled: bool,
    /// How long a session lives in the configured cache before it
    /// expires. Refreshed on every write, so the window counts from the last
    /// activity. Defaults to 30 days; set to `0` to persist indefinitely.
    /// Long-term history belongs in a database via a persistence flow, not
    /// this cache.
    #[serde(default = "default_session_history_ttl", with = "humantime_serde")]
    pub session_history_ttl: Option<Duration>,
    /// Calls the AI gateway as the signed-in user, with their login token as the bearer.
    #[serde(default)]
    pub on_behalf_of_user: bool,
    /// Cache key prefix for sessions, read state and folders; a flow
    /// opens a session under `<prefix>.sessions.`.
    #[serde(default = "default_agents_cache_prefix")]
    pub cache_prefix: String,
    /// How often a reply in progress is saved. A pending reply not saved for
    /// ten times this long is shown as interrupted.
    #[serde(default = "default_reply_save_interval", with = "humantime_serde")]
    pub reply_save_interval: Duration,
    /// How long a reply waits for the AI gateway to send anything, e.g. `5m`,
    /// before it ends with an error. Defaults to 10 minutes.
    #[serde(default = "default_reply_idle_timeout", with = "humantime_serde")]
    pub reply_idle_timeout: Duration,
}

impl Default for AgentsOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            session_history_ttl: default_session_history_ttl(),
            on_behalf_of_user: false,
            cache_prefix: default_agents_cache_prefix(),
            reply_save_interval: default_reply_save_interval(),
            reply_idle_timeout: default_reply_idle_timeout(),
        }
    }
}

impl AgentsOptions {
    fn validate(&self) -> Result<(), ValidationError> {
        if self.reply_save_interval.is_zero() {
            return Err(ValidationError::ZeroAgentsDuration {
                field: "reply_save_interval",
            });
        }
        if self.reply_idle_timeout.is_zero() {
            return Err(ValidationError::ZeroAgentsDuration {
                field: "reply_idle_timeout",
            });
        }
        let segment = |segment: &str| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        };
        match self.cache_prefix.split('.').all(segment) {
            true => Ok(()),
            false => Err(ValidationError::AgentsCachePrefix {
                prefix: self.cache_prefix.clone(),
            }),
        }
    }
}

pub(crate) fn default_agents_cache_prefix() -> String {
    "agents".to_string()
}

pub(crate) fn default_reply_save_interval() -> Duration {
    Duration::from_secs(3)
}

pub(crate) fn default_reply_idle_timeout() -> Duration {
    Duration::from_secs(10 * 60)
}

/// Default session history TTL for the built-in Agents chat: 30 days.
fn default_session_history_ttl() -> Option<Duration> {
    Some(Duration::from_secs(30 * 24 * 60 * 60))
}

/// Default health listener port.
fn default_health_port() -> u16 {
    8081
}

/// Default `enabled: true` for HealthOptions when the field is omitted.
fn default_health_enabled() -> bool {
    true
}

/// Kubernetes liveness/readiness listener configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HealthOptions {
    /// Whether the health listener is enabled. Defaults to true.
    pub enabled: bool,
    /// Health listener port. Defaults to 8081.
    pub port: u16,
}

impl Default for HealthOptions {
    fn default() -> Self {
        Self {
            enabled: default_health_enabled(),
            port: default_health_port(),
        }
    }
}

/// Default webhook HTTP server port.
fn default_http_port() -> u16 {
    3000
}

/// Default webhook HTTP server path prefix.
fn default_http_path() -> String {
    "/api/flowgen/workers/v1".to_string()
}

/// HTTP server configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HttpServerOptions {
    /// Whether HTTP server is enabled.
    pub enabled: bool,
    /// HTTP server port number. Defaults to 3000.
    #[serde(default = "default_http_port")]
    pub port: u16,
    /// Path prefix for all webhook routes. Defaults to "/api/flowgen/workers/v1".
    #[serde(default = "default_http_path")]
    pub path: String,
    /// Optional path to global credentials file for webhook authentication.
    /// Individual `http_endpoint` tasks can override this with their own `credentials_path`.
    pub credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider configuration for user identity resolution.
    /// Shared across all HTTP-facing tasks on this worker.
    pub auth: Option<flowgen_core::auth::AuthConfig>,
}

impl Default for HttpServerOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            port: default_http_port(),
            path: default_http_path(),
            credentials_path: None,
            auth: None,
        }
    }
}

/// OpenTelemetry configuration for metrics, tracing, and log export.
///
/// When `enabled` is false the runtime skips telemetry entirely. When
/// enabled without a `backend` block the runtime falls back to the
/// in-memory backend, so `enabled: true` with no other fields is a
/// valid standalone configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TelemetryOptions {
    /// Whether OpenTelemetry is enabled.
    pub enabled: bool,
    /// Backend selection. Omitted means in-memory.
    #[serde(default)]
    pub backend: Option<TelemetryBackendOptions>,
    /// Service name for resource identification (defaults to "flowgen").
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Metrics export interval (defaults to "60s").
    /// Accepts human-readable durations: "30s", "1m", "5m".
    #[serde(default = "default_metrics_interval", with = "humantime_serde")]
    pub metrics_export_interval: Duration,
}

impl Default for TelemetryOptions {
    fn default() -> Self {
        Self {
            enabled: false,
            backend: None,
            service_name: default_service_name(),
            metrics_export_interval: default_metrics_interval(),
        }
    }
}

/// User-facing backend selection.
///
/// Serialized as a tagged enum. Each variant carries only the fields
/// that make sense for that backend, so an invalid combination
/// (e.g. `logs_per_flow` on the `remote` backend) fails at parse time.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum TelemetryBackendOptions {
    /// Keep logs and flow counters in memory on each pod; with `cache`, the
    /// web UI shows every pod.
    Memory {
        /// Log records retained per flow and level before oldest entries
        /// are dropped. Defaults to 1000.
        #[serde(default = "default_logs_per_flow")]
        logs_per_flow: usize,
        /// Metric samples retained per flow before oldest entries are
        /// dropped. Defaults to 1000.
        #[serde(default = "default_metrics_per_flow")]
        metrics_per_flow: usize,
        /// Port peers read logs and flow counters from. Defaults to 8082.
        #[serde(default = "default_cluster_port")]
        port: u16,
    },
    /// Push all signals over OTLP/gRPC to a remote collector.
    Remote {
        /// gRPC endpoint of the collector (e.g. "http://otel-collector:4317").
        endpoint: String,
    },
}

fn default_service_name() -> String {
    "flowgen".to_string()
}

fn default_metrics_interval() -> Duration {
    Duration::from_secs(60)
}

/// Default port peers read logs and flow counters from.
pub const DEFAULT_CLUSTER_PORT: u16 = 8082;

fn default_cluster_port() -> u16 {
    DEFAULT_CLUSTER_PORT
}

fn default_logs_per_flow() -> usize {
    1000
}

fn default_metrics_per_flow() -> usize {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::path::PathBuf;

    /// Each file under `root` with extension `ext`, with its content.
    fn files_under(root: &std::path::Path, ext: &str) -> Vec<(PathBuf, String)> {
        walkdir::WalkDir::new(root)
            .into_iter()
            .map(|entry| entry.expect("readable dir entry").into_path())
            .filter(|path| path.extension().is_some_and(|e| e == ext))
            .map(|path| {
                let content = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
                (path, content)
            })
            .collect()
    }

    /// Every example under `examples/` must deserialize through the real task
    /// configs. Without this, a key that no struct declares is silently
    /// dropped by serde and the example documents a setting that never
    /// applies.
    #[test]
    fn test_examples_deserialize() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples");
        let mut checked = 0;
        let mut failures: Vec<String> = Vec::new();

        for (path, source) in files_under(&root, "yaml") {
            if let Err(e) = FlowConfigRaw::parse(&path.display().to_string(), &source) {
                failures.push(format!("{}: {e}", path.display()));
            }
            checked += 1;
        }

        assert!(checked > 0, "no examples found under {}", root.display());
        assert!(
            failures.is_empty(),
            "{} example(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn every_whole_flow_in_the_docs_loads() {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../docs/src/routes");
        let mut checked = 0;
        let mut failures: Vec<String> = Vec::new();

        for (path, page) in files_under(&root, "md") {
            for (index, block) in page.split("```yaml\n").skip(1).enumerate() {
                let Some((source, _)) = block.split_once("```") else {
                    continue;
                };
                if !source.starts_with("flow:") || source.contains("# ...") {
                    continue;
                }
                let loaded = match FlowConfigRaw::parse("block.yaml", source) {
                    Ok(raw) => {
                        crate::flow::resolve_parents(&raw.flow.tasks).map_err(|e| e.to_string())
                    }
                    Err(e) => Err(e.to_string()),
                };
                if let Err(e) = loaded {
                    failures.push(format!("{} block {}: {e}", path.display(), index + 1));
                }
                checked += 1;
            }
        }

        assert!(checked > 0, "no doc flows found under {}", root.display());
        assert!(
            failures.is_empty(),
            "{} doc flow(s) failed:\n{}",
            failures.len(),
            failures.join("\n")
        );
    }

    #[test]
    fn app_config_sections_reject_unknown_fields() {
        let parse = |yaml: &str| {
            config::Config::builder()
                .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize::<AppConfig>()
                .unwrap_err()
                .to_string()
        };
        let cases = [
            ("flows:\n  cache:\n    enabled: true\n    prefx: f\n", "prefx"),
            (
                "flows: {}\ncache:\n  type: nats\n  runtime:\n    name: c\n",
                "name",
            ),
            ("flows: {}\ncache:\n  enabled: true\n  type: nats\n", "enabled"),
            (
                "flows: {}\ntelemetry:\n  enabled: true\n  backend:\n    type: memory\n    logs_per_flows: 5\n",
                "logs_per_flows",
            ),
            (
                "flows: {}\nhttp_server:\n  enabled: true\n  auth:\n    type: jwt\n    jwks: https://a\n",
                "jwks",
            ),
            (
                "flows: {}\nweb:\n  enabled: true\n  auth:\n    issuer_url: https://a\n    client_id: c\n    redirect_uri: https://b\n    scopes: [x]\n",
                "scopes",
            ),
        ];
        for (yaml, field) in cases {
            let error = parse(yaml);
            assert!(
                error.contains(&format!("unknown field `{field}`")),
                "{error}"
            );
        }
    }

    #[test]
    fn an_unknown_key_fails_unless_it_is_a_top_level_environment_variable() {
        let env = |vars: &[(&str, &str)]| {
            env_source().source(Some(
                vars.iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect(),
            ))
        };
        let file = |yaml: &'static str| config::File::from_str(yaml, config::FileFormat::Yaml);

        let typo = AppConfig::load(file("flows: {}\nshutdown_timout: 5s\n"), env(&[]))
            .unwrap_err()
            .to_string();
        let loaded = AppConfig::load(
            file("flows: {}\n"),
            env(&[("APP_SHUTDOWN_TIMEOUT", "7s"), ("APP_UNRELATED", "x")]),
        )
        .unwrap();

        let nested = AppConfig::load(file("flows: {}\n"), env(&[("APP_WEB__PROT", "1")]))
            .unwrap_err()
            .to_string();

        assert!(typo.contains("shutdown_timout"), "{typo}");
        assert_eq!(loaded.shutdown_timeout, Duration::from_secs(7));
        assert!(nested.contains("prot"), "{nested}");
    }

    #[test]
    fn the_example_app_config_loads() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../config.example.yaml");
        let file = config::File::from(path).format(config::FileFormat::Yaml);
        AppConfig::load(file, env_source().source(Some(Default::default())))
            .expect("config.example.yaml loads");
    }

    #[test]
    fn test_authoring_enabled_uses_defaults() {
        let yaml = "flows: {}\nweb:\n  enabled: true\n  authoring:\n    enabled: true\n";
        let config: AppConfig = config::Config::builder()
            .add_source(config::File::from_str(yaml, config::FileFormat::Yaml))
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();
        let authoring = config.web.unwrap().authoring.unwrap();
        assert_eq!(authoring.targets.len(), 1);
        let workspace = &authoring.targets[0];
        assert_eq!(workspace.commit_flow, "system/commit_workspace");
        assert_eq!(
            authoring
                .target_of("resources/s.rhai")
                .map(|t| t.name.as_str()),
            Some("workspace")
        );
        assert_eq!(authoring.groups_claim, "groups");
    }

    #[test]
    fn a_path_belongs_to_the_most_specific_target_on_whole_folders() {
        let target = |name: &str, path: &str| AuthoringTarget {
            name: name.to_string(),
            paths: vec![path.to_string()],
            commit_flow: default_commit_flow(),
            approver_groups: Vec::new(),
            track_deployment: true,
        };
        let authoring = AuthoringOptions {
            enabled: true,
            targets: vec![
                target("all", "flows/"),
                target("system", "flows/system"),
                target("user", "flows/user/"),
            ],
            groups_claim: default_groups_claim(),
        };
        let owner = |path: &str| authoring.target_of(path).map(|t| t.name.as_str());

        assert_eq!(owner("flows/system/a.yaml"), Some("system"));
        assert_eq!(owner("flows/user/a.yaml"), Some("user"));
        assert_eq!(owner("flows/username/a.yaml"), Some("all"));
        assert_eq!(owner("flows/systems.yaml"), Some("all"));
        assert_eq!(owner("resources/a.rhai"), None);
    }

    #[test]
    fn test_flow_config_creation() {
        let raw = FlowConfigRaw {
            flow: Flow {
                name: Some("test_flow".to_string()),
                labels: None,
                tasks: vec![],
                require_leader_election: None,
                parallel_instances: 1,
                shutdown_timeout: None,
            },
        };
        let flow_config =
            FlowConfig::from_path(raw, "test_flow".to_string(), None).expect("valid identity");

        assert_eq!(flow_config.flow.name.as_deref(), Some("test_flow"));
        assert!(flow_config.flow.labels.is_none());
        assert!(flow_config.flow.tasks.is_empty());
        assert_eq!(flow_config.identity(), "test_flow");
    }

    #[test]
    fn test_flow_config_raw_serialization_roundtrips() {
        let mut labels = Map::new();
        labels.insert("environment".to_string(), Value::String("test".to_string()));

        let raw = FlowConfigRaw {
            flow: Flow {
                name: Some("serialize_test".to_string()),
                labels: Some(labels),
                tasks: vec![],
                require_leader_election: None,
                parallel_instances: 1,
                shutdown_timeout: None,
            },
        };

        let serialized = serde_json::to_string(&raw).unwrap();
        let deserialized: FlowConfigRaw = serde_json::from_str(&serialized).unwrap();
        assert_eq!(raw, deserialized);
    }

    #[test]
    fn from_path_ignores_missing_flow_name() {
        let raw = FlowConfigRaw {
            flow: Flow {
                name: None,
                labels: None,
                tasks: vec![],
                require_leader_election: None,
                parallel_instances: 1,
                shutdown_timeout: None,
            },
        };
        let config = FlowConfig::from_path(raw, "demo/reader".to_string(), None)
            .expect("path is identity, name not required");
        assert_eq!(config.identity(), "demo/reader");
        assert_eq!(config.display_name(), "reader");
    }

    #[test]
    fn test_flow_creation() {
        let mut labels = Map::new();
        labels.insert("type".to_string(), Value::String("test".to_string()));

        let flow = Flow {
            name: Some("test_flow".to_string()),
            labels: Some(labels.clone()),
            tasks: vec![],
            require_leader_election: None,
            parallel_instances: 1,
            shutdown_timeout: None,
        };

        assert_eq!(flow.name.as_deref(), Some("test_flow"));
        assert_eq!(flow.labels, Some(labels));
        assert!(flow.tasks.is_empty());
    }

    #[test]
    fn test_flow_with_tasks() {
        let convert_config = flowgen_core::task::convert::Convert::default();
        let task = TaskType::convert(convert_config);

        let flow = Flow {
            name: Some("flow_with_tasks".to_string()),
            labels: None,
            tasks: vec![task],
            require_leader_election: None,
            parallel_instances: 1,
            shutdown_timeout: None,
        };

        assert_eq!(flow.name.as_deref(), Some("flow_with_tasks"));
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
            name: Some("serialize_flow".to_string()),
            labels: Some(labels),
            tasks: vec![],
            require_leader_election: None,
            parallel_instances: 1,
            shutdown_timeout: None,
        };

        let serialized = serde_json::to_string(&flow).unwrap();
        let deserialized: Flow = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow, deserialized);
    }

    #[test]
    fn test_flow_clone() {
        let flow = Flow {
            name: Some("clone_test".to_string()),
            labels: None,
            tasks: vec![],
            require_leader_election: None,
            parallel_instances: 1,
            shutdown_timeout: None,
        };

        let cloned = flow.clone();
        assert_eq!(flow, cloned);
    }

    #[test]
    fn test_task_variants() {
        let convert_task = TaskType::convert(flowgen_core::task::convert::Convert::default());
        let generate_task = TaskType::generate(flowgen_core::task::generate::Generate::default());

        assert!(matches!(convert_task, TaskType::convert(_)));
        assert!(matches!(generate_task, TaskType::generate(_)));
    }

    #[test]
    fn test_app_config_creation() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                cache_type: CacheType::Nats,
                credentials_path: Some(PathBuf::from("/test/cache")),
                url: "localhost:4222".to_string(),
                path: default_redb_path(),
                runtime: BucketOptions::default(),
                system: SystemBucketOptions::default(),
                history: None,
                tombstone_ttl: None,
            }),
            flows: FlowOptions {
                path: Some(PathBuf::from("/test/flows/*")),
                cache: None,
                system: false,
                system_exclude: Vec::new(),
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            telemetry: None,
            shutdown_timeout: default_shutdown_timeout(),
            host: default_host(),
            ai_budget: None,
            credentials: None,
        };

        assert!(app_config.cache.is_some());
        assert_eq!(app_config.cache_options().cache_type, CacheType::Nats);
        assert!(app_config.flows.path.is_some());
        assert!(app_config.telemetry.is_none());
        assert!(app_config.http_server.is_none());
        assert!(app_config.retry.is_none());
        assert!(app_config.resources.is_none());
    }

    #[test]
    fn test_app_config_without_cache() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                path: Some(PathBuf::from("/flows/*")),
                cache: None,
                system: false,
                system_exclude: Vec::new(),
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            telemetry: None,
            shutdown_timeout: default_shutdown_timeout(),
            host: default_host(),
            ai_budget: None,
            credentials: None,
        };

        assert!(app_config.cache.is_none());
        assert!(app_config.flows.path.is_some());
        let cache = app_config.cache_options();
        assert_eq!(cache.cache_type, CacheType::Redb);
        assert_eq!(cache.path, default_redb_path());
    }

    #[test]
    fn test_app_config_serialization() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                cache_type: CacheType::Nats,
                credentials_path: Some(PathBuf::from("/serialize/cache")),
                url: "localhost:4222".to_string(),
                path: default_redb_path(),
                runtime: BucketOptions {
                    db_name: "test_db".to_string(),
                },
                system: SystemBucketOptions::default(),
                history: None,
                tombstone_ttl: None,
            }),
            flows: FlowOptions {
                path: Some(PathBuf::from("/serialize/flows/*")),
                cache: None,
                system: false,
                system_exclude: Vec::new(),
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            telemetry: None,
            shutdown_timeout: default_shutdown_timeout(),
            host: default_host(),
            ai_budget: None,
            credentials: None,
        };

        let serialized = serde_json::to_string(&app_config).unwrap();
        let deserialized: AppConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(app_config, deserialized);
    }

    #[test]
    fn test_app_config_clone() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                cache_type: CacheType::Nats,
                credentials_path: Some(PathBuf::from("/clone/cache")),
                url: "localhost:4222".to_string(),
                path: default_redb_path(),
                runtime: BucketOptions::default(),
                system: SystemBucketOptions::default(),
                history: None,
                tombstone_ttl: None,
            }),
            flows: FlowOptions {
                path: None,
                cache: None,
                system: false,
                system_exclude: Vec::new(),
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            telemetry: None,
            shutdown_timeout: default_shutdown_timeout(),
            host: default_host(),
            ai_budget: None,
            credentials: None,
        };

        let cloned = app_config.clone();
        assert_eq!(app_config, cloned);
    }

    #[test]
    fn validate_accepts_the_new_bucket_layout() {
        let yaml = r#"
cache:
  type: nats
  url: "localhost:4222"
  runtime:
    db_name: "legacy_bucket"
flows:
  path: "/flows"
"#;
        let config: AppConfig = serde_yaml::from_str(yaml).expect("parses");
        assert_eq!(
            config.cache.as_ref().unwrap().runtime.db_name,
            "legacy_bucket"
        );
        config.validate().expect("new layout is accepted");
    }

    #[test]
    fn a_system_flow_can_be_left_out_by_identity_and_an_unknown_one_is_rejected() {
        let config = |excluded: &str| -> AppConfig {
            serde_yaml::from_str(&format!(
                "flows:\n  system: true\n  system_exclude: [{excluded:?}]\n"
            ))
            .expect("parses")
        };

        config("system/release_workspace")
            .validate()
            .expect("a system flow is accepted");
        assert!(matches!(
            config("system/release").validate(),
            Err(ValidationError::UnknownSystemFlow { .. })
        ));
    }

    #[test]
    fn env_overrides_a_nested_key_and_leaves_the_rest_of_the_file() {
        unsafe {
            std::env::set_var("APP_WEB__AUTH__CLIENT_SECRET", "from-env");
            std::env::set_var("APP_WEB__PORT", "9999");
        }
        let built = config::Config::builder()
            .add_source(config::File::from_str(
                r#"
web:
  enabled: true
  port: 8080
  auth:
    issuer_url: "https://auth.example.com"
    client_id: "flowgen-web"
    client_secret: "from-file"
    redirect_uri: "https://example.com/flowgen/auth/callback"
flows:
  path: "/flows"
"#,
                config::FileFormat::Yaml,
            ))
            .add_source(env_source())
            .build()
            .expect("builds");
        unsafe {
            std::env::remove_var("APP_WEB__AUTH__CLIENT_SECRET");
            std::env::remove_var("APP_WEB__PORT");
        }

        let config: AppConfig = built.try_deserialize().expect("deserializes");
        let web = config.web.expect("web present");
        let auth = web.auth.expect("auth present");
        use secrecy::ExposeSecret;
        assert_eq!(
            auth.client_secret
                .expect("client secret present")
                .expose_secret(),
            "from-env"
        );
        assert_eq!(web.port, 9999);
        assert!(web.enabled);
    }

    #[test]
    fn cache_omitting_both_buckets_uses_the_documented_defaults() {
        let yaml = r#"
cache:
  type: nats
  url: "localhost:4222"
flows:
  path: "/flows"
"#;
        let config: AppConfig = serde_yaml::from_str(yaml).expect("parses");
        let cache = config.cache.as_ref().expect("cache present");
        assert_eq!(cache.runtime.db_name, "flowgen_cache");
        assert_eq!(cache.system.db_name, "flowgen_system");
        config.validate().expect("defaults are accepted");
    }

    fn with_agents(agents: &str) -> AppConfig {
        let indented: String = agents.lines().map(|line| format!("    {line}\n")).collect();
        serde_yaml::from_str(&format!(
            "flows:\n  path: /flows\nweb:\n  enabled: true\n  agents:\n    enabled: true\n{indented}"
        ))
        .expect("parses")
    }

    #[test]
    fn agents_defaults_are_accepted() {
        let config = with_agents("");
        let agents = config.web.as_ref().unwrap().agents.as_ref().unwrap();

        assert_eq!(agents.reply_idle_timeout, Duration::from_secs(600));
        config.validate().expect("defaults are accepted");
    }

    #[test]
    fn agents_reply_durations_must_be_longer_than_zero() {
        for field in ["reply_save_interval", "reply_idle_timeout"] {
            let err = with_agents(&format!("{field}: 0s"))
                .validate()
                .expect_err("zero is rejected");

            assert!(err.to_string().contains(field), "{err}");
        }
    }

    #[test]
    fn agents_cache_prefix_must_be_dotted_key_segments() {
        for prefix in ["a", "a.b", "a_1.b-2"] {
            with_agents(&format!("cache_prefix: {prefix:?}"))
                .validate()
                .expect("accepted");
        }
        for prefix in ["", ".a", "a.", "a..b", "a.*", "a.>", "a b", "a/b"] {
            let rejected = with_agents(&format!("cache_prefix: {prefix:?}")).validate();

            assert!(rejected.is_err(), "{prefix:?} is accepted");
        }
    }

    #[test]
    fn test_cache_options_creation() {
        let cache_options = CacheOptions {
            cache_type: CacheType::Nats,
            credentials_path: Some(PathBuf::from("/test/credentials_path")),
            url: "localhost:4222".to_string(),
            path: default_redb_path(),
            runtime: BucketOptions::default(),
            system: SystemBucketOptions::default(),
            history: None,
            tombstone_ttl: None,
        };

        assert_eq!(
            cache_options.credentials_path,
            Some(PathBuf::from("/test/credentials_path"))
        );
    }

    #[test]
    fn a_cache_section_without_a_type_is_a_redb_file() {
        let config: AppConfig = serde_yaml::from_str(
            "cache:\n  path: /var/lib/flowgen/state.redb\nflows:\n  path: /flows\n",
        )
        .expect("parses");
        let cache = config.cache_options();
        assert_eq!(cache.cache_type, CacheType::Redb);
        assert_eq!(cache.path, PathBuf::from("/var/lib/flowgen/state.redb"));
    }

    #[test]
    fn test_cache_options_serialization() {
        let cache_options = CacheOptions {
            cache_type: CacheType::Nats,
            credentials_path: Some(PathBuf::from("/serialize/credentials_path")),
            url: "localhost:4222".to_string(),
            path: default_redb_path(),
            runtime: BucketOptions::default(),
            system: SystemBucketOptions::default(),
            history: None,
            tombstone_ttl: None,
        };

        let serialized = serde_json::to_string(&cache_options).unwrap();
        let deserialized: CacheOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cache_options, deserialized);
    }

    #[test]
    fn test_flow_options_with_path() {
        let flow_options = FlowOptions {
            path: Some(PathBuf::from("/test/flows/*.toml")),
            ..FlowOptions::default()
        };

        assert!(flow_options.path.is_some());
        assert_eq!(
            flow_options.path.unwrap(),
            PathBuf::from("/test/flows/*.toml")
        );
    }

    #[test]
    fn test_flow_options_without_path() {
        let flow_options = FlowOptions::default();

        assert!(flow_options.path.is_none());
    }

    #[test]
    fn test_flow_options_serialization() {
        let flow_options = FlowOptions {
            path: Some(PathBuf::from("/serialize/flows/*.toml")),
            ..FlowOptions::default()
        };

        let serialized = serde_json::to_string(&flow_options).unwrap();
        let deserialized: FlowOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow_options, deserialized);
    }

    #[test]
    fn test_complex_flow_config() {
        let convert_config = flowgen_core::task::convert::Convert {
            name: "convert_task".to_string(),
            ..Default::default()
        };
        let generate_config = flowgen_core::task::generate::Generate {
            name: "generate_task".to_string(),
            ..Default::default()
        };

        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Complex Multi-Task Flow".to_string()),
        );
        labels.insert("complexity".to_string(), Value::String("high".to_string()));

        let raw = FlowConfigRaw {
            flow: Flow {
                name: Some("complex_flow".to_string()),
                labels: Some(labels.clone()),
                tasks: vec![
                    TaskType::convert(convert_config),
                    TaskType::generate(generate_config),
                ],
                require_leader_election: None,
                parallel_instances: 1,
                shutdown_timeout: None,
            },
        };
        let flow_config =
            FlowConfig::from_path(raw, "complex_flow".to_string(), None).expect("valid identity");

        assert_eq!(flow_config.flow.name.as_deref(), Some("complex_flow"));
        assert_eq!(flow_config.flow.labels, Some(labels));
        assert_eq!(flow_config.flow.tasks.len(), 2);
        assert!(matches!(flow_config.flow.tasks[0], TaskType::convert(_)));
        assert!(matches!(flow_config.flow.tasks[1], TaskType::generate(_)));
    }

    #[test]
    fn test_http_server_options_creation() {
        let http_server_options = HttpServerOptions {
            enabled: true,
            port: 8080,
            path: "/workers".to_string(),
            credentials_path: None,
            auth: None,
        };

        assert!(http_server_options.enabled);
        assert_eq!(http_server_options.port, 8080);
    }

    #[test]
    fn test_http_server_options_without_port() {
        let http_server_options = HttpServerOptions {
            enabled: false,
            port: 3000,
            path: "/api/flowgen/workers/v1".to_string(),
            credentials_path: None,
            auth: None,
        };

        assert!(!http_server_options.enabled);
        assert_eq!(http_server_options.port, 3000);
    }

    #[test]
    fn test_app_config_with_http_server_options() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                path: None,
                cache: None,
                system: false,
                system_exclude: Vec::new(),
            },
            resources: None,
            http_server: Some(HttpServerOptions {
                enabled: true,
                port: 8080,
                path: "/workers".to_string(),
                credentials_path: None,
                auth: None,
            }),
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            telemetry: None,
            shutdown_timeout: default_shutdown_timeout(),
            host: default_host(),
            ai_budget: None,
            credentials: None,
        };

        assert!(app_config.http_server.is_some());
        let http_server = app_config.http_server.as_ref().unwrap();
        assert!(http_server.enabled);
        assert_eq!(http_server.port, 8080);
        assert_eq!(http_server.path, "/workers");
    }

    #[test]
    fn test_telemetry_options_remote_backend() {
        let telemetry_options = TelemetryOptions {
            enabled: true,
            backend: Some(TelemetryBackendOptions::Remote {
                endpoint: "http://otel-collector:4317".to_string(),
            }),
            service_name: "flowgen".to_string(),
            metrics_export_interval: Duration::from_secs(30),
        };

        assert!(telemetry_options.enabled);
        assert!(matches!(
            &telemetry_options.backend,
            Some(TelemetryBackendOptions::Remote { endpoint }) if endpoint == "http://otel-collector:4317"
        ));
        assert_eq!(telemetry_options.service_name, "flowgen");
        assert_eq!(
            telemetry_options.metrics_export_interval,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn test_telemetry_options_defaults_to_no_backend() {
        let telemetry_options = TelemetryOptions {
            enabled: true,
            backend: None,
            service_name: default_service_name(),
            metrics_export_interval: default_metrics_interval(),
        };

        assert!(telemetry_options.enabled);
        assert!(telemetry_options.backend.is_none());
        assert_eq!(telemetry_options.service_name, "flowgen");
        assert_eq!(
            telemetry_options.metrics_export_interval,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn test_telemetry_options_serialization_roundtrip() {
        let telemetry_options = TelemetryOptions {
            enabled: true,
            backend: Some(TelemetryBackendOptions::Remote {
                endpoint: "http://grafana:4317".to_string(),
            }),
            service_name: "flowgen-prod".to_string(),
            metrics_export_interval: Duration::from_secs(120),
        };

        let serialized = serde_json::to_string(&telemetry_options).unwrap();
        let deserialized: TelemetryOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(telemetry_options, deserialized);
    }

    #[test]
    fn test_telemetry_backend_tagged_enum_yaml() {
        let yaml_remote = "type: remote\nendpoint: http://otel:4317\n";
        let parsed: TelemetryBackendOptions = serde_yaml::from_str(yaml_remote).unwrap();
        assert!(matches!(
            &parsed,
            TelemetryBackendOptions::Remote { endpoint } if endpoint == "http://otel:4317"
        ));

        let yaml_memory = "type: memory\n";
        let parsed: TelemetryBackendOptions = serde_yaml::from_str(yaml_memory).unwrap();
        assert!(matches!(
            parsed,
            TelemetryBackendOptions::Memory {
                logs_per_flow: 1000,
                metrics_per_flow: 1000,
                port: 8082,
            }
        ));

        let yaml_memory_custom =
            "type: memory\nlogs_per_flow: 250\nmetrics_per_flow: 500\nport: 9090\n";
        let parsed: TelemetryBackendOptions = serde_yaml::from_str(yaml_memory_custom).unwrap();
        assert!(matches!(
            parsed,
            TelemetryBackendOptions::Memory {
                logs_per_flow: 250,
                metrics_per_flow: 500,
                port: 9090,
            }
        ));
    }

    #[test]
    fn test_app_config_with_telemetry() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                path: None,
                cache: None,
                system: false,
                system_exclude: Vec::new(),
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            telemetry: Some(TelemetryOptions {
                enabled: true,
                backend: Some(TelemetryBackendOptions::Remote {
                    endpoint: "http://otel-collector:4317".to_string(),
                }),
                service_name: "flowgen".to_string(),
                metrics_export_interval: Duration::from_secs(60),
            }),
            shutdown_timeout: default_shutdown_timeout(),
            host: default_host(),
            ai_budget: None,
            credentials: None,
        };

        assert!(app_config.telemetry.is_some());
        let telemetry = app_config.telemetry.as_ref().unwrap();
        assert!(telemetry.enabled);
        assert!(matches!(
            &telemetry.backend,
            Some(TelemetryBackendOptions::Remote { endpoint }) if endpoint == "http://otel-collector:4317"
        ));
        assert_eq!(telemetry.service_name, "flowgen");
        assert_eq!(telemetry.metrics_export_interval, Duration::from_secs(60));
    }
}
