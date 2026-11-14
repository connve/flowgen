use crate::config::TaskType;
use crate::web::WebState;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use flowgen_client::types as api;
use flowgen_core::task::meta::{
    is_per_event, render_env, Connection, Connector, CredentialsForm, FieldKind,
};
use flowgen_core::telemetry::query::{LogFilter, LogsStore};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use tracing::warn;

/// A connection one task makes, with the account its credentials describe.
#[derive(Clone, Debug)]
pub struct TaskConnection {
    pub task: String,
    pub connection: Connection,
    pub account: Option<String>,
}

/// [`of_tasks`] on the blocking pool, since describing credentials reads files.
pub async fn of_flow(tasks: Vec<TaskType>) -> Vec<TaskConnection> {
    match tokio::task::spawn_blocking(move || of_tasks(&tasks)).await {
        Ok(connections) => connections,
        Err(source) => {
            warn!(error = %source, "Failed to gather a flow's connections");
            Vec::new()
        }
    }
}

/// Skips blocking tasks, whose `credentials_path` checks callers rather than connecting out.
pub fn of_tasks(tasks: &[TaskType]) -> Vec<TaskConnection> {
    let mut accounts: HashMap<(&'static str, String), Option<String>> = HashMap::new();
    let mut found = Vec::new();
    for task in tasks.iter().filter(|task| !task.is_blocking()) {
        for connection in task.connections() {
            let account = match (
                connection.connector.describe_credentials,
                &connection.credentials_path,
            ) {
                (Some(describe), Some(path)) if !is_per_event(path) => accounts
                    .entry((connection.connector.name, path.clone()))
                    .or_insert_with(|| match render_env(path) {
                        Some(path) => describe(std::path::Path::new(&path)),
                        None => None,
                    })
                    .clone(),
                _ => None,
            };
            found.push(TaskConnection {
                task: task.name().to_string(),
                connection,
                account,
            });
        }
    }
    found
}

/// One connection: the credentials it signs in with, or without any, where it connects.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug)]
struct Identity {
    connector: &'static str,
    credentials_path: Option<String>,
    target: Option<String>,
}

impl Identity {
    /// A credentials path that renders empty means none, as tasks read it.
    fn of(connection: &Connection) -> Self {
        let credentials_path = match &connection.credentials_path {
            Some(path) if !is_per_event(path) && render_env(path).is_none() => None,
            path => path.clone(),
        };
        Self {
            connector: connection.connector.name,
            target: match credentials_path {
                Some(_) => None,
                None => connection.target.clone(),
            },
            credentials_path,
        }
    }

    fn id(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.connector);
        hasher.update([0]);
        hasher.update(self.credentials_path.as_deref().unwrap_or_default());
        hasher.update([0]);
        hasher.update(self.target.as_deref().unwrap_or_default());
        format!("{:x}", hasher.finalize())[..16].to_string()
    }
}

#[derive(Clone, Debug, PartialEq)]
struct User {
    flow: String,
    task: String,
}

/// The tasks sharing one connection, and where they connect.
#[derive(Debug, PartialEq)]
struct Row {
    account: Option<String>,
    targets: BTreeSet<String>,
    per_event: bool,
    on_behalf_of_user: bool,
    users: Vec<User>,
}

/// Every connector by name, with its task types.
fn connectors() -> BTreeMap<&'static str, (Connector, Vec<&'static str>)> {
    let mut connectors = BTreeMap::new();
    for (task_type, connector) in TaskType::TASK_CONNECTORS {
        connectors
            .entry(connector.name)
            .or_insert((*connector, Vec::new()))
            .1
            .push(*task_type);
    }
    connectors
}

fn credentials_form(form: &CredentialsForm) -> api::CredentialsForm {
    api::CredentialsForm {
        label: form.label.to_string(),
        fields: form
            .fields
            .iter()
            .map(|field| {
                let (kind, choices, checked) = match field.kind {
                    FieldKind::Text => (api::CredentialsFieldKind::Text, Vec::new(), false),
                    FieldKind::Secret => (api::CredentialsFieldKind::Secret, Vec::new(), false),
                    FieldKind::Number => (api::CredentialsFieldKind::Number, Vec::new(), false),
                    FieldKind::Boolean(checked) => {
                        (api::CredentialsFieldKind::Boolean, Vec::new(), checked)
                    }
                    FieldKind::List => (api::CredentialsFieldKind::List, Vec::new(), false),
                    FieldKind::Choice(choices) => (
                        api::CredentialsFieldKind::Choice,
                        choices.iter().map(|choice| choice.to_string()).collect(),
                        false,
                    ),
                };
                api::CredentialsField {
                    key: field.key.to_string(),
                    label: field.label.to_string(),
                    kind,
                    choices,
                    checked,
                    required: field.required,
                }
            })
            .collect(),
    }
}

/// `GET /api/connectors`.
pub(crate) async fn list_connectors() -> Json<Vec<api::Connector>> {
    Json(
        connectors()
            .into_values()
            .map(|(connector, task_types)| api::Connector {
                name: connector.name.to_string(),
                label: connector.label.to_string(),
                task_types: task_types.into_iter().map(str::to_string).collect(),
                credentials: connector.credentials.iter().map(credentials_form).collect(),
            })
            .collect(),
    )
}

/// `GET /api/connectors/{name}/icon`.
pub(crate) async fn connector_icon(Path(name): Path<String>) -> Response {
    match connectors().get(name.as_str()) {
        Some((connector, _)) => (
            [
                (header::CONTENT_TYPE, connector.icon.content_type()),
                (header::CACHE_CONTROL, "public, max-age=3600"),
            ],
            connector.icon.bytes(),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            format!("Connector '{name}' not found"),
        )
            .into_response(),
    }
}

/// `GET /api/connections`.
pub(crate) async fn list_connections(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<api::Connection>> {
    Json(
        futures::future::join_all(
            gather(&loaded_flows(&state))
                .into_iter()
                .map(|(identity, row)| connection(&state, identity, row)),
        )
        .await,
    )
}

/// `GET /api/connections/{id}`.
pub(crate) async fn get_connection(
    State(state): State<Arc<WebState>>,
    Path(id): Path<String>,
) -> Result<Json<api::Connection>, (StatusCode, String)> {
    match gather(&loaded_flows(&state))
        .into_iter()
        .find(|(identity, _)| identity.id() == id)
    {
        Some((identity, row)) => Ok(Json(connection(&state, identity, row).await)),
        None => Err((
            StatusCode::NOT_FOUND,
            format!("Connection '{id}' not found"),
        )),
    }
}

/// `GET /api/agents/tools`: each `mcp_tool` with the connectors of its flow,
/// so the Agents chat can show what a tool call reaches.
pub(crate) async fn list_agent_tools(
    State(state): State<Arc<WebState>>,
) -> Json<Vec<api::AgentTool>> {
    let registry = match state.flow_registry.read() {
        Ok(registry) => registry,
        Err(_) => {
            warn!("Flow registry is poisoned, returning no tools");
            return Json(Vec::new());
        }
    };
    let mut tools = Vec::new();
    for handle in registry.values() {
        let mut connectors: Vec<String> = Vec::new();
        for found in &handle.connections {
            let name = found.connection.connector.name;
            if !matches!(name, "core" | "mcp") && !connectors.iter().any(|known| known == name) {
                connectors.push(name.to_string());
            }
        }
        for name in mcp_tool_names(&handle.flow_yaml) {
            tools.push(api::AgentTool {
                name,
                flow: handle.identity().to_string(),
                connectors: connectors.clone(),
            });
        }
    }
    Json(tools)
}

/// Names of the `mcp_tool` tasks in a flow's YAML.
fn mcp_tool_names(yaml: &str) -> Vec<String> {
    fn walk(value: &serde_yaml::Value, names: &mut Vec<String>) {
        match value {
            serde_yaml::Value::Mapping(map) => {
                for (key, value) in map {
                    match (key.as_str(), value.get("name").and_then(|n| n.as_str())) {
                        (Some("mcp_tool"), Some(name)) => names.push(name.to_string()),
                        _ => walk(value, names),
                    }
                }
            }
            serde_yaml::Value::Sequence(items) => items.iter().for_each(|item| walk(item, names)),
            _ => {}
        }
    }
    let mut names = Vec::new();
    if let Ok(value) = serde_yaml::from_str::<serde_yaml::Value>(yaml) {
        walk(&value, &mut names);
    }
    names
}

fn loaded_flows(state: &WebState) -> Vec<(String, Vec<TaskConnection>)> {
    match state.flow_registry.read() {
        Ok(registry) => registry
            .values()
            .map(|handle| (handle.identity().to_string(), handle.connections.clone()))
            .collect(),
        Err(_) => {
            warn!("Flow registry is poisoned, returning no connections");
            Vec::new()
        }
    }
}

async fn connection(state: &WebState, identity: Identity, row: Row) -> api::Connection {
    let flows: BTreeSet<String> = row.users.iter().map(|user| user.flow.clone()).collect();
    let status = match state.logs_store.as_ref() {
        Some(logs) => read_status(logs.as_ref(), &row.users).await,
        None => api::FlowStatus::Idle,
    };
    let credentials_name = match (
        std::env::var_os(flowgen_core::credentials::FOLDER_ENV),
        identity.credentials_path.as_deref(),
    ) {
        (Some(folder), Some(path)) => credentials_name(path, std::path::Path::new(&folder)),
        _ => None,
    };
    let credentials_found = match identity.credentials_path.as_deref().map(render_env) {
        Some(Some(path)) => {
            let path = path.to_string();
            let checked = tokio::task::spawn_blocking(move || {
                flowgen_core::credentials::exists(std::path::Path::new(&path))
            });
            checked.await.unwrap_or(true)
        }
        Some(None) | None => true,
    };
    api::Connection {
        id: identity.id(),
        connector: identity.connector.to_string(),
        credentials_name,
        credentials_found,
        credentials_path: identity.credentials_path,
        account: row.account,
        targets: row.targets.into_iter().collect(),
        per_event: row.per_event,
        on_behalf_of_user: row.on_behalf_of_user,
        flows: flows.into_iter().collect(),
        status,
    }
}

/// The credentials a path reads from `folder`, by the file name: `orders-api`
/// for `{{env.FLOWGEN_CREDENTIALS}}/orders-api.json`. A relative path is taken
/// from the working directory, as tasks read it. `None` for a path set per
/// event, a file elsewhere or a file name that cannot name credentials.
fn credentials_name(path: &str, folder: &std::path::Path) -> Option<String> {
    if is_per_event(path) {
        return None;
    }
    let file = std::path::absolute(render_env(path)?).ok()?;
    if file.parent()? != std::path::absolute(folder).ok()? {
        return None;
    }
    let name = file.file_name()?.to_str()?.strip_suffix(".json")?;
    match crate::credentials::is_name(name) {
        true => Some(name.to_string()),
        false => None,
    }
}

/// The connections of `flows`, each with the tasks using it.
fn gather(flows: &[(String, Vec<TaskConnection>)]) -> BTreeMap<Identity, Row> {
    let mut rows: BTreeMap<Identity, Row> = BTreeMap::new();
    for (flow, connections) in flows {
        for found in connections {
            let row = rows
                .entry(Identity::of(&found.connection))
                .or_insert_with(|| Row {
                    account: None,
                    targets: BTreeSet::new(),
                    per_event: false,
                    on_behalf_of_user: false,
                    users: Vec::new(),
                });
            if row.account.is_none() {
                row.account = found.account.clone();
            }
            if let Some(target) = &found.connection.target {
                row.targets.insert(target.clone());
            }
            row.per_event |= found.connection.per_event;
            row.on_behalf_of_user |= found.connection.on_behalf_of_user;
            let user = User {
                flow: flow.clone(),
                task: found.task.clone(),
            };
            if !row.users.contains(&user) {
                row.users.push(user);
            }
        }
    }
    rows
}

/// The worst level among the latest records of `users`; idle without any.
async fn read_status(logs: &dyn LogsStore, users: &[User]) -> api::FlowStatus {
    futures::future::join_all(users.iter().map(|user| latest_status(logs, user)))
        .await
        .into_iter()
        .fold(api::FlowStatus::Idle, |worst, status| {
            match severity(status) > severity(worst) {
                true => status,
                false => worst,
            }
        })
}

async fn latest_status(logs: &dyn LogsStore, user: &User) -> api::FlowStatus {
    let latest = LogFilter {
        flow: Some(user.flow.clone()),
        task: Some(user.task.clone()),
        ..Default::default()
    };
    match logs.query(latest, 1).await {
        Ok(records) => match records.last() {
            Some(record) => status_of(&record.level),
            None => api::FlowStatus::Idle,
        },
        Err(source) => {
            warn!(error = %source, flow = %user.flow, task = %user.task, "Failed to read a connection's logs");
            api::FlowStatus::Idle
        }
    }
}

fn status_of(level: &str) -> api::FlowStatus {
    match level.to_ascii_lowercase().as_str() {
        "error" => api::FlowStatus::Error,
        "warn" | "warning" => api::FlowStatus::Warn,
        _ => api::FlowStatus::Ok,
    }
}

fn severity(status: api::FlowStatus) -> u8 {
    match status {
        api::FlowStatus::Idle => 0,
        api::FlowStatus::Ok => 1,
        api::FlowStatus::Warn => 2,
        api::FlowStatus::Error => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flowgen_core::telemetry::{StoredLog, StoredSpan};

    #[test]
    fn mcp_tool_names_are_read_from_the_flow_yaml() {
        let yaml = "flow:\n  tasks:\n    - mcp_tool:\n        name: a\n    - script:\n        name: b\n    - mcp_tool:\n        name: c\n";

        assert_eq!(mcp_tool_names(yaml), vec!["a", "c"]);
    }

    fn tasks(yaml: &str) -> Vec<TaskType> {
        let indented: String = yaml.lines().map(|line| format!("    {line}\n")).collect();
        crate::config::FlowConfigRaw::parse("a.yaml", &format!("flow:\n  tasks:\n{indented}"))
            .unwrap()
            .flow
            .tasks
    }

    fn user(flow: &str, task: &str) -> User {
        User {
            flow: flow.to_string(),
            task: task.to_string(),
        }
    }

    #[test]
    fn a_connector_name_has_one_declaration() {
        for (task_type, connector) in TaskType::TASK_CONNECTORS {
            let (declared, _) = &connectors()[connector.name];
            assert_eq!(
                (connector.label, connector.icon),
                (declared.label, declared.icon),
                "{task_type}"
            );
        }
    }

    #[tokio::test]
    async fn icons_are_served_with_their_media_type() {
        let content_type = |response: &Response| {
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .map(|value| value.to_str().unwrap().to_string())
        };

        let svg = connector_icon(Path("salesforce".to_string())).await;
        let png = connector_icon(Path("braze".to_string())).await;
        let missing = connector_icon(Path("a".to_string())).await;

        assert_eq!(content_type(&svg).as_deref(), Some("image/svg+xml"));
        assert_eq!(content_type(&png).as_deref(), Some("image/png"));
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn task_types_are_listed_under_their_connector() {
        let connectors = connectors();

        assert!(connectors["salesforce"]
            .1
            .contains(&"salesforce_restapi_search"));
        assert!(connectors["mcp"].1.contains(&"mcp_tool"));
        assert!(connectors["html"].1.contains(&"html_scrape"));
    }

    #[test]
    fn tasks_of_one_connector_and_credentials_share_a_connection() {
        let flows = vec![
            (
                "a".to_string(),
                of_tasks(&tasks(
                    r#"
- http_endpoint:
    name: receive
    endpoint: /a
    method: POST
    credentials_path: /etc/a/http.json
- gcp_bigquery_query:
    name: read
    credentials_path: /etc/a/gcp.json
    project_id: p
    query: "SELECT 1"
- script:
    name: shape
    code: "event"
"#,
                )),
            ),
            (
                "b".to_string(),
                of_tasks(&tasks(
                    r#"
- gcp_bigquery_query:
    name: lookup
    credentials_path: /etc/a/gcp.json
    project_id: p
    query: "SELECT 2"
- nats_jetstream_publish:
    name: publish
    url: "nats://user:secret@nats:4222"
    subject: a
- nats_kv_store:
    name: store
    url: "nats://other:4222"
    bucket: b
    operation: put
    key_prefix: "a."
- ai_completion:
    name: agent
    provider: custom
    endpoint: "http://gateway:3002/v1"
    model: "{{event.data.model}}"
    prompt: p
    mcp_servers:
      - url: "http://localhost:3001/mcp"
        on_behalf_of_user: true
"#,
                )),
            ),
        ];

        let identity =
            |connector: &'static str, credentials: Option<&str>, target: Option<&str>| Identity {
                connector,
                credentials_path: credentials.map(str::to_string),
                target: target.map(str::to_string),
            };
        let row = |targets: &[&str], per_event: bool, users: Vec<User>| Row {
            account: None,
            targets: targets.iter().map(|target| target.to_string()).collect(),
            per_event,
            on_behalf_of_user: false,
            users,
        };

        let found: Vec<(Identity, Row)> = gather(&flows).into_iter().collect();

        assert_eq!(
            found,
            vec![
                (
                    identity("ai", None, Some("gateway:3002")),
                    row(&["gateway:3002"], true, vec![user("b", "agent")])
                ),
                (
                    identity("gcp", Some("/etc/a/gcp.json"), None),
                    row(&[], false, vec![user("a", "read"), user("b", "lookup")])
                ),
                (
                    identity("mcp", None, Some("localhost:3001")),
                    Row {
                        on_behalf_of_user: true,
                        ..row(&["localhost:3001"], false, vec![user("b", "agent")])
                    }
                ),
                (
                    identity("nats", None, Some("nats:4222")),
                    row(&["nats:4222"], false, vec![user("b", "publish")])
                ),
                (
                    identity("nats", None, Some("other:4222")),
                    row(&["other:4222"], false, vec![user("b", "store")])
                ),
            ]
        );
    }

    #[test]
    fn a_credentials_file_shows_its_account_and_never_its_secret() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mssql.json");
        std::fs::write(
            &path,
            r#"{"host": "db", "database": "sales", "username": "u", "password": "secret"}"#,
        )
        .unwrap();
        let yaml = format!(
            "- mssql_query:\n    name: read\n    credentials_path: {}\n    query: \"SELECT 1\"\n",
            path.display()
        );

        let found = of_tasks(&tasks(&yaml));

        assert_eq!(found[0].account.as_deref(), Some("db:1433/sales"));
        assert!(!format!("{found:?}").contains("secret"));
    }

    #[test]
    fn only_files_in_the_credentials_folder_are_named() {
        let name = |path: &str| credentials_name(path, std::path::Path::new("/etc/credentials"));
        assert_eq!(
            name("/etc/credentials/orders-api.json").as_deref(),
            Some("orders-api")
        );
        assert_eq!(name("/etc/a/orders-api.json"), None);
        assert_eq!(name("/etc/credentials/a/b.json"), None);
        assert_eq!(name("/etc/credentials/a.yaml"), None);
        assert_eq!(name("/etc/credentials/My Key.json"), None);
        assert_eq!(name("{{event.data.account}}.json"), None);

        let relative = credentials_name("credentials/a.json", std::path::Path::new("credentials"));
        assert_eq!(relative.as_deref(), Some("a"));
    }

    #[test]
    fn a_credentials_path_that_renders_empty_is_the_same_connection_as_none() {
        let without = Connection::new(flowgen_oci::CONNECTOR, None).target(&["a.io"]);
        let unset = Connection::new(
            flowgen_oci::CONNECTOR,
            Some(std::path::Path::new(
                "{{env.CONNECTIONS_TEST_UNSET_CREDENTIALS}}",
            )),
        )
        .target(&["a.io"]);
        let per_event = Connection::new(
            flowgen_oci::CONNECTOR,
            Some(std::path::Path::new("{{event.data.credentials}}")),
        );

        assert_eq!(Identity::of(&unset), Identity::of(&without));
        assert!(Identity::of(&per_event).credentials_path.is_some());
    }

    #[test]
    fn a_credentials_path_from_the_environment_shows_its_account() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("mssql.json"),
            r#"{"host": "db", "database": "sales", "username": "u", "password": "p"}"#,
        )
        .unwrap();
        std::env::set_var("CONNECTIONS_TEST_CREDENTIALS_DIR", dir.path());
        let yaml = "- mssql_query:\n    name: read\n    credentials_path: \"{{env.CONNECTIONS_TEST_CREDENTIALS_DIR}}/mssql.json\"\n    query: \"SELECT 1\"\n";

        let found = of_tasks(&tasks(yaml));

        assert_eq!(found[0].account.as_deref(), Some("db:1433/sales"));
        assert!(!found[0].connection.per_event);
    }

    struct Records(Vec<StoredLog>);

    #[async_trait::async_trait]
    impl LogsStore for Records {
        async fn query(
            &self,
            filter: LogFilter,
            limit: usize,
        ) -> Result<Vec<StoredLog>, flowgen_core::telemetry::query::LogsStoreError> {
            let matching: Vec<StoredLog> = self
                .0
                .iter()
                .filter(|record| filter.matches(record))
                .cloned()
                .collect();
            let start = matching.len().saturating_sub(limit);
            Ok(matching[start..].to_vec())
        }

        async fn tail(
            &self,
            _filter: LogFilter,
        ) -> Result<
            futures::stream::BoxStream<'static, StoredLog>,
            flowgen_core::telemetry::query::LogsStoreError,
        > {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    fn record(task: &str, level: &str, second: u32) -> StoredLog {
        StoredLog {
            body: String::new(),
            level: level.to_string(),
            timestamp: Some(format!("2026-01-01T00:00:{second:02}Z")),
            target: String::new(),
            spans: vec![
                StoredSpan {
                    name: "flow.run".to_string(),
                    fields: vec![("flow".to_string(), "f".to_string())],
                },
                StoredSpan {
                    name: "task.run".to_string(),
                    fields: vec![("task".to_string(), task.to_string())],
                },
            ],
            fields: Vec::new(),
        }
    }

    #[tokio::test]
    async fn the_worst_latest_level_of_its_tasks_wins() {
        let logs = Records(vec![
            record("read", "error", 1),
            record("read", "info", 2),
            record("write", "warn", 3),
        ]);

        let status = read_status(&logs, &[user("f", "read"), user("f", "write")]).await;

        assert_eq!(status, api::FlowStatus::Warn);
    }

    #[tokio::test]
    async fn a_connection_without_records_is_idle() {
        let status = read_status(&Records(Vec::new()), &[user("f", "read")]).await;

        assert_eq!(status, api::FlowStatus::Idle);
    }
}
