//! Validation of workspace files (flows and resources) without running them.

use crate::config::{FlowConfig, FlowConfigRaw, TaskType};
use flowgen_core::task::script::config::RhaiLimits;

/// One problem found in a file.
#[derive(Debug, Clone, PartialEq)]
pub struct Issue {
    /// Where in the file the problem is, e.g. `flow.tasks.2.http_request.uri`.
    pub location: Option<String>,
    /// What is wrong, e.g. ``Task 'b' depends on unknown task 'a'``.
    pub message: String,
}

/// Why a workspace file is invalid.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Flow files need a .yaml, .yml or .json extension")]
    Extension,
    #[error("Failed to parse flow: {0}")]
    Parse(#[source] config::ConfigError),
    #[error(transparent)]
    Name(#[from] flowgen_core::validate::Error),
    #[error(transparent)]
    Dag(#[from] crate::flow::DagError),
    #[error(transparent)]
    Generate(#[from] flowgen_core::task::generate::config::ConfigError),
    #[error(transparent)]
    Kafka(#[from] flowgen_kafka::config::ConfigError),
    #[error("Script does not compile: {0}")]
    Script(#[source] Box<rhai::ParseError>),
    #[error("Invalid JSON: {0}")]
    Json(#[source] serde_json::Error),
}

impl From<Error> for Issue {
    fn from(error: Error) -> Self {
        Issue {
            location: None,
            message: error.to_string(),
        }
    }
}

/// Validates a flow file. `path` is its path within the flows directory, e.g.
/// `orders/sync.yaml`; an empty result means the flow is valid.
pub fn validate_flow(path: &str, content: &str) -> Vec<Issue> {
    match path.rsplit_once('.') {
        Some((_, extension)) if crate::config::FLOW_CONFIG_EXTENSIONS.contains(&extension) => {}
        _ => return vec![Error::Extension.into()],
    }
    let raw = match FlowConfigRaw::parse(path, content) {
        Ok(raw) => raw,
        Err(source) => return vec![parse_issue(source)],
    };

    let identity = flow_identity(path).to_string();
    let config = match FlowConfig::from_path(raw, identity, None) {
        Ok(config) => config,
        Err(source) => return vec![Error::from(source).into()],
    };

    let mut issues = Vec::new();
    if let Err(source) = crate::flow::resolve_parents(&config.flow.tasks) {
        issues.push(Error::from(source).into());
    }
    for (index, task) in config.flow.tasks.iter().enumerate() {
        if let Err(error) = validate_task(task) {
            issues.push(Issue {
                location: Some(format!("flow.tasks.{index}.{}", task.as_str())),
                message: error.to_string(),
            });
        }
    }
    issues
}

/// Issue for a flow that does not parse, located at the key the parser names.
fn parse_issue(source: config::ConfigError) -> Issue {
    let location = match &source {
        config::ConfigError::At { key: Some(key), .. }
        | config::ConfigError::Type { key: Some(key), .. }
        | config::ConfigError::NotFound(key) => Some(key.replace('[', ".").replace(']', "")),
        _ => None,
    };
    Issue {
        location,
        message: Error::Parse(source).to_string(),
    }
}

/// Validates a resource file by its extension: Rhai scripts must compile and
/// JSON must parse. Other files are accepted as they are.
pub fn validate_resource(path: &str, content: &str) -> Vec<Issue> {
    let result = match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("rhai") => compile_script(content, &RhaiLimits::default()),
        Some("json") => match serde_json::from_str::<serde_json::Value>(content) {
            Ok(_) => Ok(()),
            Err(source) => Err(Error::Json(source)),
        },
        _ => Ok(()),
    };
    match result {
        Ok(()) => Vec::new(),
        Err(error) => vec![error.into()],
    }
}

/// Flow identity for a path in the flows directory: the path without its extension.
pub(crate) fn flow_identity(path: &str) -> &str {
    match path.rsplit_once('.') {
        Some((stem, extension)) if crate::config::FLOW_CONFIG_EXTENSIONS.contains(&extension) => {
            stem
        }
        _ => path,
    }
}

/// Checks a task's own settings and compiles inline scripts.
fn validate_task(task: &TaskType) -> Result<(), Error> {
    match task {
        TaskType::generate(config) => Ok(config.validate()?),
        TaskType::kafka_produce(config) => Ok(config.validate()?),
        TaskType::kafka_subscribe(config) => Ok(config.validate()?),
        TaskType::script(config) => match &config.code {
            flowgen_core::resource::Source::Inline(code) => compile_script(code, &config.limits),
            flowgen_core::resource::Source::Resource { .. } => Ok(()),
        },
        _ => Ok(()),
    }
}

/// Compiles a script with the parse limits the script task applies.
fn compile_script(code: &str, limits: &RhaiLimits) -> Result<(), Error> {
    let mut engine = rhai::Engine::new();
    limits.apply(&mut engine);
    match engine.compile(code) {
        Ok(_) => Ok(()),
        Err(source) => Err(Error::Script(Box::new(source))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
flow:
  tasks:
    - generate:
        name: tick
        interval: 10s
    - log:
        name: print
"#;

    #[test]
    fn a_valid_flow_has_no_issues() {
        assert_eq!(validate_flow("a.yaml", VALID), Vec::new());
    }

    #[test]
    fn an_unknown_task_field_is_reported_with_its_task() {
        let content = VALID.replace("interval: 10s", "interval: 10s\n        intreval: 5s");
        let issues = validate_flow("a.yaml", &content);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(
            issues[0].message.contains("unknown field `intreval`")
                && issues[0].message.contains("flow.tasks[0]"),
            "{issues:?}"
        );
        assert_eq!(issues[0].location.as_deref(), Some("flow.tasks.0"));
    }

    #[test]
    fn an_unknown_flow_setting_is_reported() {
        let content = VALID.replace("  tasks:", "  require_leader_elction: true\n  tasks:");
        let issues = validate_flow("a.yaml", &content);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(
            issues[0]
                .message
                .contains("unknown field `require_leader_elction`"),
            "{issues:?}"
        );
    }

    #[test]
    fn an_inline_script_over_the_string_size_limit_is_reported() {
        let content = VALID.replace(
            "    - log:\n        name: print",
            "    - script:\n        name: transform\n        limits:\n          max_string_size: 4\n        code: 'let x = \"abcdefgh\";'",
        );
        let issues = validate_flow("a.yaml", &content);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].message.starts_with("Script does not compile"));
    }

    #[test]
    fn a_flow_file_without_a_flow_extension_is_reported() {
        assert_eq!(validate_flow("a.yml", VALID), Vec::new());
        for path in ["a", "a.pl", "a.yaml.bak"] {
            let issues = validate_flow(path, VALID);
            assert_eq!(issues.len(), 1, "{path}: {issues:?}");
            assert!(issues[0].message.starts_with("Flow files need"));
        }
    }

    #[test]
    fn broken_yaml_is_a_parse_issue() {
        let issues = validate_flow("a.yaml", "flow: [tasks");
        assert_eq!(issues.len(), 1);
        assert!(issues[0].message.starts_with("Failed to parse flow"));
    }

    #[test]
    fn an_unknown_dependency_is_reported() {
        let content = VALID.replace("name: print", "name: print\n        depends_on: [missing]");
        let issues = validate_flow("a.yaml", &content);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].message.contains("unknown task 'missing'"));
    }

    #[test]
    fn a_duplicate_task_name_is_reported() {
        let content = VALID.replace("name: print", "name: tick");
        let issues = validate_flow("a.yaml", &content);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].message.contains("Duplicate task name 'tick'"));
    }

    #[test]
    fn an_invalid_task_setting_is_reported_on_the_task() {
        let content = VALID.replace(
            "interval: 10s",
            "interval: 10s\n        cron: '* * * * * *'",
        );
        let issues = validate_flow("a.yaml", &content);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].location.as_deref(), Some("flow.tasks.0.generate"));
    }

    #[test]
    fn an_inline_script_that_does_not_compile_is_reported() {
        let content = VALID.replace(
            "    - log:\n        name: print",
            "    - script:\n        name: transform\n        code: \"let x = ;\"",
        );
        let issues = validate_flow("a.yaml", &content);
        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].message.starts_with("Script does not compile"));
    }

    #[test]
    fn resources_are_checked_by_extension() {
        assert_eq!(validate_resource("scripts/a.rhai", "event"), Vec::new());
        assert_eq!(validate_resource("scripts/a.rhai", "let x = ;").len(), 1);
        assert_eq!(
            validate_resource("schemas/a.json", "{\"a\": 1}"),
            Vec::new()
        );
        assert_eq!(validate_resource("schemas/a.json", "{").len(), 1);
        assert_eq!(validate_resource("queries/a.sql", "select"), Vec::new());
    }

    /// Every flow file under `examples/`, as `(path, content)`.
    fn example_flows() -> Vec<(String, String)> {
        let examples = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples");
        let mut flows = Vec::new();
        for entry in walkdir::WalkDir::new(examples) {
            let entry = entry.unwrap();
            let path = entry.path();
            let is_flow = matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yaml" | "yml")
            );
            if !is_flow || path.components().any(|c| c.as_os_str() == "resources") {
                continue;
            }
            let content = std::fs::read_to_string(path).unwrap();
            flows.push((path.display().to_string(), content));
        }
        flows
    }

    /// Flows for the task types no example uses.
    const WITHOUT_EXAMPLE: [&str; 2] = [
        r#"
flow:
  tasks:
    - html_scrape:
        name: rows
        row_selector: tr
        fields: {}
"#,
        r#"
flow:
  tasks:
    - salesforce_toolingapi:
        name: subscribe
        operation: create_managed_event_subscription
        credentials_path: /c.json
"#,
    ];

    /// Every task type name, read from serde's unknown-variant error.
    fn task_types() -> std::collections::BTreeSet<String> {
        let error = serde_json::from_str::<TaskType>(r#"{"": {}}"#)
            .unwrap_err()
            .to_string();
        let expected = match error.split_once("expected one of ") {
            Some((_, expected)) => expected,
            None => panic!("{error}"),
        };
        expected
            .split(", ")
            .map(|name| name.split('`').nth(1).unwrap().to_string())
            .collect()
    }

    fn parse_json(content: &str) -> Result<FlowConfigRaw, config::ConfigError> {
        config::Config::builder()
            .add_source(config::File::from_str(content, config::FileFormat::Json))
            .build()?
            .try_deserialize()
    }

    #[test]
    fn every_example_flow_is_valid() {
        let mut invalid = Vec::new();
        for (path, content) in example_flows() {
            let issues = validate_flow(&path, &content);
            if !issues.is_empty() {
                invalid.push((path, issues));
            }
        }
        assert!(invalid.is_empty(), "{invalid:#?}");
    }

    /// Adds `unknown_field` to each task of every example in turn; every task type must reject it.
    #[test]
    fn every_task_type_rejects_unknown_fields() {
        let mut checked = std::collections::BTreeSet::new();
        let mut accepted = Vec::new();
        let flows = example_flows().into_iter().chain(
            WITHOUT_EXAMPLE
                .iter()
                .map(|content| ("inline".to_string(), content.to_string())),
        );
        for (path, content) in flows {
            let flow: serde_json::Value = config::Config::builder()
                .add_source(config::File::from_str(&content, config::FileFormat::Yaml))
                .build()
                .unwrap()
                .try_deserialize()
                .unwrap();
            let task_count = match flow["flow"]["tasks"].as_array() {
                Some(tasks) => tasks.len(),
                None => 0,
            };
            for index in 0..task_count {
                let mut flow = flow.clone();
                let serde_json::Value::Object(task) = &mut flow["flow"]["tasks"][index] else {
                    continue;
                };
                let Some((task_type, serde_json::Value::Object(config))) = task.iter_mut().next()
                else {
                    continue;
                };
                let task_type = task_type.clone();
                config.insert("unknown_field".to_string(), 1.into());
                match parse_json(&flow.to_string()) {
                    Err(e) if e.to_string().contains("unknown_field") => {}
                    _ => accepted.push(format!("{path}: {task_type}")),
                }
                checked.insert(task_type);
            }
        }
        assert!(accepted.is_empty(), "{accepted:#?}");
        let unchecked: Vec<_> = task_types().difference(&checked).cloned().collect();
        assert!(
            unchecked.is_empty(),
            "No example or WITHOUT_EXAMPLE flow uses {unchecked:?}"
        );
    }
}
