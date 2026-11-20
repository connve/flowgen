//! The JSON Schema of a flow file, for the web UI's forms: generated from the
//! task configs, with the hints the forms read added from the conventions
//! the configs follow, so a task that follows them needs nothing more.

use crate::config::FlowConfigRaw;
use serde_json::{Map, Value};
use std::sync::LazyLock;

/// The schema of a flow file, built once.
static FLOW: LazyLock<Value> = LazyLock::new(|| {
    let mut schema = schemars::schema_for!(FlowConfigRaw).to_value();
    add_hints(&mut schema);
    schema
});

/// The JSON Schema of a flow file, with the forms' hints.
pub fn flow() -> &'static Value {
    &FLOW
}

/// The hint a field gets from its name.
fn field_hint(name: &str) -> Option<(&'static str, Value)> {
    match name {
        "credentials_path" => Some(("x-widget", Value::from("credentials"))),
        "depends_on" => Some(("x-hidden", Value::from(true))),
        "retry" | "max_in_flight" => Some(("x-advanced", Value::from(true))),
        name if name.ends_with("timeout") => Some(("x-advanced", Value::from(true))),
        _ => None,
    }
}

/// Adds the hints to every field of `schema`, and marks `Source`, inline
/// content or a resource, for its own widget.
fn add_hints(schema: &mut Value) {
    hint_fields(schema);
    if let Some(Value::Object(source)) = schema.pointer_mut("/$defs/Source") {
        source.insert("x-widget".to_string(), Value::from("source"));
    }
}

fn hint_fields(node: &mut Value) {
    match node {
        Value::Object(map) => {
            if let Some(Value::Object(properties)) = map.get_mut("properties") {
                hint_properties(properties);
            }
            for value in map.values_mut() {
                hint_fields(value);
            }
        }
        Value::Array(items) => {
            for item in items {
                hint_fields(item);
            }
        }
        _ => {}
    }
}

fn hint_properties(properties: &mut Map<String, Value>) {
    for (name, property) in properties.iter_mut() {
        if let (Some((key, value)), Value::Object(property)) = (field_hint(name), property) {
            property.insert(key.to_string(), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The schema of each task type, by its YAML tag.
    fn task_schemas() -> Map<String, Value> {
        let schema = flow();
        let variants = schema
            .pointer("/$defs/TaskType/oneOf")
            .and_then(Value::as_array)
            .expect("TaskType is one of its variants");
        variants
            .iter()
            .filter_map(|variant| variant.get("properties")?.as_object())
            .flat_map(|properties| properties.clone())
            .map(|(tag, reference)| (tag, resolve(schema, reference)))
            .collect()
    }

    /// `node`, followed through its `$ref`s to the schema they name.
    fn resolve(schema: &Value, mut node: Value) -> Value {
        while let Some(pointer) = node.get("$ref").and_then(Value::as_str) {
            match schema.pointer(pointer.trim_start_matches('#')) {
                Some(target) => node = target.clone(),
                None => break,
            }
        }
        node
    }

    #[test]
    fn every_task_type_has_a_schema() {
        let schemas = task_schemas();
        for (tag, _) in crate::config::TaskType::TASK_CONNECTORS {
            assert!(schemas.contains_key(*tag), "{tag}");
        }
    }

    #[test]
    fn every_example_flow_matches_the_schema() {
        let validator = jsonschema::validator_for(flow()).expect("a valid schema");
        let mut mismatches = Vec::new();
        for (path, content) in crate::validation::tests::example_flows() {
            let instance: Value = serde_yaml::from_str(&content).expect("YAML");
            let errors: Vec<String> = validator
                .iter_errors(&instance)
                .map(|error| format!("{}: {error}", error.instance_path()))
                .collect();
            if !errors.is_empty() {
                mismatches.push((path, errors));
            }
        }
        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[test]
    fn fields_carry_the_hints_of_their_conventions() {
        let http_request = task_schemas().remove("http_request").expect("http_request");
        let field = |name: &str| {
            http_request
                .pointer(&format!("/properties/{name}"))
                .cloned()
        };

        assert_eq!(
            field("credentials_path").and_then(|f| f.get("x-widget").cloned()),
            Some(Value::from("credentials"))
        );
        assert_eq!(
            field("depends_on").and_then(|f| f.get("x-hidden").cloned()),
            Some(Value::from(true))
        );
        assert_eq!(
            field("retry").and_then(|f| f.get("x-advanced").cloned()),
            Some(Value::from(true))
        );
        assert_eq!(
            flow().pointer("/$defs/Source/x-widget"),
            Some(&Value::from("source"))
        );
    }
}
