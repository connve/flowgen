//! Configuration templating and rendering utilities.
//!
//! Provides template rendering capabilities for configuration files using Handlebars,
//! allowing dynamic configuration generation with variable substitution.

use handlebars::Handlebars;
use serde::{de::DeserializeOwned, Serialize};

/// Errors that can occur during configuration rendering operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Template rendering failed due to invalid syntax or missing variables.
    #[error("Template rendering error: {source}")]
    Render {
        #[source]
        source: handlebars::RenderError,
    },
    /// JSON serialization or deserialization error during template processing.
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
}

/// Extract a simple path from a template string like "{{event.data}}" or "{{event.data.batch}}".
/// Returns None if the template is complex (contains helpers, multiple expressions, etc.).
fn extract_simple_path(template: &str) -> Option<String> {
    let trimmed = template.trim();
    // Check if it's a simple {{path}} template.
    if trimmed.starts_with("{{") && trimmed.ends_with("}}") && trimmed.matches("{{").count() == 1 {
        let inner = &trimmed[2..trimmed.len() - 2].trim();
        // Make sure it doesn't contain any helper syntax or complex expressions.
        if !inner.contains(' ') && !inner.contains('(') && !inner.contains(')') {
            return Some(inner.to_string());
        }
    }
    None
}

/// Get a value from a JSON object by path like "event.data" or "event.data.batch".
fn get_value_by_path<'a>(data: &'a serde_json::Value, path: &str) -> Option<&'a serde_json::Value> {
    let parts: Vec<&str> = path.split('.').collect();
    let mut current = data;

    for part in parts {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(part)?;
            }
            _ => return None,
        }
    }

    Some(current)
}

/// Recursively renders all string values in a JSON value tree that contain Handlebars templates.
///
/// This function traverses the entire JSON structure to ensure nested templates are properly resolved.
/// For simple path templates like "{{event.data}}", it preserves the original type by directly accessing
/// the value instead of rendering it as a string. This prevents unwanted string conversion of objects,
/// arrays, booleans, and numbers.
fn render_json_value(
    value: &mut serde_json::Value,
    handlebars: &Handlebars,
    data: &serde_json::Value,
) -> Result<(), handlebars::RenderError> {
    match value {
        serde_json::Value::String(s)
            // Only render if the string contains template syntax.
            if s.contains("{{") => {
                // Check if this is a template that references a path in the data.
                // If so, try to directly access and use that value instead of rendering as string.
                // This preserves type information for objects, arrays, and primitives.
                if let Some(path) = extract_simple_path(s) {
                    if let Some(direct_value) = get_value_by_path(data, &path) {
                        *value = direct_value.clone();
                        return Ok(());
                    }
                }

                let rendered = handlebars.render_template(s, data)?;

                // Try to parse the rendered string as JSON to preserve type information.
                // This allows boolean/number values from templates to be correctly typed.
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&rendered) {
                    // Only use the parsed value if it's a non-string type (bool, number, etc.).
                    // For string types, keep as rendered string to avoid double-quoting.
                    if !matches!(parsed, serde_json::Value::String(_)) {
                        *value = parsed;
                        return Ok(());
                    }
                }

                // Fallback to string if parsing fails or parsed value is a string.
                *s = rendered;
            }
        serde_json::Value::Object(map) => {
            for v in map.values_mut() {
                render_json_value(v, handlebars, data)?;
            }
        }
        serde_json::Value::Array(arr) => {
            for item in arr.iter_mut() {
                render_json_value(item, handlebars, data)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Renders a string template with provided data context, injecting process
/// environment variables under the `env` key.
///
/// Use this for **operator-controlled** templates such as flow YAML
/// configuration values, where surfacing environment variables via
/// `{{env.VAR_NAME}}` is the documented mechanism.
///
/// Do NOT use this for templates that originate from flow-author content
/// (e.g. Rhai scripts loaded from the cache or git): such callers must
/// use [`render_template_no_env`] to avoid leaking pod environment
/// variables to untrusted code.
pub fn render_template<T>(template: &str, data: &T) -> Result<String, Error>
where
    T: Serialize,
{
    let mut data_value = serde_json::to_value(data).map_err(|e| Error::SerdeJson { source: e })?;

    if let serde_json::Value::Object(ref mut map) = data_value {
        let env_vars: std::collections::HashMap<String, String> = std::env::vars().collect();
        let env_value =
            serde_json::to_value(&env_vars).map_err(|e| Error::SerdeJson { source: e })?;
        map.insert("env".to_string(), env_value);
    }

    render_with_value(template, &data_value)
}

/// Renders a string template with provided data context **without** injecting
/// process environment variables.
///
/// Use this when the template comes from untrusted or semi-trusted content
/// (e.g. flow-author Rhai scripts), where exposing the pod's full
/// environment to the script would let the author exfiltrate secrets.
pub fn render_template_no_env<T>(template: &str, data: &T) -> Result<String, Error>
where
    T: Serialize,
{
    let data_value = serde_json::to_value(data).map_err(|e| Error::SerdeJson { source: e })?;
    render_with_value(template, &data_value)
}

fn render_with_value(template: &str, data_value: &serde_json::Value) -> Result<String, Error> {
    let mut handlebars = Handlebars::new();
    handlebars.register_escape_fn(handlebars::no_escape);
    handlebars
        .render_template(template, data_value)
        .map_err(|e| Error::Render { source: e })
}

/// Extension trait for configuration types that support template rendering.
///
/// Enables configuration structures to render themselves as Handlebars templates
/// with dynamic variable substitution from provided data context.
pub trait ConfigExt {
    /// Renders the configuration as a Handlebars template with provided data.
    ///
    /// # Arguments
    /// * `data` - Template variables for substitution
    ///
    /// # Returns
    /// A new instance of the configuration with template variables resolved
    fn render<T>(&self, data: &T) -> Result<Self, Error>
    where
        Self: Serialize + DeserializeOwned + Sized,
        T: Serialize,
    {
        let mut config_value =
            serde_json::to_value(self).map_err(|e| Error::SerdeJson { source: e })?;
        let mut data_value =
            serde_json::to_value(data).map_err(|e| Error::SerdeJson { source: e })?;

        // Add environment variables to the data context under "env" key.
        // This allows templates to use {{env.VAR_NAME}} syntax.
        if let serde_json::Value::Object(ref mut map) = data_value {
            let env_vars: std::collections::HashMap<String, String> = std::env::vars().collect();
            let env_value =
                serde_json::to_value(&env_vars).map_err(|e| Error::SerdeJson { source: e })?;
            map.insert("env".to_string(), env_value);
        }

        let mut handlebars = Handlebars::new();
        // Disable HTML escaping since we're rendering JSON, not HTML.
        handlebars.register_escape_fn(handlebars::no_escape);

        render_json_value(&mut config_value, &handlebars, &data_value)
            .map_err(|e| Error::Render { source: e })?;

        serde_json::from_value(config_value).map_err(|e| Error::SerdeJson { source: e })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use serde_json::json;

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestConfig {
        name: String,
        value: i32,
        url: String,
    }

    impl ConfigExt for TestConfig {}

    #[test]
    fn test_config_render_with_variables() {
        let config = TestConfig {
            name: "{{user_name}}".to_string(),
            value: 42,
            url: "https://{{domain}}/api".to_string(),
        };

        let data = json!({
            "user_name": "john",
            "domain": "example.com"
        });

        let rendered = config.render(&data).unwrap();

        assert_eq!(rendered.name, "john");
        assert_eq!(rendered.value, 42);
        assert_eq!(rendered.url, "https://example.com/api");
    }

    #[test]
    fn test_config_render_no_variables() {
        let config = TestConfig {
            name: "static_name".to_string(),
            value: 100,
            url: "https://static.com".to_string(),
        };

        let data = json!({});
        let rendered = config.render(&data).unwrap();

        assert_eq!(rendered, config);
    }

    #[test]
    fn test_config_render_missing_variable() {
        let config = TestConfig {
            name: "{{missing_var}}".to_string(),
            value: 0,
            url: "test".to_string(),
        };

        let data = json!({});
        let result = config.render(&data).unwrap();

        assert_eq!(result.name, "");
        assert_eq!(result.url, "test");
    }

    #[test]
    fn test_config_render_nested_data() {
        let config = TestConfig {
            name: "{{user.first_name}}".to_string(),
            value: 0,
            url: "{{config.base_url}}/{{user.id}}".to_string(),
        };

        let data = json!({
            "user": {
                "first_name": "Jane",
                "id": "123"
            },
            "config": {
                "base_url": "https://api.example.com"
            }
        });

        let rendered = config.render(&data).unwrap();

        assert_eq!(rendered.name, "Jane");
        assert_eq!(rendered.url, "https://api.example.com/123");
    }

    #[test]
    fn test_render_template_simple() {
        let template = "Hello {{name}}!";
        let data = json!({"name": "World"});
        let rendered = render_template(template, &data).unwrap();
        assert_eq!(rendered, "Hello World!");
    }

    #[test]
    fn test_render_template_soql() {
        let template = "SELECT Id, Name FROM Account WHERE LastModifiedDate > {{date}}";
        let data = json!({"date": "2024-01-01T00:00:00Z"});
        let rendered = render_template(template, &data).unwrap();
        assert_eq!(
            rendered,
            "SELECT Id, Name FROM Account WHERE LastModifiedDate > 2024-01-01T00:00:00Z"
        );
    }

    #[test]
    fn test_render_template_nested() {
        let template = "{{event.data.field}}";
        let data = json!({"event": {"data": {"field": "value"}}});
        let rendered = render_template(template, &data).unwrap();
        assert_eq!(rendered, "value");
    }

    #[test]
    fn test_config_render_with_env_vars() {
        // Set test environment variable
        std::env::set_var("TEST_PROJECT_ID", "my-test-project");
        std::env::set_var("TEST_DATASET", "analytics");

        let config = TestConfig {
            name: "{{env.TEST_PROJECT_ID}}".to_string(),
            value: 42,
            url: "https://example.com/{{env.TEST_DATASET}}".to_string(),
        };

        let data = json!({});
        let rendered = config.render(&data).unwrap();

        assert_eq!(rendered.name, "my-test-project");
        assert_eq!(rendered.url, "https://example.com/analytics");
        assert_eq!(rendered.value, 42);

        // Clean up
        std::env::remove_var("TEST_PROJECT_ID");
        std::env::remove_var("TEST_DATASET");
    }

    #[test]
    fn test_config_render_with_env_and_event_vars() {
        std::env::set_var("TEST_BASE_URL", "https://api.example.com");

        let config = TestConfig {
            name: "{{user.name}}".to_string(),
            value: 100,
            url: "{{env.TEST_BASE_URL}}/{{user.id}}".to_string(),
        };

        let data = json!({
            "user": {
                "name": "Alice",
                "id": "456"
            }
        });

        let rendered = config.render(&data).unwrap();

        assert_eq!(rendered.name, "Alice");
        assert_eq!(rendered.url, "https://api.example.com/456");

        std::env::remove_var("TEST_BASE_URL");
    }

    #[test]
    fn test_render_template_with_curly_braces() {
        let data = json!({"event": {"data": {"search_term": "Acme"}}});

        let r =
            render_template("FIND { {{event.data.search_term}} } IN ALL FIELDS", &data).unwrap();
        assert_eq!(r, "FIND { Acme } IN ALL FIELDS");
    }

    // -- Template rendering as from_event replacement --------------------------
    //
    // These tests prove that a simple path template like "{{event.data}}" can
    // substitute an entire JSON object into a config field, preserving structure.
    // This allows templates to replace `from_event: true` on payload fields.

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct ObjectFieldConfig {
        name: String,
        payload: Option<serde_json::Map<String, serde_json::Value>>,
    }

    impl ConfigExt for ObjectFieldConfig {}

    #[test]
    fn test_render_object_template_into_map_field() {
        let config = ObjectFieldConfig {
            name: "test".to_string(),
            payload: Some(
                serde_json::from_value(json!({
                    "placeholder": "{{event.data}}"
                }))
                .unwrap(),
            ),
        };

        let data = json!({
            "event": {
                "data": {
                    "Name": "Acme Corp",
                    "Industry": "Technology",
                    "Revenue": 1000000
                }
            }
        });

        let rendered = config.render(&data).unwrap();
        assert_eq!(rendered.name, "test");

        let payload = rendered.payload.unwrap();
        let placeholder = payload.get("placeholder").unwrap();
        assert_eq!(placeholder.get("Name").unwrap(), "Acme Corp");
        assert_eq!(placeholder.get("Industry").unwrap(), "Technology");
        assert_eq!(placeholder.get("Revenue").unwrap(), 1000000);
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    #[serde(untagged)]
    enum UntaggedPayload {
        Template(String),
        Fields(serde_json::Map<String, serde_json::Value>),
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct UntaggedPayloadConfig {
        name: String,
        payload: UntaggedPayload,
    }

    impl ConfigExt for UntaggedPayloadConfig {}

    #[test]
    fn test_render_object_template_into_untagged_enum() {
        let config = UntaggedPayloadConfig {
            name: "test".to_string(),
            payload: UntaggedPayload::Template("{{event.data}}".to_string()),
        };

        let data = json!({
            "event": {
                "data": {
                    "FirstName": "Jane",
                    "LastName": "Doe",
                    "Active": true
                }
            }
        });

        let rendered = config.render(&data).unwrap();
        assert_eq!(rendered.name, "test");

        match rendered.payload {
            UntaggedPayload::Fields(map) => {
                assert_eq!(map.get("FirstName").unwrap(), "Jane");
                assert_eq!(map.get("LastName").unwrap(), "Doe");
                assert_eq!(map.get("Active").unwrap(), true);
            }
            UntaggedPayload::Template(_) => {
                panic!("Expected Fields variant after rendering, got Template");
            }
        }
    }

    #[test]
    fn test_render_array_template_preserves_array() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct ArrayConfig {
            name: String,
            records: serde_json::Value,
        }
        impl ConfigExt for ArrayConfig {}

        let config = ArrayConfig {
            name: "test".to_string(),
            records: json!("{{event.data.batch}}"),
        };

        let data = json!({
            "event": {
                "data": {
                    "batch": [
                        {"id": 1, "name": "first"},
                        {"id": 2, "name": "second"}
                    ]
                }
            }
        });

        let rendered = config.render(&data).unwrap();
        let records = rendered.records.as_array().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get("id").unwrap(), 1);
        assert_eq!(records[1].get("name").unwrap(), "second");
    }
}
