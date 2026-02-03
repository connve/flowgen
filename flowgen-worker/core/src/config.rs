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
    #[error("Template rendering failed: {source}")]
    Render {
        #[source]
        source: handlebars::RenderError,
    },
    /// JSON serialization or deserialization error during template processing.
    #[error("JSON serialization/deserialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
}

/// Extract a simple path from a template string like "{{event.data}}" or "{{event.data.batch}}".
/// Returns None if the template is complex (contains helpers, multiple expressions, etc.).
fn extract_simple_path(template: &str) -> Option<String> {
    let trimmed = template.trim();
    // Check if it's a simple {{path}} template
    if trimmed.starts_with("{{") && trimmed.ends_with("}}") && trimmed.matches("{{").count() == 1 {
        let inner = &trimmed[2..trimmed.len() - 2].trim();
        // Make sure it doesn't contain any helper syntax or complex expressions
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
fn render_json_value(
    value: &mut serde_json::Value,
    handlebars: &Handlebars,
    data: &serde_json::Value,
) -> Result<(), handlebars::RenderError> {
    match value {
        serde_json::Value::String(s) => {
            // Only render if the string contains template syntax
            if s.contains("{{") {
                // Check if this is a template that references a path in the data
                // If so, try to directly access and use that value instead of rendering as string.
                if let Some(path) = extract_simple_path(s) {
                    if let Some(direct_value) = get_value_by_path(data, &path) {
                        *value = direct_value.clone();
                        return Ok(());
                    }
                }

                let rendered = handlebars.render_template(s, data)?;

                // Try to parse the rendered string as JSON to preserve type information
                // This allows boolean/number values from templates to be correctly typed.
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(&rendered) {
                    // Only use the parsed value if it's a non-string type (bool, number, etc.)
                    // For string types, keep as rendered string to avoid double-quoting.
                    if !matches!(parsed, serde_json::Value::String(_)) {
                        *value = parsed;
                        return Ok(());
                    }
                }

                // Fallback to string if parsing fails or parsed value is a string.
                *s = rendered;
            }
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

/// Renders a string template with provided data context.
///
/// # Arguments
/// * `template` - The template string containing Handlebars syntax
/// * `data` - Template variables for substitution
///
/// # Returns
/// The rendered string with all template variables resolved
pub fn render_template<T>(template: &str, data: &T) -> Result<String, Error>
where
    T: Serialize,
{
    let data_value = serde_json::to_value(data).map_err(|e| Error::SerdeJson { source: e })?;
    let mut handlebars = Handlebars::new();
    // Disable HTML escaping since we're rendering data, not HTML
    handlebars.register_escape_fn(handlebars::no_escape);
    handlebars
        .render_template(template, &data_value)
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
        let data_value = serde_json::to_value(data).map_err(|e| Error::SerdeJson { source: e })?;

        let mut handlebars = Handlebars::new();
        // Disable HTML escaping since we're rendering JSON, not HTML
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
}
