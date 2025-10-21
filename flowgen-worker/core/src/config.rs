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
                *s = handlebars.render_template(s, data)?;
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
}
