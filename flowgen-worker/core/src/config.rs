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
        let template = serde_json::to_string(self).map_err(|e| Error::SerdeJson { source: e })?;
        let data = serde_json::to_value(data).map_err(|e| Error::SerdeJson { source: e })?;

        let handlebars = Handlebars::new();
        let rendered = handlebars
            .render_template(&template, &data)
            .map_err(|e| Error::Render { source: e })?;

        let result: Self =
            serde_json::from_str(&rendered).map_err(|e| Error::SerdeJson { source: e })?;
        Ok(result)
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
