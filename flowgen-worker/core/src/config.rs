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
    #[error(transparent)]
    Render(#[from] handlebars::RenderError),
    /// JSON serialization or deserialization error during template processing.
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
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
        let template = serde_json::to_string(self)?;
        let data = serde_json::to_value(data)?;

        let handlebars = Handlebars::new();
        let rendered = handlebars.render_template(&template, &data)?;

        let result: Self = serde_json::from_str(&rendered)?;
        Ok(result)
    }
}
