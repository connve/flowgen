//! Configuration structures for convert task types.
//!
//! Defines configuration options for data transformation tasks that convert
//! events between different formats within workflows.

use serde::{Deserialize, Serialize};

/// Configuration for convert processor tasks that transform event data formats.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Target format for event data conversion.
    pub target_format: TargetFormat,
    /// Optional schema definition for target format validation.
    /// Can be specified as inline schema or loaded from external resource file.
    pub schema: Option<crate::resource::Source>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
}

/// Supported target formats for event data conversion.
#[derive(PartialEq, Eq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetFormat {
    /// Convert to Apache Avro binary format.
    #[default]
    Avro,
    /// Convert to JSON format.
    Json,
}
