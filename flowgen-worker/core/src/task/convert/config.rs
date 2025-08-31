//! Configuration structures for convert task types.
//!
//! Defines configuration options for data transformation tasks that convert
//! events between different formats within workflows.

use serde::{Deserialize, Serialize};

/// Configuration for convert processor tasks that transform event data formats.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// Optional label for event subject generation.
    pub label: Option<String>,
    /// Target format for event data conversion.
    pub target_format: TargetFormat,
    /// Optional schema definition for target format validation.
    pub schema: Option<String>,
}

/// Supported target formats for event data conversion.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum TargetFormat {
    /// Convert to Apache Avro binary format.
    #[default]
    Avro,
}
