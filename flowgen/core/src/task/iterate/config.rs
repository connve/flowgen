//! Configuration structures for loop task types.
//!
//! Defines configuration options for loop tasks that iterate over arrays
//! and emit individual events for each element.

use serde::{Deserialize, Serialize};

/// Configuration for loop processor tasks that iterate over JSON arrays.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Optional key to extract array from JSON object.
    /// If None, assumes the root element is an array.
    /// If Some("key"), extracts the array from data["key"].
    pub iterate_key: Option<String>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<crate::retry::RetryConfig>,
}
