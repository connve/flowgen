//! Configuration structures for mongo read operations.
use serde::{Deserialize, Serialize};

/// Object Store reader configuration.
#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct Reader {
    /// The unique name / identifier of the task.
    pub name: String,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

#[derive(PartialEq, Default, Clone, Debug, Deserialize, Serialize)]
pub struct ChangeStream {
    /// The unique name / identifier of the task.
    pub name: String,
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}
