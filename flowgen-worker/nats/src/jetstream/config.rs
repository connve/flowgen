use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Subscriber {
    pub credentials: PathBuf,
    pub stream: String,
    pub subject: String,
    pub durable_name: String,
    pub batch_size: usize,
    pub delay_secs: Option<u64>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Publisher {
    pub credentials: PathBuf,
    pub stream: String,
    pub stream_description: Option<String>,
    pub subjects: Vec<String>,
    pub max_age: Option<u64>,
}
