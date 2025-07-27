use serde::{Deserialize, Serialize};

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Subscriber {
    pub label: Option<String>,
    pub message: Option<String>,
    pub interval: u64,
    pub count: Option<u64>,
}
