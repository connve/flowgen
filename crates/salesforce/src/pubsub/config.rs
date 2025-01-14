use flowgen_core::config::Inputs;
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize, Clone, Debug)]
pub struct Source {
    pub credentials: String,
    pub topic_list: Vec<String>,
    pub next_node: Option<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Target {
    pub credentials: String,
    pub topic: String,
    pub payload: HashMap<String, String>,
    pub inputs: Option<HashMap<String, Inputs>>,
}
