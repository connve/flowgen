use flowgen_core::input::Input;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Source {
    pub credentials: String,
    pub topic_list: Vec<String>,
    pub next_node: Option<String>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Target {
    pub credentials: String,
    pub topic: String,
    pub payload: Map<String, Value>,
    pub inputs: Option<HashMap<String, Input>>,
}
