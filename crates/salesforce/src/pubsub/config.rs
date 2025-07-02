use flowgen_core::config::input::Input;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Subscriber {
    pub label: Option<String>,
    pub credentials: String,
    pub topic: Topic,
    pub endpoint: Option<String>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Topic {
    pub name: String,
    pub durable_consumer_options: Option<DurableConsumerOptions>,
    pub num_requested: Option<i32>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Publisher {
    pub label: Option<String>,
    pub credentials: String,
    pub topic: String,
    pub payload: Map<String, Value>,
    pub inputs: Option<HashMap<String, Input>>,
    pub endpoint: Option<String>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct DurableConsumerOptions {
    pub enabled: bool,
    pub managed_subscription: bool,
    pub name: String,
}
