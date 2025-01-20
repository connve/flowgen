use flowgen_core::input::Input;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    pub endpoint: String,
    pub payload: Option<HashMap<String, String>>,
    pub headers: Option<HashMap<String, String>>,
    pub credentials: Option<String>,
    pub inputs: Option<HashMap<String, Input>>,
}
