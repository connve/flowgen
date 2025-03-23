use crate::input::Input;
use serde::{Deserialize, Serialize};

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    pub label: Option<String>,
    pub array: Input,
}
