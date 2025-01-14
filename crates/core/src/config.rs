use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Inputs {
    pub value: String,
    pub is_static: bool,
    pub is_extension: bool,
}
