use serde::{Deserialize, Serialize};

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Target {
    pub credentials: String,
    pub uri: String,
}
