use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Source {
    pub credentials: String,
    pub topic_list: Vec<String>,
}

#[derive(Deserialize, Clone, Debug)]
pub struct Target {
    pub credentials: String,
    pub topic: String,
}
