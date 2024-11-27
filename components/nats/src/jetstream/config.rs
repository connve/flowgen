#[derive(serde::Deserialize, Clone, Debug)]
pub struct Target {
    pub credentials: String,
    pub stream_name: String,
    pub stream_description: Option<String>,
    pub subjects: Vec<String>,
    pub max_age: Option<u64>,
}
