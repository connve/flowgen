use serde::Deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct Source {
    pub credentials: String,
    pub object: String,
    pub bucket: String,
}
