use serde::Deserialize;

#[derive(Deserialize, Clone)]
pub struct Config {
    pub flow: Flow,
}

#[derive(Deserialize, Clone)]
pub struct Flow {
    pub source: Source,
    pub target: Target,
}

#[derive(Deserialize, Clone)]
pub struct Nats {
    pub credentials: String,
    pub host: String,
    pub stream_name: String,
    pub stream_description: Option<String>,
    pub subjects: Vec<String>,
    pub kv_bucket_name: String,
    pub kv_bucket_description: String,
}

#[derive(Deserialize, Clone)]
#[allow(non_camel_case_types)]
pub enum Source {
    salesforce_pubsub(flowgen_salesforce::pubsub::config::Source),
}

#[derive(Deserialize, Clone)]
#[allow(non_camel_case_types)]
pub enum Target {
    nats(Nats),
}
