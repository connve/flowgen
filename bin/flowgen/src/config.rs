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
#[allow(non_camel_case_types)]
pub enum Source {
    file(flowgen_file::config::Source),
    salesforce_pubsub(flowgen_salesforce::pubsub::config::Source),
    gcp_storage(flowgen_google::storage::config::Source),
}

#[derive(Deserialize, Clone)]
#[allow(non_camel_case_types)]
pub enum Target {
    nats_jetstream(flowgen_nats::jetstream::config::Target),
}
