use serde::{Deserialize, Serialize};
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub flow: Flow,
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Flow {
    pub tasks: Vec<Task>,
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum Task {
    source(Source),
    processor(Processor),
    target(Target),
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum Source {
    file(flowgen_file::config::Source),
    salesforce_pubsub(flowgen_salesforce::pubsub::config::Source),
    nats_jetstream(flowgen_nats::jetstream::config::Source),
    generate(flowgen_core::task::generate::config::Source),
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum Processor {
    http(flowgen_http::config::Processor),
    enumerate(flowgen_core::task::enumerate::config::Processor),
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum Target {
    file(flowgen_file::config::Target),
    nats_jetstream(flowgen_nats::jetstream::config::Target),
    deltalake(flowgen_deltalake::config::Target),
    salesforce_pubsub(flowgen_salesforce::pubsub::config::Target),
}
