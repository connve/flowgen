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
    enumerate(flowgen_core::task::enumerate::config::Processor),
    file_reader(flowgen_file::config::Reader),
    file_writer(flowgen_file::config::Writer),
    generate(flowgen_core::task::generate::config::Subscriber),
    http(flowgen_http::config::Processor),
    nats_jetstream_subscriber(flowgen_nats::jetstream::config::Subscriber),
    nats_jetstream_publisher(flowgen_nats::jetstream::config::Publisher),
    salesforce_pubsub_subscriber(flowgen_salesforce::pubsub::config::Subscriber),
    salesforce_pubsub_publisher(flowgen_salesforce::pubsub::config::Publisher),
    object_store_subscriber(flowgen_nats::jetstream::object_store::config::Source),
}
