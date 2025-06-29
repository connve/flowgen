use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowConfig {
    pub flow: Flow,
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Flow {
    pub tasks: Vec<Task>,
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[allow(non_camel_case_types)]
pub enum Task {
    deltalake_writer(flowgen_deltalake::config::Writer),
    enumerate(flowgen_core::task::enumerate::config::Processor),
    file_reader(flowgen_file::config::Reader),
    file_writer(flowgen_file::config::Writer),
    generate(flowgen_core::task::generate::config::Subscriber),
    http(flowgen_http::config::Processor),
    nats_jetstream_subscriber(flowgen_nats::jetstream::config::Subscriber),
    nats_jetstream_publisher(flowgen_nats::jetstream::config::Publisher),
    object_store_subscriber(flowgen_nats::jetstream::object_store::config::Source),
    render(flowgen_core::task::render::config::Processor),
    salesforce_pubsub_subscriber(flowgen_salesforce::pubsub::config::Subscriber),
    salesforce_pubsub_publisher(flowgen_salesforce::pubsub::config::Publisher),
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct AppConfig {
    pub cache: Option<CacheOptions>,
    pub flows: FlowOptions,
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CacheOptions {
    pub enabled: bool,
    pub credentials_path: PathBuf,
}

#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowOptions {
    pub dir: Option<PathBuf>,
}
