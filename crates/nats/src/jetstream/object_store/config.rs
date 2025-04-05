use serde::{Deserialize, Serialize};


/// A configuration options for the nats object store.
///
/// Example:
/// ```json
/// {
///     "flow": {
///         "tasks": [
///             {
///                 "source": {
///                     "object_store": {
///                         "credentials": "/etc/nats/nats_dev.json",
///                         "bucket": "nats_bucket",
///                         "batch_size": 50,
///                         "durable_name": "nats_object_store"
///                     }
///                 }
///             },
///             {
///                 "processor": {
///                     "enumerate": {
///                         "label": "sample_data.csv",
///                         "array": {
///                             "column": "Email",
///                             "is_static": false,
///                             "is_extension": false
///                         }
///                     }
///                 }
///             },
///             {
///                 "target": {
///                     "nats_jetstream": {
///                         "credentials": "/etc/nats/nats_dev.json",
///                         "stream": "external",
///                         "stream_description": "external streaming events",
///                         "subjects": [
///                             "enumerate.>"
///                         ]
///                     }
///                 }
///             }
///         ]
///     }
/// }


#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Source {
    pub credentials: String,
    pub bucket: String,
    pub batch_size: Option<usize>,
    pub has_header: Option<bool>,
}

