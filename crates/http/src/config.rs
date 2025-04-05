use flowgen_core::config::input::Input;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

/// A configuration option for the HTTP processor.
///
/// Example:
/// ```json
/// {
///     "http": {
///         "label": "example",
///         "method": "POST",
///         "endpoint": "https://example.com",
///         "payload": {
///             "input": "{{payload}}",
///             "send_as": "UrlEncoded"
///         },
///         "credentials": "/etc/example/credentials.json",
///         "inputs": {
///             "payload": {
///                 "column": "payload",
///                 "is_static": false,
///                 "is_extension": false,
///                 "index": 0
///             }
///         }
///     }
/// }
/// ```

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    pub label: Option<String>,
    pub endpoint: String,
    pub method: HttpMethod,
    pub payload: Option<Payload>,
    pub headers: Option<HashMap<String, String>>,
    pub credentials: Option<String>,
    pub inputs: Option<HashMap<String, Input>>,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Payload {
    pub object: Option<Map<String, Value>>,
    pub input: Option<String>,
    pub send_as: PayloadSendAs,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum PayloadSendAs {
    #[default]
    Json,
    UrlEncoded,
    QueryParams,
}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum HttpMethod {
    #[default]
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
    HEAD,
}
