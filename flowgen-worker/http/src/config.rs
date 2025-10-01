//! HTTP processor configuration structures and types.
//!
//! Provides configuration structs for HTTP request processors, including
//! method types, payload formats, and authentication settings.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::{collections::HashMap, path::PathBuf};

/// HTTP processor configuration.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Processor {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Target endpoint URL for HTTP requests.
    pub endpoint: String,
    /// HTTP method to use for requests.
    pub method: Method,
    /// Optional payload configuration.
    pub payload: Option<Payload>,
    /// Optional HTTP headers to include in requests.
    pub headers: Option<HashMap<String, String>>,
    /// Optional path to credentials file.
    pub credentials: Option<PathBuf>,
}

impl ConfigExt for Processor {}

/// HTTP request payload configuration.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Payload {
    /// JSON object to send as payload.
    pub object: Option<Map<String, Value>>,
    /// Raw JSON string input.
    pub input: Option<String>,
    /// Format for sending the payload.
    pub send_as: PayloadSendAs,
}

/// Payload encoding format options.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum PayloadSendAs {
    /// Send payload as JSON (default).
    #[default]
    Json,
    /// Send payload as URL-encoded form data.
    UrlEncoded,
    /// Send payload as query parameters.
    QueryParams,
}

/// HTTP method types supported by the processor.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub enum Method {
    /// HTTP GET method (default).
    #[default]
    GET,
    /// HTTP POST method.
    POST,
    /// HTTP PUT method.
    PUT,
    /// HTTP DELETE method.
    DELETE,
    /// HTTP PATCH method.
    PATCH,
    /// HTTP HEAD method.
    HEAD,
}

/// Authentication credentials for HTTP requests.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct Credentials {
    /// Bearer token for authorization header.
    pub bearer_auth: Option<String>,
    /// Basic authentication credentials.
    pub basic_auth: Option<BasicAuth>,
}

/// Basic authentication username and password.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct BasicAuth {
    /// Username for basic authentication.
    pub username: String,
    /// Password for basic authentication.
    pub password: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Map, Value};
    use std::collections::HashMap;

    #[test]
    fn test_processor_default() {
        let processor = Processor::default();
        assert_eq!(processor.name, String::new());
        assert_eq!(processor.endpoint, String::new());
        assert_eq!(processor.method, Method::GET);
        assert_eq!(processor.payload, None);
        assert_eq!(processor.headers, None);
        assert_eq!(processor.credentials, None);
    }

    #[test]
    fn test_processor_creation() {
        let mut headers = HashMap::new();
        headers.insert("Content-Type".to_string(), "application/json".to_string());
        headers.insert("Authorization".to_string(), "Bearer token123".to_string());

        let mut payload_object = Map::new();
        payload_object.insert("key1".to_string(), Value::String("value1".to_string()));
        payload_object.insert("key2".to_string(), Value::Number(42.into()));

        let payload = Payload {
            object: Some(payload_object.clone()),
            input: Some("{\"test\": \"input\"}".to_string()),
            send_as: PayloadSendAs::Json,
        };

        let processor = Processor {
            name: "test_processor".to_string(),
            endpoint: "https://api.example.com/webhook".to_string(),
            method: Method::POST,
            payload: Some(payload),
            headers: Some(headers.clone()),
            credentials: Some(PathBuf::from("/path/to/creds.json")),
        };

        assert_eq!(processor.name, "test_processor".to_string());
        assert_eq!(processor.endpoint, "https://api.example.com/webhook");
        assert_eq!(processor.method, Method::POST);
        assert!(processor.payload.is_some());
        assert_eq!(processor.headers, Some(headers));
        assert_eq!(
            processor.credentials,
            Some(PathBuf::from("/path/to/creds.json"))
        );
    }

    #[test]
    fn test_processor_serialization() {
        let processor = Processor {
            name: "serialize_test".to_string(),
            endpoint: "https://test.api.com".to_string(),
            method: Method::PUT,
            payload: None,
            headers: None,
            credentials: Some(PathBuf::from("/test/credentials.json")),
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_processor_clone() {
        let processor = Processor {
            name: "clone_test".to_string(),
            endpoint: "https://clone.test.com".to_string(),
            method: Method::DELETE,
            payload: None,
            headers: None,
            credentials: None,
        };

        let cloned = processor.clone();
        assert_eq!(processor, cloned);
    }

    #[test]
    fn test_payload_default() {
        let payload = Payload::default();
        assert_eq!(payload.object, None);
        assert_eq!(payload.input, None);
        assert_eq!(payload.send_as, PayloadSendAs::Json);
    }

    #[test]
    fn test_payload_creation() {
        let mut object = Map::new();
        object.insert("name".to_string(), Value::String("test".to_string()));
        object.insert("count".to_string(), Value::Number(10.into()));

        let payload = Payload {
            object: Some(object.clone()),
            input: Some("{\"input\": \"data\"}".to_string()),
            send_as: PayloadSendAs::UrlEncoded,
        };

        assert_eq!(payload.object, Some(object));
        assert_eq!(payload.input, Some("{\"input\": \"data\"}".to_string()));
        assert_eq!(payload.send_as, PayloadSendAs::UrlEncoded);
    }

    #[test]
    fn test_payload_serialization() {
        let mut object = Map::new();
        object.insert("test".to_string(), Value::Bool(true));

        let payload = Payload {
            object: Some(object),
            input: None,
            send_as: PayloadSendAs::QueryParams,
        };

        let json = serde_json::to_string(&payload).unwrap();
        let deserialized: Payload = serde_json::from_str(&json).unwrap();
        assert_eq!(payload, deserialized);
    }

    #[test]
    fn test_payload_send_as_variants() {
        assert_eq!(PayloadSendAs::default(), PayloadSendAs::Json);

        let json_variant = PayloadSendAs::Json;
        let url_encoded_variant = PayloadSendAs::UrlEncoded;
        let query_params_variant = PayloadSendAs::QueryParams;

        assert_ne!(json_variant, url_encoded_variant);
        assert_ne!(url_encoded_variant, query_params_variant);
        assert_ne!(json_variant, query_params_variant);
    }

    #[test]
    fn test_payload_send_as_serialization() {
        let variants = vec![
            PayloadSendAs::Json,
            PayloadSendAs::UrlEncoded,
            PayloadSendAs::QueryParams,
        ];

        for variant in variants {
            let json = serde_json::to_string(&variant).unwrap();
            let deserialized: PayloadSendAs = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, deserialized);
        }
    }

    #[test]
    fn test_method_default() {
        assert_eq!(Method::default(), Method::GET);
    }

    #[test]
    fn test_method_variants() {
        let methods = [
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
            Method::HEAD,
        ];

        // Test that all methods are unique
        for (i, method1) in methods.iter().enumerate() {
            for (j, method2) in methods.iter().enumerate() {
                if i != j {
                    assert_ne!(method1, method2);
                }
            }
        }
    }

    #[test]
    fn test_method_serialization() {
        let methods = vec![
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::PATCH,
            Method::HEAD,
        ];

        for method in methods {
            let json = serde_json::to_string(&method).unwrap();
            let deserialized: Method = serde_json::from_str(&json).unwrap();
            assert_eq!(method, deserialized);
        }
    }

    #[test]
    fn test_processor_with_complex_payload() {
        let mut object = Map::new();
        object.insert(
            "string_field".to_string(),
            Value::String("test_value".to_string()),
        );
        object.insert("number_field".to_string(), Value::Number(123.into()));
        object.insert("bool_field".to_string(), Value::Bool(true));
        object.insert("null_field".to_string(), Value::Null);

        let inner_object = json!({"nested": "value"});
        object.insert("object_field".to_string(), inner_object);

        let payload = Payload {
            object: Some(object),
            input: Some("{\"alternative\": \"input\"}".to_string()),
            send_as: PayloadSendAs::Json,
        };

        let mut headers = HashMap::new();
        headers.insert("X-Custom-Header".to_string(), "custom-value".to_string());

        let processor = Processor {
            name: "complex_test".to_string(),
            endpoint: "https://complex.example.com/api/v1/endpoint".to_string(),
            method: Method::PATCH,
            payload: Some(payload),
            headers: Some(headers),
            credentials: Some(PathBuf::from("/secure/path/to/creds.json")),
        };

        let json = serde_json::to_string(&processor).unwrap();
        let deserialized: Processor = serde_json::from_str(&json).unwrap();
        assert_eq!(processor, deserialized);
    }

    #[test]
    fn test_config_ext_trait() {
        let processor = Processor::default();
        let _: &dyn ConfigExt = &processor;
    }
}
