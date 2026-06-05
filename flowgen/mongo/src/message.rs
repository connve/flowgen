use flowgen_core::event::{EventBuilder, EventData};
use mongodb::bson::{self, Document};

/// Errors that can occur during message conversion between flowgen and Mongo formats.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// JSON serialization or deserialization error.
    #[error("JSON serialization/deserialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    /// Flowgen core event system error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Expected record batch data is missing or unavailable.
    #[error("Error getting record batch")]
    NoRecordBatch(),
    /// Missing full document.
    #[error("Full document is not available for this operation")]
    NoFullDocument(),
}

/// Trait for converting Mongo change stream messages to flowgen events.
pub trait MongoEventsExt {
    type Error;
    /// Convert a Mongo change stream message to a flowgen event.
    fn to_event(
        &self,
        task_type: &'static str,
        task_id: usize,
    ) -> Result<flowgen_core::event::Event, Self::Error>;
}

impl MongoEventsExt for Document {
    type Error = Error;

    fn to_event(
        &self,
        task_type: &'static str,
        task_id: usize,
    ) -> Result<flowgen_core::event::Event, Self::Error> {
        let mut event_builder = EventBuilder::new()
            .subject("document".to_string()) // or derive from collection name
            .task_id(task_id)
            .task_type(task_type);

        // Extract id from mongo document.
        if let Some(id) = self.get("_id") {
            event_builder = event_builder.id(id.to_string());
        }

        // Convert BSON Document to JSON.
        let event_data = if let Ok(json_value) = bson::to_bson(self) {
            // Convert BSON to serde_json::Value.
            match json_value {
                bson::Bson::Document(d) => {
                    // Convert to JSON
                    let json_str =
                        serde_json::to_string(&d).map_err(|e| Error::SerdeJson { source: e })?;
                    let json_value: serde_json::Value = serde_json::from_str(&json_str)
                        .map_err(|e| Error::SerdeJson { source: e })?;
                    EventData::Json(json_value)
                }
                _ => {
                    // Fallback: serialize the document as JSON.
                    let json_value =
                        serde_json::to_value(self).map_err(|e| Error::SerdeJson { source: e })?;
                    EventData::Json(json_value)
                }
            }
        } else {
            // If BSON conversion fails, try direct JSON serialization.
            let json_value =
                serde_json::to_value(self).map_err(|e| Error::SerdeJson { source: e })?;
            EventData::Json(json_value)
        };

        event_builder.data(event_data).build().map_err(Error::Event)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;
    use serde_json::json;

    #[test]
    fn test_error_display_serde_json() {
        let serde_err = serde_json::from_str::<serde_json::Value>("invalid").unwrap_err();
        let err = Error::SerdeJson { source: serde_err };
        assert!(err
            .to_string()
            .contains("JSON serialization/deserialization failed"));
    }

    #[test]
    fn test_error_display_no_record_batch() {
        let err = Error::NoRecordBatch();
        assert_eq!(err.to_string(), "Error getting record batch");
    }

    #[test]
    fn test_error_display_no_full_document() {
        let err = Error::NoFullDocument();
        assert_eq!(
            err.to_string(),
            "Full document is not available for this operation"
        );
    }

    #[test]
    fn test_error_display_event() {
        let inner_err: flowgen_core::event::Error =
            flowgen_core::event::Error::MissingBuilderAttribute("test".to_string());
        let err: Error = inner_err.into();
        assert_eq!(err.to_string(), "Missing required builder attribute: test");
    }

    #[test]
    fn test_document_to_event_basic() {
        let id = ObjectId::new();
        let mut d = Document::new();
        d.insert("_id", id);
        d.insert("name", "alice");

        let evt = d.to_event("task_type", 5).expect("to_event");
        assert_eq!(evt.task_id, 5);
        assert_eq!(evt.task_type, "task_type");
        assert!(evt.id.is_some());

        // data should be JSON and contain name
        match evt.data {
            EventData::Json(v) => {
                assert_eq!(v["name"], json!("alice"));
            }
            _ => panic!("expected json data"),
        }
    }

    #[test]
    fn test_document_to_event_no_id() {
        let mut d = Document::new();
        d.insert("name", "bob");

        let evt = d.to_event("task", 2).expect("to_event");
        assert_eq!(evt.task_id, 2);
        assert!(evt.id.is_none());
        match evt.data {
            EventData::Json(v) => {
                assert_eq!(v["name"], serde_json::json!("bob"));
            }
            _ => panic!("expected json data"),
        }
    }

    #[test]
    fn test_document_to_event_numeric_id() {
        let mut d = Document::new();
        d.insert("_id", 123_i64);
        d.insert("name", "charlie");

        let evt = d.to_event("task", 3).expect("to_event");
        assert_eq!(evt.task_id, 3);
        assert!(evt.id.is_some());
    }
}
