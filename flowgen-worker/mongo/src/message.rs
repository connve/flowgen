use flowgen_core::event::{EventBuilder, EventData};
use mongodb::bson::{self, Document};
use mongodb::change_stream::event::ChangeStreamEvent;

/// Errors that can occur during message conversion between flowgen and Mongo formats.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Apache Arrow error during data serialization or deserialization.
    #[error("Apache Arrow serialization/deserialization failed: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    /// JSON serialization or deserialization error.
    #[error("JSON serialization/deserialization failed: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    /// Flowgen core event system error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Binary encoding or decoding error.
    #[error("Binary encoding/decoding failed: {source}")]
    Bincode {
        #[source]
        source: bincode::Error,
    },
    /// BSON error
    #[error("BSON serialization/deserialization failed: {source}")]
    Bson {
        #[source]
        source: bson::ser::Error,
    },
    /// Expected record batch data is missing or unavailable.
    #[error("Error getting record batch")]
    NoRecordBatch(),
    /// Missing full document in change stream event
    #[error("Full document is not available for this operation")]
    NoFullDocument(),
}

/// Trait for converting Mongo change stream messages to flowgen events.
pub trait MongoStreamEventsExt {
    type Error;
    /// Convert a Mongo change stream message to a flowgen event.
    fn to_event(
        &self,
        task_type: &'static str,
        task_id: usize,
    ) -> Result<flowgen_core::event::Event, Self::Error>;
}

impl MongoStreamEventsExt for ChangeStreamEvent<Document> {
    type Error = Error;

    fn to_event(
        &self,
        task_type: &'static str,
        task_id: usize,
    ) -> Result<flowgen_core::event::Event, Self::Error> {
        let mut event_builder = EventBuilder::new()
            .subject(format!("{:?}", self.operation_type))
            .task_id(task_id)
            .task_type(task_type);

        if let Some(id) = &self.document_key {
            event_builder = event_builder.id(id.to_string());
        }

        // Get the full document or return error if it doesn't exist
        let doc = self.full_document.as_ref().ok_or(Error::NoFullDocument())?;

        // Convert BSON Document to JSON 
        let event_data = if let Ok(json_value) = bson::to_bson(doc) {
            // Convert BSON to serde_json::Value
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
                    // Fallback: serialize the document as JSON
                    let json_value =
                        serde_json::to_value(doc).map_err(|e| Error::SerdeJson { source: e })?;
                    EventData::Json(json_value)
                }
            }
        } else {
            // If BSON conversion fails, try direct JSON serialization
            let json_value =
                serde_json::to_value(doc).map_err(|e| Error::SerdeJson { source: e })?;
            EventData::Json(json_value)
        };

        event_builder.data(event_data).build().map_err(Error::Event)
    }
}
