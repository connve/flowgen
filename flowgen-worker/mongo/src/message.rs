use flowgen_core::event::{EventBuilder, EventData};
use mongodb::bson::{self, Document};
use mongodb::change_stream::event::ChangeStreamEvent;

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
    /// Missing full document in change stream event
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

impl MongoEventsExt for ChangeStreamEvent<Document> {
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

        // Extract id from mongo document.
        if let Some(bsonid) = self.document_key.as_ref().and_then(|doc| doc.get("_id")) {
            event_builder = event_builder.id(bsonid.to_string());
        }

        // Get the full document or return error if it doesn't exist.
        let doc = self.full_document.as_ref().ok_or(Error::NoFullDocument())?;

        // Convert BSON Document to JSON.
        let event_data = if let Ok(json_value) = bson::to_bson(doc) {
            // Convert BSON to serde_json::Value.
            match json_value {
                bson::Bson::Document(d) => {
                    // Convert to JSON.
                    let json_str =
                        serde_json::to_string(&d).map_err(|e| Error::SerdeJson { source: e })?;
                    let json_value: serde_json::Value = serde_json::from_str(&json_str)
                        .map_err(|e| Error::SerdeJson { source: e })?;
                    EventData::Json(json_value)
                }
                _ => {
                    // Fallback: serialize the document as JSON.
                    let json_value =
                        serde_json::to_value(doc).map_err(|e| Error::SerdeJson { source: e })?;
                    EventData::Json(json_value)
                }
            }
        } else {
            // If BSON conversion fails, try direct JSON serialization.
            let json_value =
                serde_json::to_value(doc).map_err(|e| Error::SerdeJson { source: e })?;
            EventData::Json(json_value)
        };

        event_builder.data(event_data).build().map_err(Error::Event)
    }
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
