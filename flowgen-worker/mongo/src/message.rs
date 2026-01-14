use bincode::deserialize;
use flowgen_core::event::{AvroData, EventBuilder, EventData};
use mongodb::bson::{self, Document};
use mongodb::change_stream::event::ChangeStreamEvent;

/// Errors that can occur during message conversion between flowgen and NATS formats.
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
    /// Expected record batch data is missing or unavailable.
    #[error("Error getting record batch")]
    NoRecordBatch(),
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

fn to_bytes(doc_opt: &Option<Document>) -> Option<Vec<u8>> {
    doc_opt.as_ref().and_then(|doc| bson::to_vec(doc).ok())
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

        let event_data = match deserialize::<AvroData>(&to_bytes(&self.full_document).unwrap()) {
            Ok(data) => EventData::Avro(data),
            Err(_) => match serde_json::from_slice(&to_bytes(&self.full_document).unwrap()) {
                Ok(data) => EventData::Json(data),
                Err(_) => {
                    let arrow_bytes =
                        deserialize::<Vec<u8>>(&to_bytes(&self.full_document).unwrap())
                            .map_err(|e| Error::Bincode { source: e })?;

                    let cursor = std::io::Cursor::new(arrow_bytes);
                    let mut stream_reader = arrow::ipc::reader::StreamReader::try_new(cursor, None)
                        .map_err(|e| Error::Arrow { source: e })?;

                    let recordbatch = stream_reader
                        .next()
                        .ok_or_else(Error::NoRecordBatch)?
                        .map_err(|e| Error::Arrow { source: e })?;

                    EventData::ArrowRecordBatch(recordbatch)
                }
            },
        };

        event_builder.data(event_data).build().map_err(Error::Event)
    }
}
