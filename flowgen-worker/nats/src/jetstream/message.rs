use arrow::ipc::writer::StreamWriter;
use async_nats::jetstream::message::PublishMessage;
use bincode::{deserialize, serialize};
use flowgen_core::event::{AvroData, EventBuilder, EventData};

/// Errors that can occur during message conversion between flowgen and NATS formats.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Apache Arrow error during data serialization or deserialization.
    #[error("Apache Arrow error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    /// JSON serialization or deserialization error.
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    /// Flowgen core event system error.
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    /// Binary encoding or decoding error.
    #[error("Binary encoding error: {source}")]
    Bincode {
        #[source]
        source: bincode::Error,
    },
    /// Expected record batch data is missing or unavailable.
    #[error("Record batch is missing")]
    NoRecordBatch(),
}

/// Trait for converting flowgen events to NATS publish messages.
pub trait FlowgenMessageExt {
    type Error;
    /// Convert a flowgen event to a NATS JetStream publish message.
    fn to_publish(&self) -> Result<PublishMessage, Self::Error>;
}

/// Trait for converting NATS messages to flowgen events.
pub trait NatsMessageExt {
    type Error;
    /// Convert a NATS message to a flowgen event.
    fn to_event(
        &self,
        task_type: &'static str,
        task_id: usize,
    ) -> Result<flowgen_core::event::Event, Self::Error>;
}

impl FlowgenMessageExt for flowgen_core::event::Event {
    type Error = Error;
    fn to_publish(&self) -> Result<PublishMessage, Self::Error> {
        let mut event = PublishMessage::build();
        if let Some(id) = &self.id {
            event = event.message_id(id)
        }

        match &self.data {
            EventData::ArrowRecordBatch(data) => {
                let mut buffer = Vec::new();
                {
                    let mut stream_writer = StreamWriter::try_new(&mut buffer, &data.schema())
                        .map_err(|e| Error::Arrow { source: e })?;
                    stream_writer
                        .write(data)
                        .map_err(|e| Error::Arrow { source: e })?;
                    stream_writer
                        .finish()
                        .map_err(|e| Error::Arrow { source: e })?;
                }

                let serialized = serialize(&buffer).map_err(|e| Error::Bincode { source: e })?;
                event = event.payload(serialized.into());
            }
            EventData::Avro(data) => {
                let serialized = serialize(data).map_err(|e| Error::Bincode { source: e })?;
                event = event.payload(serialized.into());
            }
            EventData::Json(data) => {
                let serialized =
                    serde_json::to_vec(data).map_err(|e| Error::SerdeJson { source: e })?;
                event = event.payload(serialized.into());
            }
        }
        Ok(event)
    }
}

impl NatsMessageExt for async_nats::Message {
    type Error = Error;
    fn to_event(
        &self,
        task_type: &'static str,
        task_id: usize,
    ) -> Result<flowgen_core::event::Event, Self::Error> {
        let mut event_builder = EventBuilder::new()
            .subject(self.subject.to_string())
            .task_id(task_id)
            .task_type(task_type);

        if let Some(headers) = &self.headers {
            if let Some(id) = headers.get(async_nats::header::NATS_MESSAGE_ID) {
                event_builder = event_builder.id(id.to_string());
            }
        }

        let event_data = match deserialize::<AvroData>(&self.payload) {
            Ok(data) => EventData::Avro(data),
            Err(_) => match serde_json::from_slice(&self.payload) {
                Ok(data) => EventData::Json(data),
                Err(_) => {
                    let arrow_bytes = deserialize::<Vec<u8>>(&self.payload)
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

#[cfg(test)]
mod tests {
    use super::*;
    use async_nats::HeaderMap;
    use flowgen_core::event::{EventBuilder, EventData};
    use serde_json::json;

    #[test]
    fn test_flowgen_message_ext_json() {
        let json_data = json!({
            "test": "value",
            "number": 42
        });

        let event = EventBuilder::new()
            .subject("test.subject".to_string())
            .id("test-id-123".to_string())
            .data(EventData::Json(json_data))
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        let result = event.to_publish();
        assert!(result.is_ok());

        // We can't easily verify the contents without accessing private fields,
        // but we can verify the conversion succeeds
    }

    #[test]
    fn test_flowgen_message_ext_avro() {
        let avro_data = AvroData {
            schema: "test_schema".to_string(),
            raw_bytes: vec![1, 2, 3, 4],
        };

        let event = EventBuilder::new()
            .subject("avro.test".to_string())
            .data(EventData::Avro(avro_data))
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        let result = event.to_publish();
        assert!(result.is_ok());
    }

    #[test]
    fn test_flowgen_message_ext_no_id() {
        let json_data = json!({"test": "no_id"});

        let event = EventBuilder::new()
            .subject("test.no.id".to_string())
            .data(EventData::Json(json_data))
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        let result = event.to_publish();
        assert!(result.is_ok());
    }

    #[test]
    fn test_nats_message_ext_json() {
        let json_payload = serde_json::to_vec(&json!({"message": "test"})).unwrap();

        // Create a mock NATS message
        let message = async_nats::Message {
            subject: "test.subject".into(),
            payload: json_payload.into(),
            reply: None,
            headers: None,
            status: None,
            description: None,
            length: 0,
        };

        let result = message.to_event("test", 0);
        assert!(result.is_ok());

        let event = result.unwrap();
        assert_eq!(event.subject, "test.subject");
        assert!(event.id.is_none()); // No headers provided
        assert_eq!(event.task_id, 0);
        assert_eq!(event.task_type, "test");

        // Verify the data is JSON type
        assert!(matches!(event.data, EventData::Json(_)));
    }

    #[test]
    fn test_nats_message_ext_with_headers() {
        let json_payload = serde_json::to_vec(&json!({"with": "headers"})).unwrap();

        let mut headers = HeaderMap::new();
        headers.insert(async_nats::header::NATS_MESSAGE_ID, "msg-123");

        let message = async_nats::Message {
            subject: "test.headers".into(),
            payload: json_payload.into(),
            reply: None,
            headers: Some(headers),
            status: None,
            description: None,
            length: 0,
        };

        let result = message.to_event("test", 1);
        assert!(result.is_ok());

        let event = result.unwrap();
        assert_eq!(event.subject, "test.headers");
        assert_eq!(event.id, Some("msg-123".to_string()));
        assert_eq!(event.task_id, 1);
        assert_eq!(event.task_type, "test");
    }

    #[test]
    fn test_nats_message_ext_avro() {
        let avro_data = AvroData {
            schema: "test_avro_schema".to_string(),
            raw_bytes: vec![5, 6, 7, 8],
        };

        let serialized = serialize(&avro_data).unwrap();

        let message = async_nats::Message {
            subject: "avro.test".into(),
            payload: serialized.into(),
            reply: None,
            headers: None,
            status: None,
            description: None,
            length: 0,
        };

        let result = message.to_event("test", 2);
        assert!(result.is_ok());

        let event = result.unwrap();
        assert_eq!(event.subject, "avro.test");
        assert_eq!(event.task_id, 2);
        assert_eq!(event.task_type, "test");
        assert!(matches!(event.data, EventData::Avro(_)));
    }

    #[test]
    fn test_traits_exist() {
        // Verify that our traits are properly defined
        fn accepts_flowgen_message_ext<T: FlowgenMessageExt>(_: T) {}
        fn accepts_nats_message_ext<T: NatsMessageExt>(_: T) {}

        let json_data = json!({"trait": "test"});
        let event = EventBuilder::new()
            .subject("trait.test".to_string())
            .data(EventData::Json(json_data))
            .task_id(0)
            .task_type("test")
            .build()
            .unwrap();

        accepts_flowgen_message_ext(event);

        let message = async_nats::Message {
            subject: "test".into(),
            payload: vec![].into(),
            reply: None,
            headers: None,
            status: None,
            description: None,
            length: 0,
        };

        accepts_nats_message_ext(message);
    }

    // Note: Arrow RecordBatch tests would require more complex setup
    // with actual Arrow schemas and data, but the basic trait functionality
    // is covered by the tests above
}
