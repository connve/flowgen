use arrow::ipc::writer::StreamWriter;
use async_nats::jetstream::context::Publish;
use bincode::{deserialize, serialize};
use flowgen_core::event::{AvroData, EventBuilder, EventData};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    Bincode(#[from] bincode::Error),
    #[error("error getting recordbatch")]
    NoRecordBatch(),
}

pub trait FlowgenMessageExt {
    type Error;
    fn to_publish(&self) -> Result<Publish, Self::Error>;
}

pub trait NatsMessageExt {
    type Error;
    fn to_event(&self) -> Result<flowgen_core::event::Event, Self::Error>;
}

impl FlowgenMessageExt for flowgen_core::event::Event {
    type Error = Error;
    fn to_publish(&self) -> Result<Publish, Self::Error> {
        let mut event = Publish::build();
        if let Some(id) = &self.id {
            event = event.message_id(id)
        }

        match &self.data {
            EventData::ArrowRecordBatch(data) => {
                let mut buffer = Vec::new();
                {
                    let mut stream_writer = StreamWriter::try_new(&mut buffer, &data.schema())?;
                    stream_writer.write(data)?;
                    stream_writer.finish()?;
                }

                let serialized = serialize(&buffer)?;
                event = event.payload(serialized.into());
            }
            EventData::Avro(data) => {
                let serialized = serialize(data)?;
                event = event.payload(serialized.into());
            }
            EventData::Json(data) => {
                let serialized = serde_json::to_vec(data)?;
                event = event.payload(serialized.into());
            }
        }
        Ok(event)
    }
}

impl NatsMessageExt for async_nats::Message {
    type Error = Error;
    fn to_event(&self) -> Result<flowgen_core::event::Event, Self::Error> {
        let mut event = EventBuilder::new().subject(self.subject.to_string());
        if let Some(headers) = &self.headers {
            if let Some(id) = headers.get(async_nats::header::NATS_MESSAGE_ID) {
                event = event.id(id.to_string());
            }
        }

        let event_data = match deserialize::<AvroData>(&self.payload) {
            Ok(data) => EventData::Avro(data),
            Err(_) => match serde_json::from_slice(&self.payload) {
                Ok(data) => EventData::Json(data),
                Err(_) => {
                    let arrow_bytes = deserialize::<Vec<u8>>(&self.payload)?;

                    let cursor = std::io::Cursor::new(arrow_bytes);
                    let mut stream_reader =
                        arrow::ipc::reader::StreamReader::try_new(cursor, None)?;

                    let recordbatch = stream_reader.next().ok_or_else(Error::NoRecordBatch)??;

                    EventData::ArrowRecordBatch(recordbatch)
                }
            },
        };

        event.data(event_data).build().map_err(Error::Event)
    }
}
