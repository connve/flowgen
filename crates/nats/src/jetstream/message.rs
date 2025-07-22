use arrow::ipc::{reader::StreamDecoder, writer::StreamWriter};
use async_nats::{jetstream::context::Publish, HeaderName};
use bincode::{deserialize, serialize};
use flowgen_core::stream::event::{AvroData, EventBuilder, EventData};

#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error(transparent)]
    Arrow(#[from] arrow::error::ArrowError),
    #[error(transparent)]
    Event(#[from] flowgen_core::stream::event::Error),
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
    fn to_event(&self) -> Result<flowgen_core::stream::event::Event, Self::Error>;
}

impl FlowgenMessageExt for flowgen_core::stream::event::Event {
    type Error = Error;
    fn to_publish(&self) -> Result<Publish, Self::Error> {
        let mut event = Publish::build();
        if let Some(id) = &self.id {
            event = event.message_id(id)
        }

        match &self.data {
            flowgen_core::stream::event::EventData::ArrowRecordBatch(data) => {
                let buffer: Vec<u8> = Vec::new();
                let mut stream_writer =
                    StreamWriter::try_new(buffer, &data.schema()).map_err(Error::Arrow)?;
                stream_writer.write(data).map_err(Error::Arrow)?;
                stream_writer.finish().map_err(Error::Arrow)?;
                let serialized = serialize(stream_writer.get_mut())?;
                event = event.payload(serialized.into());
            }
            flowgen_core::stream::event::EventData::Avro(data) => {
                let serialized = serialize(&data)?;
                event = event.payload(serialized.into());
            }
        }
        Ok(event)
    }
}

impl NatsMessageExt for async_nats::Message {
    type Error = Error;
    fn to_event(&self) -> Result<flowgen_core::stream::event::Event, Self::Error> {
        let mut event = EventBuilder::new().subject(self.subject.to_string());
        if let Some(headers) = &self.headers {
            if let Some(id) = headers.get(async_nats::header::NATS_MESSAGE_ID) {
                event = event.id(id.to_string());
            }
        }

        let event_data = match deserialize::<AvroData>(&self.payload) {
            Ok(data) => EventData::Avro(data),
            Err(_) => {
                let mut buffer = arrow::buffer::Buffer::from_vec(self.payload.clone().into());
                let mut decoder = StreamDecoder::new();

                let recordbatch = decoder
                    .decode(&mut buffer)
                    .map_err(Error::Arrow)?
                    .ok_or_else(Error::NoRecordBatch)?;

                decoder.finish().map_err(Error::Arrow)?;
                EventData::ArrowRecordBatch(recordbatch)
            }
        };

        event.data(event_data).build().map_err(Error::Event)
    }
}
