use arrow::ipc::{reader::StreamDecoder, writer::StreamWriter};
use async_nats::jetstream::context::Publish;
use flowgen_core::stream::event::EventBuilder;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with an Apache Arrow data")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("error constructing event")]
    Event(#[source] flowgen_core::stream::event::Error),
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
        let buffer: Vec<u8> = Vec::new();
        let mut stream_writer =
            StreamWriter::try_new(buffer, &self.data.schema()).map_err(Error::Arrow)?;
        stream_writer.write(&self.data).map_err(Error::Arrow)?;
        stream_writer.finish().map_err(Error::Arrow)?;
        let payload = stream_writer.get_mut().to_vec();
        let event = Publish::build().payload(payload.into());
        Ok(event)
    }
}

impl NatsMessageExt for async_nats::Message {
    type Error = Error;
    fn to_event(&self) -> Result<flowgen_core::stream::event::Event, Self::Error> {
        let mut buffer = arrow::buffer::Buffer::from_vec(self.payload.to_vec());
        let mut decoder = StreamDecoder::new();

        let record_batch = decoder
            .decode(&mut buffer)
            .map_err(Error::Arrow)?
            .ok_or_else(Error::NoRecordBatch)?;

        let event = EventBuilder::new()
            .data(record_batch)
            .subject(self.subject.to_string())
            .build()
            .map_err(Error::Event)?;

        decoder.finish().map_err(Error::Arrow)?;
        Ok(event)
    }
}
