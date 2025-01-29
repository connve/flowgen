use arrow::ipc::{reader::StreamDecoder, writer::StreamWriter};
use async_nats::jetstream::context::Publish;
use flowgen_core::event::EventBuilder;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("There was an error with an Apache Arrow data.")]
    Arrow(#[source] arrow::error::ArrowError),
    #[error("There was an error constructing Flowgen Event.")]
    FlowgenEvent(#[source] flowgen_core::event::Error),
    #[error("There was an error getting RecordBatch.")]
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
    fn to_event(&self) -> Result<flowgen_core::event::Event, Self::Error> {
        let mut buffer = arrow::buffer::Buffer::from_vec(self.payload.to_vec());
        let mut decoder = StreamDecoder::new();

        let record_batch = decoder
            .decode(&mut buffer)
            .map_err(Error::Arrow)?
            .ok_or_else(Error::NoRecordBatch)?;

        let e = EventBuilder::new()
            .data(record_batch)
            .subject(self.subject.to_string())
            .build()
            .map_err(Error::FlowgenEvent)?;

        decoder.finish().map_err(Error::Arrow)?;
        Ok(e)
    }
}
