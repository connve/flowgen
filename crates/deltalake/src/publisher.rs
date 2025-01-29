use std::collections::HashMap;

use deltalake::{
    kernel::{DataType, PrimitiveType, StructField},
    parquet::{
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    },
    writer::RecordBatchWriter,
    DeltaOps, DeltaTable,
};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Nats Client is missing / not initialized properly.")]
    NatsClientMissing(),
    #[error("Failed to publish message to Nats Jetstream.")]
    NatsPublish(#[source] async_nats::jetstream::context::PublishError),
    #[error("Failed to create or update Nats Jetstream.")]
    NatsCreateStream(#[source] async_nats::jetstream::context::CreateStreamError),
    #[error("Failed to get Nats Jetstream.")]
    NatsGetStream(#[source] async_nats::jetstream::context::GetStreamError),
    #[error("Failed to get process request to Nats Server.")]
    NatsRequest(#[source] async_nats::jetstream::context::RequestError),
}

pub struct Publisher {
    pub table: DeltaTable,
    pub writer: RecordBatchWriter,
}

pub struct Builder {
    config: super::config::Target,
}

impl Builder {
    // Creates a new instance of a Builder.
    pub fn new(config: super::config::Target) -> Builder {
        Builder { config }
    }

    pub async fn build(self) -> Result<Publisher, Error> {
        deltalake_gcp::register_handlers(None);
        let mut map = HashMap::new();
        map.insert(
            "google_service_account".to_string(),
            self.config.credentials,
        );
        let table = DeltaOps::try_from_uri_with_storage_options(self.config.uri, map)
            .await
            .unwrap()
            .create()
            .with_columns(vec![StructField::new(
                "Id".to_string(),
                DataType::Primitive(PrimitiveType::String),
                false,
            )])
            .await
            .unwrap();

        let writer_properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3).unwrap()))
            .build();

        let writer = RecordBatchWriter::for_table(&table)
            .expect("Failed to make RecordBatchWriter")
            .with_writer_properties(writer_properties);

        Ok(Publisher { table, writer })
    }
}
