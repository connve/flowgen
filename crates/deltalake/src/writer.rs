use chrono::Utc;
use deltalake::{
    kernel::{DataType, PrimitiveType, StructField},
    parquet::{
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    },
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps,
};
use flowgen_core::stream::event::Event;

use std::collections::HashMap;
use std::sync::Arc;
use tokio::{
    sync::{broadcast::Receiver, Mutex},
    task::JoinHandle,
};
use tracing::{error, event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "deltalake.writer";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("error with processing Parquet file")]
    Parquet(#[source] deltalake::parquet::errors::ParquetError),
    #[error(transparent)]
    DeltaTable(#[from] deltalake::DeltaTableError),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
    #[error("error executing task to completion")]
    TaskJoin(#[source] tokio::task::JoinError),
    #[error("missing required config value path")]
    MissingPath(),
    #[error("no filename in provided path")]
    EmptyFileName(),
    #[error("no value in provided str")]
    EmptyStr(),
}

#[derive(Debug)]
pub struct Writer {
    config: Arc<super::config::Writer>,
    rx: Receiver<Event>,
    current_task_id: usize,
}

impl flowgen_core::task::runner::Runner for Writer {
    type Error = Error;
    async fn run(mut self) -> Result<(), Error> {
        let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
            deltalake_gcp::register_handlers(None);
            let mut storage_options = HashMap::new();
            storage_options.insert(
                "google_service_account".to_string(),
                self.config.credentials.clone(),
            );

            let path = self.config.path.to_str().ok_or_else(Error::MissingPath)?;

            let ops = DeltaOps::try_from_uri_with_storage_options(path, storage_options.clone())
                .await
                .map_err(Error::DeltaTable)?;

            let mut columns = Vec::new();
            for c in &self.config.columns {
                let data_type = match c.data_type {
                    crate::config::DataType::Utf8 => DataType::Primitive(PrimitiveType::String),
                };
                let struct_field = StructField::new(c.name.to_string(), data_type, c.nullable);
                columns.push(struct_field);
            }

            let table = ops
                .create()
                .with_columns(columns)
                .await
                .map_err(Error::DeltaTable)?;
            let table = Arc::new(Mutex::new(table));

            while let Ok(event) = self.rx.recv().await {
                if event.current_task_id == Some(self.current_task_id - 1) {
                    let table = Arc::clone(&table);
                    let config = Arc::clone(&self.config);
                    let handle: JoinHandle<Result<(), Error>> = tokio::spawn(async move {
                        let table = Arc::clone(&table);
                        let mut table = table.lock().await;

                        let writer_properties = WriterProperties::builder()
                            .set_compression(Compression::ZSTD(
                                ZstdLevel::try_new(3).map_err(Error::Parquet)?,
                            ))
                            .build();

                        let mut writer = RecordBatchWriter::for_table(&table)
                            .map_err(Error::DeltaTable)?
                            .with_writer_properties(writer_properties);

                        writer.write(event.data).await.map_err(Error::DeltaTable)?;
                        writer
                            .flush_and_commit(&mut table)
                            .await
                            .map_err(Error::DeltaTable)?;

                        let file_stem = config
                            .path
                            .file_stem()
                            .ok_or_else(Error::EmptyFileName)?
                            .to_str()
                            .ok_or_else(Error::EmptyStr)?
                            .trim();

                        let timestamp = Utc::now().timestamp_micros();
                        let subject =
                            format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, file_stem, timestamp);
                        event!(Level::INFO, "{}", format!("event processed: {}", subject));
                        Ok(())
                    });
                    match handle.await {
                        Ok(result) => {
                            if let Err(e) = result {
                                event!(Level::ERROR, "{}", e);
                            }
                        }
                        Err(e) => {
                            event!(Level::ERROR, "{}", e);
                        }
                    };
                }
            }
            Ok(())
        });
        match handle.await {
            Ok(result) => {
                if let Err(e) = result {
                    event!(Level::ERROR, "{}", e);
                }
            }
            Err(e) => {
                event!(Level::ERROR, "{}", e);
            }
        };

        Ok(())
    }
}

#[derive(Default)]
pub struct WriterBuilder {
    config: Option<Arc<super::config::Writer>>,
    rx: Option<Receiver<Event>>,
    current_task_id: usize,
}

impl WriterBuilder {
    pub fn new() -> WriterBuilder {
        WriterBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Writer>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn current_task_id(mut self, current_task_id: usize) -> Self {
        self.current_task_id = current_task_id;
        self
    }

    pub async fn build(self) -> Result<Writer, Error> {
        Ok(Writer {
            config: self
                .config
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            current_task_id: self.current_task_id,
        })
    }
}
