use chrono::Utc;
use deltalake::{
    datafusion::{
        datasource::MemTable,
        logical_expr::function::ExpressionArgs,
        prelude::{col, lit, DataFrame, SessionContext},
    },
    parquet::{
        basic::{Compression, ZstdLevel},
        file::properties::WriterProperties,
    },
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaOps, DeltaTable,
};
use flowgen_core::{connect::client::Client, stream::event::Event};
use std::sync::Arc;
use tokio::sync::{broadcast::Receiver, Mutex};
use tracing::{error, event, Level};

const DEFAULT_MESSAGE_SUBJECT: &str = "deltalake.writer";

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Parquet(#[from] deltalake::parquet::errors::ParquetError),
    #[error(transparent)]
    DeltaTable(#[from] deltalake::DeltaTableError),
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error(transparent)]
    Client(#[from] super::client::Error),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
    #[error("missing required config value path")]
    MissingPath(),
    #[error("missing required value DeltaTable")]
    MissingDeltaTable(),
    #[error("no filename in provided path")]
    EmptyFileName(),
    #[error("no value in provided str")]
    EmptyStr(),
}

pub struct EventHandler {
    client: Arc<Mutex<super::client::Client>>,
    config: Arc<super::config::Writer>,
}

impl EventHandler {
    async fn process(self, event: Event) -> Result<(), Error> {
        let mut client = self.client.lock().await;

        let table = client.table.as_mut().ok_or_else(Error::MissingDeltaTable)?;
        let ops = DeltaOps::from(table.clone());

        let ctx = SessionContext::new();
        let schema = event.data.schema();
        let provider = MemTable::try_new(schema.clone(), vec![vec![event.data]]).unwrap();
        ctx.register_table("source", Arc::new(provider)).unwrap();

        // Now, get a DataFusion DataFrame handle for the source:
        let df = ctx.table("source").await.unwrap();
        let predicate = "target.Id = source.Id";
        ops.merge(df, predicate)
            .with_source_alias("source")
            .with_target_alias("target")
            // .when_not_matched_insert(|insert| insert.set("Id", col("source.\"Id\"")))?
            .await
            .unwrap();

        // let writer_properties = WriterProperties::builder()
        //     .set_compression(Compression::ZSTD(
        //         ZstdLevel::try_new(3).map_err(Error::Parquet)?,
        //     ))
        //     .build();

        // let mut writer = RecordBatchWriter::for_table(table)
        //     .map_err(Error::DeltaTable)?
        //     .with_writer_properties(writer_properties);

        // writer.write(event.data).await.map_err(Error::DeltaTable)?;
        // writer
        //     .flush_and_commit(table)
        //     .await
        //     .map_err(Error::DeltaTable)?;

        let file_stem = self
            .config
            .path
            .file_stem()
            .ok_or_else(Error::EmptyFileName)?
            .to_str()
            .ok_or_else(Error::EmptyStr)?
            .trim();

        let timestamp = Utc::now().timestamp_micros();
        let subject = format!("{}.{}.{}", DEFAULT_MESSAGE_SUBJECT, file_stem, timestamp);
        event!(Level::INFO, "{}", format!("event processed: {}", subject));
        Ok(())
    }
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
        // Build a DeltaTable client with credentials and storage path.
        // Return thread-safe reference to use in the downstream components.
        let client = match super::client::ClientBuilder::new()
            .credentials(self.config.credentials.clone())
            .path(self.config.path.clone())
            .columns(self.config.columns.clone())
            .build()
        {
            Ok(client) => match client.connect().await {
                Ok(client) => Arc::new(Mutex::new(client)),
                Err(err) => {
                    event!(Level::ERROR, "{}", err);
                    return Err(Error::Client(err));
                }
            },
            Err(err) => {
                event!(Level::ERROR, "{}", err);
                return Err(Error::Client(err));
            }
        };

        // Receive events, validate if the event should be processed.
        while let Ok(event) = self.rx.recv().await {
            if event.current_task_id == Some(self.current_task_id - 1) {
                // Setup thread-safe reference to variables and pass to EventHandler.
                let client = Arc::clone(&client);
                let config = Arc::clone(&self.config);
                let event_handler = EventHandler { client, config };

                tokio::spawn(async move {
                    // Process the events and in case of error log it.
                    if let Err(err) = event_handler.process(event).await {
                        event!(Level::ERROR, "{}", err);
                    }
                });
            }
        }
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
