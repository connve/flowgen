use deltalake::{
    kernel::{DataType, PrimitiveType, StructField},
    DeltaOps, DeltaTable,
};
use std::{collections::HashMap, path::PathBuf};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Parquet(#[from] deltalake::parquet::errors::ParquetError),
    #[error(transparent)]
    DeltaTable(#[from] deltalake::DeltaTableError),
    #[error(transparent)]
    TaskJoin(#[from] tokio::task::JoinError),
    #[error("missing required event attrubute")]
    MissingRequiredAttribute(String),
    #[error("missing required config value path")]
    MissingPath(),
    #[error("no filename in provided path")]
    EmptyFileName(),
    #[error("no value in provided str")]
    EmptyStr(),
}

pub struct Client {
    credentials: String,
    path: PathBuf,
    columns: Option<Vec<super::config::Column>>,
    pub(crate) table: Option<DeltaTable>,
}

impl flowgen_core::connect::client::Client for Client {
    type Error = Error;
    async fn connect(mut self) -> Result<Client, Error> {
        deltalake_gcp::register_handlers(None);
        let mut storage_options = HashMap::new();
        storage_options.insert(
            "google_service_account".to_string(),
            self.credentials.clone(),
        );

        let path = self.path.to_str().ok_or_else(Error::MissingPath)?;

        let ops = DeltaOps::try_from_uri_with_storage_options(path, storage_options.clone())
            .await
            .map_err(Error::DeltaTable)?;

        match deltalake::open_table_with_storage_options(path, storage_options).await {
            Ok(ops) => {
                let table = ops;
                self.table = Some(table);
            }
            Err(_) => {
                if let Some(ref config_columns) = self.columns {
                    let mut columns = Vec::new();

                    for c in config_columns {
                        let data_type = match c.data_type {
                            crate::config::DataType::Utf8 => {
                                DataType::Primitive(PrimitiveType::String)
                            }
                        };
                        let struct_field =
                            StructField::new(c.name.to_string(), data_type, c.nullable);
                        columns.push(struct_field);
                    }
                    let table = ops
                        .create()
                        .with_columns(columns)
                        .await
                        .map_err(Error::DeltaTable)?;
                    self.table = Some(table);
                }
            }
        };

        Ok(self)
    }
}

#[derive(Default)]
pub struct ClientBuilder {
    credentials: Option<String>,
    path: Option<PathBuf>,
    columns: Option<Vec<super::config::Column>>,
}

impl ClientBuilder {
    pub fn new() -> ClientBuilder {
        ClientBuilder {
            ..Default::default()
        }
    }

    pub fn credentials(mut self, credentials: String) -> Self {
        self.credentials = Some(credentials);
        self
    }

    pub fn path(mut self, path: PathBuf) -> Self {
        self.path = Some(path);
        self
    }

    pub fn columns(mut self, columns: Vec<super::config::Column>) -> Self {
        self.columns = Some(columns);
        self
    }

    pub fn build(self) -> Result<Client, Error> {
        Ok(Client {
            credentials: self
                .credentials
                .ok_or_else(|| Error::MissingRequiredAttribute("credentials".to_string()))?,
            path: self
                .path
                .ok_or_else(|| Error::MissingRequiredAttribute("path".to_string()))?,
            columns: self.columns,
            table: None,
        })
    }
}
