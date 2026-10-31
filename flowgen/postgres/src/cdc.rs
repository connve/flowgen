//! The `postgres_cdc` task: change data capture from a publication, read
//! through logical replication (pgoutput).

use crate::client::{connection_config, read_credentials, Error as ClientError};
use crate::config::Credentials;
use flowgen_core::config::ConfigExt;
use flowgen_core::delivery::{Delivered, Delivery};
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::retry::RetryConfig;
use flowgen_core::task::context::TaskContext;
use flowgen_core::task::TaskConfig;
use futures_util::{StreamExt, TryStreamExt};
use postgres_replication::protocol::{
    LogicalReplicationMessage, RelationBody, ReplicationMessage, Tuple, TupleData,
};
use postgres_replication::LogicalReplicationStream;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc::Sender;
use tokio::time::Instant;
use tokio_postgres::config::ReplicationMode;
use tokio_postgres::types::{PgLsn, Type};
use tokio_postgres::SimpleQueryMessage;
use tracing::{error, info, warn};

/// Seconds from the Unix epoch to the PostgreSQL epoch, 2000-01-01.
const POSTGRES_EPOCH_SECS: u64 = 946_684_800;

/// Longest identifier PostgreSQL keeps (`NAMEDATALEN - 1`).
const MAX_NAME_LEN: usize = 63;

/// Configuration for the `postgres_cdc` task.
///
/// Emits each row change of the tables in a publication.
///
/// ```yaml
/// - postgres_cdc:
///     name: watch_orders
///     credentials_path: /etc/postgres/credentials.json
///     publication: orders_pub
///     slot: flowgen_orders
/// ```
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, TaskConfig)]
#[serde(deny_unknown_fields)]
pub struct Cdc {
    /// Unique task identifier.
    pub name: String,
    /// Path to the PostgreSQL credentials file (JSON).
    pub credentials_path: PathBuf,
    /// Publication whose tables are captured.
    pub publication: String,
    /// Logical replication slot that keeps the position; created when missing.
    pub slot: String,
    /// Emits every row of the publication's tables before the changes, read
    /// at the position the slot starts from.
    #[serde(default)]
    pub initial_copy: bool,
    /// Rows per event of the initial copy.
    #[serde(default = "default_copy_batch_size")]
    pub copy_batch_size: NonZeroUsize,
    /// How long to wait for the flow to complete a change. Waits
    /// indefinitely when omitted.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<Duration>,
    /// How many times a change is sent through the flow before it is skipped.
    /// Delivered until the flow completes it when omitted.
    #[serde(default)]
    pub max_deliver: Option<NonZeroU32>,
    /// Maximum time to open the replication connection and log in.
    #[serde(default = "default_connection_timeout", with = "humantime_serde")]
    pub connection_timeout: Duration,
    /// Longest time between two reports of the position to the server; must
    /// stay below the server's `wal_sender_timeout`.
    #[serde(default = "default_status_interval", with = "humantime_serde")]
    pub status_interval: Duration,
    /// Optional list of upstream task names this task depends on.
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration for connecting and redelivery (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<RetryConfig>,
}

impl ConfigExt for Cdc {}

fn default_connection_timeout() -> Duration {
    Duration::from_secs(30)
}

fn default_status_interval() -> Duration {
    Duration::from_secs(10)
}

fn default_copy_batch_size() -> NonZeroUsize {
    NonZeroUsize::MIN.saturating_add(999)
}

/// Errors that can occur while reading changes.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to render config: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error("`{field}` must be 1 to 63 characters of a-z, 0-9 and _, got '{value}'")]
    InvalidName { field: &'static str, value: String },
    #[error("Failed to connect for replication: {source}")]
    Connect {
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("Connecting for replication took longer than {timeout:?}")]
    ConnectTimeout { timeout: Duration },
    #[error("Replication command failed: {source}")]
    Command {
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("Replication slot '{slot}' has no position")]
    MissingSlotPosition { slot: String },
    #[error("Replication slot '{slot}' has an invalid position '{lsn}'")]
    InvalidSlotPosition { slot: String, lsn: String },
    #[error("Replication slot '{slot}' lost WAL it needs; drop it to start over")]
    SlotLost { slot: String },
    #[error("The server returned an invalid WAL position '{lsn}'")]
    InvalidWalPosition { lsn: String },
    #[error("The server exported an invalid snapshot name '{snapshot}'")]
    InvalidSnapshotName { snapshot: String },
    #[error("Table {table} has an invalid OID")]
    InvalidTableOid { table: String },
    #[error("The server reported an invalid version '{version}'")]
    InvalidServerVersion { version: String },
    #[error("The initial copy stopped before it finished")]
    CopyInterrupted,
    #[error("Cache error for initial copy key '{key}': {source}")]
    Cache {
        key: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    #[error("Replication stream failed: {source}")]
    Stream {
        #[source]
        source: tokio_postgres::Error,
    },
    #[error("The replication stream ended")]
    StreamEnded,
    #[error("Failed to read a replication message: {source}")]
    Decode {
        #[source]
        source: std::io::Error,
    },
    #[error("Change for unknown relation {rel_id}")]
    UnknownRelation { rel_id: u32 },
    #[error("Failed to build event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to send event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(&'static str),
}

/// Checks that `value` is a plain identifier, safe to put in a replication command.
fn validate_name(field: &'static str, value: &str) -> Result<(), Error> {
    let valid = !value.is_empty()
        && value.len() <= MAX_NAME_LEN
        && value
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    match valid {
        true => Ok(()),
        false => Err(Error::InvalidName {
            field,
            value: value.to_string(),
        }),
    }
}

/// Relation column flag marking a column of the replica identity key.
const KEY_COLUMN_FLAG: i8 = 1;

/// A column of a captured table.
#[derive(Debug, Clone, PartialEq)]
struct RelationColumn {
    name: String,
    type_id: u32,
    key: bool,
}

/// A captured table and its columns, from the stream's Relation message.
#[derive(Debug, Clone, PartialEq)]
struct Relation {
    schema: String,
    table: String,
    columns: Vec<RelationColumn>,
}

impl Relation {
    fn from_body(body: &RelationBody) -> Result<Self, Error> {
        let decode = |source| Error::Decode { source };
        let columns = body
            .columns()
            .iter()
            .map(|column| {
                column.name().map(|name| RelationColumn {
                    name: name.to_string(),
                    type_id: column.type_id() as u32,
                    key: column.flags() & KEY_COLUMN_FLAG != 0,
                })
            })
            .collect::<Result<_, _>>()
            .map_err(decode)?;
        Ok(Self {
            schema: body.namespace().map_err(decode)?.to_string(),
            table: body.name().map_err(decode)?.to_string(),
            columns,
        })
    }

    /// The row as a JSON object, with only the key columns for a key tuple;
    /// unchanged TOAST values are left out.
    fn row(&self, tuple: &Tuple, keys_only: bool) -> Map<String, Value> {
        self.columns
            .iter()
            .zip(tuple.tuple_data())
            .filter(|(column, _)| !keys_only || column.key)
            .filter_map(|(column, data)| match data {
                TupleData::Null => Some((column.name.clone(), Value::Null)),
                TupleData::UnchangedToast => None,
                TupleData::Text(text) | TupleData::Binary(text) => Some((
                    column.name.clone(),
                    value(column.type_id, &String::from_utf8_lossy(text)),
                )),
            })
            .collect()
    }

    /// The old row of an update or delete: the full row with `REPLICA
    /// IDENTITY FULL`, otherwise the key.
    fn before(&self, old: Option<&Tuple>, key: Option<&Tuple>) -> Option<Map<String, Value>> {
        match (old, key) {
            (Some(tuple), _) => Some(self.row(tuple, false)),
            (None, Some(tuple)) => Some(self.row(tuple, true)),
            (None, None) => None,
        }
    }
}

/// A column value in text format as JSON: booleans and numbers as such,
/// JSON as JSON, `numeric` and everything else as a string.
fn value(type_id: u32, text: &str) -> Value {
    let string = || Value::String(text.to_string());
    match Type::from_oid(type_id) {
        Some(Type::BOOL) => Value::Bool(text == "t"),
        Some(Type::INT2 | Type::INT4 | Type::INT8 | Type::OID) => match text.parse::<i64>() {
            Ok(number) => Value::from(number),
            Err(_) => string(),
        },
        Some(Type::FLOAT4 | Type::FLOAT8) => match text
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
        {
            Some(number) => Value::Number(number),
            None => string(),
        },
        Some(Type::JSON | Type::JSONB) => match serde_json::from_str(text) {
            Ok(json) => json,
            Err(_) => string(),
        },
        _ => string(),
    }
}

/// Change metadata the event has no field for, merged into `event.meta`.
#[derive(Debug, Serialize)]
struct ChangeMeta<'a> {
    schema: &'a str,
    table: &'a str,
    operation_type: &'static str,
    lsn: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    before: Option<Map<String, Value>>,
}

/// One row change, ready to become an event.
#[derive(Debug, PartialEq)]
struct Change {
    table: String,
    data: Value,
    meta: Map<String, Value>,
}

/// A change to a row of `schema.table`.
fn change(
    schema: &str,
    table: &str,
    operation_type: &'static str,
    data: Value,
    before: Option<Map<String, Value>>,
    lsn: PgLsn,
) -> Change {
    let meta = match serde_json::to_value(ChangeMeta {
        schema,
        table,
        operation_type,
        lsn: lsn.to_string(),
        before,
    }) {
        Ok(Value::Object(meta)) => meta,
        _ => Map::new(),
    };
    Change {
        table: table.to_string(),
        data,
        meta,
    }
}

/// The event for `change`.
fn change_event(
    id: String,
    change: Change,
    task_id: usize,
    task_type: &'static str,
) -> Result<Event, Error> {
    let mut event = EventBuilder::new()
        .data(EventData::Json(change.data))
        .subject(change.table)
        .id(id)
        .task_id(task_id)
        .task_type(task_type)
        .build()
        .map_err(|source| Error::EventBuilder { source })?;
    event.meta.get_or_insert_with(Map::new).extend(change.meta);
    Ok(event)
}

/// The changes of a decoded message: one for a row change, one per table for
/// a truncate, none for anything else.
fn changes(
    message: &LogicalReplicationMessage,
    relations: &HashMap<u32, Relation>,
    lsn: PgLsn,
) -> Result<Vec<Change>, Error> {
    let relation = |rel_id: u32| {
        relations
            .get(&rel_id)
            .ok_or(Error::UnknownRelation { rel_id })
    };
    let change = |relation: &Relation,
                  operation_type: &'static str,
                  data: Value,
                  before: Option<Map<String, Value>>| {
        change(
            &relation.schema,
            &relation.table,
            operation_type,
            data,
            before,
            lsn,
        )
    };
    Ok(match message {
        LogicalReplicationMessage::Insert(body) => {
            let relation = relation(body.rel_id())?;
            vec![change(
                relation,
                "insert",
                Value::Object(relation.row(body.tuple(), false)),
                None,
            )]
        }
        LogicalReplicationMessage::Update(body) => {
            let relation = relation(body.rel_id())?;
            vec![change(
                relation,
                "update",
                Value::Object(relation.row(body.new_tuple(), false)),
                relation.before(body.old_tuple(), body.key_tuple()),
            )]
        }
        LogicalReplicationMessage::Delete(body) => {
            let relation = relation(body.rel_id())?;
            let before = match relation.before(body.old_tuple(), body.key_tuple()) {
                Some(row) => Value::Object(row),
                None => Value::Null,
            };
            vec![change(relation, "delete", before, None)]
        }
        LogicalReplicationMessage::Truncate(body) => body
            .rel_ids()
            .iter()
            .map(|rel_id| Ok(change(relation(*rel_id)?, "truncate", Value::Null, None)))
            .collect::<Result<_, Error>>()?,
        _ => Vec::new(),
    })
}

/// Microseconds since the PostgreSQL epoch, as standby status updates expect.
fn postgres_now() -> i64 {
    let epoch = UNIX_EPOCH + Duration::from_secs(POSTGRES_EPOCH_SECS);
    match SystemTime::now()
        .duration_since(epoch)
        .map(|elapsed| i64::try_from(elapsed.as_micros()))
    {
        Ok(Ok(micros)) => micros,
        _ => 0,
    }
}

type Stream = Pin<Box<LogicalReplicationStream>>;

/// Reads the publication's changes and sends them through the flow.
pub struct EventHandler {
    config: Arc<Cdc>,
    /// Kept alive for the replication connection.
    _client: tokio_postgres::Client,
    stream: Stream,
    /// Position of the last change the flow completed, confirmed to the server.
    flushed: PgLsn,
    /// When the position was last reported to the server.
    last_report: Instant,
    retry_config: RetryConfig,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_type: &'static str,
    task_context: Arc<TaskContext>,
}

/// Reports `flushed` to the server as written, flushed and applied.
async fn report(
    stream: &mut Stream,
    flushed: PgLsn,
    last_report: &mut Instant,
) -> Result<(), Error> {
    stream
        .as_mut()
        .standby_status_update(flushed, flushed, flushed, postgres_now(), 0)
        .await
        .map_err(|source| Error::Stream { source })?;
    *last_report = Instant::now();
    Ok(())
}

impl EventHandler {
    /// When the next report is due so the server keeps the connection.
    fn report_due(&self) -> Instant {
        self.last_report + self.config.status_interval
    }

    /// Sends `change` through the flow, reporting the position meanwhile so
    /// the server keeps the connection.
    async fn deliver(&mut self, id: String, change: Change) -> Result<Delivered, Error> {
        let event = change_event(id, change, self.task_id, self.task_type)?;
        let delivery = Delivery {
            downstream: self.tx.is_some(),
            leaf_count: self.task_context.leaf_count,
            ack_timeout: self.config.ack_timeout,
            max_deliver: self.config.max_deliver,
            retry: &self.retry_config,
            cancellation: &self.task_context.cancellation_token,
        };
        let tx = self.tx.as_ref();
        let delivered = delivery.deliver(&event, |attempt| attempt.send_with_logging(tx));
        tokio::pin!(delivered);
        loop {
            tokio::select! {
                biased;
                delivered = &mut delivered => {
                    return delivered.map_err(|source| Error::SendMessage { source });
                }
                _ = tokio::time::sleep_until(self.report_due()) => {
                    report(&mut self.stream, self.flushed, &mut self.last_report).await?
                }
            }
        }
    }

    /// Streams the slot's changes, one at a time, and confirms each
    /// transaction once the flow has completed all of its changes. Outside a
    /// transaction, a keepalive moves the position to the server's end of
    /// WAL, so changes to tables outside the publication don't hold WAL back.
    /// A stop first reads the messages already received, so a transaction
    /// whose changes the flow completed is confirmed.
    async fn handle(mut self) -> Result<(), Error> {
        let mut relations = HashMap::new();
        let mut transaction = PgLsn::from(0);
        let mut in_transaction = false;
        let mut position = 0usize;
        let cancellation = self.task_context.cancellation_token.clone();

        loop {
            if Instant::now() >= self.report_due() {
                report(&mut self.stream, self.flushed, &mut self.last_report).await?;
            }
            let report_due = self.report_due();
            let message = tokio::select! {
                biased;
                message = self.stream.next() => message,
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep_until(report_due) => continue,
            };
            let message = match message {
                None => return Err(Error::StreamEnded),
                Some(message) => message.map_err(|source| Error::Stream { source })?,
            };
            let body = match message {
                ReplicationMessage::XLogData(body) => body.into_data(),
                ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                    let wal_end = PgLsn::from(keepalive.wal_end());
                    let advanced = !in_transaction && wal_end > self.flushed;
                    if advanced {
                        self.flushed = wal_end;
                    }
                    if advanced || keepalive.reply() == 1 {
                        report(&mut self.stream, self.flushed, &mut self.last_report).await?;
                    }
                    continue;
                }
                _ => continue,
            };
            match &body {
                LogicalReplicationMessage::Relation(relation) => {
                    relations.insert(relation.rel_id(), Relation::from_body(relation)?);
                }
                LogicalReplicationMessage::Begin(begin) => {
                    transaction = PgLsn::from(begin.final_lsn());
                    in_transaction = true;
                    position = 0;
                }
                LogicalReplicationMessage::Commit(commit) => {
                    self.flushed = PgLsn::from(commit.end_lsn());
                    in_transaction = false;
                    report(&mut self.stream, self.flushed, &mut self.last_report).await?;
                }
                _ => {
                    for change in changes(&body, &relations, transaction)? {
                        position += 1;
                        let id = format!("{transaction}-{position}");
                        if self.deliver(id, change).await? == Delivered::Stopped {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
}

/// Opens a connection, in logical replication mode with `replication`.
async fn connect(
    config: &Cdc,
    credentials: &Credentials,
    replication: bool,
) -> Result<tokio_postgres::Client, Error> {
    let (mut connection_config, tls) = connection_config(credentials, config.connection_timeout)?;
    if replication {
        connection_config.replication_mode(ReplicationMode::Logical);
    }
    let connected =
        tokio::time::timeout(config.connection_timeout, connection_config.connect(tls)).await;
    let (client, connection) = match connected {
        Ok(connected) => connected.map_err(|source| Error::Connect { source })?,
        Err(_) => {
            return Err(Error::ConnectTimeout {
                timeout: config.connection_timeout,
            })
        }
    };
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            warn!(error = %e, "PostgreSQL connection closed");
        }
    });
    Ok(client)
}

/// The confirmed position of `slot`, or `None` when the slot doesn't exist.
/// A slot that lost WAL (PostgreSQL 13+) is an error, since streaming from it
/// fails until it is dropped.
async fn slot_position(
    client: &tokio_postgres::Client,
    slot: &str,
) -> Result<Option<PgLsn>, Error> {
    let wal_status = match server_version(client).await? >= 130_000 {
        true => "wal_status",
        false => "NULL AS wal_status",
    };
    let rows = rows(
        client,
        &format!(
            "SELECT confirmed_flush_lsn, {wal_status} FROM pg_replication_slots \
             WHERE slot_name = '{slot}'"
        ),
    )
    .await?;
    let Some(row) = rows.first() else {
        return Ok(None);
    };
    match (row.get("wal_status"), row.get("confirmed_flush_lsn")) {
        (Some("lost"), _) => Err(Error::SlotLost {
            slot: slot.to_string(),
        }),
        (_, Some(lsn)) => parse_lsn(slot, lsn).map(Some),
        (_, None) => Ok(None),
    }
}

/// Creates `slot` at the current position; with `export`, also returns a
/// snapshot of the data at that position, valid until the next command on
/// this connection.
async fn create_slot(
    client: &tokio_postgres::Client,
    slot: &str,
    export: bool,
) -> Result<(PgLsn, Option<String>), Error> {
    let snapshot = match export {
        true => "EXPORT_SNAPSHOT",
        false => "NOEXPORT_SNAPSHOT",
    };
    let rows = rows(
        client,
        &format!(r#"CREATE_REPLICATION_SLOT "{slot}" LOGICAL pgoutput {snapshot}"#),
    )
    .await?;
    let Some(row) = rows.first() else {
        return Err(Error::MissingSlotPosition {
            slot: slot.to_string(),
        });
    };
    let lsn = match row.get("consistent_point") {
        Some(lsn) => parse_lsn(slot, lsn)?,
        None => {
            return Err(Error::MissingSlotPosition {
                slot: slot.to_string(),
            })
        }
    };
    Ok((lsn, row.get("snapshot_name").map(str::to_string)))
}

/// The server's WAL position, on a standby the last replayed one.
async fn wal_position(client: &tokio_postgres::Client) -> Result<PgLsn, Error> {
    let rows = rows(
        client,
        "SELECT CASE WHEN pg_is_in_recovery() THEN pg_last_wal_replay_lsn() \
         ELSE pg_current_wal_lsn() END AS lsn",
    )
    .await?;
    match rows.first().map(|row| row.get("lsn")) {
        Some(Some(lsn)) => lsn.parse().map_err(|_| Error::InvalidWalPosition {
            lsn: lsn.to_string(),
        }),
        _ => Err(Error::InvalidWalPosition { lsn: String::new() }),
    }
}

fn parse_lsn(slot: &str, lsn: &str) -> Result<PgLsn, Error> {
    lsn.parse().map_err(|_| Error::InvalidSlotPosition {
        slot: slot.to_string(),
        lsn: lsn.to_string(),
    })
}

/// Starts streaming the publication's changes after `lsn`.
async fn start_replication(
    client: &tokio_postgres::Client,
    config: &Cdc,
    lsn: PgLsn,
) -> Result<Stream, Error> {
    let command = format!(
        r#"START_REPLICATION SLOT "{}" LOGICAL {lsn} ("proto_version" '1', "publication_names" '{}')"#,
        config.slot, config.publication
    );
    let duplex = client
        .copy_both_simple::<bytes::Bytes>(&command)
        .await
        .map_err(|source| Error::Command { source })?;
    Ok(Box::pin(LogicalReplicationStream::new(duplex)))
}

/// The rows `query` returns, as text.
async fn rows(
    client: &tokio_postgres::Client,
    query: &str,
) -> Result<Vec<tokio_postgres::SimpleQueryRow>, Error> {
    let messages = client
        .simple_query(query)
        .await
        .map_err(|source| Error::Command { source })?;
    Ok(messages
        .into_iter()
        .filter_map(|message| match message {
            SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect())
}

/// Cache key marking that the initial copy into `slot` finished.
fn copied_key(flow_key: &str, slot: &str) -> String {
    format!("flow.{flow_key}.postgres_initial_copy.{slot}")
}

/// A table of the publication: the query that reads the rows and columns the
/// stream sends of it, and its columns' type OIDs by name.
struct PublishedTable {
    schema: String,
    table: String,
    qualified: String,
    query: String,
    types: HashMap<String, u32>,
}

/// The server's version, such as `170002` for 17.2.
async fn server_version(client: &tokio_postgres::Client) -> Result<u32, Error> {
    let rows = rows(
        client,
        "SELECT current_setting('server_version_num') AS version",
    )
    .await?;
    match rows.first().and_then(|row| row.get("version")) {
        Some(version) => version.parse().map_err(|_| Error::InvalidServerVersion {
            version: version.to_string(),
        }),
        None => Err(Error::InvalidServerVersion {
            version: String::new(),
        }),
    }
}

/// The tables of `publication`. Each one is read with the publication's row
/// filter and column list (PostgreSQL 15+), without generated columns, and
/// without the rows of tables that inherit from it, as the stream sends them.
async fn published_tables(
    client: &tokio_postgres::Client,
    publication: &str,
) -> Result<Vec<PublishedTable>, Error> {
    let version = server_version(client).await?;
    let (row_filter, column_list) = match version >= 150_000 {
        true => (
            "p.rowfilter",
            "(SELECT string_agg(quote_ident(a), ', ' ORDER BY i) \
             FROM unnest(p.attnames) WITH ORDINALITY AS u(a, i))",
        ),
        false => ("NULL", "NULL"),
    };
    let not_generated = match version >= 120_000 {
        true => "AND attgenerated = ''",
        false => "",
    };
    let tables = rows(
        client,
        &format!(
            "SELECT p.schemaname, p.tablename, \
             quote_ident(p.schemaname) || '.' || quote_ident(p.tablename) AS qualified, \
             c.oid, c.relkind = 'p' AS partitioned, {row_filter} AS row_filter, \
             COALESCE({column_list}, \
               (SELECT string_agg(quote_ident(attname), ', ' ORDER BY attnum) FROM pg_attribute \
                WHERE attrelid = c.oid AND attnum > 0 AND NOT attisdropped {not_generated})) AS selected \
             FROM pg_publication_tables p \
             JOIN pg_namespace n ON n.nspname = p.schemaname \
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = p.tablename \
             WHERE p.pubname = '{publication}' ORDER BY 1, 2"
        ),
    )
    .await?;
    let mut published = Vec::with_capacity(tables.len());
    for table in tables {
        let (Some(schema), Some(name), Some(qualified), Some(oid), Some(selected)) = (
            table.get("schemaname"),
            table.get("tablename"),
            table.get("qualified"),
            table.get("oid"),
            table.get("selected"),
        ) else {
            continue;
        };
        let only = match table.get("partitioned") {
            Some("t") => "",
            _ => "ONLY ",
        };
        let filter = match table.get("row_filter") {
            Some(filter) => format!(" WHERE {filter}"),
            None => String::new(),
        };
        let columns = rows(
            client,
            &format!(
                "SELECT attname, atttypid FROM pg_attribute \
                 WHERE attrelid = {} AND attnum > 0 AND NOT attisdropped",
                oid.parse::<u32>().map_err(|_| Error::InvalidTableOid {
                    table: qualified.to_string(),
                })?
            ),
        )
        .await?;
        let types = columns
            .iter()
            .filter_map(|column| {
                let name = column.get("attname")?;
                let type_id = column.get("atttypid")?.parse().ok()?;
                Some((name.to_string(), type_id))
            })
            .collect();
        published.push(PublishedTable {
            schema: schema.to_string(),
            table: name.to_string(),
            qualified: qualified.to_string(),
            query: format!("SELECT {selected} FROM {only}{qualified}{filter}"),
            types,
        });
    }
    Ok(published)
}

/// A row read for the initial copy, as JSON typed like the streamed changes.
fn copied_row(table: &PublishedTable, row: &tokio_postgres::SimpleQueryRow) -> Value {
    let values = row
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| {
            let value = match (row.get(index), table.types.get(column.name())) {
                (None, _) => Value::Null,
                (Some(text), Some(type_id)) => value(*type_id, text),
                (Some(text), None) => Value::String(text.to_string()),
            };
            (column.name().to_string(), value)
        })
        .collect();
    Value::Object(values)
}

/// The events of the initial copy of `table`: one per `fetch` from its
/// cursor, with the fetched rows as an array.
fn copy_batches<'a>(
    client: &'a tokio_postgres::Client,
    fetch: &'a str,
    table: &'a PublishedTable,
    lsn: PgLsn,
    task_id: usize,
    task_type: &'static str,
) -> impl futures_util::Stream<Item = Result<Event, Error>> + Send + 'a {
    futures_util::stream::try_unfold(1usize, move |batch| async move {
        let fetched = rows(client, fetch).await?;
        if fetched.is_empty() {
            return Ok(None);
        }
        let data = fetched.iter().map(|row| copied_row(table, row)).collect();
        let event = change_event(
            format!("{lsn}-copy-{}.{}-{batch}", table.schema, table.table),
            change(
                &table.schema,
                &table.table,
                "copy",
                Value::Array(data),
                None,
                lsn,
            ),
            task_id,
            task_type,
        )?;
        Ok(Some((event, batch + 1)))
    })
}

/// Reads changes, reconnecting whenever the stream breaks.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Cdc>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<TaskContext>,
    task_type: &'static str,
}

impl Processor {
    /// The position to stream from: the slot's, or, for a new slot, where it
    /// starts. With `initial_copy` the copy runs first; for an existing slot
    /// it reads the rows as they are now, so the slot's pending changes are
    /// kept and the rows they touch arrive twice.
    async fn open_slot(
        &self,
        config: &Cdc,
        credentials: &Credentials,
        client: &tokio_postgres::Client,
    ) -> Result<PgLsn, Error> {
        let slot = &config.slot;
        let key = copied_key(&self.task_context.flow.id(), slot);
        let copy = match config.initial_copy {
            true => self
                .task_context
                .cache
                .get(&key)
                .await
                .map_err(|source| Error::Cache {
                    key: key.clone(),
                    source,
                })?
                .is_none(),
            false => false,
        };
        let (lsn, snapshot) = match (slot_position(client, slot).await?, copy) {
            (Some(lsn), false) => return Ok(lsn),
            (Some(lsn), true) => {
                warn!(
                    slot,
                    "No finished initial copy recorded, copying the tables and keeping the slot"
                );
                (lsn, None)
            }
            (None, false) => {
                let (lsn, _) = create_slot(client, slot, false).await?;
                warn!(
                    slot,
                    "Created the replication slot, changes are read from now on"
                );
                return Ok(lsn);
            }
            (None, true) => match create_slot(client, slot, true).await? {
                (lsn, Some(snapshot)) => (lsn, Some(snapshot)),
                (_, None) => {
                    return Err(Error::InvalidSnapshotName {
                        snapshot: String::new(),
                    })
                }
            },
        };
        self.copy(config, credentials, snapshot.as_deref(), lsn)
            .await?;
        self.task_context
            .cache
            .put(&key, bytes::Bytes::from_static(b"done"), None)
            .await
            .map_err(|source| Error::Cache { key, source })?;
        Ok(lsn)
    }

    /// Emits every row of the publication's tables as they were in
    /// `snapshot` at `lsn`, or as they are now without one, `copy_batch_size`
    /// rows per event and up to `max_in_flight` events in the flow at once.
    async fn copy(
        &self,
        config: &Cdc,
        credentials: &Credentials,
        snapshot: Option<&str>,
        lsn: PgLsn,
    ) -> Result<(), Error> {
        let begin = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
        let begin = match snapshot {
            None => begin.to_string(),
            Some(snapshot)
                if !snapshot.is_empty()
                    && snapshot.chars().all(|c| c.is_ascii_hexdigit() || c == '-') =>
            {
                format!("{begin}; SET TRANSACTION SNAPSHOT '{snapshot}'")
            }
            Some(snapshot) => {
                return Err(Error::InvalidSnapshotName {
                    snapshot: snapshot.to_string(),
                })
            }
        };
        let client = connect(config, credentials, false).await?;
        let command = |source| Error::Command { source };
        client
            .batch_execute("SET idle_in_transaction_session_timeout = 0")
            .await
            .map_err(command)?;
        client.batch_execute(&begin).await.map_err(command)?;
        let lsn = match snapshot {
            Some(_) => lsn,
            None => wal_position(&client).await?,
        };

        let retry = RetryConfig::merge(&self.task_context.retry, &config.retry);
        let delivery = Delivery {
            downstream: self.tx.is_some(),
            leaf_count: self.task_context.leaf_count,
            ack_timeout: config.ack_timeout,
            max_deliver: config.max_deliver,
            retry: &retry,
            cancellation: &self.task_context.cancellation_token,
        };
        let tx = self.tx.as_ref();
        let fetch = format!("FETCH {} FROM initial_copy", config.copy_batch_size);
        for table in published_tables(&client, &config.publication).await? {
            info!(table = %table.qualified, "Copying table");
            client
                .batch_execute(&format!(
                    "DECLARE initial_copy NO SCROLL CURSOR FOR {}",
                    table.query
                ))
                .await
                .map_err(command)?;
            let batches = copy_batches(&client, &fetch, &table, lsn, self.task_id, self.task_type);
            let delivered =
                delivery.deliver_each(batches, self.task_context.max_in_flight, tx, |source| {
                    Error::SendMessage { source }
                });
            let mut delivered = std::pin::pin!(delivered);
            while let Some(delivered) = delivered.try_next().await? {
                if delivered == Delivered::Stopped {
                    return Err(Error::CopyInterrupted);
                }
            }
            client
                .batch_execute("CLOSE initial_copy")
                .await
                .map_err(command)?;
        }
        client.batch_execute("COMMIT").await.map_err(command)?;
        info!("Initial copy finished");
        Ok(())
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<EventHandler, Error> {
        let config = self
            .config
            .render(&Map::new())
            .map_err(|source| Error::ConfigRender { source })?;
        validate_name("slot", &config.slot)?;
        validate_name("publication", &config.publication)?;
        let credentials = read_credentials(&config.credentials_path)?;
        let client = connect(&config, &credentials, true).await?;
        let flushed = self.open_slot(&config, &credentials, &client).await?;
        let stream = start_replication(&client, &config, flushed).await?;
        Ok(EventHandler {
            config: Arc::new(config),
            _client: client,
            stream,
            flushed,
            last_report: Instant::now(),
            retry_config: RetryConfig::merge(&self.task_context.retry, &self.config.retry),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config = RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let cancellation = self.task_context.cancellation_token.clone();
        let mut reconnect_backoff = retry_config.reconnect_strategy();
        loop {
            let init = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    self.init().await.map_err(|e| {
                        error!(error = %e, "Failed to start reading changes");
                        tokio_retry::RetryError::transient(e)
                    })
                },
            );
            let event_handler = tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                result = init => result?,
            };
            let started = Instant::now();
            let handled = event_handler.handle().await;
            match handled {
                _ if cancellation.is_cancelled() => return Ok(()),
                Ok(()) => return Ok(()),
                Err(e) => error!(error = %e, "Reading changes failed, reconnecting"),
            }
            if started.elapsed() >= retry_config.max_delay() {
                reconnect_backoff = retry_config.reconnect_strategy();
            }
            let delay = match reconnect_backoff.next() {
                Some(delay) => delay,
                None => retry_config.initial_backoff,
            };
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                _ = tokio::time::sleep(delay) => {}
            }
        }
    }
}

/// Builds a [`Processor`].
#[derive(Default)]
pub struct ProcessorBuilder {
    config: Option<Arc<Cdc>>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn config(mut self, config: Arc<Cdc>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(mut self, task_context: Arc<TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or(Error::MissingBuilderAttribute("config"))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or(Error::MissingBuilderAttribute("task_context"))?,
            task_type: self
                .task_type
                .ok_or(Error::MissingBuilderAttribute("task_type"))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_deser_minimal() {
        let yaml = r#"
            name: watch_orders
            credentials_path: /etc/postgres/credentials.json
            publication: orders_pub
            slot: flowgen_orders
        "#;
        let config: Cdc = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.publication, "orders_pub");
        assert_eq!(config.slot, "flowgen_orders");
        assert_eq!(config.connection_timeout, Duration::from_secs(30));
        assert!(config.max_deliver.is_none());
    }

    #[test]
    fn config_deser_rejects_unknown_fields_and_zero_max_deliver() {
        let base = "name: n\ncredentials_path: c\npublication: p\nslot: s\n";
        assert!(serde_yaml::from_str::<Cdc>(&format!("{base}tables: [a]\n")).is_err());
        assert!(serde_yaml::from_str::<Cdc>(&format!("{base}max_deliver: 0\n")).is_err());
    }

    #[test]
    fn names_must_be_plain_identifiers() {
        assert!(validate_name("slot", "flowgen_orders_1").is_ok());
        assert!(validate_name("slot", "").is_err());
        assert!(validate_name("slot", "Orders").is_err());
        assert!(validate_name("slot", "a'; DROP TABLE x; --").is_err());
        assert!(validate_name("slot", &"a".repeat(64)).is_err());
    }

    #[test]
    fn values_keep_their_json_type() {
        assert_eq!(value(Type::BOOL.oid(), "t"), Value::Bool(true));
        assert_eq!(value(Type::BOOL.oid(), "f"), Value::Bool(false));
        assert_eq!(
            value(Type::INT8.oid(), "9000000000"),
            Value::from(9_000_000_000i64)
        );
        assert_eq!(value(Type::FLOAT8.oid(), "2.5"), Value::from(2.5));
        assert_eq!(value(Type::FLOAT8.oid(), "NaN"), Value::from("NaN"));
        assert_eq!(
            value(Type::NUMERIC.oid(), "12345.678"),
            Value::from("12345.678")
        );
        assert_eq!(
            value(Type::JSONB.oid(), r#"{"a": 1}"#),
            serde_json::json!({"a": 1})
        );
        assert_eq!(
            value(Type::TIMESTAMPTZ.oid(), "2026-10-28 12:00:00+00"),
            Value::from("2026-10-28 12:00:00+00")
        );
    }

    #[test]
    fn postgres_now_counts_from_2000() {
        let since_2000 = Duration::from_micros(postgres_now() as u64);
        assert!(since_2000 > Duration::from_secs(26 * 365 * 24 * 3600));
    }
}
