//! Cache persisted to a local file with redb, for a single process without NATS.

use super::{CacheError, Error, WatchEvent};
use ::redb::{Database, ReadableDatabase, ReadableTable, Table, TableDefinition};
use bytes::Bytes;
use futures_util::stream::BoxStream;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_stream::wrappers::ReceiverStream;

const SWEEP_INTERVAL: Duration = Duration::from_secs(30);
/// Events a watcher may fall behind by before its stream ends, after which
/// it watches again and reads what it missed, as after a lost connection.
const WATCH_CAPACITY: usize = 4096;
/// The key of the bucket's last revision in its meta table.
const SEQUENCE: &str = "sequence";
/// The tables of a bucket, each named `<bucket>.<table>`.
const ENTRIES_TABLE: &str = "entries";
const META_TABLE: &str = "meta";
/// Revision and expiry, each a little-endian `u64`, before the value.
const HEADER: usize = 16;

type Source = Box<dyn std::error::Error + Send + Sync>;
/// The `CacheError` variant a failed operation reports.
type Failed = fn(Source) -> Error;
type Registry<K, V> = LazyLock<Mutex<HashMap<K, Weak<V>>>>;

type EntriesTable<'t> = Table<'t, &'static str, &'static [u8]>;
type MetaTable<'t> = Table<'t, &'static str, u64>;

/// One open database per file, shared by its buckets: redb locks the file.
static DATABASES: Registry<PathBuf, Database> = LazyLock::new(Default::default);

/// One instance per bucket, so every handle sees the same watchers.
static BUCKETS: Registry<(PathBuf, String), Bucket> = LazyLock::new(Default::default);

struct Watcher {
    prefix: String,
    tx: tokio::sync::mpsc::Sender<Result<WatchEvent, Error>>,
}

struct Bucket {
    db: Arc<Database>,
    entries: String,
    meta: String,
    watchers: Mutex<Vec<Watcher>>,
}

/// The `u64` at `at` in a stored entry's header, 0 when the entry is too short.
fn word(raw: &[u8], at: usize) -> u64 {
    match raw
        .get(at..at + 8)
        .and_then(|bytes| <[u8; 8]>::try_from(bytes).ok())
    {
        Some(bytes) => u64::from_le_bytes(bytes),
        None => 0,
    }
}

/// Whether a stored entry has not expired at `now`, read from its header only.
fn live(raw: &[u8], now: u64) -> bool {
    let expires_at = word(raw, 8);
    expires_at == 0 || expires_at > now
}

/// A stored value with its revision.
struct Stored {
    revision: u64,
    value: Bytes,
}

impl Stored {
    fn decode(raw: &[u8]) -> Self {
        let value = match raw.get(HEADER..) {
            Some(value) => value,
            None => &[],
        };
        Self {
            revision: word(raw, 0),
            value: Bytes::copy_from_slice(value),
        }
    }

    fn encode(revision: u64, expires_at: u64, value: &[u8]) -> Vec<u8> {
        let mut raw = Vec::with_capacity(HEADER + value.len());
        raw.extend_from_slice(&revision.to_le_bytes());
        raw.extend_from_slice(&expires_at.to_le_bytes());
        raw.extend_from_slice(value);
        raw
    }
}

fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_millis() as u64,
        Err(_) => 0,
    }
}

fn expiry(ttl_secs: Option<u64>) -> u64 {
    match ttl_secs {
        Some(secs) => now_ms().saturating_add(secs.saturating_mul(1000)),
        None => 0,
    }
}

fn boxed(error: impl Into<::redb::Error>) -> Source {
    Box::new(error.into())
}

/// Locks `mutex`, also after a panic elsewhere: it guards only registries and
/// watcher lists, which stay whole.
fn locked<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Sends `event` to the watchers of its key, dropping closed ones and ones
/// [`WATCH_CAPACITY`] events behind, whose streams then end.
fn broadcast(watchers: &mut Vec<Watcher>, event: WatchEvent) {
    let key = match &event {
        WatchEvent::Put { key, .. } | WatchEvent::Delete { key } => key.clone(),
    };
    watchers.retain(|watcher| {
        if !key.starts_with(&watcher.prefix) {
            return !watcher.tx.is_closed();
        }
        match watcher.tx.try_send(Ok(event.clone())) {
            Ok(()) => true,
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(prefix = %watcher.prefix, "Ended a cache watch that fell behind");
                false
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
        }
    });
}

/// A cache bucket in a redb file. Revisions grow across the whole bucket, as
/// in NATS, so a key deleted and created again never reuses a revision.
#[derive(Clone)]
pub struct RedbCache {
    bucket: Arc<Bucket>,
}

impl std::fmt::Debug for RedbCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedbCache")
            .field("bucket", &self.bucket.entries)
            .finish()
    }
}

impl RedbCache {
    /// Opens `bucket` in the file at `path`, creating both as needed.
    pub fn open(path: &Path, bucket: &str) -> Result<Self, Error> {
        let path = absolute(path)?;
        let mut buckets = locked(&BUCKETS);
        let id = (path.clone(), bucket.to_string());
        if let Some(open) = buckets.get(&id).and_then(Weak::upgrade) {
            return Ok(Self { bucket: open });
        }
        let opened = Arc::new(Bucket {
            db: database(&path)?,
            entries: format!("{bucket}.{ENTRIES_TABLE}"),
            meta: format!("{bucket}.{META_TABLE}"),
            watchers: Mutex::new(Vec::new()),
        });
        opened.create_tables().map_err(CacheError::OpenFailed)?;
        buckets.insert(id, Arc::downgrade(&opened));
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(sweep(Arc::downgrade(&opened)));
        }
        Ok(Self { bucket: opened })
    }

    /// Runs `op` on a blocking thread, as redb does file I/O.
    async fn blocking<T, F>(&self, failed: Failed, op: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&Bucket) -> Result<T, Error> + Send + 'static,
    {
        let bucket = Arc::clone(&self.bucket);
        tokio::task::spawn_blocking(move || op(&bucket))
            .await
            .map_err(|join| failed(Box::new(join)))?
    }

    /// Runs `op` in a write transaction on a blocking thread.
    async fn write<T, F>(&self, failed: Failed, op: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&mut Writer<'_>) -> Result<T, Error> + Send + 'static,
    {
        self.blocking(failed, move |bucket| bucket.write(failed, op))
            .await
    }

    async fn read(&self, key: &str) -> Result<Option<Stored>, Error> {
        let key = key.to_string();
        self.blocking(CacheError::GetFailed, move |bucket| bucket.read(&key))
            .await
    }
}

/// The path with its folder created and resolved through links, so one file
/// opened by two spellings shares one database.
fn absolute(path: &Path) -> Result<PathBuf, Error> {
    let open_failed = |source: std::io::Error| CacheError::OpenFailed(Box::new(source));
    let file = match path.file_name() {
        Some(file) => file,
        None => {
            return Err(open_failed(std::io::Error::other(
                "The cache path names no file",
            )))
        }
    };
    let folder = match path.parent() {
        Some(parent) if !parent.as_os_str().is_empty() => {
            std::fs::create_dir_all(parent).map_err(open_failed)?;
            parent
        }
        _ => Path::new("."),
    };
    Ok(std::fs::canonicalize(folder)
        .map_err(open_failed)?
        .join(file))
}

fn database(path: &Path) -> Result<Arc<Database>, Error> {
    let mut databases = locked(&DATABASES);
    if let Some(open) = databases.get(path).and_then(Weak::upgrade) {
        return Ok(open);
    }
    let db = Database::create(path).map_err(|source| CacheError::OpenFailed(boxed(source)))?;
    let db = Arc::new(db);
    databases.insert(path.to_path_buf(), Arc::downgrade(&db));
    Ok(db)
}

/// Removes expired keys every [`SWEEP_INTERVAL`], while the bucket is open.
async fn sweep(bucket: Weak<Bucket>) {
    let mut interval = tokio::time::interval(SWEEP_INTERVAL);
    interval.tick().await;
    loop {
        interval.tick().await;
        let Some(open) = bucket.upgrade() else { return };
        let removed = tokio::task::spawn_blocking(move || open.remove_expired()).await;
        match removed {
            Ok(Ok(())) => {}
            Ok(Err(error)) => tracing::warn!(error = %error, "Failed to remove expired cache keys"),
            Err(error) => tracing::warn!(error = %error, "Failed to remove expired cache keys"),
        }
    }
}

impl Bucket {
    fn entries(&self) -> TableDefinition<'_, &'static str, &'static [u8]> {
        TableDefinition::new(&self.entries)
    }

    fn meta(&self) -> TableDefinition<'_, &'static str, u64> {
        TableDefinition::new(&self.meta)
    }

    fn create_tables(&self) -> Result<(), Source> {
        let txn = self.db.begin_write().map_err(boxed)?;
        txn.open_table(self.entries()).map_err(boxed)?;
        txn.open_table(self.meta()).map_err(boxed)?;
        txn.commit().map_err(boxed)
    }

    /// Runs a write transaction, commits it and tells watchers of its changes
    /// under one lock, so they see writes in the order they committed.
    fn write<T>(
        &self,
        failed: Failed,
        op: impl FnOnce(&mut Writer<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let fail = |error: ::redb::Error| failed(Box::new(error));
        let mut watchers = locked(&self.watchers);
        let txn = self.db.begin_write().map_err(|e| fail(e.into()))?;
        let (result, events) = {
            let mut writer = Writer {
                entries: txn.open_table(self.entries()).map_err(|e| fail(e.into()))?,
                meta: txn.open_table(self.meta()).map_err(|e| fail(e.into()))?,
                failed,
                events: Vec::new(),
            };
            let result = op(&mut writer)?;
            (result, writer.events)
        };
        txn.commit().map_err(|e| fail(e.into()))?;
        for event in events {
            broadcast(&mut watchers, event);
        }
        Ok(result)
    }

    /// The live entry for `key`, if any.
    fn read(&self, key: &str) -> Result<Option<Stored>, Error> {
        let fail = |error: ::redb::Error| CacheError::GetFailed(Box::new(error));
        let txn = self.db.begin_read().map_err(|e| fail(e.into()))?;
        let table = txn.open_table(self.entries()).map_err(|e| fail(e.into()))?;
        match table.get(key).map_err(|e| fail(e.into()))? {
            Some(raw) if live(raw.value(), now_ms()) => Ok(Some(Stored::decode(raw.value()))),
            _ => Ok(None),
        }
    }

    /// The live keys under `prefix`, reading only the header of each entry.
    fn list(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let fail = |error: ::redb::Error| CacheError::ListKeysFailed(Box::new(error));
        let txn = self.db.begin_read().map_err(|e| fail(e.into()))?;
        let table = txn.open_table(self.entries()).map_err(|e| fail(e.into()))?;
        let now = now_ms();
        let mut keys = Vec::new();
        for item in table.range(prefix..).map_err(|e| fail(e.into()))? {
            let (key, raw) = item.map_err(|e| fail(e.into()))?;
            let key = key.value();
            if !key.starts_with(prefix) {
                break;
            }
            if live(raw.value(), now) {
                keys.push(key.to_string());
            }
        }
        Ok(keys)
    }

    /// The current value of each live key under `prefix`.
    fn replay(&self, prefix: &str) -> Result<Vec<WatchEvent>, Error> {
        let fail = |error: ::redb::Error| CacheError::WatchFailed(Box::new(error));
        let txn = self.db.begin_read().map_err(|e| fail(e.into()))?;
        let entries = txn.open_table(self.entries()).map_err(|e| fail(e.into()))?;
        let now = now_ms();
        let mut events = Vec::new();
        for item in entries.range(prefix..).map_err(|e| fail(e.into()))? {
            let (key, raw) = item.map_err(|e| fail(e.into()))?;
            let key = key.value();
            if !key.starts_with(prefix) {
                break;
            }
            if live(raw.value(), now) {
                events.push(WatchEvent::Put {
                    key: key.to_string(),
                    value: Stored::decode(raw.value()).value,
                });
            }
        }
        Ok(events)
    }

    /// Deletes the keys past their TTL, writing only when some are.
    fn remove_expired(&self) -> Result<(), Error> {
        let fail = |error: ::redb::Error| CacheError::DeleteFailed(Box::new(error));
        let now = now_ms();
        let expired = {
            let txn = self.db.begin_read().map_err(|e| fail(e.into()))?;
            let table = txn.open_table(self.entries()).map_err(|e| fail(e.into()))?;
            let mut expired = Vec::new();
            for item in table.iter().map_err(|e| fail(e.into()))? {
                let (key, raw) = item.map_err(|e| fail(e.into()))?;
                if !live(raw.value(), now) {
                    expired.push(key.value().to_string());
                }
            }
            expired
        };
        if expired.is_empty() {
            return Ok(());
        }
        self.write(CacheError::DeleteFailed, |writer| {
            for key in &expired {
                if writer.expired(key)? {
                    writer.remove(key)?;
                }
            }
            Ok(())
        })
    }
}

/// The tables of one write transaction and the changes it tells watchers of.
struct Writer<'t> {
    entries: EntriesTable<'t>,
    meta: MetaTable<'t>,
    failed: Failed,
    events: Vec<WatchEvent>,
}

impl Writer<'_> {
    fn fail(&self, error: impl Into<::redb::Error>) -> Error {
        (self.failed)(boxed(error))
    }

    /// Like [`Self::fail`], without borrowing the writer, for use while a table is borrowed.
    fn failer(&self) -> impl Fn(::redb::StorageError) -> Error + use<> {
        let failed = self.failed;
        move |error| failed(boxed(error))
    }

    /// The revision of the live entry for `key`, if any.
    fn live_revision(&self, key: &str) -> Result<Option<u64>, Error> {
        match self.entries.get(key).map_err(|e| self.fail(e))? {
            Some(raw) if live(raw.value(), now_ms()) => Ok(Some(word(raw.value(), 0))),
            _ => Ok(None),
        }
    }

    /// Whether `key` is stored and past its TTL.
    fn expired(&self, key: &str) -> Result<bool, Error> {
        match self.entries.get(key).map_err(|e| self.fail(e))? {
            Some(raw) => Ok(!live(raw.value(), now_ms())),
            None => Ok(false),
        }
    }

    fn next_revision(&mut self) -> Result<u64, Error> {
        let next = match self.meta.get(SEQUENCE).map_err(|e| self.fail(e))? {
            Some(revision) => revision.value() + 1,
            None => 1,
        };
        let fail = self.failer();
        self.meta.insert(SEQUENCE, next).map_err(fail)?;
        Ok(next)
    }

    /// Stores `value` under `key` with a new revision.
    fn store(&mut self, key: &str, value: Bytes, ttl_secs: Option<u64>) -> Result<u64, Error> {
        let revision = self.next_revision()?;
        let raw = Stored::encode(revision, expiry(ttl_secs), &value);
        let fail = self.failer();
        self.entries.insert(key, raw.as_slice()).map_err(fail)?;
        self.events.push(WatchEvent::Put {
            key: key.to_string(),
            value,
        });
        Ok(revision)
    }

    fn remove(&mut self, key: &str) -> Result<(), Error> {
        let fail = self.failer();
        self.entries.remove(key).map_err(fail)?;
        self.events.push(WatchEvent::Delete {
            key: key.to_string(),
        });
        Ok(())
    }
}

#[async_trait::async_trait]
impl super::Cache for RedbCache {
    async fn put(&self, key: &str, value: Bytes, ttl_secs: Option<u64>) -> Result<(), Error> {
        let key = key.to_string();
        self.write(CacheError::PutFailed, move |writer| {
            writer.store(&key, value, ttl_secs)
        })
        .await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Bytes>, Error> {
        Ok(self.read(key).await?.map(|stored| stored.value))
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let key = key.to_string();
        self.write(CacheError::DeleteFailed, move |writer| writer.remove(&key))
            .await
    }

    async fn create(&self, key: &str, value: Bytes, ttl_secs: Option<u64>) -> Result<u64, Error> {
        let key = key.to_string();
        self.write(CacheError::CreateFailed, move |writer| {
            match writer.live_revision(&key)? {
                Some(_) => Err(CacheError::AlreadyExists),
                None => writer.store(&key, value, ttl_secs),
            }
        })
        .await
    }

    async fn update(
        &self,
        key: &str,
        value: Bytes,
        expected_revision: u64,
        ttl_secs: Option<u64>,
    ) -> Result<u64, Error> {
        let key = key.to_string();
        self.write(CacheError::UpdateFailed, move |writer| {
            match writer.live_revision(&key)? {
                Some(actual) if actual == expected_revision => writer.store(&key, value, ttl_secs),
                Some(actual) => Err(CacheError::RevisionMismatch {
                    expected: expected_revision,
                    actual,
                }),
                None => Err(CacheError::NotFound),
            }
        })
        .await
    }

    async fn get_with_revision(&self, key: &str) -> Result<Option<(Bytes, u64)>, Error> {
        Ok(self
            .read(key)
            .await?
            .map(|stored| (stored.value, stored.revision)))
    }

    async fn delete_with_revision(&self, key: &str, expected_revision: u64) -> Result<(), Error> {
        let key = key.to_string();
        self.write(CacheError::DeleteFailed, move |writer| {
            match writer.live_revision(&key)? {
                Some(actual) if actual == expected_revision => writer.remove(&key),
                Some(actual) => Err(CacheError::RevisionMismatch {
                    expected: expected_revision,
                    actual,
                }),
                None => Err(CacheError::NotFound),
            }
        })
        .await
    }

    async fn get_revision(&self, key: &str) -> Result<Option<u64>, Error> {
        Ok(self.read(key).await?.map(|stored| stored.revision))
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>, Error> {
        let prefix = prefix.to_string();
        self.blocking(CacheError::ListKeysFailed, move |bucket| {
            bucket.list(&prefix)
        })
        .await
    }

    /// Holding the watchers lock while replaying orders the replay before the
    /// events of any write that commits after it.
    async fn watch(
        &self,
        prefix: &str,
        include_history: bool,
    ) -> Result<BoxStream<'static, Result<WatchEvent, Error>>, Error> {
        let prefix = prefix.to_string();
        let rx = self
            .blocking(CacheError::WatchFailed, move |bucket| {
                let mut watchers = locked(&bucket.watchers);
                let replayed = match include_history {
                    true => bucket.replay(&prefix)?,
                    false => Vec::new(),
                };
                let (tx, rx) = tokio::sync::mpsc::channel(replayed.len() + WATCH_CAPACITY);
                for event in replayed {
                    let _ = tx.try_send(Ok(event));
                }
                watchers.push(Watcher { prefix, tx });
                Ok(rx)
            })
            .await?;
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::Cache;
    use futures_util::StreamExt;

    fn file() -> PathBuf {
        std::env::temp_dir()
            .join(format!("flowgen_test_{}", uuid::Uuid::now_v7()))
            .join(crate::workspace::CACHE_FILE)
    }

    fn open(path: &Path, bucket: &str) -> RedbCache {
        RedbCache::open(path, bucket).unwrap()
    }

    fn text(value: Bytes) -> String {
        String::from_utf8(value.to_vec()).unwrap()
    }

    async fn next_event(events: &mut BoxStream<'static, Result<WatchEvent, Error>>) -> WatchEvent {
        tokio::time::timeout(Duration::from_secs(5), events.next())
            .await
            .expect("A watch event arrives")
            .expect("The watch is open")
            .unwrap()
    }

    #[tokio::test]
    async fn values_are_stored_listed_and_deleted() {
        let cache = open(&file(), "runtime");
        cache.put("flows.a", Bytes::from("1"), None).await.unwrap();
        cache.put("flows.b", Bytes::from("2"), None).await.unwrap();
        cache.put("other", Bytes::from("3"), None).await.unwrap();

        assert_eq!(cache.get("flows.a").await.unwrap(), Some(Bytes::from("1")));
        assert_eq!(
            cache.list_keys("flows.").await.unwrap(),
            vec!["flows.a", "flows.b"]
        );

        cache.delete("flows.a").await.unwrap();
        assert_eq!(cache.get("flows.a").await.unwrap(), None);
        assert_eq!(cache.list_keys("flows.").await.unwrap(), vec!["flows.b"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn one_file_reached_through_a_link_is_one_bucket() {
        let path = file();
        let folder = path.parent().unwrap();
        let linked = folder.with_extension("link");
        let direct = open(&path, "system");
        std::os::unix::fs::symlink(folder, &linked).unwrap();
        let through_link = open(&linked.join(crate::workspace::CACHE_FILE), "system");
        let mut events = direct.watch("flows.", false).await.unwrap();

        through_link
            .put("flows.a", Bytes::from("1"), None)
            .await
            .unwrap();

        assert_eq!(direct.get("flows.a").await.unwrap(), Some(Bytes::from("1")));
        assert!(
            matches!(next_event(&mut events).await, WatchEvent::Put { key, .. } if key == "flows.a")
        );
    }

    #[tokio::test]
    async fn create_update_and_delete_check_the_revision() {
        let cache = open(&file(), "system");
        let first = cache.create("lease", Bytes::from("a"), None).await.unwrap();
        assert!(matches!(
            cache.create("lease", Bytes::from("b"), None).await,
            Err(CacheError::AlreadyExists)
        ));

        let second = cache
            .update("lease", Bytes::from("b"), first, None)
            .await
            .unwrap();
        assert!(second > first);
        assert!(matches!(
            cache.update("lease", Bytes::from("c"), first, None).await,
            Err(CacheError::RevisionMismatch { .. })
        ));
        assert!(matches!(
            cache.delete_with_revision("lease", first).await,
            Err(CacheError::RevisionMismatch { .. })
        ));
        assert_eq!(
            cache.get_with_revision("lease").await.unwrap(),
            Some((Bytes::from("b"), second))
        );

        cache.delete_with_revision("lease", second).await.unwrap();
        assert!(matches!(
            cache.update("lease", Bytes::from("d"), second, None).await,
            Err(CacheError::NotFound)
        ));
        let third = cache.create("lease", Bytes::from("d"), None).await.unwrap();
        assert!(third > second, "A key created again gets a new revision");
    }

    #[tokio::test]
    async fn a_key_past_its_ttl_is_gone() {
        let cache = open(&file(), "runtime");
        let revision = cache
            .create("lease", Bytes::from("a"), Some(1))
            .await
            .unwrap();
        assert_eq!(cache.get("lease").await.unwrap(), Some(Bytes::from("a")));

        tokio::time::sleep(Duration::from_millis(1100)).await;

        assert_eq!(cache.get("lease").await.unwrap(), None);
        assert_eq!(cache.get_revision("lease").await.unwrap(), None);
        assert!(cache.list_keys("").await.unwrap().is_empty());
        assert!(matches!(
            cache
                .update("lease", Bytes::from("b"), revision, None)
                .await,
            Err(CacheError::NotFound)
        ));
        cache.create("lease", Bytes::from("b"), None).await.unwrap();
        assert_eq!(cache.get("lease").await.unwrap(), Some(Bytes::from("b")));
    }

    #[tokio::test]
    async fn the_sweep_deletes_only_expired_keys_and_tells_watchers() {
        let cache = open(&file(), "runtime");
        cache.put("a", Bytes::from("short"), Some(1)).await.unwrap();
        cache.put("b", Bytes::from("kept"), None).await.unwrap();
        let mut events = cache.watch("", false).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;

        let bucket = Arc::clone(&cache.bucket);
        tokio::task::spawn_blocking(move || bucket.remove_expired())
            .await
            .unwrap()
            .unwrap();

        assert!(matches!(next_event(&mut events).await, WatchEvent::Delete { key } if key == "a"));
        assert_eq!(cache.get("b").await.unwrap(), Some(Bytes::from("kept")));
    }

    #[tokio::test]
    async fn values_and_revisions_survive_reopening_the_file() {
        let path = file();
        let revision = {
            let cache = open(&path, "runtime");
            cache.put("k", Bytes::from("kept"), None).await.unwrap();
            cache.get_revision("k").await.unwrap().unwrap()
        };
        let closed = DATABASES
            .lock()
            .unwrap()
            .get(&absolute(&path).unwrap())
            .and_then(Weak::upgrade)
            .is_none();
        assert!(closed, "The file is closed once its last handle drops");

        let cache = open(&path, "runtime");
        assert_eq!(cache.get("k").await.unwrap(), Some(Bytes::from("kept")));
        let next = cache.create("other", Bytes::from("x"), None).await.unwrap();
        assert!(next > revision, "Revisions continue after reopening");
    }

    #[tokio::test]
    async fn buckets_in_one_file_are_separate() {
        let path = file();
        let runtime = open(&path, "runtime");
        let system = open(&path, "system");
        runtime
            .put("k", Bytes::from("runtime"), None)
            .await
            .unwrap();

        assert_eq!(system.get("k").await.unwrap(), None);
        assert!(system.list_keys("").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn handles_to_one_bucket_share_watchers() {
        let path = file();
        let watched = open(&path, "runtime");
        let written = open(&path, "runtime");
        let mut events = watched.watch("flows.", false).await.unwrap();

        written.put("other", Bytes::from("x"), None).await.unwrap();
        written
            .put("flows.a", Bytes::from("1"), None)
            .await
            .unwrap();
        written.delete("flows.a").await.unwrap();

        match next_event(&mut events).await {
            WatchEvent::Put { key, value } => {
                assert_eq!((key.as_str(), text(value).as_str()), ("flows.a", "1"))
            }
            other => panic!("Expected a put, got {other:?}"),
        }
        match next_event(&mut events).await {
            WatchEvent::Delete { key } => assert_eq!(key, "flows.a"),
            other => panic!("Expected a delete, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_watch_with_history_replays_current_values_then_follows() {
        let cache = open(&file(), "runtime");
        for value in ["v1", "v2", "v3"] {
            cache.put("k", Bytes::from(value), None).await.unwrap();
        }

        let mut events = cache.watch("k", true).await.unwrap();
        match next_event(&mut events).await {
            WatchEvent::Put { value, .. } => assert_eq!(text(value), "v3"),
            other => panic!("Expected a put, got {other:?}"),
        }

        cache.put("k", Bytes::from("v4"), None).await.unwrap();
        match next_event(&mut events).await {
            WatchEvent::Put { value, .. } => assert_eq!(text(value), "v4"),
            other => panic!("Expected a put, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_commits_only_while_it_can_tell_watchers() {
        let cache = open(&file(), "runtime");
        let bucket = Arc::clone(&cache.bucket);
        let (held_tx, held_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = std::thread::spawn(move || {
            let _watchers = locked(&bucket.watchers);
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        held_rx.recv().unwrap();
        let writer = {
            let cache = cache.clone();
            tokio::spawn(async move { cache.put("k", Bytes::from("v"), None).await })
        };
        tokio::time::sleep(Duration::from_millis(300)).await;

        let while_held = cache.get("k").await.unwrap();
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        writer.await.unwrap().unwrap();

        assert_eq!(
            while_held, None,
            "Nothing commits before watchers can be told, so they see writes in commit order"
        );
        assert_eq!(cache.get("k").await.unwrap(), Some(Bytes::from("v")));
    }

    #[tokio::test]
    async fn a_watcher_that_falls_behind_is_ended() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let mut watchers = vec![Watcher {
            prefix: "k".to_string(),
            tx,
        }];
        let put = |value: &'static str| WatchEvent::Put {
            key: "k".to_string(),
            value: Bytes::from(value),
        };

        broadcast(&mut watchers, put("1"));
        broadcast(&mut watchers, put("2"));

        assert!(
            watchers.is_empty(),
            "The watcher is dropped once its channel is full"
        );
        assert!(matches!(rx.recv().await, Some(Ok(WatchEvent::Put { value, .. })) if value == "1"));
        assert!(
            rx.recv().await.is_none(),
            "Its stream ends after what it got"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn only_one_of_concurrent_creates_wins() {
        let cache = open(&file(), "system");
        let attempts = (0..16).map(|i| {
            let cache = cache.clone();
            tokio::spawn(async move {
                cache
                    .create("lease", Bytes::from(i.to_string()), None)
                    .await
            })
        });
        let results = futures_util::future::join_all(attempts).await;
        let won = results
            .into_iter()
            .filter(|result| matches!(result, Ok(Ok(_))))
            .count();
        assert_eq!(won, 1);
    }
}
