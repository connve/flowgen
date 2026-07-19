//! Integration tests for `flowgen_nats::cache::Cache` against a real
//! NATS JetStream instance running in a Docker container.
//!
//! Covers every method on the [`flowgen_core::cache::Cache`] trait
//! (`put/get/delete`, `create/update/delete_with_revision`,
//! `list_keys`, `watch`) plus regression cases that map to real
//! production incidents (bucket config divergence, per-message TTL).
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use bytes::Bytes;
use flowgen_core::cache::{Cache as CacheTrait, CacheError, WatchEvent};
use flowgen_nats::cache::CacheBuilder;
use futures_util::StreamExt;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn start_nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.11.8-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start nats container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("map nats port");
    let url = format!("nats://127.0.0.1:{port}");
    (container, url)
}

async fn cache_with_bucket(url: &str, bucket: &str) -> impl CacheTrait {
    CacheBuilder::new()
        .url(url.to_string())
        .history(64)
        .tombstone_ttl(Duration::from_secs(3600))
        .build()
        .expect("build cache")
        .init(bucket)
        .await
        .expect("init cache")
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn put_then_get_roundtrips() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_put_get").await;

    cache
        .put("greeting", Bytes::from_static(b"hello"), None)
        .await
        .expect("put");
    let fetched = cache.get("greeting").await.expect("get").expect("present");
    assert_eq!(&fetched[..], b"hello");
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn get_missing_key_returns_none() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_missing").await;

    assert!(cache.get("no_such_key").await.expect("get").is_none());
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn delete_removes_key() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_delete").await;

    cache
        .put("k", Bytes::from_static(b"v"), None)
        .await
        .expect("put");
    cache.delete("k").await.expect("delete");
    assert!(cache.get("k").await.expect("get").is_none());
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn create_returns_revision_and_rejects_existing_key() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_create").await;

    let rev = cache
        .create("lease", Bytes::from_static(b"holder-a"), None)
        .await
        .expect("first create succeeds");
    assert!(rev > 0);

    let err = cache
        .create("lease", Bytes::from_static(b"holder-b"), None)
        .await
        .expect_err("second create must fail");
    assert!(
        matches!(err, CacheError::AlreadyExists),
        "expected AlreadyExists, got {err:?}"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn update_requires_matching_revision() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_update").await;

    let rev = cache
        .create("counter", Bytes::from_static(b"1"), None)
        .await
        .expect("create");
    let next = cache
        .update("counter", Bytes::from_static(b"2"), rev, None)
        .await
        .expect("update with correct revision");
    assert!(next > rev);

    let err = cache
        .update("counter", Bytes::from_static(b"3"), rev, None)
        .await
        .expect_err("stale revision must be rejected");
    assert!(matches!(err, CacheError::RevisionMismatch { .. }));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn delete_with_revision_enforces_ownership() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_del_rev").await;

    let rev = cache
        .create("lease", Bytes::from_static(b"me"), None)
        .await
        .expect("create");

    let err = cache
        .delete_with_revision("lease", rev + 999)
        .await
        .expect_err("wrong revision must fail");
    assert!(matches!(err, CacheError::RevisionMismatch { .. }));

    cache
        .delete_with_revision("lease", rev)
        .await
        .expect("delete with matching revision");
    assert!(cache.get("lease").await.expect("get").is_none());
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn get_with_revision_returns_value_and_revision() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_get_rev").await;

    let rev = cache
        .create("k", Bytes::from_static(b"v1"), None)
        .await
        .expect("create");
    let (value, seen) = cache
        .get_with_revision("k")
        .await
        .expect("get_with_revision")
        .expect("present");
    assert_eq!(&value[..], b"v1");
    assert_eq!(seen, rev);
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn list_keys_returns_only_entries_under_prefix() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_list").await;

    cache
        .put("orders.a", Bytes::from_static(b"1"), None)
        .await
        .expect("put a");
    cache
        .put("orders.b", Bytes::from_static(b"2"), None)
        .await
        .expect("put b");
    cache
        .put("payments.c", Bytes::from_static(b"3"), None)
        .await
        .expect("put c");

    let mut orders = cache.list_keys("orders.").await.expect("list");
    orders.sort();
    assert_eq!(orders, vec!["orders.a", "orders.b"]);

    let payments = cache.list_keys("payments.").await.expect("list");
    assert_eq!(payments, vec!["payments.c"]);
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn watch_emits_put_and_delete_events() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_watch").await;

    let mut stream = cache
        .watch("watched", false)
        .await
        .expect("watch subscription");

    cache
        .put("watched.k", Bytes::from_static(b"first"), None)
        .await
        .expect("put");
    let put = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("put event arrives")
        .expect("stream open")
        .expect("watch event");
    assert!(matches!(
        put,
        WatchEvent::Put { key, value } if key == "watched.k" && &value[..] == b"first"
    ));

    cache.delete("watched.k").await.expect("delete");
    let del = tokio::time::timeout(Duration::from_secs(2), stream.next())
        .await
        .expect("delete event arrives")
        .expect("stream open")
        .expect("watch event");
    assert!(matches!(del, WatchEvent::Delete { key } if key == "watched.k"));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn watch_include_history_replays_existing_entries_first() {
    let (_nats, url) = start_nats().await;
    let cache = cache_with_bucket(&url, "flowgen_watch_hist").await;

    cache
        .put("h.a", Bytes::from_static(b"1"), None)
        .await
        .expect("put a");
    cache
        .put("h.b", Bytes::from_static(b"2"), None)
        .await
        .expect("put b");

    let mut stream = cache.watch("h", true).await.expect("watch");
    let mut seen: Vec<String> = Vec::new();
    for _ in 0..2 {
        let ev = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .expect("history event arrives")
            .expect("stream open")
            .expect("event");
        if let WatchEvent::Put { key, .. } = ev {
            seen.push(key);
        }
    }
    seen.sort();
    assert_eq!(seen, vec!["h.a", "h.b"]);
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn put_with_ttl_expires_the_value() {
    // Uses a 1s tombstone TTL so the server sweeps expired
    // per-message TTL entries fast enough for the test to
    // deterministically observe expiry. Production defaults to
    // 3600s tombstone TTL, which yields much slower sweep cadence.
    let (_nats, url) = start_nats().await;
    let cache = CacheBuilder::new()
        .url(url)
        .history(64)
        .tombstone_ttl(Duration::from_secs(1))
        .build()
        .expect("build cache")
        .init("flowgen_ttl")
        .await
        .expect("init cache");

    cache
        .put("short", Bytes::from_static(b"gone soon"), Some(1))
        .await
        .expect("put with 1s TTL");
    assert!(cache.get("short").await.expect("get").is_some());

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(cache.get("short").await.expect("get").is_none());
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn native_kv_create_with_ttl_expires_the_value() {
    // Control test using async-nats' native `create_with_ttl` with
    // the same `limit_markers` value the upstream KV TTL test uses.
    // If this passes with a short marker window and our version
    // fails with a long one, tombstone TTL controls sweep cadence
    // and our production default (3600s) is too long for tests.
    let (_nats, url) = start_nats().await;
    let client = async_nats::connect(&url).await.expect("nats connect");
    let js = async_nats::jetstream::new(client);
    let kv = js
        .create_key_value(async_nats::jetstream::kv::Config {
            bucket: "native_ttl".to_string(),
            history: 64,
            limit_markers: Some(Duration::from_secs(1)),
            ..Default::default()
        })
        .await
        .expect("create native kv bucket");

    kv.create_with_ttl(
        "short",
        Bytes::from_static(b"gone soon"),
        Duration::from_secs(1),
    )
    .await
    .expect("native create_with_ttl");
    assert!(kv.get("short").await.expect("get").is_some());

    tokio::time::sleep(Duration::from_secs(5)).await;
    assert!(
        kv.get("short").await.expect("get").is_none(),
        "native KV TTL must expire — if this fails the server config is at fault"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn two_buckets_from_the_same_builder_share_configuration() {
    // Regression: the split-init bug wired only one bucket with
    // `tombstone_ttl`, so the other rejected per-message TTL puts.
    // The unified `App::init_cache` now goes through one builder, so
    // both buckets must accept the same operation set.
    let (_nats, url) = start_nats().await;

    let builder = || {
        CacheBuilder::new()
            .url(url.clone())
            .history(64)
            .tombstone_ttl(Duration::from_secs(3600))
    };

    let flows = builder()
        .build()
        .expect("build flows cache")
        .init("flowgen_system")
        .await
        .expect("init flows");
    let runtime = builder()
        .build()
        .expect("build runtime cache")
        .init("flowgen_cache")
        .await
        .expect("init runtime");

    for cache in [&flows, &runtime] {
        cache
            .put("probe", Bytes::from_static(b"ok"), Some(60))
            .await
            .expect("both buckets must accept per-message TTL puts");
    }
}
