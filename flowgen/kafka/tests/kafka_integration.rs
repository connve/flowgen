//! Integration tests for the `kafka_produce` and `kafka_subscribe` tasks
//! against a real Kafka broker in a Docker container.
//!
//! The broker runs in KRaft (combined controller + broker) mode as a
//! single node. The host port is pinned before the container starts so
//! `KAFKA_ADVERTISED_LISTENERS` points at an address the test client can
//! actually reach (`127.0.0.1:<host_port>`), which Kafka requires or it
//! advertises a container-internal address.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use flowgen_core::event::{Event, EventBuilder, EventData};
use flowgen_kafka::config::{Produce, StartOffset, Subscribe};
use flowgen_kafka::produce::ProducerBuilder;
use flowgen_kafka::subscribe::SubscriberBuilder;
use rskafka::client::partition::{Compression, UnknownTopicHandling};
use rskafka::record::{Record, RecordAndOffset};
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{ExecCommand, IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;

/// Kafka integration tests start a real broker container. Running several
/// of them in parallel exhausts Docker resources on typical developer
/// machines and causes startup timeouts, so they serialize on this mutex.
static KAFKA_TEST_MUTEX: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn lock_kafka_test() -> tokio::sync::MutexGuard<'static, ()> {
    KAFKA_TEST_MUTEX.lock().await
}

async fn start_kafka() -> (ContainerAsync<GenericImage>, String) {
    // Reserve a host port and release it again; the container binds it via
    // `with_mapped_port` so the broker can advertise a reachable listener.
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).expect("bind free host port");
    let host_port = listener.local_addr().expect("local addr").port();
    drop(listener);

    let container = GenericImage::new("apache/kafka", "3.8.0")
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_mapped_port(host_port, 9092.tcp())
        .with_env_var("KAFKA_NODE_ID", "1")
        .with_env_var("KAFKA_PROCESS_ROLES", "broker,controller")
        .with_env_var(
            "KAFKA_LISTENERS",
            "PLAINTEXT://:9092,INTERNAL://:19092,CONTROLLER://:9093",
        )
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://127.0.0.1:{host_port},INTERNAL://localhost:19092"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,INTERNAL:PLAINTEXT",
        )
        .with_env_var("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
        .with_env_var("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .with_env_var("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .with_startup_timeout(Duration::from_secs(120))
        .start()
        .await
        .expect("start kafka container");

    (container, format!("127.0.0.1:{host_port}"))
}

fn test_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
        as Arc<dyn flowgen_core::cache::Cache>;
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("build TaskContext"),
    )
}

async fn spawn_producer(config: Produce) -> (mpsc::Sender<Event>, mpsc::Receiver<Event>) {
    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, out_rx) = mpsc::channel(4);

    let processor = ProducerBuilder::new()
        .config(Arc::new(config))
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(0)
        .task_type("kafka_producer")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build producer");

    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = processor.run().await;
    });

    (in_tx, out_rx)
}

fn drive_event(data: serde_json::Value) -> Event {
    EventBuilder::new()
        .subject("trigger".to_string())
        .data(EventData::Json(data))
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event")
}

async fn kafka_client(brokers: &str) -> rskafka::client::Client {
    rskafka::client::ClientBuilder::new(vec![brokers.to_string()])
        .build()
        .await
        .expect("connect to kafka")
}

async fn read_partition(brokers: &str, topic: &str, partition: i32) -> Vec<RecordAndOffset> {
    kafka_client(brokers)
        .await
        .partition_client(topic, partition, UnknownTopicHandling::Retry)
        .await
        .expect("partition client")
        .fetch_records(0, 1..1_000_000, 1_000)
        .await
        .expect("fetch records")
        .records
}

async fn topic_configs(kafka: &ContainerAsync<GenericImage>, topic: &str) -> String {
    let mut result = kafka
        .exec(ExecCommand::new([
            "/opt/kafka/bin/kafka-configs.sh",
            "--bootstrap-server",
            "localhost:19092",
            "--entity-type",
            "topics",
            "--entity-name",
            topic,
            "--describe",
        ]))
        .await
        .expect("run kafka-configs.sh");
    String::from_utf8(result.stdout_to_vec().await.expect("read stdout")).expect("utf8 stdout")
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn produce_round_trips_through_real_kafka() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let (produce_tx, mut produce_rx) = spawn_producer(Produce {
        name: "produce_customer".to_string(),
        brokers: brokers.clone(),
        topic: "customers".to_string(),
        create_or_update: true,
        ..Default::default()
    })
    .await;
    produce_tx
        .send(drive_event(
            serde_json::json!({"name": "Ada", "status": "active"}),
        ))
        .await
        .expect("send produce event");
    let result = tokio::time::timeout(Duration::from_secs(10), produce_rx.recv())
        .await
        .expect("producer emits result")
        .expect("channel open")
        .data_as_json()
        .expect("json");
    assert_eq!(
        result.get("topic").and_then(|v| v.as_str()),
        Some("customers")
    );
    assert!(
        result.get("partition").is_some(),
        "produce result must carry partition, got {result:?}"
    );
    assert!(
        result.get("offset").is_some(),
        "produce result must carry offset, got {result:?}"
    );

    let records = read_partition(&brokers, "customers", 0).await;
    let payload: serde_json::Value =
        serde_json::from_slice(records[0].record.value.as_deref().expect("payload"))
            .expect("json payload");
    assert_eq!(
        payload,
        serde_json::json!({"name": "Ada", "status": "active"})
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn produce_to_missing_topic_without_creation_fails() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, mut out_rx) = mpsc::channel(4);

    let processor = ProducerBuilder::new()
        .config(Arc::new(Produce {
            name: "produce_missing".to_string(),
            brokers,
            topic: "does_not_exist".to_string(),
            create_or_update: false,
            retry: Some(flowgen_core::retry::RetryConfig {
                max_attempts: Some(1),
                initial_backoff: Duration::from_millis(1),
            }),
            ..Default::default()
        }))
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(0)
        .task_type("kafka_producer")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build producer");

    let handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        processor.run().await
    });

    let result = tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("producer init fails fast")
        .expect("task did not panic");
    assert!(
        matches!(
            result,
            Err(flowgen_kafka::produce::Error::TopicNotFound { .. })
        ),
        "missing topic without create_or_update must fail init, got {result:?}"
    );

    // No result event was emitted downstream.
    drop(in_tx);
    assert!(
        out_rx.try_recv().is_err(),
        "failed init must not emit events"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn produce_emits_one_result_per_message() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let (produce_tx, mut produce_rx) = spawn_producer(Produce {
        name: "produce_batch".to_string(),
        brokers: brokers.clone(),
        topic: "batch".to_string(),
        message_key: Some("key-{{event.id}}".to_string()),
        create_or_update: true,
        ..Default::default()
    })
    .await;

    for name in ["Ada", "Grace", "Katherine"] {
        produce_tx
            .send(drive_event(serde_json::json!({"name": name, "batch": "x"})))
            .await
            .expect("send produce event");
        let _ = tokio::time::timeout(Duration::from_secs(10), produce_rx.recv())
            .await
            .expect("producer emits result");
    }

    let records = read_partition(&brokers, "batch", 0).await;
    assert_eq!(records.len(), 3);

    let mut offsets = Vec::new();
    for (expected, message) in ["Ada", "Grace", "Katherine"].into_iter().zip(&records) {
        let payload: serde_json::Value =
            serde_json::from_slice(message.record.value.as_deref().expect("payload"))
                .expect("json payload");
        assert_eq!(payload.get("name").and_then(|v| v.as_str()), Some(expected));
        offsets.push(message.offset);

        // The incoming events carry no id, so the producer patches a UUID
        // v7 fallback into the render context; the rendered message key
        // must therefore be `key-<uuid>`.
        let key = std::str::from_utf8(message.record.key.as_deref().expect("message key"))
            .expect("utf8 message key");
        let id = key
            .strip_prefix("key-")
            .expect("rendered key carries the template prefix");
        assert!(
            uuid::Uuid::parse_str(id).is_ok(),
            "message key must end in a patched UUID fallback, got {key:?}"
        );
    }
    offsets.sort_unstable();
    assert_eq!(offsets, vec![0, 1, 2]);
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn created_topic_uses_configured_partitions() {
    let _lock = lock_kafka_test().await;
    let (kafka, brokers) = start_kafka().await;

    let (produce_tx, mut produce_rx) = spawn_producer(Produce {
        name: "produce_partitioned".to_string(),
        brokers: brokers.clone(),
        topic: "partitioned".to_string(),
        create_or_update: true,
        topic_options: flowgen_kafka::config::TopicOptions {
            partitions: 3,
            replication_factor: 1,
            retention: Some(Duration::from_secs(7 * 24 * 60 * 60)),
            config: [("cleanup.policy".to_string(), serde_json::json!("delete"))]
                .into_iter()
                .collect(),
        },
        ..Default::default()
    })
    .await;
    produce_tx
        .send(drive_event(serde_json::json!({"n": 1})))
        .await
        .expect("send produce event");
    tokio::time::timeout(Duration::from_secs(10), produce_rx.recv())
        .await
        .expect("producer emits result")
        .expect("channel open");

    let topics = kafka_client(&brokers)
        .await
        .list_topics()
        .await
        .expect("list topics");
    let topic = topics
        .iter()
        .find(|t| t.name == "partitioned")
        .expect("topic exists");
    let configs = topic_configs(&kafka, "partitioned").await;

    assert_eq!(topic.partitions.len(), 3);
    assert!(
        configs.contains("retention.ms=604800000"),
        "retention.ms must be set, got {configs}"
    );
    assert!(
        configs.contains("cleanup.policy=delete"),
        "cleanup.policy must be set, got {configs}"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn a_just_created_topic_is_found_by_the_next_task() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let (creator_tx, mut creator_rx) = spawn_producer(Produce {
        name: "create_shared".to_string(),
        brokers: brokers.clone(),
        topic: "shared".to_string(),
        create_or_update: true,
        ..Default::default()
    })
    .await;
    creator_tx
        .send(drive_event(serde_json::json!({"n": 1})))
        .await
        .expect("send creator event");
    tokio::time::timeout(Duration::from_secs(10), creator_rx.recv())
        .await
        .expect("creator emits result")
        .expect("channel open");

    let (reuse_tx, mut reuse_rx) = spawn_producer(Produce {
        name: "reuse_shared".to_string(),
        brokers: brokers.clone(),
        topic: "shared".to_string(),
        create_or_update: false,
        retry: Some(flowgen_core::retry::RetryConfig {
            max_attempts: Some(1),
            initial_backoff: Duration::from_millis(1),
        }),
        ..Default::default()
    })
    .await;
    reuse_tx
        .send(drive_event(serde_json::json!({"n": 2})))
        .await
        .expect("send reuse event");
    let result = tokio::time::timeout(Duration::from_secs(10), reuse_rx.recv())
        .await
        .expect("second producer emits result")
        .expect("channel open");

    assert_eq!(
        result.error, None,
        "a topic created moments ago must not read as missing"
    );
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn missing_topic_is_not_created_by_the_existence_check() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let (produce_tx, mut produce_rx) = spawn_producer(Produce {
        name: "produce_absent".to_string(),
        brokers: brokers.clone(),
        topic: "absent".to_string(),
        create_or_update: false,
        retry: Some(flowgen_core::retry::RetryConfig {
            max_attempts: Some(1),
            initial_backoff: Duration::from_millis(1),
        }),
        ..Default::default()
    })
    .await;
    produce_tx
        .send(drive_event(serde_json::json!({"n": 1})))
        .await
        .expect("send produce event");
    let _ = tokio::time::timeout(Duration::from_secs(10), produce_rx.recv()).await;

    let topics = kafka_client(&brokers)
        .await
        .list_topics()
        .await
        .expect("list topics");

    assert!(
        !topics.iter().any(|t| t.name == "absent"),
        "checking for a missing topic must not create it"
    );
}

async fn produced_partitions(brokers: &str, config: Produce, keys: &[&str]) -> Vec<i64> {
    let (produce_tx, mut produce_rx) = spawn_producer(Produce {
        brokers: brokers.to_string(),
        create_or_update: true,
        topic_options: flowgen_kafka::config::TopicOptions {
            partitions: 3,
            ..Default::default()
        },
        ..config
    })
    .await;
    let mut partitions = Vec::new();
    for key in keys {
        produce_tx
            .send(drive_event(serde_json::json!({ "k": key })))
            .await
            .expect("send produce event");
        let result = tokio::time::timeout(Duration::from_secs(10), produce_rx.recv())
            .await
            .expect("producer emits result")
            .expect("channel open")
            .data_as_json()
            .expect("json");
        partitions.push(result["partition"].as_i64().expect("partition"));
    }
    partitions
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn messages_with_the_same_key_land_on_the_same_partition() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let partitions = produced_partitions(
        &brokers,
        Produce {
            name: "produce_keyed".to_string(),
            topic: "keyed".to_string(),
            message_key: Some("{{event.data.k}}".to_string()),
            ..Default::default()
        },
        &["a", "b", "a", "b", "a"],
    )
    .await;

    assert_eq!(partitions, vec![1, 2, 1, 2, 1]);
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn messages_without_a_key_spread_across_partitions() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let mut partitions = produced_partitions(
        &brokers,
        Produce {
            name: "produce_unkeyed".to_string(),
            topic: "unkeyed".to_string(),
            ..Default::default()
        },
        &["a", "a", "a"],
    )
    .await;
    partitions.sort_unstable();

    assert_eq!(partitions, vec![0, 1, 2]);
}

async fn create_topic(brokers: &str, topic: &str, partitions: i32) {
    kafka_client(brokers)
        .await
        .controller_client()
        .expect("controller client")
        .create_topic(topic, partitions, 1, 5_000)
        .await
        .expect("create topic");
}

async fn write_records(brokers: &str, topic: &str, partition: i32, values: &[&str]) {
    let records = values
        .iter()
        .map(|value| Record {
            key: Some(format!("key-{value}").into_bytes()),
            value: Some(value.as_bytes().to_vec()),
            headers: [("source".to_string(), b"test".to_vec())]
                .into_iter()
                .collect(),
            timestamp: chrono::Utc::now(),
        })
        .collect();
    kafka_client(brokers)
        .await
        .partition_client(topic, partition, UnknownTopicHandling::Retry)
        .await
        .expect("partition client")
        .produce(records, Compression::NoCompression)
        .await
        .expect("produce records");
}

fn spawn_subscriber(
    config: Subscribe,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
) -> mpsc::Receiver<Event> {
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let subscriber = SubscriberBuilder::new()
            .config(Arc::new(config))
            .sender(tx)
            .task_id(0)
            .task_type("kafka_subscribe")
            .task_context(task_context)
            .build()
            .await
            .expect("build subscriber");
        let _ = subscriber.run().await;
    });
    rx
}

async fn next_event(rx: &mut mpsc::Receiver<Event>) -> Event {
    tokio::time::timeout(Duration::from_secs(30), rx.recv())
        .await
        .expect("subscriber emits an event")
        .expect("channel open")
}

fn complete(event: &Event) {
    event
        .completion_tx
        .as_ref()
        .expect("completion channel")
        .signal_completion(None);
}

fn fail(event: &Event) {
    event
        .completion_tx
        .as_ref()
        .expect("completion channel")
        .signal_completion_with_error("Write failed".to_string());
}

fn record_meta(event: &Event) -> flowgen_kafka::subscribe::RecordMeta {
    let meta = event.meta.clone().expect("event meta");
    serde_json::from_value(serde_json::Value::Object(meta)).expect("record meta")
}

fn offset_key(
    task_context: &flowgen_core::task::context::TaskContext,
    topic: &str,
    partition: i32,
) -> String {
    format!(
        "flow.{}.kafka_offset.{topic}.{partition}",
        task_context.flow.id()
    )
}

async fn wait_for_offset(
    task_context: &flowgen_core::task::context::TaskContext,
    topic: &str,
    partition: i32,
    expected: i64,
) {
    let key = offset_key(task_context, topic, partition);
    let stored = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if let Ok(Some(value)) = task_context.cache.get(&key).await {
                if value.as_ref() == expected.to_string().as_bytes() {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        stored.is_ok(),
        "offset {expected} must be stored under {key}"
    );
}

fn subscribe_config(brokers: &str, topic: &str) -> Subscribe {
    Subscribe {
        name: "subscribe".to_string(),
        brokers: brokers.to_string(),
        topic: topic.to_string(),
        start_offset: StartOffset::Earliest,
        ..Default::default()
    }
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_consumes_every_partition_and_stores_offsets() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 2).await;
    write_records(&brokers, "t", 0, &[r#"{"n":1}"#, r#"{"n":2}"#]).await;
    write_records(&brokers, "t", 1, &["plain"]).await;

    let task_context = test_task_context();
    let mut rx = spawn_subscriber(subscribe_config(&brokers, "t"), Arc::clone(&task_context));

    let mut received = Vec::new();
    for _ in 0..3 {
        let event = next_event(&mut rx).await;
        complete(&event);
        received.push(event);
    }
    wait_for_offset(&task_context, "t", 0, 2).await;
    wait_for_offset(&task_context, "t", 1, 1).await;

    let mut metas: Vec<_> = received.iter().map(record_meta).collect();
    metas.sort_by_key(|meta| (meta.partition, meta.offset));
    let positions: Vec<_> = metas.iter().map(|m| (m.partition, m.offset)).collect();
    assert_eq!(positions, vec![(0, 0), (0, 1), (1, 0)]);
    assert_eq!(metas[0].key.as_deref(), Some(r#"key-{"n":1}"#));
    assert_eq!(
        metas[0].headers.get("source").map(String::as_str),
        Some("test")
    );

    let first = received
        .iter()
        .find(|e| record_meta(e).partition == 0 && record_meta(e).offset == 0)
        .expect("first record");
    assert_eq!(first.id.as_deref(), Some("t-0-0"));
    assert!(matches!(&first.data, EventData::Json(json) if *json == serde_json::json!({"n": 1})));
    let plain = received
        .iter()
        .find(|e| record_meta(e).partition == 1)
        .expect("plain record");
    assert!(matches!(&plain.data, EventData::Bytes(bytes) if bytes.as_ref() == b"plain"));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_resumes_from_the_stored_offset() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["a", "b", "c"]).await;

    let task_context = test_task_context();
    task_context
        .cache
        .put(&offset_key(&task_context, "t", 0), "2".into(), None)
        .await
        .expect("store offset");
    let mut rx = spawn_subscriber(subscribe_config(&brokers, "t"), Arc::clone(&task_context));

    let event = next_event(&mut rx).await;
    complete(&event);

    assert_eq!(record_meta(&event).offset, 2);
    wait_for_offset(&task_context, "t", 0, 3).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_restarts_from_start_offset_when_the_stored_offset_is_past_the_end() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["a"]).await;

    let task_context = test_task_context();
    task_context
        .cache
        .put(&offset_key(&task_context, "t", 0), "100".into(), None)
        .await
        .expect("store offset");
    let mut rx = spawn_subscriber(subscribe_config(&brokers, "t"), Arc::clone(&task_context));

    let event = next_event(&mut rx).await;
    complete(&event);

    assert_eq!(record_meta(&event).offset, 0);
    wait_for_offset(&task_context, "t", 0, 1).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_continues_from_the_earliest_offset_when_retention_removed_the_stored_one() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["a", "b", "c"]).await;
    kafka_client(&brokers)
        .await
        .partition_client("t", 0, UnknownTopicHandling::Retry)
        .await
        .expect("partition client")
        .delete_records(2, 5_000)
        .await
        .expect("delete records");

    let task_context = test_task_context();
    task_context
        .cache
        .put(&offset_key(&task_context, "t", 0), "1".into(), None)
        .await
        .expect("store offset");
    let mut rx = spawn_subscriber(
        Subscribe {
            start_offset: StartOffset::Latest,
            ..subscribe_config(&brokers, "t")
        },
        Arc::clone(&task_context),
    );

    let event = next_event(&mut rx).await;
    complete(&event);

    assert_eq!(record_meta(&event).offset, 2);
    wait_for_offset(&task_context, "t", 0, 3).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_reads_compressed_batches() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    let partition = kafka_client(&brokers)
        .await
        .partition_client("t", 0, UnknownTopicHandling::Retry)
        .await
        .expect("partition client");
    for (value, compression) in [
        ("gzip", Compression::Gzip),
        ("lz4", Compression::Lz4),
        ("snappy", Compression::Snappy),
        ("zstd", Compression::Zstd),
    ] {
        let record = Record {
            key: None,
            value: Some(value.as_bytes().to_vec()),
            headers: Default::default(),
            timestamp: chrono::Utc::now(),
        };
        partition
            .produce(vec![record], compression)
            .await
            .expect("produce compressed batch");
    }

    let mut rx = spawn_subscriber(subscribe_config(&brokers, "t"), test_task_context());
    let mut values = Vec::new();
    for _ in 0..4 {
        let event = next_event(&mut rx).await;
        complete(&event);
        match &event.data {
            EventData::Bytes(bytes) => values.push(String::from_utf8(bytes.to_vec()).unwrap()),
            other => panic!("expected bytes, got {other:?}"),
        }
    }

    assert_eq!(values, ["gzip", "lz4", "snappy", "zstd"]);
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_delivers_a_failing_record_until_the_flow_completes_it() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["a", "b"]).await;

    let task_context = test_task_context();
    let mut rx = spawn_subscriber(
        Subscribe {
            backoff: vec![Duration::from_millis(1)],
            ..subscribe_config(&brokers, "t")
        },
        Arc::clone(&task_context),
    );

    let mut deliveries = Vec::new();
    for _ in 0..3 {
        let event = next_event(&mut rx).await;
        fail(&event);
        deliveries.push(event);
    }
    let completed = next_event(&mut rx).await;
    complete(&completed);
    let next = next_event(&mut rx).await;
    complete(&next);

    assert!(deliveries.iter().all(|e| record_meta(e).offset == 0));
    assert!(deliveries.iter().all(|e| e.error.is_none()));
    assert_eq!(record_meta(&completed).offset, 0);
    assert_eq!(record_meta(&next).offset, 1);
    wait_for_offset(&task_context, "t", 0, 2).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_skips_a_record_after_max_deliver_failed_deliveries() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["a", "b"]).await;

    let task_context = test_task_context();
    let mut rx = spawn_subscriber(
        Subscribe {
            max_deliver: Some(2),
            backoff: vec![Duration::from_millis(1)],
            ..subscribe_config(&brokers, "t")
        },
        Arc::clone(&task_context),
    );

    let first = next_event(&mut rx).await;
    fail(&first);
    let second = next_event(&mut rx).await;
    fail(&second);
    let next = next_event(&mut rx).await;
    complete(&next);

    assert_eq!(record_meta(&first).offset, 0);
    assert_eq!(record_meta(&second).offset, 0);
    assert_eq!(record_meta(&next).offset, 1);
    assert_eq!(next.error, None);
    wait_for_offset(&task_context, "t", 0, 2).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_from_latest_skips_existing_records() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["old"]).await;

    let task_context = test_task_context();
    let mut rx = spawn_subscriber(
        Subscribe {
            start_offset: StartOffset::Latest,
            ..subscribe_config(&brokers, "t")
        },
        Arc::clone(&task_context),
    );
    wait_for_offset(&task_context, "t", 0, 1).await;
    write_records(&brokers, "t", 0, &["new"]).await;

    let event = next_event(&mut rx).await;
    complete(&event);

    assert_eq!(record_meta(&event).offset, 1);
    assert!(matches!(&event.data, EventData::Bytes(bytes) if bytes.as_ref() == b"new"));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_from_a_timestamp_skips_earlier_records() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["old"]).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    let from = chrono::Utc::now();
    write_records(&brokers, "t", 0, &["new"]).await;

    let mut rx = spawn_subscriber(
        Subscribe {
            start_offset: StartOffset::Timestamp(from),
            ..subscribe_config(&brokers, "t")
        },
        test_task_context(),
    );

    let event = next_event(&mut rx).await;
    complete(&event);

    assert_eq!(record_meta(&event).offset, 1);
    assert!(matches!(&event.data, EventData::Bytes(bytes) if bytes.as_ref() == b"new"));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_from_a_timestamp_after_every_record_starts_at_latest() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;
    create_topic(&brokers, "t", 1).await;
    write_records(&brokers, "t", 0, &["old"]).await;

    let task_context = test_task_context();
    let mut rx = spawn_subscriber(
        Subscribe {
            start_offset: StartOffset::Timestamp(chrono::Utc::now() + chrono::Duration::days(1)),
            ..subscribe_config(&brokers, "t")
        },
        Arc::clone(&task_context),
    );
    wait_for_offset(&task_context, "t", 0, 1).await;
    write_records(&brokers, "t", 0, &["new"]).await;

    let event = next_event(&mut rx).await;
    complete(&event);

    assert_eq!(record_meta(&event).offset, 1);
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn subscribe_to_a_missing_topic_does_not_create_it() {
    let _lock = lock_kafka_test().await;
    let (_kafka, brokers) = start_kafka().await;

    let mut rx = spawn_subscriber(subscribe_config(&brokers, "absent"), test_task_context());
    tokio::time::sleep(Duration::from_secs(3)).await;

    let topics = kafka_client(&brokers)
        .await
        .list_topics()
        .await
        .expect("list topics");
    assert!(!topics.iter().any(|t| t.name == "absent"));
    assert!(rx.try_recv().is_err());
}
