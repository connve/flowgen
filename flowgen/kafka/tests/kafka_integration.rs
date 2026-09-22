//! Integration tests for the `kafka_producer` processor against a real
//! Kafka broker in a Docker container.
//!
//! Exercises `produce` with the same event-flow shape a YAML task uses in
//! production: an upstream event drives the operation, the processor emits
//! a result event downstream, and the produced message is consumed back
//! from the topic with a plain rdkafka consumer to verify the round trip.
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
use flowgen_kafka::config::Produce;
use flowgen_kafka::produce::ProducerBuilder;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
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
        .with_env_var("KAFKA_LISTENERS", "PLAINTEXT://:9092,CONTROLLER://:9093")
        .with_env_var(
            "KAFKA_ADVERTISED_LISTENERS",
            format!("PLAINTEXT://127.0.0.1:{host_port}"),
        )
        .with_env_var("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .with_env_var(
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
            "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
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

fn start_consumer(brokers: &str, group_id: &str) -> rdkafka::consumer::StreamConsumer {
    use rdkafka::config::ClientConfig;
    ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group_id)
        .set("auto.offset.reset", "earliest")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .expect("create kafka consumer")
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

    // Consume the message back from the topic and assert the payload
    // round-trips byte-for-byte as JSON.
    use futures_util::StreamExt;
    use rdkafka::consumer::Consumer;
    use rdkafka::Message;
    let consumer = start_consumer(&brokers, "flowgen-test-customers");
    consumer
        .subscribe(&["customers"])
        .expect("subscribe to customers");
    let message = tokio::time::timeout(Duration::from_secs(10), consumer.stream().next())
        .await
        .expect("consumed message arrives")
        .expect("stream open")
        .expect("poll ok");
    let payload: serde_json::Value =
        serde_json::from_slice(message.payload().expect("payload")).expect("json payload");
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

    // Subscribe only once the topic exists and all messages are in the
    // single partition; a fresh group with `earliest` reset then reads
    // every message at offsets 0..=2. Subscribing before the topic exists
    // makes rdkafka surface `UnknownTopicOrPartition` as a stream error.
    use futures_util::StreamExt;
    use rdkafka::consumer::Consumer;
    use rdkafka::Message;
    let consumer = start_consumer(&brokers, "flowgen-test-batch");
    consumer.subscribe(&["batch"]).expect("subscribe to batch");
    let mut stream = consumer.stream();

    let mut offsets = Vec::new();
    for expected in ["Ada", "Grace", "Katherine"] {
        let message = tokio::time::timeout(Duration::from_secs(10), stream.next())
            .await
            .expect("consumed message arrives")
            .expect("stream open")
            .expect("poll ok");
        let payload: serde_json::Value =
            serde_json::from_slice(message.payload().expect("payload")).expect("json payload");
        assert_eq!(payload.get("name").and_then(|v| v.as_str()), Some(expected));
        offsets.push(message.offset());

        // The incoming events carry no id, so the producer patches a UUID
        // v7 fallback into the render context; the rendered message key
        // must therefore be `key-<uuid>`.
        let key =
            std::str::from_utf8(message.key().expect("message key")).expect("utf8 message key");
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
    let (_kafka, brokers) = start_kafka().await;

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

    let consumer = start_consumer(&brokers, "flowgen-test-partitions");
    use rdkafka::consumer::Consumer;
    let metadata = consumer
        .fetch_metadata(Some("partitioned"), Duration::from_secs(10))
        .expect("fetch metadata");
    let topic = metadata
        .topics()
        .iter()
        .find(|t| t.name() == "partitioned")
        .expect("topic exists");

    assert_eq!(topic.partitions().len(), 3);

    use rdkafka::admin::{AdminClient, AdminOptions, ResourceSpecifier};
    use rdkafka::client::DefaultClientContext;
    let admin: AdminClient<DefaultClientContext> = rdkafka::config::ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("create admin client");
    let configs = admin
        .describe_configs(
            &[ResourceSpecifier::Topic("partitioned")],
            &AdminOptions::new(),
        )
        .await
        .expect("describe configs");
    let resource = configs
        .into_iter()
        .next()
        .expect("one resource")
        .expect("resource described");
    let entry = resource
        .entries
        .iter()
        .find(|e| e.name == "retention.ms")
        .expect("retention.ms is set");

    assert_eq!(entry.value.as_deref(), Some("604800000"));
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

    use rdkafka::consumer::Consumer;
    let consumer = start_consumer(&brokers, "flowgen-test-absent");
    let metadata = consumer
        .fetch_metadata(None, Duration::from_secs(10))
        .expect("fetch metadata");

    assert!(
        !metadata.topics().iter().any(|t| t.name() == "absent"),
        "checking for a missing topic must not create it"
    );
}
