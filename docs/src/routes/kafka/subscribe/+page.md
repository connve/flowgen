# Kafka Subscribe

Consumes every partition of a Kafka topic and emits each record as an event.

## Configuration

```yaml
- kafka_subscribe:
    name: consume_orders
    credentials_path: /etc/kafka/credentials.json
    brokers: "{{env.KAFKA_BROKERS}}"
    topic: orders
    start_offset: earliest
    ack_timeout: 60s
    backoff: ["1s", "10s", "1m"]
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | | Path to Kafka credentials file. Omit to connect without authentication. See [Credentials](/docs/flowgen/kafka#credentials). |
| `brokers` | string | `localhost:9092` | Comma-separated bootstrap broker addresses. An address without a port uses `9092`. |
| `topic` | string | required | Topic to consume. |
| `start_offset` | string | `latest` | Where a partition starts when no offset is stored for it: `earliest` (the oldest retained record), `latest` (the end of the partition when the subscriber first reads it), or an RFC 3339 timestamp from 1970 on, such as `2026-09-01T00:00:00Z` (the first record written at or after that time, or the end of the partition when there is none). |
| `ack_timeout` | duration | | How long to wait for the flow to complete a record before delivering it again. Waits indefinitely when omitted. |
| `max_deliver` | int | | How many times a record is sent through the flow before it is skipped. When omitted, a record is delivered until the flow completes it. |
| `backoff` | list | | Delays between deliveries of a record the flow failed to complete, e.g. `["1s", "10s", "1m"]`. The last entry repeats. When omitted, the delay grows exponentially from the [retry](/docs/flowgen/concepts/retry) `initial_backoff`. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry) for connecting to the brokers. |

## Output

| Format | Description |
|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | The record value, when it parses as JSON. A tombstone (a record with no value) is `null`. |
| Bytes | The raw record value, when it is not JSON. |

| Event field | Value |
|---|---|
| `event.subject` | Topic the record was read from. |
| `event.id` | `<topic>-<partition>-<offset>`. |
| `event.timestamp` | Record timestamp, in microseconds since the Unix epoch. |
| `event.meta.partition` | Partition the record was read from. |
| `event.meta.offset` | Offset of the record. |
| `event.meta.key` | Record key decoded as UTF-8, or `null` for a record without one. |
| `event.meta.headers` | Record headers, with values decoded as UTF-8. |

Bytes that are not valid UTF-8 in a key or header value are replaced with `U+FFFD`.

## Behaviour

Each partition is consumed in order, one record at a time; partitions are consumed concurrently. A record is emitted with a completion channel, and the next record of that partition is read once every leaf of the flow has completed it.

The next offset of each partition is stored in the [cache](/docs/flowgen/concepts/caching) under `flow.<flow id>.kafka_offset.<topic>.<partition>`, where `<flow id>` is the base64url-encoded flow identity. A partition's starting offset is stored when the subscriber first reads the partition, and the stored offset advances after the flow completes each record. On restart, a partition resumes from its stored offset, so changing `start_offset` does not move a partition that already has one; delete its cache key to start it over.

A stored offset below the earliest record the topic retains continues from the earliest record. A stored offset past the end of the partition continues from `start_offset`.

A record the flow fails to complete, or does not complete within `ack_timeout`, is delivered again after the `backoff` delay, and the partition waits on it. With `max_deliver`, the record is skipped after that many failed deliveries and logged with its partition and offset, and the partition moves on. A delivery that timed out keeps running in the flow while the next one starts. Delivery is at least once: a record can be delivered again after a crash between completion and the offset write.

A failed fetch resumes the partition from its next offset with exponential backoff, without stopping the other partitions. Partitions added to the topic are picked up within five minutes and read from their earliest record.

The subscriber does not join a Kafka consumer group and commits no offsets to Kafka, so consumer lag does not show in Kafka tooling. Every replica running the flow consumes every partition; set [`require_leader_election: true`](/docs/flowgen/concepts/flows) on the flow when it runs on more than one replica. The cache holds the offsets, so without a persistent cache backend the offsets do not survive a restart.

The topic must exist; a missing topic is not created. Batches compressed with gzip, snappy, lz4, or zstd are read.
