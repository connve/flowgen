# Why Flowgen

## Capture the change. Stream it anywhere, in milliseconds.

Flowgen is an open-source data activation engine. You define flows in YAML or code, run them on one node or many, and data moves between systems in milliseconds. Retries, leader election, backpressure, and circuit breakers are part of the runtime — not something you wire up per pipeline.

## The problem we kept hitting

We started Flowgen while running data pipelines at a consumer business with hundreds of millions of customer profiles. Three things kept getting in our way:

**Everything was batch.** Schedulers fired jobs once a day. Activation systems saw data that was already stale by the time it arrived. The business felt it; we couldn't fix it without rewriting the foundations.

**Reliability was DIY.** Retry with backoff, dead-letter paths, idempotency, deduplication, coordination across replicas — all hand-rolled, all slightly different per pipeline. The team spent more time rebuilding the same primitives than shipping flows.

**The modern alternatives weren't actually open.** The features we needed lived behind commercial licenses, and multi-node deployment was either an afterthought or a paid tier.

## What Flowgen does

- **Streams by default, batches when you want.** Event-driven, scheduled, and streaming flows use the same model.
- **Distributed from day one.** Run one replica or many. Leader election, distributed cache, and coordination are built into the runtime.
- **Resiliency you don't write.** Retries with jitter, circuit breakers, backpressure, dead-letter routing, leader election — configured in YAML, applied uniformly.
- **Real scripting, not a DSL.** Embedded [Rhai](https://rhai.rs) with direct access to the distributed cache, event metadata, and resource files.
- **Columnar data stays columnar.** Arrow RecordBatch and Avro are first-class event payloads alongside JSON — no per-task serialization tax.
- **Rust under the hood.** Memory safety, predictable performance, single binary to deploy.
- **OpenTelemetry ready.** Traces, metrics, and logs flow into whatever you already run.
- **Open source, no tiers.** MPL-2.0, built and maintained by [CONNVE](https://connve.com). Every connector, every feature, open. No paid edition.

## Architecture

- **Flows** — YAML definitions containing a sequence of tasks. Event-driven, scheduled (cron/interval), or streaming.
- **Tasks** — Subscribers that ingest data, and processors that transform, route, query, or write it. All tasks emit events to the next task in the flow.
- **Events** — Data records flowing between tasks, carrying JSON, Arrow RecordBatch, or Avro payloads.
- **Cache** — Distributed key-value store for state management, deduplication, and leader election. Pluggable backend, currently NATS JetStream.

```yaml
flow:
  name: webhook_to_nats
  tasks:
    - http_webhook:
        name: ingest
        endpoint: /events
        method: POST
        credentials_path: /etc/http/credentials.json

    - script:
        name: transform
        code: |
          let body = event.data;
          #{
            id: body.id,
            type: body.event_type,
            received_at: timestamp_now()
          }

    - nats_jetstream_publisher:
        name: publish
        credentials_path: /etc/nats/credentials.json
        url: nats://localhost:4222
        subject: events.normalized
        stream:
          name: events
          subjects:
            - events.>
          create_or_update: true
```

## Integrations

| Integration | Tasks |
|---|---|
| **NATS JetStream** | Subscriber, Publisher, KV Store |
| **Salesforce** | PubSub API, REST API, Bulk API, Tooling API |
| **Google Cloud** | BigQuery Query, Storage Read, Storage Write, Jobs |
| **HTTP** | Webhook, Request |
| **Object Store** | Read, Write, List, Move (S3, GCS, Azure, local) |
| **MSSQL** | Query |
| **AI** | Completion (multi-provider), AI Gateway (OpenAI-compatible), MCP Tools |
| **Git** | Git Sync |
| **Core** | Script (Rhai), Convert, Iterate, Buffer, Generate, Log |

Enjoy :)
