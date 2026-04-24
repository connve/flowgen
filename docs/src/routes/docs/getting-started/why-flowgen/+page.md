# Why Flowgen

Flowgen is an open-source data activation engine written in Rust. It compiles to a single binary that includes every connector and integration. Define flows — event-driven, scheduled, or streaming — that connect systems, transform data, and activate it across your stack. Declarative YAML, distributed by design.

## Single binary

One binary. Every connector built in. Nothing to install, nothing to configure at runtime. Deploy and run.

## The problem

Building data flows in distributed systems is hard.

**It is expensive.** Enterprise integration platforms charge per connector, per row, per execution. At scale, the integration layer becomes one of the largest line items in your cloud budget.

**It is not truly open.** Many tools call themselves open-source but gate connectors and features behind commercial licenses. After an acquisition, features you depend on become "enterprise only." Your flows run on someone else's terms.

**It is not built for distributed deployment.** Most integration tools were designed as single-node applications. Running them across multiple nodes means building your own orchestration — leader election, state coordination, scaling.

**It compromises on flexibility.** Proprietary query languages work for simple cases but break down when you need real logic — conditional processing, stateful deduplication, dynamic routing.

## How Flowgen solves this

**Single binary.** Every connector ships inside one executable. No plugins, no downloads, no version matrix.

**Written in Rust.** Memory safety, performance, no garbage collector.

**Fully open-source, MPL-2.0.** Built by [CONNVE](https://connve.com) and used in production for large-scale data activation. Every feature, every connector is available to everyone. No gated tiers, no enterprise-only modules.

**Run it on your infrastructure.** No per-connector fees, no per-row pricing. Teams switching from managed platforms reduce their integration spend by 50% or more.

**Distributed by design.** Leader election, multi-node scaling, and distributed caching are part of the runtime. Deploy multiple replicas and they coordinate automatically. The cache layer is pluggable — currently backed by NATS JetStream.

**Real scripting.** Flowgen uses [Rhai](https://rhai.rs) — a lightweight, sandboxed scripting language with familiar syntax. Scripts access distributed cache, event metadata, and resource files directly. No proprietary DSL.

**Efficient data formats.** Arrow RecordBatch and Avro are first-class event types. Columnar data stays in Arrow format through the entire flow — no serialization overhead.

**Declarative YAML.** Each flow is a YAML file. Version-controlled, reviewed in pull requests, deployed as Kubernetes ConfigMaps. Event-driven, scheduled, or streaming — same model.

## Architecture

- **Flows** — YAML definitions containing a sequence of tasks. Event-driven, scheduled (cron/interval), or streaming.
- **Tasks** — Subscribers that ingest data, and processors that transform, route, query, or write it. All tasks emit events to the next task in the flow.
- **Events** — Data records flowing between tasks, carrying JSON, Arrow RecordBatch, or Avro payloads.
- **Cache** — Distributed key-value store for state management, deduplication, and leader election. Pluggable backend, currently NATS JetStream.

```yaml
flow:
  name: salesforce_to_bigquery
  tasks:
    - salesforce_pubsubapi_subscriber:
        name: account_changes
        topic: /data/AccountChangeEvent

    - script:
        name: transform
        code: |
          let record = event.data;
          #{
            account_id: record.Id,
            name: record.Name,
            updated_at: timestamp_now()
          }

    - gcp_bigquery_storage_write:
        name: write_accounts
        project: my-project
        dataset: salesforce
        table: accounts
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
