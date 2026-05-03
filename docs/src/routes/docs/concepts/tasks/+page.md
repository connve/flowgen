# Tasks

Tasks are the building blocks of a flow. Each task performs a specific operation — subscribing to a message stream, transforming data, querying a database, or writing results. All tasks receive events and emit events to the next task in the flow.

## Task categories

Flowgen has two categories of tasks: **subscribers** and **processors**.

### Subscribers

Subscribers are source tasks that ingest data into the flow. They connect to external systems and produce events.

| Task | Description |
|---|---|
| `nats_jetstream_subscriber` | Consumes messages from a NATS JetStream stream with durable consumers. |
| `salesforce_pubsubapi_subscriber` | Subscribes to Salesforce Platform Events via gRPC. |
| `http_webhook` | Listens for incoming HTTP requests and converts them to events. |
| `generate` | Produces events on a schedule (cron or interval). |

Subscribers appear as the first task in a flow. They manage acknowledgment — a message is only acked when the entire downstream flow completes successfully.

### Processors

Processors receive events, do something with them, and emit events to the next task.

| Task | Description |
|---|---|
| `script` | Executes [Rhai](https://rhai.rs) scripts for transformation, filtering, and routing. |
| `convert` | Converts between data formats (JSON, Arrow, Avro). |
| `iterate` | Fans out array data into individual events. |
| `buffer` | Accumulates events into batches before forwarding. |
| `log` | Logs event data for debugging. |
| `http_request` | Makes outbound HTTP calls. |
| `nats_jetstream_publisher` | Publishes events to a NATS JetStream subject. |
| `nats_kv_store` | Read, write, list, and delete keys in a NATS KV bucket. |
| `salesforce_pubsubapi_publisher` | Publishes Salesforce Platform Events. |
| `salesforce_restapi_sobject` | Salesforce CRUD operations (create, get, update, upsert, delete). |
| `salesforce_restapi_composite` | Batch Salesforce operations. |
| `salesforce_bulkapi_query_job` | Salesforce Bulk API query jobs. |
| `salesforce_toolingapi` | Salesforce metadata management. |
| `gcp_bigquery_query` | BigQuery SQL queries. |
| `gcp_bigquery_storage_read` | BigQuery Storage Read API. |
| `gcp_bigquery_storage_write` | BigQuery Storage Write API. |
| `gcp_bigquery_job` | BigQuery async jobs (load, monitor, cancel). |
| `mssql_query` | Microsoft SQL Server queries. |
| `object_store` | Object storage operations (read, write, list, move) on S3, GCS, Azure, local. |
| `git_sync` | Clone/pull a Git repository and emit one event per file. |
| `ai_completion` | LLM completions from multiple providers. |
| `ai_gateway` | OpenAI-compatible chat completions endpoint. |
| `mcp_tool` | Expose flows as MCP tools for LLM agents. |

## Task wiring

By default, tasks are wired sequentially — each task receives from the previous task and sends to the next. Linear chains, fan-out, fan-in, and end-to-end acknowledgement semantics are covered in [Flows](/docs/concepts/flows).

## Common configuration

All tasks share these optional fields:

```yaml
- script:
    name: my_transform        # required: unique within the flow
    depends_on: [source]       # optional: receive from named tasks instead of previous
    retry:                     # optional: overrides app-level retry
      max_attempts: 5
      initial_backoff: "2s"
    code: |
      event.data
```

## Event data formats

Tasks can produce and consume three event data formats:

- **JSON** — Default for REST APIs, webhooks, and script output.
- **Arrow RecordBatch** — Columnar data (BigQuery, Parquet). Zero-copy through the flow.
- **Avro** — Binary format for gRPC and Pub/Sub (Salesforce Platform Events).

The `convert` task translates between formats when needed.
