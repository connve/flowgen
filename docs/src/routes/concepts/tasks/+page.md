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
| `mongodb_change_stream` | Watches a MongoDB database for real-time document changes. |
| `kafka_subscribe` | Consumes every partition of a Kafka topic. |
| `http_endpoint` | Listens for incoming HTTP requests and converts them to events. |
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
| `html_scrape` | Extracts rows from HTML with CSS selectors. |
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
| `mongodb_collection` | Reads or writes documents in a MongoDB collection. |
| `object_store` | Object storage operations (read, write, list, move) on S3, GCS, Azure, local. |
| `git_sync` | Clone/pull a Git repository and emit one event per file. |
| `oci_sync` | Pull an OCI artifact from a registry and emit one event per layer. |
| `ai_completion` | LLM completions from multiple providers. |
| `llm_proxy` | Registers a flow as a backend on the AI gateway server. |
| `mcp_tool` | Expose flows as MCP tools for LLM agents. |
| `mcp_resource` | Expose read-only content (schemas, glossaries, docs) as MCP resources. |
| `mcp_prompt` | Publish slash-command templates with typed arguments for MCP clients. |

## Task wiring

By default, tasks are wired sequentially — each task receives from the previous task and sends to the next. Linear chains, fan-out, fan-in, and end-to-end acknowledgement semantics are covered in [Flows](/docs/flowgen/concepts/flows).

## Common configuration

All tasks share these optional fields:

```yaml
- script:
    name: my_transform        # required: unique within the flow
    depends_on: [source]       # optional: receive from named tasks instead of previous
    retry:                     # optional: overrides app-level retry
      max_attempts: 5
      initial_backoff: "2s"
    max_in_flight: 4           # optional: events processed at a time (processors)
    code: |
      event.data
```

## Backpressure

Each processor works on at most `max_in_flight` events at a time and picks up the next one only when one finishes. Only one event can wait between two tasks, so when a task is slow, the tasks before it wait too, all the way back to the source. Memory depends on `max_in_flight` and how big each event is (for example the `batch_size` of an `object_store` read), not on how much data comes in.

`max_in_flight` defaults to 64 for tasks that mostly wait on an external service, and to the number of CPUs for tasks that parse, encode or batch data.

Set it to `1` when order matters, for example for updates to the same record: the task then handles one event at a time and passes them on in the order they arrived. Each task has its own limit, so every task that could reorder events needs it. After a fan-out, each branch gets events in the order the parent sent them. Where branches merge, each branch's events stay in order, but events from different branches can arrive in any order.

An event that is being retried keeps its slot until it succeeds or runs out of attempts. When an external service is down, its task fills up with retrying events and the flow stops taking new input until the service is back, instead of piling up events in memory.

`http_endpoint`, `llm_proxy`, and `mcp_tool` count the wait for the flow to take a request as part of `ack_timeout`. If the flow is too busy to take it in time, HTTP clients get `503` and MCP clients a "Server is too busy" error.

## Event data formats

Tasks can produce and consume three event data formats:

- **JSON** — Default for REST APIs, webhooks, and script output.
- **Arrow RecordBatch** — Columnar data (BigQuery, Parquet). Zero-copy through the flow.
- **Avro** — Binary format for gRPC and Pub/Sub (Salesforce Platform Events).

The `convert` task translates between formats when needed.
