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
| `generate` | Produces events on a schedule (cron or interval) using inline scripts. |

Subscribers appear as the first task in a flow. They manage acknowledgment — a message is only acked when the entire downstream flow completes successfully.

### Processors

Processors receive events, do something with them, and emit events to the next task. They can transform, enrich, query, write, publish — any operation. A processor can appear anywhere in the flow, including as the last task.

| Task | Description |
|---|---|
| `script` | Executes [Rhai](https://rhai.rs) scripts for data transformation, filtering, and routing. |
| `convert` | Converts between data formats (JSON, Arrow, Avro, CSV). |
| `iterate` | Fans out array data into individual events. |
| `buffer` | Accumulates events into batches before forwarding. |
| `log` | Logs event data for debugging and observability. |
| `http_request` | Makes HTTP calls for enrichment or side effects. |
| `nats_jetstream_publisher` | Publishes events to a NATS JetStream subject. |
| `salesforce_pubsubapi_publisher` | Publishes Salesforce Platform Events. |
| `salesforce_restapi_sobject` | Performs Salesforce CRUD operations. |
| `salesforce_restapi_composite` | Executes batch Salesforce operations. |
| `salesforce_bulkapi_query_job` | Manages Salesforce Bulk API query jobs. |
| `salesforce_toolingapi` | Manages Salesforce metadata. |
| `gcp_bigquery_query` | Executes BigQuery SQL queries. |
| `gcp_bigquery_storage_read` | Reads data from BigQuery via the Storage Read API. |
| `gcp_bigquery_storage_write` | Writes data to BigQuery via the Storage Write API. |
| `gcp_bigquery_job` | Manages BigQuery async jobs (create, monitor, cancel). |
| `mssql_query` | Executes MSSQL queries. |
| `object_store_read` | Reads files from object storage (S3, GCS, Azure, local). |
| `object_store_write` | Writes event data to files in object storage. |
| `object_store_list` | Lists files in object storage. |
| `object_store_move` | Moves files between object storage locations. |

## Common configuration

All tasks share a `name` field and an optional `retry` configuration:

```yaml
- script:
    name: my_transform        # required: unique within the flow
    retry:                     # optional: overrides app-level retry
      max_attempts: 5
      initial_backoff: "2s"
    code: |
      event.data
```

## Event data formats

Tasks can produce and consume three event data formats:

- **JSON** — Default for REST APIs, webhooks, and script output.
- **Arrow RecordBatch** — Preferred for columnar data (BigQuery, Parquet). Zero-copy through the flow.
- **Avro** — Preferred for gRPC and Pub/Sub (Salesforce Platform Events).

The `convert` task translates between formats when needed.
