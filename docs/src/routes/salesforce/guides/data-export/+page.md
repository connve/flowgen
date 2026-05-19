<script>
  import Mermaid from '$lib/components/Mermaid.svelte';
</script>

# Salesforce Data Export

Export Salesforce records incrementally using the Bulk API and write results to object storage and a data warehouse.

## Architecture

The pipeline uses three flows:

1. **Query** — runs on a cron schedule, creates a Bulk API query job for records modified since the last run.
2. **Job listener** — subscribes to `BulkApi2JobEvent`, filters for final results, downloads CSV as Arrow batches, and publishes to NATS.
3. **Writer** — consumes from NATS and fans out to object store and a data warehouse using `depends_on`.

<Mermaid chart={`
graph LR
  subgraph Query Flow
    CRON[cron trigger] --> SOQL[Bulk API query job]
  end
  SOQL -.-> SF[(Salesforce)]
  SF -.->|BulkApi2JobEvent| LISTEN
  subgraph Job Listener Flow
    LISTEN[listen_job_events] --> FILTER[filter + get results]
    FILTER --> PUB[NATS Publisher]
  end
  PUB --> NATS[(NATS JetStream)]
  subgraph Writer Flow
    NATS --> SUB[Subscriber]
    SUB --> CAST[cast_schema]
    CAST --> OBJ[Object Store]
    CAST --> DW[Data Warehouse]
  end
`} />

Unlike CDC replication which streams individual change events in real time, the Bulk API exports records in batches on a schedule. Use this pattern when you need full or incremental snapshots of Salesforce objects.

## Query flow

Creates an incremental Bulk API query job. The `generate` task tracks `last_run_time` in cache, so each run only fetches records modified since the previous run.

<Mermaid chart={`
graph LR
  A[trigger] --> B[prepare_query_time] --> C[query_account]
`} />

```yaml
flow:
  name: salesforce_bulkapi_query_account
  require_leader_election: true
  tasks:

    - generate:
        name: trigger
        cron: "*/5 * * * *"

    - script:
        name: prepare_query_time
        code: |
          let timestamp = if event.data.system_info.last_run_time != () { event.data.system_info.last_run_time - 3600 } else { timestamp_now() - 86400 };
          #{ last_run_unix: timestamp, last_run_iso: timestamp_to_iso(timestamp) }

    - salesforce_bulkapi_query_job:
        name: query_account
        operation: create
        credentials_path: /etc/sfdc/credentials.json
        query_operation: query
        query:
          resource: salesforce/query_account.soql
        content_type: csv
        column_delimiter: comma
        line_ending: lf
```

The SOQL query lives in an external resource file so it can be changed without modifying the flow:

```sql
-- resources/salesforce/query_account.soql
SELECT
  Id, Name, Type, Industry, AnnualRevenue,
  CreatedDate, LastModifiedDate
FROM Account
WHERE LastModifiedDate >= {{event.data.last_run_iso}}
ORDER BY LastModifiedDate ASC
```

Deploy one query flow per object type, each with its own SOQL resource file.

## Job listener flow

Subscribes to Salesforce `BulkApi2JobEvent` via Pub/Sub API. When a query job completes, it downloads the CSV results, converts them to Arrow batches, and publishes to NATS with a subject based on the object name.

<Mermaid chart={`
graph LR
  A[listen_job_events] --> B[filter_available_results] --> C[get_job_info]
  C --> D[add_metadata_to_meta] --> E[filter_final_results]
  E --> F[get_job_results] --> G[publish_to_nats]
`} />

```yaml
flow:
  name: salesforce_bulkapi_job_listener
  require_leader_election: true
  tasks:

    - salesforce_pubsubapi_subscriber:
        name: listen_job_events
        credentials_path: /etc/sfdc/credentials.json
        topic:
          name: /event/BulkApi2JobEvent
          durable_consumer_options:
            enabled: true
            managed_subscription: false
            name: salesforce_bulkapi_job_listener

    - script:
        name: filter_available_results
        code: |
          if event.data.Type == "RESULT" && event.data.ResultType == "FINAL" {
            #{
              id: event.data.JobIdentifier,
              result_type: event.data.ResultType,
              result_url: event.data.ResultUrl,
              state: event.data.JobState,
              event_type: event.data.Type
            }
          } else {
            ()
          }

    - salesforce_bulkapi_query_job:
        name: get_job_info
        operation: get
        credentials_path: /etc/sfdc/credentials.json
        job_id: "{{event.data.id}}"

    - script:
        name: add_metadata_to_meta
        code: |
          ctx.meta.object_name = event.data.object.to_lower();
          ctx.meta.job_id = event.data.id;
          ctx.meta.job_state = event.data.state;
          event

    - script:
        name: filter_final_results
        code: |
          if event.meta.job_state == "JobComplete" {
            event
          } else {
            ()
          }

    - salesforce_bulkapi_query_job:
        name: get_job_results
        operation: get_results
        credentials_path: /etc/sfdc/credentials.json
        job_id: "{{event.meta.job_id}}"
        batch_size: 10000
        has_header: true

    - nats_jetstream_publisher:
        name: publish_to_nats
        credentials_path: /etc/nats/credentials.json
        url: "{{env.NATS_URL}}"
        subject: salesforce_bulkapi.query_jobs.{{event.meta.object_name}}
        ack_timeout: 120s
        stream:
          create_or_update: true
          name: salesforce_bulkapi
          description: "Salesforce Bulk API query results."
          subjects: ["salesforce_bulkapi.>"]
          max_age: 1h
          max_bytes: 2147483648
          retention: limits
          discard: old
```

A single job listener handles all object types. It routes each result to a per-object NATS subject via `{{event.meta.object_name}}`, so `Account` results land on `salesforce_bulkapi.query_jobs.account`, `Contact` on `salesforce_bulkapi.query_jobs.contact`, etc.

## Writer flow

Consumes query results from NATS, casts the Arrow schema to match the destination table, and fans out to object store and a data warehouse using `depends_on`.

<Mermaid chart={`
graph TD
  SUB[start_subscription] --> CAST[cast_schema]
  CAST --> OBJ[write_to_object_store]
  CAST --> DW[write_to_data_warehouse]
`} />

```yaml
flow:
  name: salesforce_bulkapi_writer_account
  tasks:

    - nats_jetstream_subscriber:
        name: start_subscription
        credentials_path: /etc/nats/credentials.json
        url: "{{env.NATS_URL}}"
        subject: salesforce_bulkapi.query_jobs.account
        stream:
          create_or_update: true
          name: salesforce_bulkapi
          description: "Salesforce Bulk API query results."
          subjects: ["salesforce_bulkapi.>"]
          max_age: 1h
          max_bytes: 2147483648
          retention: limits
          discard: old
        durable_name: salesforce_bulkapi_account_writer
        ack_timeout: "120s"
        max_deliver: 5
        backoff:
          - "5s"
          - "30s"
          - "1m"
          - "5m"

    - convert:
        name: cast_schema
        target_format: arrow
        schema:
          resource: salesforce/schema_account.json

    - object_store:
        name: write_to_object_store
        operation: write
        credentials_path: /etc/gcp/credentials.json
        path: "{{env.GCS_BUCKET_PATH}}/salesforce/account"
        hive_partition_options:
          enabled: true
          partition_keys:
            - EventDate
        depends_on: [cast_schema]

    - gcp_bigquery_storage_write:
        name: write_to_data_warehouse
        credentials_path: /etc/gcp/credentials.json
        project_id: "{{env.GCP_PROJECT_ID}}"
        dataset_id: salesforce
        table_id: account
        change_type: upsert
        depends_on: [cast_schema]
```

Bulk API CSV has no type metadata, so the `convert` task casts the Arrow schema before writing. Both sinks depend on `cast_schema`, so flowgen clones each batch and writes to both destinations in parallel.

Deploy one writer per object type, changing the NATS subject, schema resource, and table name.

### Swapping destinations

Both branches are independent — remove or replace either one without affecting the other.

**Object store** is cloud-agnostic. The same `object_store` task works with GCS, S3, and Azure by changing the path prefix and credentials:

| Cloud | Path prefix | Credentials |
|---|---|---|
| GCS | `gs://bucket/path` | GCP service account JSON |
| AWS S3 | `s3://bucket/path` | AWS access key JSON + `client_options.aws_region` |
| Azure | `az://container/path` | Azure storage account JSON |

**Data warehouse** — the example above uses `gcp_bigquery_storage_write`. Replace it with the appropriate warehouse task for your stack. See the task reference for configuration details.

## CDC vs Bulk API

| | CDC Replication | Bulk API Export |
|---|---|---|
| **Latency** | Real-time (seconds) | Scheduled (minutes) |
| **Data** | Individual change events | Full or incremental snapshots |
| **Volume** | Per-record | Batched CSV |
| **Schema** | Avro (typed) | CSV (requires schema casting) |
| **Use case** | Keep warehouse in sync | Periodic snapshots, backfills |

Both patterns can coexist — use CDC for real-time sync and Bulk API for periodic full reconciliation. See [Salesforce CDC Replication](/docs/flowgen/salesforce/guides/cdc-replication) for the CDC pattern.

## Credentials

| File | Purpose | Contents |
|---|---|---|
| `/etc/sfdc/credentials.json` | Salesforce Pub/Sub + Bulk API | `client_id`, `client_secret`, `username`, `password`, `login_url` |
| `/etc/nats/credentials.json` | NATS JetStream | `url`, `nkey_seed` or `credentials_path` |
| `/etc/gcp/credentials.json` | GCS + BigQuery | GCP service account JSON key |

See [Credentials](/docs/flowgen/concepts/credentials) for format details.
