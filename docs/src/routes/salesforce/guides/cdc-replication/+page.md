<script>
  import Mermaid from '$lib/components/Mermaid.svelte';
</script>

# Salesforce CDC Replication

Replicate Salesforce Change Data Capture events to object storage and a data warehouse using NATS as a durable buffer.

## Architecture

The pipeline is split into two flows connected by a NATS JetStream stream:

<Mermaid chart={`
graph LR
  subgraph Subscriber Flow
    CDC[Salesforce CDC] --> PUB[NATS Publisher]
  end
  PUB --> NATS[(NATS JetStream\npubsub.>)]
  subgraph Writer Flow
    NATS --> SUB[Subscriber]
    SUB --> STRIP[strip_subject_prefix]
    STRIP --> OBJ[Object Store]
    STRIP --> DW[Data Warehouse]
  end
`} />

NATS decouples Salesforce event delivery from downstream writes. If the data warehouse or object storage is temporarily unavailable, events stay in the stream and the writer retries from where it left off.

## Subscriber flow

<Mermaid chart={`
graph LR
  A[start_subscription] --> B[publish_events]
`} />

Connects to Salesforce Pub/Sub API and republishes every change event into NATS. Deploy one subscriber per CDC topic.

```yaml
flow:
  name: salesforce_account_subscriber
  require_leader_election: true
  tasks:

    - salesforce_pubsubapi_subscriber:
        name: start_subscription
        credentials_path: /etc/sfdc/credentials.json
        topic:
          name: /data/AccountChangeEvent
          durable_consumer_options:
            enabled: true
            managed_subscription: false
            name: salesforce_account_subscriber

    - nats_jetstream_publisher:
        name: publish_events
        credentials_path: /etc/nats/credentials.json
        url: "{{env.NATS_URL}}"
        subject: pubsub.data.{{event.subject}}
        stream:
          create_or_update: true
          name: salesforce
          description: "Salesforce platform events and change events."
          subjects: ["pubsub.>"]
          max_age: "24h"
          retention: limits
          discard: old
```

`require_leader_election: true` ensures only one pod runs the subscriber — Salesforce CDC topics should not have competing consumers.

The NATS subject encodes the event type via `{{event.subject}}`. The subscriber lowercases the topic name, so `AccountChangeEvent` lands on `pubsub.data.accountchangeevent`, `OpportunityChangeEvent` on `pubsub.data.opportunitychangeevent`, etc. A single NATS stream with subject filter `pubsub.>` captures all of them.

## Writer flow

<Mermaid chart={`
graph TD
  SUB[start_subscription] --> STRIP[strip_subject_prefix]
  STRIP --> OBJ[write_to_object_store]
  STRIP --> DW[write_to_data_warehouse]
`} />

Consumes from the NATS stream and branches to multiple destinations. Both sinks declare `depends_on: [strip_subject_prefix]`, so flowgen clones every event into both paths and runs them in parallel. The NATS message is acknowledged only after **both** leaf tasks complete successfully.

```yaml
flow:
  name: salesforce_writer
  tasks:

    - nats_jetstream_subscriber:
        name: start_subscription
        credentials_path: /etc/nats/credentials.json
        url: "{{env.NATS_URL}}"
        subject: pubsub.>
        stream:
          create_or_update: true
          name: salesforce
          description: "Salesforce platform events and change events."
          subjects: ["pubsub.>"]
          max_age: "24h"
          retention: limits
          discard: old
        durable_name: salesforce_writer

    - script:
        name: strip_subject_prefix
        code: |
          let parts = event.subject.split(".");
          if parts.len() >= 3 && parts[0] == "pubsub" && (parts[1] == "data" || parts[1] == "event") {
              event.subject = parts[1] + "/" + parts[2];
          }
          event

    - object_store:
        name: write_to_object_store
        operation: write
        credentials_path: /etc/gcp/credentials.json
        path: "{{env.GCS_BUCKET_PATH}}/salesforce/{{event.subject}}"
        hive_partition_options:
          enabled: true
          partition_keys:
            - EventDate
        depends_on: [strip_subject_prefix]

    - gcp_bigquery_storage_write:
        name: write_to_data_warehouse
        credentials_path: /etc/gcp/credentials.json
        project_id: "{{env.GCP_PROJECT_ID}}"
        dataset_id: salesforce
        table_id: account
        change_type: upsert
        depends_on: [strip_subject_prefix]
```

### Swapping destinations

Both branches are independent — remove or replace either one without affecting the other.

**Object store** is cloud-agnostic. The same `object_store` task works with GCS, S3, and Azure by changing the path prefix and credentials:

| Cloud | Path prefix | Credentials |
|---|---|---|
| GCS | `gs://bucket/path` | GCP service account JSON |
| AWS S3 | `s3://bucket/path` | AWS access key JSON + `client_options.aws_region` |
| Azure | `az://container/path` | Azure storage account JSON |

```yaml
    # AWS S3 example
    - object_store:
        name: write_to_object_store
        operation: write
        credentials_path: /etc/aws/credentials.json
        path: s3://my-bucket/salesforce/{{event.subject}}
        client_options:
          aws_region: us-east-1
        hive_partition_options:
          enabled: true
          partition_keys:
            - EventDate
        depends_on: [strip_subject_prefix]
```

**Data warehouse** — the example above uses `gcp_bigquery_storage_write`. Replace it with the appropriate warehouse task for your stack. See the task reference for configuration details.

## Credentials

| File | Purpose | Contents |
|---|---|---|
| `/etc/sfdc/credentials.json` | Salesforce Pub/Sub API | `client_id`, `client_secret`, `username`, `password`, `login_url` |
| `/etc/nats/credentials.json` | NATS JetStream | `url`, `nkey_seed` or `credentials_path` |
| `/etc/gcp/credentials.json` | GCS + BigQuery | GCP service account JSON key |

See [Credentials](/docs/flowgen/concepts/credentials) for format details.

## Backfilling

To replay from the start of the Salesforce retention window, set `replay_preset: earliest` on the subscriber:

```yaml
    - salesforce_pubsubapi_subscriber:
        name: start_subscription
        credentials_path: /etc/sfdc/credentials.json
        topic:
          name: /data/AccountChangeEvent
          durable_consumer_options:
            enabled: true
            managed_subscription: false
            name: salesforce_account_subscriber
            replay_preset: earliest
```

Once the subscriber catches up it resumes from the latest position. Remove `replay_preset` after the backfill completes.

## Multiple CDC topics

Deploy one subscriber flow per topic. All subscribers publish to the same NATS stream, and the writer consumes from all of them via the `pubsub.>` wildcard:

<Mermaid chart={`
graph LR
  A[account_subscriber] --> NATS[(NATS JetStream\npubsub.>)]
  B[opportunity_subscriber] --> NATS
  C[contact_subscriber] --> NATS
  NATS --> W[writer flow]
  W --> OBJ[Object Store]
  W --> DW[Data Warehouse]
`} />

The writer routes each event type by `{{event.subject}}` in the object store path. For per-topic warehouse tables, use a script task to set the table name dynamically or deploy separate writer flows per topic.

## CDC vs Bulk API

| | CDC Replication | Bulk API Export |
|---|---|---|
| **Latency** | Real-time (seconds) | Scheduled (minutes) |
| **Data** | Individual change events | Full or incremental snapshots |
| **Volume** | Per-record | Batched CSV |
| **Schema** | Avro (typed) | CSV (requires schema casting) |
| **Use case** | Keep warehouse in sync | Periodic snapshots, backfills |

Both patterns can coexist — use CDC for real-time sync and Bulk API for periodic full reconciliation. See [Salesforce Data Export](/docs/flowgen/salesforce/guides/data-export) for the Bulk API pattern.
