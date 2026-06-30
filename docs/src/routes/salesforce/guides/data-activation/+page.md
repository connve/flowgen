<script>
  import Mermaid from '$lib/components/Mermaid.svelte';
</script>

# Salesforce Data Activation

Push data into Salesforce from external systems. Use the Composite API for batch record upserts and the Pub/Sub API for publishing platform events that trigger Salesforce automation.

## Architecture

Data activation flows consume events from NATS and write them into Salesforce:

<Mermaid chart={`
graph LR
  SRC[(External System)] --> NATS[(NATS JetStream)]
  subgraph Composite Upsert Flow
    SUB1[Subscriber] --> UPSERT[Composite API upsert]
  end
  subgraph Platform Event Flow
    SUB2[Subscriber] --> PUB[Pub/Sub API publisher]
  end
  NATS --> SUB1
  NATS --> SUB2
  UPSERT --> SF[(Salesforce)]
  PUB --> SF
`} />

**Composite API** — batch create, update, upsert, or delete up to 200 records per request with transactional `all_or_none` semantics.

**Platform Events** — publish custom platform events to trigger Salesforce Flows, Process Builder, or Apex triggers.

## Composite API upsert

Upsert records into Salesforce using the Composite SObject Collections API. Each request handles up to 200 records atomically.

<Mermaid chart={`
graph LR
  NATS[(NATS)] --> SUB[nats_jetstream_subscriber]
  SUB --> UPSERT[salesforce_restapi_composite]
  UPSERT --> SF[(Salesforce)]
`} />

```yaml
flow:
  name: salesforce_composite_upsert_contact
  tasks:

    - nats_jetstream_subscriber:
        name: start_subscription
        credentials_path: /etc/nats/credentials.json
        url: "{{env.NATS_URL}}"
        subject: crm.contacts.upsert
        stream:
          create_or_update: true
          name: crm
          subjects: ["crm.>"]
          max_age: 1h
          max_bytes: 2147483648
          retention: limits
          discard: old
        durable_name: salesforce_composite_upsert_contact
        ack_timeout: "120s"
        max_deliver: 5
        backoff:
          - "10s"
          - "30s"
          - "1m"
          - "5m"

    - salesforce_restapi_composite:
        name: upsert_contacts
        operation: upsert
        credentials_path: /etc/sfdc/credentials.json
        sobject_type: Contact
        external_id_field: ExternalId__c
        all_or_none: true
        payload:
          from_event: true
```

The `from_event: true` payload passes the incoming event data directly as the record payload. The event must contain an array of record objects matching the SObject field names.

### Composite operations

The same task supports all SObject Collection operations:

| Operation | Description |
|---|---|
| `create` | Insert new records |
| `get` | Retrieve records by ID |
| `update` | Update existing records by ID |
| `upsert` | Upsert by external ID (create or update) |
| `delete` | Delete records by ID |
| `tree` | Create records with parent-child relationships |

### all_or_none

When `all_or_none: true`, the entire batch succeeds or fails atomically. If any record fails validation, no records are committed. Set to `false` to allow partial success — successfully validated records are committed while failed records return errors.

### allow_duplicate_save

Salesforce duplicate detection rules ship with a default **Block** action on Account / Lead / Contact, so a `create` or `upsert` that matches an existing record on a rule-watched field (`Email`, `Phone`, …) is silently rejected with `DUPLICATES_DETECTED` in the per-record errors array.

Set `allow_duplicate_save: true` to attach `Sforce-Duplicate-Rule-Header: allowSave=true` to the request. Salesforce then saves the record and downgrades the duplicate to a warning. See [Duplicate-rule override](/docs/flowgen/salesforce/rest#duplicate-rule-override) for details.

## Platform event publisher

Publish platform events to Salesforce to trigger automation — Salesforce Flows, Process Builder, or Apex triggers that subscribe to the event.

<Mermaid chart={`
graph LR
  NATS[(NATS)] --> SUB[nats_jetstream_subscriber]
  SUB --> PUB[salesforce_pubsubapi_publisher]
  PUB --> SF[(Salesforce Pub/Sub)]
`} />

```yaml
flow:
  name: salesforce_platform_event_publisher
  tasks:

    - nats_jetstream_subscriber:
        name: start_subscription
        credentials_path: /etc/nats/credentials.json
        url: "{{env.NATS_URL}}"
        subject: salesforce.platform_events.order_status
        stream:
          create_or_update: true
          name: salesforce_platform_events
          subjects: ["salesforce.platform_events.>"]
          max_age: 1h
          max_bytes: 2147483648
          retention: limits
          discard: old
        durable_name: salesforce_platform_event_publisher
        ack_timeout: "60s"
        max_deliver: 5
        backoff:
          - "10s"
          - "30s"
          - "1m"
          - "5m"

    - salesforce_pubsubapi_publisher:
        name: publish_order_status
        credentials_path: /etc/sfdc/credentials.json
        topic: /event/OrderStatus__e
        payload:
          from_event: true
```

The publisher sends the event payload to the specified platform event topic. The event data must match the platform event field structure defined in Salesforce Setup.

### Explicit payload

Instead of `from_event: true`, you can map fields explicitly with template expressions:

```yaml
    - salesforce_pubsubapi_publisher:
        name: publish_order_status
        credentials_path: /etc/sfdc/credentials.json
        topic: /event/OrderStatus__e
        payload:
          Order_ID__c: "{{event.data.order_id}}"
          Status__c: "{{event.data.status}}"
          Timestamp__c: "{{event.data.updated_at}}"
```

## Choosing an approach

| | Composite API | Platform Events |
|---|---|---|
| **Direction** | Write records directly | Trigger Salesforce automation |
| **Volume** | Up to 200 records per request | Single events |
| **Atomicity** | `all_or_none` transactional | Fire-and-forget |
| **Use case** | Sync CRM data from external systems | Notify Salesforce of external events |

Use the Composite API when you need to create or update records directly. Use platform events when external changes should trigger Salesforce-side logic (Flows, triggers, Process Builder).

## Credentials

Both flows require Salesforce credentials at the configured `credentials_path`:

```json
{
  "client_id": "your_connected_app_client_id",
  "client_secret": "your_connected_app_client_secret",
  "username": "your_username",
  "password": "your_password",
  "login_url": "https://login.salesforce.com"
}
```
