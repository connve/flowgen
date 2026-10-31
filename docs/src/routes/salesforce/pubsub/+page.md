# Salesforce Pub/Sub API

Subscribe to and publish Salesforce platform events and Change Data Capture events via the gRPC-based Pub/Sub API.

## Subscribe

```yaml
- salesforce_pubsubapi_subscribe:
    name: account_changes
    credentials_path: /etc/salesforce/credentials.json
    topic:
      name: /data/AccountChangeEvent
      num_requested: 100
      durable_consumer_options:
        enabled: true
        managed_subscription: true
        name: flowgen_account_sub
        replay_preset: latest
```

### Subscriber fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `topic.name` | string | required | Topic name (e.g., `/data/AccountChangeEvent`). |
| `topic.num_requested` | int | `100` | Events per batch, 1 to 100. |
| `topic.durable_consumer_options` | object | | Durable consumer settings (see below). |
| `endpoint` | string | | Custom Pub/Sub API endpoint. |
| `ack_timeout` | duration | wait indefinitely | How long to wait for the flow to complete an event. |
| `max_deliver` | int | | How many times an event is sent through the flow before it is skipped. When omitted, an event is delivered until the flow completes it. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry) for connecting and for redelivery. |

### Delivery

Batches run through the flow one at a time, in order; the events of a batch run concurrently. When a batch starts, the subscriber asks Salesforce for the next `num_requested` events, so at most one batch waits. Salesforce closes a stream that has no requested events outstanding for 60 seconds, so while a batch waits longer than 30 seconds the subscriber asks for one more event at a time.

An event the flow fails to complete, or does not complete within `ack_timeout`, is delivered again after the [redelivery delay](/docs/flowgen/concepts/retry#redelivery), and the next batch waits for it. With `max_deliver`, the event is skipped after that many failed deliveries and logged with its event ID.

When a batch is done, a durable consumer stores the replay ID of its last event: a managed subscription commits it to Salesforce, otherwise it is kept in the flowgen cache. On a quiet topic it also stores the latest replay ID from Salesforce keepalives once every batch is done, so the stored position stays within the retention window. After a restart, reading continues after it. Delivery is at least once.

A cached replay ID that Salesforce rejects as invalid is deleted, and the next subscription starts from `replay_preset`.

When the stream breaks, the subscriber reconnects with the [reconnect backoff](/docs/flowgen/concepts/retry#infinite-reconnect-subscribers). The delay resets to `initial_backoff` once the new stream delivers a response, so a subscription that fails before its first response, such as one without access to the topic, keeps backing off.

### Durable consumer options

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Enable durable consumer. |
| `managed_subscription` | bool | | Salesforce manages the subscription lifecycle. |
| `name` | string | required | Consumer name. |
| `replay_preset` | string | `latest` | Start position: `latest` or `earliest`. |

## Publish

```yaml
- salesforce_pubsubapi_publish:
    name: publish_event
    credentials_path: /etc/salesforce/credentials.json
    topic: /event/Order_Update__e
    payload:
      Order_Id__c: "{{event.data.order_id}}"
      Status__c: "{{event.data.status}}"
```

### Publisher fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `topic` | string | required | Target topic name. |
| `payload` | object | required | Event payload — explicit fields or `from_event: true`. |
| `endpoint` | string | | Custom Pub/Sub API endpoint. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

| Format | Crate | Description |
|---|---|---|
| [Avro](https://docs.rs/apache-avro/latest/apache_avro/) | [salesforce_core](https://crates.io/crates/salesforce_core) | Avro-decoded event payload as defined by the Salesforce event schema. CDC events include change data and change descriptor. Event ID is set as `event.id`. |
