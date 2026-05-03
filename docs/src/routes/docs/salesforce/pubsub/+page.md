# Salesforce Pub/Sub API

Subscribe to and publish Salesforce platform events and Change Data Capture events via the gRPC-based Pub/Sub API.

## Subscriber

```yaml
- salesforce_pubsubapi_subscriber:
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
| `topic.num_requested` | int | | Messages per batch. |
| `topic.durable_consumer_options` | object | | Durable consumer settings (see below). |
| `endpoint` | string | | Custom Pub/Sub API endpoint. |
| `ack_timeout` | duration | | Flow completion timeout. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Durable consumer options

| Field | Type | Default | Description |
|---|---|---|---|
| `enabled` | bool | required | Enable durable consumer. |
| `managed_subscription` | bool | | Salesforce manages the subscription lifecycle. |
| `name` | string | required | Consumer name. |
| `replay_preset` | string | `latest` | Start position: `latest` or `earliest`. |

## Publisher

```yaml
- salesforce_pubsubapi_publisher:
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
| `retry` | object | | Retry configuration. |
