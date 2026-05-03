# Salesforce Tooling API

Manage Salesforce metadata. Currently supports creating managed event subscriptions for Change Data Capture.

## Configuration

```yaml
- salesforce_toolingapi:
    name: create_subscription
    operation: create_managed_event_subscription
    credentials_path: /etc/salesforce/credentials.json
    full_name: AccountChangeEvent_flowgen
    metadata:
      label: Account CDC Subscription
      topic_name: /data/AccountChangeEvent
      default_replay: latest
      state: run
      error_recovery_replay: latest
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create_managed_event_subscription`. |
| `credentials_path` | string | required | Path to Salesforce credentials. |
| `full_name` | string | | API name for the subscription. |
| `metadata` | object | | Subscription metadata (see below). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Metadata fields

| Field | Type | Description |
|---|---|---|
| `label` | string | Human-readable label. |
| `topic_name` | string | CDC topic (e.g., `/data/AccountChangeEvent`). |
| `default_replay` | string | `latest` or `earliest`. |
| `state` | string | `run` or `stop`. |
| `error_recovery_replay` | string | `latest` or `earliest`. |
