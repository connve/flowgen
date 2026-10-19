# Braze Messaging

Sends messages, campaigns, and Canvases through the Braze API.

## Configuration

```yaml
- braze_messaging:
    name: notify_customer
    operation: trigger_campaign
    credentials_path: /etc/braze/credentials.json
    base_url: "{{env.BRAZE_REST_ENDPOINT}}"
    payload:
      campaign_id: "{{env.BRAZE_CAMPAIGN_ID}}"
      recipients:
        - external_user_id: "{{event.data.user_id}}"
          trigger_properties:
            order_id: "{{event.data.order_id}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | See the table below. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/braze) file. |
| `base_url` | string | required | REST endpoint of the Braze instance, e.g. `https://rest.iad-01.braze.com`. |
| `campaign_id` | string | | Transactional email campaign ID for `send_transactional`. |
| `payload` | object | required | The request body, or `from_event: true`. See [Payload](/docs/flowgen/braze#payload). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Operations

| Operation | Endpoint | Payload |
|---|---|---|
| `send_messages` | `POST /messages/send` | `messages` per channel, and who to send to: `external_user_ids`, `user_aliases`, `segment_id`, `audience`, or `broadcast`. |
| `trigger_campaign` | `POST /campaigns/trigger/send` | `campaign_id`, and `recipients` with optional `trigger_properties`, or `audience` or `broadcast`. |
| `trigger_canvas` | `POST /canvas/trigger/send` | `canvas_id`, and `recipients` with optional `context`, or `audience` or `broadcast`. |
| `create_send_ids` | `POST /sends/id/create` | `campaign_id`, and optionally the `send_id` to create. |
| `send_transactional` | `POST /transactional/v1/campaigns/{campaign_id}/send` | `recipient`, and optionally `trigger_properties` and `external_send_id`. |

Every operation except `create_send_ids` delivers real messages and is never retried.

## Output

The Braze response as JSON, such as the `dispatch_id` of a send. Sends and triggers list problems that didn't stop the send in `errors`.
