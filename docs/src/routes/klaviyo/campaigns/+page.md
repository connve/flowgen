# Klaviyo Campaigns

Reads and writes Klaviyo campaigns.

## Configuration

```yaml
- klaviyo_campaigns:
    name: assign_template
    operation: assign_template
    credentials_path: /etc/klaviyo/credentials.json
    campaign_id: "{{event.data.data.id}}"
    template_id: "{{event.meta.template_id}}"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | See the table below. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/klaviyo) file. |
| `base_url` | string | Klaviyo's | Klaviyo API address, for example a proxy. |
| `campaign_id` | string | | Campaign ID. |
| `message_id` | string | | Campaign message ID for `assign_template`. |
| `template_id` | string | | Template ID for `assign_template`. |
| `filter` | string | | Klaviyo filter for `list`. It must name a channel, e.g. `equals(messages.channel,'email')`. |
| `payload` | object | | The campaign's attributes, or `from_event: true`. See [Payload](/docs/flowgen/klaviyo#payload). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Operations

| Operation | Needs | What it does |
|---|---|---|
| `get` | `campaign_id` | Gets the campaign. |
| `list` | `filter` | Lists campaigns matching `filter`. |
| `create` | `payload` | Creates a draft campaign. Attributes: `name`, `audiences.included` (list or segment IDs), and `campaign-messages`. |
| `update` | `campaign_id`, `payload` | Updates the campaign. |
| `delete` | `campaign_id` | Deletes the campaign. |
| `clone` | `campaign_id` | Copies the campaign, named `payload.new_name` if set. |
| `messages` | `campaign_id` | Lists the campaign's messages. |
| `assign_template` | `template_id`, and `message_id` or `campaign_id` | Uses the template for the message, or for every message of the campaign. |
| `send` | `campaign_id` | Sends or schedules the campaign as its send strategy says. Delivers real messages and is never retried. |

A `campaign-messages` entry is a JSON:API resource of its own, so it carries its `type`:

```json
{
  "data": [{
    "type": "campaign-message",
    "attributes": {
      "definition": {
        "channel": "email",
        "label": "Spring newsletter",
        "content": { "subject": "What's new this spring" }
      }
    }
  }]
}
```

## Output

The Klaviyo response as JSON. `list` emits one event per page, with the campaigns in `data`. `delete` emits `{"id": "...", "done": true}`. `assign_template` with `campaign_id` emits one event per message.
