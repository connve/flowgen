# Klaviyo Campaigns

Reads and writes Klaviyo campaigns, their messages and their variations.

## Configuration

```yaml
- klaviyo_campaigns:
    name: find_campaign
    operation: list
    credentials_path: /etc/klaviyo/credentials.json
    filter: "contains(name,'{{event.data.name}}')"
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | See the table below. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/klaviyo) file. |
| `base_url` | string | Klaviyo's | Klaviyo API address, for example a proxy. |
| `campaign_id` | string | | Campaign ID. |
| `message_id` | string | | Campaign message ID for `variations`. |
| `variation_id` | string | | Campaign variation ID for `update_variation`. |
| `filter` | string | | Klaviyo filter for `list`, e.g. `equals(archived,false)`. |
| `payload` | object | | The campaign's or variation's attributes, or `from_event: true`. See [Payload](/docs/flowgen/klaviyo#payload). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Operations

| Operation | Needs | What it does |
|---|---|---|
| `get` | `campaign_id` | Gets the campaign. |
| `list` | | Lists campaigns, narrowed by `filter` if set. Klaviyo filters campaigns on `id`, `name`, `archived`, `builder`, `created_at` and `updated_at`; `name` takes only `contains`, so an exact name match is checked after the list. |
| `create` | `payload` | Creates a draft campaign with its audiences, messages and variations. |
| `update` | `campaign_id`, `payload` | Updates the campaign's `definition`. |
| `delete` | `campaign_id` | Deletes the campaign. |
| `clone` | `campaign_id` | Copies the campaign, named `payload.name` if set. |
| `messages` | `campaign_id` | Lists the campaign's messages. |
| `variations` | `message_id` | Lists the message's variations, each with its subject, sender and `template_id`. |
| `update_variation` | `variation_id`, `payload` | Updates the variation. Only the fields in `payload` are sent, so `{definition: {details: {channel: email, template_id: ...}}}` changes the template and keeps the subject. |
| `send` | `campaign_id` | Sends or schedules the campaign as its send settings say. Delivers real messages and is never retried. |

A campaign is created with its audiences and messages inline. A message refers to its audience by `audience_temporary_id`, and each variation carries the subject, sender and template:

```json
{
  "definition": { "name": "Spring newsletter" },
  "audiences": [{ "temporary_id": "newsletter", "included": ["LIST_ID"] }],
  "messages": [{
    "audience_temporary_id": "newsletter",
    "name": "Spring newsletter",
    "variations": [{
      "definition": {
        "name": "Spring newsletter",
        "details": {
          "channel": "email",
          "subject": "What's new this spring",
          "template_id": "TEMPLATE_ID"
        }
      }
    }]
  }]
}
```

## Output

The Klaviyo response as JSON. `list` emits one event per page, with the campaigns in `data`. `delete` emits `{"id": "...", "done": true}`.
