# Klaviyo Events

Sends events to Klaviyo, such as placed orders, where Klaviyo flows and segments can react to them. Klaviyo creates the profile and the metric if they don't exist yet.

## Configuration

```yaml
- klaviyo_events:
    name: track_order
    operation: create
    credentials_path: /etc/klaviyo/credentials.json
    payload:
      from_event: true
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | `create`. |
| `credentials_path` | string | required | Path to the [credentials](/docs/flowgen/klaviyo) file. |
| `base_url` | string | Klaviyo's | Klaviyo API address, for example a proxy. |
| `payload` | object | required | The event's attributes, or `from_event: true`. See [Payload](/docs/flowgen/klaviyo#payload). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Attributes

| Attribute | Description |
|---|---|
| `metric` | The event's name: `{"data": {"type": "metric", "attributes": {"name": "Placed Order"}}}`. |
| `profile` | Who did it: `{"data": {"type": "profile", "attributes": {"email": "..."}}}`. |
| `properties` | Anything else about the event, such as the order's items. |
| `value` | A number, such as the order total. |
| `value_currency` | The currency of `value`, e.g. `EUR`. |
| `unique_id` | Makes Klaviyo count an event sent twice only once, e.g. the order ID. |
| `time` | When it happened, if not now. |

`value` must be a number, while config templates render to text, so build the attributes in a `script` and use `from_event: true`.

## Output

Klaviyo accepts the event without returning it, so the task emits `{"accepted": true}`.
