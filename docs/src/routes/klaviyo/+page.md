# Klaviyo

Flowgen reads and writes Klaviyo data through the [Klaviyo API](https://developers.klaviyo.com/en/reference/api_overview).

- [Templates](/docs/flowgen/klaviyo/templates) — get, list, create, update, upsert, delete, render, and clone email templates.
- [Campaigns](/docs/flowgen/klaviyo/campaigns) — create draft campaigns, assign templates, and send.
- [Lists](/docs/flowgen/klaviyo/lists) — get a list, or find lists by name.
- [Events](/docs/flowgen/klaviyo/events) — track events such as orders, which Klaviyo flows and segments react to.
- [Profiles](/docs/flowgen/klaviyo/profiles) — create, update, and upsert profiles.

## Credentials

The `credentials_path` field points to a JSON file with a private API key, created in Klaviyo under *Settings → API keys*:

```json
{
  "api_key": "pk_..."
}
```

A key belongs to one Klaviyo account, so a task talks to the account of its `credentials_path`. Tasks with the same `credentials_path` and `base_url` share one client. Set `base_url` only to reach Klaviyo through another address, such as a proxy.

A task stops at startup when its operation lacks a field it needs, such as `template_id` for `get`, so a config mistake shows before any event arrives.

## Payload

Klaviyo requests are JSON:API documents. `payload` holds only the resource's `attributes`; the task adds the `type` and `id` and checks the result against the Klaviyo request before sending it:

```yaml
payload:
  name: "Welcome"
  editor_type: "CODE"
  html: "{{event.data.html}}"
```

With `from_event: true`, the event data is the attributes. Use it when a `script` builds them, for example to keep numbers as numbers or to build nested lists:

```yaml
payload:
  from_event: true
```

Config values are [templates](/docs/flowgen/concepts/templating), so Klaviyo's own `{{ first_name }}` tags written straight into the config would be rendered by flowgen. Pass HTML that contains Klaviyo tags through the event data instead.

## Output

Each task emits the Klaviyo response as JSON. A `list` operation emits one event per page of results, so a long list is never held in memory at once.

## Retries

Rate limits (`429`), `5xx` responses, and connection errors and timeouts are retried per the task's [retry config](/docs/flowgen/concepts/retry). Other Klaviyo errors fail the event. An invalid payload or a missing field fails the event without retrying. The campaign `send` operation is never retried, since a send repeated after a timeout can deliver the campaign twice.
