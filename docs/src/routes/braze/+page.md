# Braze

Flowgen connects to the [Braze REST API](https://www.braze.com/docs/api/endpoints) to export user profiles and to send messages, campaigns, and Canvases.

- [Export User Data](/docs/flowgen/braze/export-user-data) — exports users by identifier, by segment, or from the global control group.
- [Messaging](/docs/flowgen/braze/messaging) — sends messages, triggers campaigns and Canvases, and sends transactional emails.

## Credentials

The `credentials_path` field points to a JSON file with a Braze REST API key, created in the Braze dashboard under *Settings → API Keys*:

```json
{
  "api_key": "your-api-key"
}
```

Each task also sets `base_url`, the REST endpoint of your Braze instance, such as `https://rest.iad-01.braze.com`. The dashboard shows it next to the API keys. Tasks with the same `credentials_path` and `base_url` share one client.

## Payload

`payload` is the request body Braze expects, written in the config or taken from the event with `from_event: true`. It is checked against the Braze request before it's sent, so a misspelled or mistyped field fails the event without calling Braze:

```yaml
payload:
  campaign_id: "{{env.BRAZE_CAMPAIGN_ID}}"
  recipients:
    - external_user_id: "{{event.data.user_id}}"
```

Config values are [templates](/docs/flowgen/concepts/templating) and render to text. When a field must be a list or a number, build the body in a `script` and use `from_event: true`.

A task stops at startup when its operation lacks a field it needs, such as `payload`, so a config mistake shows before any event arrives.

## Retries

Rate limits (`429`), `5xx` responses, and connection errors and timeouts are retried per the task's [retry config](/docs/flowgen/concepts/retry). Other Braze errors fail the event. Messaging sends are never retried, since a send repeated after a timeout can deliver the message twice.
