# Salesforce

Flowgen connects to Salesforce via the [REST API](/docs/flowgen/salesforce/rest), [Bulk API](/docs/flowgen/salesforce/bulk), [SOAP API](/docs/flowgen/salesforce/merge), [Tooling API](/docs/flowgen/salesforce/tooling), and [Pub/Sub API](/docs/flowgen/salesforce/pubsub).

## Credentials

The `credentials_path` field points to a JSON file with your Connected App credentials. Flowgen authenticates using the OAuth2 Client Credentials flow.

```json
{
  "client_id": "your_connected_app_consumer_key",
  "client_secret": "your_consumer_secret",
  "instance_url": "https://your-instance.salesforce.com",
  "tenant_id": "00Dxxxxxxxxxxxxxxx"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `client_id` | string | yes | Connected App Consumer Key. |
| `client_secret` | string | yes | Connected App Consumer Secret. |
| `instance_url` | string | yes | e.g. `https://your-instance.salesforce.com`. |
| `tenant_id` | string | Pub/Sub only | 15 or 18-character Salesforce Org ID. |

## Retries

Rate limits (`429`), `5xx` responses, and connection errors and timeouts are retried per the task's [retry config](/docs/flowgen/concepts/retry). An expired session is retried after logging in again. Other Salesforce errors fail the event, and so does a missing or invalid field. Operations that create something (records, record trees, merges, query jobs, event subscriptions) are retried only after an expired session, since a call repeated after a timeout can create it twice.

