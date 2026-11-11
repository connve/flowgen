# NATS

Flowgen connects to NATS JetStream via [Subscribe](/docs/flowgen/nats/subscribe) and [Publish](/docs/flowgen/nats/publish). Key-value buckets are read and written with [KV Store](/docs/flowgen/kv-store/kv-store) and a `nats://` URL.

## Credentials

The `credentials_path` field points to a JSON file with NKey authentication:

```json
{
  "nkey": {
    "seed": "your-private-key"
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `nkey.seed` | string | yes | NKey seed (private key). Starts with `S`. Generate with `nsc generate nkey -u`. |
