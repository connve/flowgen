# NATS

Flowgen connects to NATS JetStream via [Subscribe](/docs/flowgen/nats/subscribe), [Publish](/docs/flowgen/nats/publish), and [KV Store](/docs/flowgen/nats/kv-store).

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
