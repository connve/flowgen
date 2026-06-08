# NATS

Flowgen connects to NATS JetStream via [Subscriber](/docs/flowgen/nats/subscriber), [Publisher](/docs/flowgen/nats/publisher), and [KV Store](/docs/flowgen/nats/kv-store).

## Credentials

The `credentials_path` field points to a JSON file with NKey authentication:

```json
{
  "nkey": {
    "seed": "SUACSSL3UAHUDXKFSNVUZRF5UHPMWZ6BFDTJ7M6USDXIEDNPPQYYYCU3VY"
  }
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `nkey.seed` | string | yes | NKey seed (private key). Starts with `S`. Generate with `nsc generate nkey -u`. |
