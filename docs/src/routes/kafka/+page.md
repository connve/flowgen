# Kafka

Flowgen produces messages to Apache Kafka topics.

- [Produce](/docs/flowgen/kafka/produce) — sends the incoming event to a topic and emits the delivery result downstream.

## Credentials

`credentials_path` is optional and points to a JSON file with authentication details. Omitting it connects to the brokers without authentication.

Both `sasl` and `ssl` are optional, and which blocks are present decides the security protocol:

| `sasl` | `ssl` | Protocol |
|---|---|---|
| ✓ | ✓ | `SASL_SSL` |
| ✓ | | `SASL_PLAINTEXT` |
| | ✓ | `SSL` |

`SASL_SSL` is what managed Kafka (Confluent Cloud, Amazon MSK) expects. Set `security_protocol` to override the derived value; a credentials file with no `sasl` and no `ssl` is rejected rather than silently connecting in plaintext.

```json
{
  "sasl": {
    "username": "user",
    "password": "pass",
    "mechanism": "SCRAM-SHA-256"
  },
  "ssl": {
    "ca_location": "/etc/kafka/ca.pem",
    "certificate_location": "/etc/kafka/client.pem",
    "key_location": "/etc/kafka/client.key",
    "key_password": "secret"
  }
}
```

| Field | Type | Default | Description |
|---|---|---|---|
| `sasl.username` | string | required | SASL username. |
| `sasl.password` | string | required | SASL password. |
| `sasl.mechanism` | string | `SCRAM-SHA-256` | SASL mechanism. |
| `ssl.ca_location` | string | | Path to the CA certificate bundle. |
| `ssl.certificate_location` | string | | Path to the client certificate. |
| `ssl.key_location` | string | | Path to the client private key. |
| `ssl.key_password` | string | | Password protecting the private key. |
| `security_protocol` | string | derived | Overrides the protocol implied by the blocks above. |
