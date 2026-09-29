# Kafka

Flowgen produces messages to Apache Kafka topics and consumes topics into flows.

- [Produce](/docs/flowgen/kafka/produce) — sends the incoming event to a topic and emits the delivery result downstream.
- [Subscribe](/docs/flowgen/kafka/subscribe) — consumes every partition of a topic and emits each record as an event.

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
| `sasl.mechanism` | string | `SCRAM-SHA-256` | `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`. |
| `ssl.ca_location` | string | | Path to a PEM file with the CA certificates. Omit to trust the system's root certificates. |
| `ssl.certificate_location` | string | | Path to the client certificate chain (PEM). Requires `ssl.key_location`. |
| `ssl.key_location` | string | | Path to the client private key (PEM: PKCS#8, PKCS#1, or SEC1). |
| `ssl.key_password` | string | | Password of an encrypted PKCS#8 key (`BEGIN ENCRYPTED PRIVATE KEY`). Ignored for an unencrypted key. |
| `security_protocol` | string | derived | `PLAINTEXT`, `SSL`, `SASL_PLAINTEXT`, or `SASL_SSL`. Overrides the protocol implied by the blocks above. |
