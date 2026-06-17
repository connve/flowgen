# HTTP

Flowgen provides HTTP tasks for both inbound and outbound traffic.

- [Endpoint](/docs/flowgen/http/endpoint) — registers an HTTP route on the worker server and injects requests into the flow as events.
- [Request](/docs/flowgen/http/request) — makes outbound HTTP requests with templated URL, headers, and payload.

## Credentials

Both tasks support optional authentication via `credentials_path`. See [Credentials](/docs/flowgen/concepts/credentials) for the JSON format.
