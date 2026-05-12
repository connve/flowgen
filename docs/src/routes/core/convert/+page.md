# Convert

Converts event data between formats.

## Configuration

```yaml
- convert:
    name: to_arrow
    target_format: arrow
```

### Fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `target_format` | string | `avro` | Target format: `avro`, `json`, `arrow`. |
| `schema` | string/resource | | Optional schema for the conversion. |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

## Example

```yaml
- convert:
    name: json_to_arrow
    target_format: arrow
    schema:
      resource: schemas/orders.json
```
