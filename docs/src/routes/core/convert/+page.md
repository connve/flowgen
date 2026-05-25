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
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

## Output

| Format | Crate | Description |
|---|---|---|
| [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) / [Arrow RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) / [Avro](https://docs.rs/apache-avro/latest/apache_avro/) | target format | Converted payload. Field values are preserved; only the serialisation format changes. Hyphens in JSON keys are replaced with underscores for Avro compatibility. |

## Example

```yaml
- convert:
    name: json_to_arrow
    target_format: arrow
    schema:
      resource: schemas/orders.json
```
