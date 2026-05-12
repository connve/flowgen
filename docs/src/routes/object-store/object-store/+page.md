# Object Store

Read, write, list, and move files on S3, GCS, Azure Blob Storage, or local filesystem.

## Operations

| Operation | Description |
|---|---|
| `read` | Read a file and emit its content as events (supports CSV, Parquet, Avro, JSON). |
| `write` | Write event data to a file. |
| `list` | List files matching a path or glob pattern. |
| `move` | Copy files to a destination, then delete the originals. |

## Configuration

```yaml
- object_store:
    name: read_orders
    operation: read
    path: gs://my-bucket/data/orders.csv
    credentials_path: /path/to/credentials.json
```

### Common fields

| Field | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Task name. |
| `operation` | string | required | One of `read`, `write`, `list`, `move`. |
| `path` | string | | Object store path (required for read, write, list). |
| `credentials_path` | string | | Path to credentials file (GCS service account, AWS credentials). |
| `client_options` | map | | Additional client options (region, endpoint, etc.). |
| `depends_on` | list | | Upstream task names. |
| `retry` | object | | Retry configuration. |

### Read fields

| Field | Type | Default | Description |
|---|---|---|---|
| `batch_size` | int | 8192 | Records per batch for CSV/Parquet reading. |
| `has_header` | bool | true | Whether CSV has a header row. |
| `delimiter` | string | `,` | CSV delimiter character. |
| `delete_after_read` | bool | false | Delete the file after reading. |

### Write fields

| Field | Type | Default | Description |
|---|---|---|---|
| `format` | string | `auto` | Output format: `auto`, `parquet`, `csv`, `avro`, `json`. |
| `hive_partition_options` | object | | Hive-style partitioning (by date, hour). |

### Move fields

| Field | Type | Default | Description |
|---|---|---|---|
| `source` | string | | Source path or glob pattern. |
| `source_files` | list/template | | Explicit list of file URIs to move. Takes precedence over `source`. |
| `destination` | string | required | Destination path. |

## Examples

### Read CSV from GCS

```yaml
- object_store:
    name: read_orders
    operation: read
    path: gs://my-bucket/data/orders.csv
    credentials_path: /path/to/gcs-creds.json
    batch_size: 1000
    has_header: true
```

### Write Parquet to S3

```yaml
- object_store:
    name: write_results
    operation: write
    path: s3://output-bucket/results/
    credentials_path: /path/to/aws-creds.json
    format: parquet
    hive_partition_options:
      enabled: true
      partition_keys:
        - EventDate
```

### List files

```yaml
- object_store:
    name: find_files
    operation: list
    path: gs://my-bucket/incoming/*.parquet
    credentials_path: /path/to/gcs-creds.json
```

Returns `{path, files}` where each file has `location`, `last_modified`, `size`, and `e_tag`.

### Move files

```yaml
- object_store:
    name: archive
    operation: move
    source: gs://my-bucket/incoming/*.csv
    destination: gs://my-bucket/processed/
    credentials_path: /path/to/gcs-creds.json
```

### Move specific files from event data

```yaml
- object_store:
    name: archive_processed
    operation: move
    source_files: "{{event.meta.source_uris}}"
    destination: gs://my-bucket/archive/
    credentials_path: /path/to/gcs-creds.json
```
