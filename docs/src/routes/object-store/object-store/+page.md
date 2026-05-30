# Object Store

Read, write, list, and move files on S3, GCS, Azure Blob Storage, or local filesystem.

## Operations

| Operation | Description |
|---|---|
| `read` | Read a single file and emit its content as events (CSV, Parquet, Avro, JSON). Path must point to a concrete file — no wildcards or directory traversal. To read multiple files, use `list → iterate → read`. |
| `write` | Write event data to a file. |
| `list` | List files at a path prefix. Supports glob patterns (`*.parquet`) and recursive traversal into nested subdirectories. |
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
| `retry` | object | | [Retry configuration](/docs/flowgen/concepts/retry). |

### Read fields

Read requires a concrete file path (e.g. `gs://bucket/dir/file.csv`). It does not support wildcards or directory listing. To process multiple files, chain `list → iterate → read` and template the path from the list output.

| Field | Type | Default | Description |
|---|---|---|---|
| `batch_size` | int | 8192 | Records per batch for CSV/Parquet reading. |
| `has_header` | bool | true | Whether CSV has a header row. |
| `delimiter` | string | `,` | CSV delimiter character. |
| `delete_after_read` | bool | false | Delete the file after reading. |

### List fields

| Field | Type | Default | Description |
|---|---|---|---|
| `recursive` | bool | false | Traverse nested subdirectories. When false, only lists files at the immediate prefix level. |

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

## Credentials

### Google Cloud Storage

Pass a GCP service account JSON file via `credentials_path`:

```yaml
- object_store:
    name: read_gcs
    operation: read
    path: gs://my-bucket/data/orders.csv
    credentials_path: /etc/gcp/service-account.json
```

### Amazon S3

Three authentication methods are supported, in order of precedence:

**1. Inline credentials via `client_options`:**

```yaml
- object_store:
    name: read_s3
    operation: read
    path: s3://my-bucket/data/
    client_options:
      aws_access_key_id: "{{env.AWS_ACCESS_KEY_ID}}"
      aws_secret_access_key: "{{env.AWS_SECRET_ACCESS_KEY}}"
      aws_region: eu-west-1
```

**2. Credentials JSON file via `credentials_path`:**

```yaml
- object_store:
    name: read_s3
    operation: read
    path: s3://my-bucket/data/
    credentials_path: /etc/aws/credentials.json
```

The file must contain a JSON object with one or more of these keys:

```json
{
  "aws_access_key_id": "AKIA...",
  "aws_secret_access_key": "...",
  "aws_session_token": "...",
  "aws_region": "eu-west-1"
}
```

Values from `client_options` take precedence over the credentials file.

**3. Environment variables and IAM roles (automatic):**

When no `credentials_path` or inline AWS keys in `client_options` are provided, the client resolves credentials automatically in this order: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` environment variables, IRSA (EKS web identity token), EKS Pod Identity, ECS task roles, or EC2 instance profiles. You still need `aws_region` in `client_options`.

S3 requests use virtual-hosted style URLs by default (`{bucket}.s3.{region}.amazonaws.com`), matching the AWS SDK convention. Set `aws_virtual_hosted_style_request: "false"` in `client_options` for path-style URLs (e.g. MinIO, LocalStack).

**Additional `client_options` for S3:**

| Key | Description |
|---|---|
| `aws_region` | AWS region (required unless `AWS_REGION` / `AWS_DEFAULT_REGION` is set). |
| `aws_role_session_name` | Override the STS session name for IRSA (`AssumeRoleWithWebIdentity`). Defaults to `WebIdentitySession`. |
| `aws_virtual_hosted_style_request` | `"true"` (default) for virtual-hosted URLs, `"false"` for path-style. |
| `aws_endpoint` | Custom S3 endpoint URL (for S3-compatible stores). |

### Local filesystem

No credentials needed. Use `file://` paths:

```yaml
- object_store:
    name: read_local
    operation: read
    path: file:///data/orders.csv
```

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
    credentials_path: /etc/aws/credentials.json
    format: parquet
    client_options:
      aws_region: eu-west-1
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

### List files recursively

When files are spread across nested subdirectories (e.g. `bucket/a/file.csv`, `bucket/b/c/file.csv`), use `recursive: true` to traverse the entire tree:

```yaml
- object_store:
    name: find_all_files
    operation: list
    path: gs://my-bucket/data/
    recursive: true
    credentials_path: /path/to/gcs-creds.json
```

### Read multiple files (list + iterate + read)

Read requires a concrete file path — it cannot list or traverse directories. To process multiple files, chain list, iterate over the results, and read each file using a template:

```yaml
- object_store:
    name: find_files
    operation: list
    path: gs://my-bucket/data/
    recursive: true
    credentials_path: /path/to/gcs-creds.json

- iterate:
    name: each_file
    iterate_key: files

- object_store:
    name: read_file
    operation: read
    path: "gs://my-bucket{{event.data.location}}"
    credentials_path: /path/to/gcs-creds.json
```

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

## Output

### Read

| Format | Crate | Description |
|---|---|---|
| [Arrow RecordBatch](https://docs.rs/arrow/latest/arrow/record_batch/struct.RecordBatch.html) / [JSON](https://docs.rs/serde_json/latest/serde_json/enum.Value.html) | [object_store](https://docs.rs/object_store/latest/object_store/) | File contents. Format depends on source file type (Parquet, ORC, CSV, JSON). Each record batch becomes one event. |

### Write

| Field | Type | Description |
|---|---|---|
| `path` | string | Written file path. |
| `size` | int | Bytes written. |
| `e_tag` | string / null | S3 ETag if available. |

### List

| Field | Type | Description |
|---|---|---|
| `path` | string | Pattern searched. |
| `files` | array | Matched files, each with `location`, `last_modified`, `size`, and `e_tag`. |
