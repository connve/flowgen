# MSSQL Query Examples

This directory contains examples for using the MSSQL query task type in Flowgen.

## Prerequisites

1. **SQL Server Instance**: Access to a Microsoft SQL Server instance
2. **Credentials File**: Create a credentials JSON file with your connection details

## Setup

### 1. Create Credentials File

Copy the example credentials file and update with your SQL Server details:

```bash
cp examples/mssql/credentials.example.json /var/secrets/mssql/credentials.json
```

Edit the credentials file with your actual connection details:

```json
{
  "host": "your-server.example.com",
  "port": 1433,
  "database": "your_database",
  "username": "your_username",
  "password": "your_password",
  "trust_server_certificate": true
}
```

**Security Note**: Never commit credentials files to version control. Keep them in a secure location with restricted permissions.

### 2. Set File Permissions

Restrict access to the credentials file:

```bash
chmod 600 /var/secrets/mssql/credentials.json
```

## Examples

### Basic Query (`query.basic.yaml`)

Simple SQL Server query that runs every 5 minutes. Demonstrates:
- Inline SQL query with TOP clause
- Arrow RecordBatch to JSON conversion
- Logging results to console

**Flow Pattern:**
```
generate (trigger) → mssql_query → convert → log
```

Run:
```bash
flowgen_worker run --flow examples/mssql/query.basic.yaml
```

### Parameterized Query (`query.parametrized.yaml`)

Demonstrates parameterized queries for SQL injection prevention. Shows:
- Using `@p1`, `@p2` parameters in SQL
- Static parameter values
- Processing individual rows with iterate
- Structured logging

**Flow Pattern:**
```
generate (trigger) → mssql_query → convert → iterate → log
```

Run:
```bash
flowgen_worker run --flow examples/mssql/query.parametrized.yaml
```

### Resource Query (`query.resource.yaml`)

Loading SQL queries from external files. Demonstrates:
- Using `resource:` to load SQL from external files
- Custom retry configuration
- Longer connection timeout for complex queries
- Daily scheduled execution

**Flow Pattern:**
```
generate (trigger) → mssql_query → convert → log
```

**Resource File:** `examples/resources/mssql/queries/monthly_sales_report.sql`

Run:
```bash
flowgen_worker run --flow examples/mssql/query.resource.yaml \
  --resource-path examples/resources
```

## Configuration Options

### Required Fields

- `name`: Unique task identifier
- `credentials_path`: Path to SQL Server credentials JSON file
- `query`: SQL query (inline or resource file)

### Optional Fields

- `parameters`: Array of query parameters (values for `@p1`, `@p2`, etc.)
- `batch_size`: Rows per Arrow RecordBatch (default: 10000)
- `max_connections`: Connection pool size (default: 10)
- `connection_timeout`: Connection timeout (default: "30s")
- `retry`: Custom retry configuration

## Output Format

Results are returned as Arrow RecordBatch, which provides:
- Efficient columnar data format
- Zero-copy operations
- Native integration with analytics tools

Use the `convert` task to transform to JSON or other formats for downstream processing.

## Type Mapping

SQL Server types are mapped to Arrow types:

| SQL Server Type | Arrow Type |
|----------------|------------|
| BIT, BITN | Boolean |
| TINYINT | Int8 |
| SMALLINT | Int16 |
| INT, INTN | Int32 |
| BIGINT | Int64 |
| REAL, FLOAT(24) | Float32 |
| FLOAT, FLOAT(53) | Float64 |
| MONEY, SMALLMONEY | Decimal128(19, 4) |
| DATETIME, DATETIME2 | Timestamp(Microsecond) |
| DATE | Date32 |
| TIME | Time64(Microsecond) |
| DATETIMEOFFSET | Timestamp(Microsecond, UTC) |
| VARBINARY, BINARY, IMAGE | Binary |
| UNIQUEIDENTIFIER | FixedSizeBinary(16) |
| VARCHAR, NVARCHAR, TEXT, NTEXT | Utf8 |

## Common Patterns

### Scheduled Queries

Use `generate` with `interval` to run queries on a schedule:

```yaml
- generate:
    name: trigger
    interval: 300s  # Every 5 minutes
```

### One-Time Execution

Use `generate` with `count: 1` for a single execution:

```yaml
- generate:
    name: trigger
    interval: 10s
    count: 1
```

### Processing Individual Rows

Convert Arrow to JSON, then use `iterate` to fan-out:

```yaml
- convert:
    name: to_json
    target_format: json

- iterate:
    name: split_rows
```

### Publishing to NATS

Chain MSSQL query results to NATS JetStream:

```yaml
- mssql_query:
    name: fetch_data
    # ... config ...

- convert:
    name: to_json
    target_format: json

- nats_jetstream_publisher:
    name: publish
    # ... config ...
```

## Troubleshooting

### Connection Issues

If you encounter connection errors:
1. Verify SQL Server is accessible from your network
2. Check firewall rules allow TCP/1433
3. Ensure credentials are correct
4. Try setting `trust_server_certificate: true` for self-signed certs

### Query Timeouts

For long-running queries:
- Increase `connection_timeout`
- Add custom retry configuration
- Use TOP clause to limit result size
- Add indexes to improve query performance

### Performance

For large result sets:
- Adjust `batch_size` for memory vs. throughput tradeoff
- Use `max_connections` to control concurrency
- Add WHERE clauses to filter data at source
- Consider pagination with OFFSET/FETCH NEXT

## Additional Resources

- [Flowgen Documentation](https://docs.flowgen.io)
- [SQL Server Connection Strings](https://docs.microsoft.com/en-us/sql/connect/)
- [Arrow Format Specification](https://arrow.apache.org/docs/)
