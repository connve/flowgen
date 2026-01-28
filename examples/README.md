# Flowgen Examples

This directory contains example flows demonstrating various flowgen features and use cases.

## Directory Structure

- **`data/`** - Synthetic test data for examples
- **`resources/`** - External resource files (SQL queries, templates, etc.)
- **`gcp/`** - Examples for Google Cloud Platform integrations
- **`object-store/`** - Examples for reading and writing files
- **`cloudflare/`** - Examples for Cloudflare integrations
- **`salesforce/`** - Examples for Salesforce integrations

## Test Data

The `data/` directory contains synthetic datasets generated for testing and demonstration purposes:

- **`orders.csv`** - 10,000 synthetic order records with fields: id, order_number, order_date, payment_type, amount, customer_id, status

All data in this directory is randomly generated and does not represent real customer or business data.

## Resource Files

The `resources/` directory contains external resource files (SQL queries, templates, etc.) that can be loaded by tasks.

Configure the resource path in `config.yaml`:

```yaml
resources:
  path: "examples/resources"
```

Reference resources in flow configuration:

```yaml
- gcp_bigquery_query:
    name: fetch_data
    query:
      resource: "queries/fetch_completed_orders.sql"
```

## Running Examples

Examples reference paths like `file:///flowgen/examples/data/orders.csv`. Update these paths to match your deployment location.
