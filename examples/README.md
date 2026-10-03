# Flowgen Examples

This directory contains example flows demonstrating various flowgen features and use cases.

## Directory Structure

- **`ai-completion/`** - `ai_completion` task (RAG, resource-loaded prompts)
- **`ai-gateway/`** - `llm_proxy` fronting OpenAI + Anthropic clients
- **`authoring/`** - Publish and release flows for approved changes, operator agent and its tools
- **`cloudflare/`** - Cloudflare integrations
- **`data/`** - Synthetic test data used by examples
- **`gcp/`** - Google Cloud Platform integrations (BigQuery, Pub/Sub, ...)
- **`git/`** - `git_sync` and `git_push` tasks (pull a repository, commit and push files)
- **`inproc/`** - `inproc_endpoint` and `inproc_request` tasks (one flow calling another in-process)
- **`mcp/`** - MCP server tasks (`mcp_tool`, `mcp_prompt`, `mcp_resource`)
- **`mssql/`** - Microsoft SQL Server integrations
- **`object-store/`** - Reading/writing files (local, S3, GCS)
- **`oci/`** - `oci_sync` and `oci_push` tasks (pull and push workspace artifacts)
- **`resources/`** - Shared resource files (SQL, templates, scripts, schemas)
- **`salesforce/`** - Salesforce (CDC replication, data activation/export, sobject CRUD)
- **`script/`** - `script` task patterns (inline, resource, cache, fan-in join)
- **`parallel_instances.yaml`** - Starter showcasing the `parallel_instances` flow-level knob

## Test Data

The `data/` directory contains synthetic datasets generated for testing and demonstration purposes:

- **`orders.csv`** - 10,000 synthetic order records with fields: id, order_number, order_date, payment_type, amount, customer_id, status

All data in this directory is randomly generated and does not represent real customer or business data.

## Resource Files

The `resources/` directory holds external files that tasks load via `resource:` references. Organised by the example domain they belong to:

- **`ai-completion/context/`** - RAG context snippets for `ai-completion/` flows
- **`ai-completion/prompts/`** - System / user prompt templates for `ai-completion/` flows
- **`gcp/`** - SQL queries and DDL for `gcp/` BigQuery examples
- **`salesforce/`** - SOQL queries for `salesforce/` examples
- **`schemas/`** - JSON schemas shared across examples
- **`scripts/`** - Rhai scripts shared across flows (e.g. `ai-gateway/route_provider.rhai`)

Configure the resource path in `config.yaml`:

```yaml
resources:
  path: "examples/resources"
```

Reference resources in a flow:

```yaml
- gcp_bigquery_query:
    name: fetch_data
    query:
      resource: "gcp/fetch_completed_orders.sql"
```

## Running Examples

Examples reference paths like `file:///flowgen/examples/data/orders.csv`. Update these paths to match your deployment location.
