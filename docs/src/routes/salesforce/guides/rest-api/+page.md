<script>
  import Mermaid from '$lib/components/Mermaid.svelte';
</script>

# Salesforce REST API

Look up, search, and upsert Salesforce records using the REST API. Each operation is a flow that can be triggered via HTTP webhook or exposed as an MCP tool for LLM agents.

## Entrypoints

The flow logic is identical regardless of the entrypoint. Swap the source task to switch between HTTP and MCP:

<Mermaid chart={`
graph LR
  subgraph Entrypoints
    HTTP[http_webhook]
    MCP[mcp_tool]
  end
  HTTP --> OP[Salesforce operation]
  MCP --> OP
`} />

**HTTP webhook** — accepts requests via the flowgen HTTP server. Use for service-to-service integrations, scripts, and CI/CD pipelines.

**MCP tool** — registered with the MCP server so LLM agents can discover and call the operation. Use for AI-powered workflows where agents interact with Salesforce.

## SObject lookup

Retrieve a Salesforce record by ID.

<Mermaid chart={`
graph LR
  A[receive_request] --> B[get_record]
`} />

### HTTP

```yaml
flow:
  name: salesforce_sobject_lookup
  tasks:

    - http_webhook:
        name: receive_request
        method: POST
        endpoint: /salesforce/lookup

    - salesforce_restapi_sobject:
        name: get_record
        operation: get
        credentials_path: /etc/sfdc/credentials.json
        sobject_type: "{{event.data.sobject_type}}"
        record_id: "{{event.data.record_id}}"
        fields: "{{event.data.fields}}"
```

### MCP

```yaml
flow:
  name: salesforce_mcp_sobject_lookup
  tasks:

    - mcp_tool:
        name: salesforce_lookup
        description: "Look up a Salesforce record by object type and record ID."
        input_schema:
          type: object
          properties:
            sobject_type:
              type: string
              description: "Salesforce object type (e.g. Account, Contact, Opportunity)"
            record_id:
              type: string
              description: "Salesforce record ID"
            fields:
              type: string
              description: "Comma-separated list of fields to return"
          required: [sobject_type, record_id]

    - salesforce_restapi_sobject:
        name: get_record
        operation: get
        credentials_path: /etc/sfdc/credentials.json
        sobject_type: "{{event.data.sobject_type}}"
        record_id: "{{event.data.record_id}}"
        fields: "{{event.data.fields}}"
```

The `salesforce_restapi_sobject` task is identical in both flows — only the source task changes.

## SObject upsert

Create or update a Salesforce record by external ID.

<Mermaid chart={`
graph LR
  A[receive_request] --> B[upsert_record]
`} />

### HTTP

```yaml
flow:
  name: salesforce_sobject_upsert
  tasks:

    - http_webhook:
        name: receive_request
        method: POST
        endpoint: /salesforce/upsert

    - salesforce_restapi_sobject:
        name: upsert_record
        operation: upsert
        credentials_path: /etc/sfdc/credentials.json
        sobject_type: "{{event.data.sobject_type}}"
        external_id_field: "{{event.data.external_id_field}}"
        external_id_value: "{{event.data.external_id_value}}"
        payload:
          from_event: true
```

### MCP

```yaml
flow:
  name: salesforce_mcp_sobject_upsert
  tasks:

    - mcp_tool:
        name: salesforce_upsert
        description: "Create or update a Salesforce record by external ID."
        input_schema:
          type: object
          properties:
            sobject_type:
              type: string
              description: "Salesforce object type (e.g. Account, Contact, Opportunity)"
            external_id_field:
              type: string
              description: "External ID field name (e.g. ExternalId__c)"
            external_id_value:
              type: string
              description: "External ID value to match"
            payload:
              type: object
              description: "Record fields to create or update"
          required: [sobject_type, external_id_field, external_id_value, payload]

    - salesforce_restapi_sobject:
        name: upsert_record
        operation: upsert
        credentials_path: /etc/sfdc/credentials.json
        sobject_type: "{{event.data.sobject_type}}"
        external_id_field: "{{event.data.external_id_field}}"
        external_id_value: "{{event.data.external_id_value}}"
        payload:
          from_event: true
```

## SOSL search

Search across Salesforce objects using SOSL.

<Mermaid chart={`
graph LR
  A[receive_request] --> B[sosl_search]
`} />

### HTTP

```yaml
flow:
  name: salesforce_sosl_search
  tasks:

    - http_webhook:
        name: receive_request
        method: POST
        endpoint: /salesforce/search

    - salesforce_restapi_search:
        name: sosl_search
        credentials_path: /etc/sfdc/credentials.json
        query: "FIND {{{event.data.search_term}}} IN ALL FIELDS RETURNING Account(Id, Name, Industry), Contact(Id, FirstName, LastName, Email)"
```

### MCP

```yaml
flow:
  name: salesforce_mcp_sosl_search
  tasks:

    - mcp_tool:
        name: salesforce_search
        description: "Search Salesforce records across Account and Contact objects using a search term."
        input_schema:
          type: object
          properties:
            search_term:
              type: string
              description: "Search term to find across all fields"
          required: [search_term]

    - salesforce_restapi_search:
        name: sosl_search
        credentials_path: /etc/sfdc/credentials.json
        query: "FIND {{{event.data.search_term}}} IN ALL FIELDS RETURNING Account(Id, Name, Industry), Contact(Id, FirstName, LastName, Email)"
```

## MCP server configuration

To expose flows as MCP tools, enable the MCP server in the worker config:

```yaml
worker:
  mcp_server:
    enabled: true
    port: 3001
    path: /mcp/v1
    credentials_path: /etc/mcp/api-keys.json
```

LLM agents connect to the MCP endpoint and discover all registered tools via the `tools/list` method. See [MCP Tools](/docs/flowgen/ai/mcp) for details.

## Credentials

| File | Purpose | Contents |
|---|---|---|
| `/etc/sfdc/credentials.json` | Salesforce REST API | `client_id`, `client_secret`, `username`, `password`, `login_url` |
| `/etc/mcp/api-keys.json` | MCP server (optional) | `api_keys` array |

See [Credentials](/docs/flowgen/concepts/credentials) for format details.
