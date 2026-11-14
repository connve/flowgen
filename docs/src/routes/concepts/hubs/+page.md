# Hubs

A hub is a catalog of templates that new flows and resources start from. In the web UI, Templates in the editor of a new file lists the templates of every hub that returns some; using one adds its files to your pending change, which opens for review and is validated and approved like any other.

## Hub flow

A hub is a flow named `flows/system/<id>_hub.yaml`: an [`inproc_endpoint`](/docs/flowgen/core/inproc) that the web UI calls in-process, followed by a task that returns the catalog's files as `{files: [{path, content}]}`, such as [OCI Sync](/docs/flowgen/oci/sync) of an artifact. The flow's `display_name` names the hub. A workspace started by `flowgen` without `--config` holds `connve_hub.yaml`, reading `ghcr.io/connve/hub/flowgen:<version>` for the running version of flowgen.

```yaml
flow:
  labels:
    display_name: "Team Hub"
  tasks:
    - inproc_endpoint:
        name: on_request
        ack_timeout: 60s

    - oci_sync:
        name: read_hub
        artifact: "registry.example.com/team/hub/flowgen:{{env.FLOWGEN_VERSION}}"
        credentials_path: "{{env.FLOWGEN_CREDENTIALS}}/registry.json"
        force_pull: true
        retry:
          max_attempts: 1
```

`force_pull` reads the catalog on every request, and one attempt lets the web UI move on when the hub cannot be reached. A hub that fails or returns no templates is left out of Templates.

## Catalog

Each top folder of the catalog is a template when it holds a `template.yaml`:

```
order_sync/
├── template.yaml
├── README.md
├── flows/
│   └── orders/sync.yaml
└── resources/
    └── scripts/sync.rhai
```

| File | Description |
|---|---|
| `template.yaml` | The template's `name`, and optionally its `description`, `category` and `tags`. |
| `README.md` | Optional; shown with the template. |
| `flows/`, `resources/` | The files the template adds to the workspace, at the same paths. Other files are left out, and so are binary ones. |

`template.yaml`:

| Field | Type | Description |
|---|---|---|
| `name` | string | Name shown in Templates. Required. |
| `description` | string | One line about what the template does. |
| `category` | string | Shown next to the name. |
| `tags` | list | Shown with the template. |

## Publishing a catalog

A catalog is any artifact [OCI Push](/docs/flowgen/oci/push) writes. [`examples/hub/publish_catalog.yaml`](https://github.com/connve/flowgen/blob/main/examples/hub/publish_catalog.yaml) publishes the `flowgen/` folder of a repository to a local OCI Image Layout on each commit, and [`examples/hub/local_hub.yaml`](https://github.com/connve/flowgen/blob/main/examples/hub/local_hub.yaml) is a hub reading it, so a catalog can be tried before it is published to a registry.
