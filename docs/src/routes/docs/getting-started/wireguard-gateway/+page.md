# WireGuard Gateway

When flowgen needs to reach resources on a private network (e.g. an on-prem MSSQL server behind a VPN), the Helm chart can deploy a standalone WireGuard gateway pod. Flowgen pods connect to the gateway's ClusterIP Service instead of reaching through the tunnel directly.

## Why a separate pod?

WireGuard identifies each peer by its private key and IP address. If multiple flowgen replicas each run a WireGuard sidecar with the same identity, the VPN server sees conflicting handshakes and flaps connections — causing intermittent timeouts on every query.

A standalone gateway pod holds a single VPN identity. Flowgen pods connect to the forwarded ports via a normal Kubernetes Service, so the number of replicas doesn't matter.

## Architecture

```
flowgen pod 0 ──┐
flowgen pod 1 ──┼── ClusterIP Service ──► gateway pod ──► WireGuard tunnel ──► on-prem DB
flowgen pod 2 ──┘       :1433               (socat)            (wg0)           10.0.0.5:1433
```

The gateway pod runs three components:

1. **Init container** — sets up iptables MASQUERADE and IP forwarding.
2. **WireGuard container** — establishes the VPN tunnel from a config secret.
3. **socat sidecar** — listens on local ports and forwards TCP to the remote host through the tunnel.

## Configuration

### 1. Create the tunnel secret

Each tunnel needs a Kubernetes Secret containing the WireGuard config file. The secret key must match the tunnel name (e.g. `wg0.conf` for a tunnel named `wg0`).

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: wireguard-client-a
type: Opaque
stringData:
  wg0.conf: |
    [Interface]
    PrivateKey: <YOUR_PRIVATE_KEY>
    Address: 10.13.13.2/24
    DNS: 1.1.1.1
    MTU = 1420

    [Peer]
    PublicKey: <SERVER_PUBLIC_KEY>
    Endpoint: vpn.example.com:51820
    AllowedIPs: 10.0.0.0/8, 172.16.0.0/12
    PersistentKeepalive = 25
```

Setting `MTU = 1420` prevents silent packet drops on large payloads (e.g. SQL result sets) caused by WireGuard encapsulation overhead.

### 2. Configure the gateway in values.yaml

```yaml
wireguardGateway:
  enabled: true
  tunnels:
    - name: wg0
      secretName: wireguard-client-a
      portForwards:
        - name: mssql
          port: 1433
          targetHost: 10.0.0.5
          targetPort: 1433
```

Each entry in `portForwards` starts a socat process that listens on `port` and forwards to `targetHost:targetPort` through the tunnel.

### 3. Point flowgen at the gateway

In your flow config, use the gateway Service DNS name instead of the private IP:

```yaml
tasks:
  - mssql_query:
      name: fetch_orders
      host: "<release-name>-flowgen-wireguard-gateway"
      port: 1433
      database: production
      query: { resource: orders.sql }
```

The Service name follows the pattern `<release-name>-flowgen-wireguard-gateway`. If your Helm release is called `flowgen`, the host is `flowgen-wireguard-gateway`.

## Multiple port forwards

A single tunnel can forward multiple ports. Each socat process runs independently.

```yaml
tunnels:
  - name: wg0
    secretName: wireguard-client-a
    portForwards:
      - name: mssql
        port: 1433
        targetHost: 10.0.0.5
        targetPort: 1433
      - name: postgres
        port: 5432
        targetHost: 10.0.0.6
        targetPort: 5432
```

If two remote services use the same port number, use `servicePort` to expose one on a different port in the Service:

```yaml
portForwards:
  - name: mssql-a
    port: 1433
    targetHost: 10.0.0.5
    targetPort: 1433
  - name: mssql-b
    port: 1434
    targetHost: 10.0.0.6
    targetPort: 1433
    servicePort: 1434
```

## Deployment details

| Setting | Value | Reason |
|---|---|---|
| Replicas | 1 | Only one pod can hold a given VPN identity. |
| Strategy | Recreate | Avoids two pods fighting over the same WireGuard key during rollouts. |
| Liveness probe | `wg show wg0 \| grep 'latest handshake'` | Restarts the pod if the tunnel drops. |
| Readiness probe | Same as liveness | Keeps the Service endpoint out of rotation until the handshake completes. |

## Values reference

| Field | Type | Default | Description |
|---|---|---|---|
| `wireguardGateway.enabled` | bool | `false` | Deploy the gateway pod and Service. |
| `wireguardGateway.image.repository` | string | `ghcr.io/linuxserver/wireguard` | WireGuard container image. |
| `wireguardGateway.image.tag` | string | `1.0.20250521-r1-ls105` | Image tag. |
| `wireguardGateway.tunnels` | array | `[]` | List of tunnel configurations. |
| `wireguardGateway.tunnels[].name` | string | | Tunnel name. Maps to the config file name (`<name>.conf`). |
| `wireguardGateway.tunnels[].secretName` | string | | Kubernetes Secret containing the WireGuard config. |
| `wireguardGateway.tunnels[].portForwards` | array | | Ports to forward through this tunnel via socat. |
| `wireguardGateway.tunnels[].portForwards[].name` | string | | Port name (used in Service port naming). |
| `wireguardGateway.tunnels[].portForwards[].port` | int | | Local listen port for socat. |
| `wireguardGateway.tunnels[].portForwards[].targetHost` | string | | Remote host reachable through the tunnel. |
| `wireguardGateway.tunnels[].portForwards[].targetPort` | int | | Remote port on the target host. |
| `wireguardGateway.tunnels[].portForwards[].servicePort` | int | same as `port` | Port exposed in the Kubernetes Service. |
| `wireguardGateway.socat.image` | string | `alpine/socat:1.8.0.1` | socat sidecar image. |
| `wireguardGateway.resources` | object | | Resource requests/limits for the WireGuard container. |
| `wireguardGateway.socat.resources` | object | | Resource requests/limits for the socat sidecar. |
| `wireguardGateway.nodeSelector` | object | `{}` | Node selector for the gateway pod. |
| `wireguardGateway.tolerations` | array | `[]` | Tolerations for the gateway pod. |
| `wireguardGateway.affinity` | object | `{}` | Affinity rules for the gateway pod. |
