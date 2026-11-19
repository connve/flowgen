{{/*
Expand the name of the chart.
*/}}
{{- define "flowgen.name" -}}
{{- $worker := index .Values "flowgen" -}}
{{- default "flowgen" $worker.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "flowgen.fullname" -}}
{{- $worker := index .Values "flowgen" -}}
{{- if $worker.fullnameOverride }}
{{- $worker.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "flowgen" $worker.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "flowgen.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "flowgen.labels" -}}
helm.sh/chart: {{ include "flowgen.chart" . }}
{{ include "flowgen.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "flowgen.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flowgen.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: flowgen
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{/*
Selector labels for the WireGuard gateway component.
*/}}
{{- define "flowgen.wireguardGateway.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flowgen.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: wireguard-gateway
{{- end }}

{{/*
The app config: `config.settings`, with `flows.system` on when `systemFlows`
is enabled, leaving out the release flow when `systemFlows.release` is false.
Rendered as YAML; read it back with `fromYaml`.
*/}}
{{- define "flowgen.settings" -}}
{{- $worker := index .Values "flowgen" -}}
{{- $settings := deepCopy (default dict $worker.config.settings) -}}
{{- if $worker.systemFlows.enabled }}
{{- $flows := default dict (get $settings "flows") -}}
{{- $_ := set $flows "system" true -}}
{{- if eq (toString $worker.systemFlows.release) "false" }}
{{- $excluded := default list (get $flows "system_exclude") -}}
{{- $_ := set $flows "system_exclude" (append $excluded "system/release_workspace" | uniq) -}}
{{- end }}
{{- $_ := set $settings "flows" $flows -}}
{{- end }}
{{- toYaml $settings }}
{{- end }}

{{/*
The servers the config enables, by settings key: their port name, container
port and Service type.
*/}}
{{- define "flowgen.servers" -}}
{{- $worker := index .Values "flowgen" -}}
{{- $settings := include "flowgen.settings" . | fromYaml -}}
{{- $defaults := dict "http_server" (list "http" 3000) "mcp_server" (list "mcp" 3001) "ai_gateway" (list "ai-gateway" 3002) "web" (list "web" 8080) -}}
{{- $servers := dict -}}
{{- range $key, $default := $defaults }}
{{- $server := get $settings $key }}
{{- if and $server (get $server "enabled") }}
{{- $type := dig $key "type" "ClusterIP" (default dict $worker.services) }}
{{- $_ := set $servers $key (dict "name" (index $default 0) "port" (default (index $default 1) (get $server "port")) "type" $type) }}
{{- end }}
{{- end }}
{{- toYaml $servers }}
{{- end }}

{{- define "flowgen.serviceAccountName" -}}
{{- $worker := index .Values "flowgen" -}}
{{- if $worker.serviceAccount.create }}
{{- default (include "flowgen.fullname" .) $worker.serviceAccount.name }}
{{- else }}
{{- default "default" $worker.serviceAccount.name }}
{{- end }}
{{- end }}
