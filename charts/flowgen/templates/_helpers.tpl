{{/*
Expand the name of the chart.
*/}}
{{- define "flowgen-worker.name" -}}
{{- $worker := index .Values "flowgen-worker" -}}
{{- default "flowgen-worker" $worker.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "flowgen-worker.fullname" -}}
{{- $worker := index .Values "flowgen-worker" -}}
{{- if $worker.fullnameOverride }}
{{- $worker.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default "worker" $worker.nameOverride }}
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
{{- define "flowgen-worker.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "flowgen-worker.labels" -}}
helm.sh/chart: {{ include "flowgen-worker.chart" . }}
{{ include "flowgen-worker.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "flowgen-worker.selectorLabels" -}}
app.kubernetes.io/name: {{ include "flowgen-worker.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app: flowgen-worker
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "flowgen-worker.serviceAccountName" -}}
{{- $worker := index .Values "flowgen-worker" -}}
{{- if $worker.serviceAccount.create }}
{{- default (include "flowgen-worker.fullname" .) $worker.serviceAccount.name }}
{{- else }}
{{- default "default" $worker.serviceAccount.name }}
{{- end }}
{{- end }}
