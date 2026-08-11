{{/* Base name, overridable. */}}
{{- define "redepanda.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Release-qualified name. With the conventional release name "redepanda" this collapses to
"redepanda", which is what keeps the service names in the README short: redepanda-backend,
redepanda-otel-collector and so on.
*/}}
{{- define "redepanda.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
The broker's service name is deliberately NOT release-qualified. It is written into the
broker's own --advertise-kafka-addr and into every client's bootstrap default, and a short
stable name keeps those readable and matching the documentation.
*/}}
{{- define "redepanda.brokerService" -}}
{{- .Values.redpanda.serviceName -}}
{{- end -}}

{{- define "redepanda.bootstrapServers" -}}
{{- printf "%s:9092" (include "redepanda.brokerService" .) -}}
{{- end -}}

{{- define "redepanda.adminHost" -}}
{{- printf "%s:9644" (include "redepanda.brokerService" .) -}}
{{- end -}}

{{- define "redepanda.collectorService" -}}
{{- printf "%s-otel-collector" (include "redepanda.fullname" .) -}}
{{- end -}}

{{- define "redepanda.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Labels applied to every object. */}}
{{- define "redepanda.labels" -}}
helm.sh/chart: {{ include "redepanda.chart" . }}
app.kubernetes.io/name: {{ include "redepanda.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels for one component. Call as (dict "ctx" . "component" "backend"). */}}
{{- define "redepanda.selectorLabels" -}}
app.kubernetes.io/name: {{ include "redepanda.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}
