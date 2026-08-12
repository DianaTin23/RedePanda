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

{{/*
Where every client looks for the broker: the bundled one, or whatever redpanda.external names
when this chart deploys none. Empty is a hard error rather than a default, for the same reason
an empty image tag is: the alternative is a backend that comes up and fails every connection
against a Service nobody deployed.
*/}}
{{- define "redepanda.bootstrapServers" -}}
{{- if .Values.redpanda.enabled -}}
{{- printf "%s:9092" (include "redepanda.brokerService" .) -}}
{{- else if .Values.redpanda.external.bootstrapServers -}}
{{- .Values.redpanda.external.bootstrapServers -}}
{{- else -}}
{{- fail "redpanda.enabled is false, so redpanda.external.bootstrapServers must name the broker to use." -}}
{{- end -}}
{{- end -}}

{{/*
Whether the configured protocol authenticates over SASL, i.e. whether the credentials from
redpanda.auth.existingSecret have to be mounted into the pods. Underscores are stripped so the
spelling from the broker documentation (SASL_SSL) matches here as well as in the application.
*/}}
{{- define "redepanda.saslEnabled" -}}
{{- $protocol := .Values.redpanda.auth.securityProtocol | replace "_" "" | lower -}}
{{- if or (eq $protocol "saslssl") (eq $protocol "saslplaintext") -}}
true
{{- end -}}
{{- end -}}

{{/*
The SASL credentials, as environment variables from the referenced Secret. Rendered into both
pod templates that speak Kafka, which is why it is a helper rather than eight duplicated lines.
Call as (dict "ctx" .).
*/}}
{{- define "redepanda.saslEnv" -}}
{{- with .ctx -}}
{{- if include "redepanda.saslEnabled" . }}
- name: REDPANDA_SASL_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redpanda.auth.existingSecret | quote }}
      key: username
- name: REDPANDA_SASL_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.redpanda.auth.existingSecret | quote }}
      key: password
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "redepanda.collectorService" -}}
{{- printf "%s-otel-collector" (include "redepanda.fullname" .) -}}
{{- end -}}

{{/*
The version of the running release, not of the chart. .Chart.AppVersion is the fallback so
`helm lint` and a bare `helm show` still produce something readable; in an actual deployment the
release file always supplies it.
*/}}
{{- define "redepanda.releaseVersion" -}}
{{- .Values.release.version | default .Chart.AppVersion -}}
{{- end -}}

{{/*
Image reference for a locally built image. Call as (dict "ctx" . "component" "backend").

The empty tag is a hard error rather than a default, because every plausible default is a
mutable name: deploying one would put an unidentifiable image in the cluster and make the next
`helm rollback` a no-op. Failing here costs one command; failing in the cluster costs an hour.
*/}}
{{- define "redepanda.image" -}}
{{- $image := index .ctx.Values .component "image" -}}
{{- if not $image.tag -}}
{{- fail (printf "%s.image.tag is empty: no release selected. Run scripts/build-images.sh, then deploy with -f deploy/releases/<version>.yaml" .component) -}}
{{- end -}}
{{- printf "%s:%s" $image.repository $image.tag -}}
{{- end -}}

{{/*
Provenance of the running images, as annotations on the two pod templates that carry them.
Empty values are omitted rather than rendered blank, so `kubectl describe` stays quiet when a
chart is rendered without a release file (helm lint, for instance).
*/}}
{{- define "redepanda.releaseAnnotations" -}}
{{- with .Values.release.gitSha }}
redepanda.dev/git-sha: {{ . | quote }}
{{- end }}
{{- with .Values.release.builtAt }}
redepanda.dev/built-at: {{ . | quote }}
{{- end }}
{{- if .Values.release.dirty }}
redepanda.dev/dirty-build: "true"
{{- end }}
{{- end -}}

{{- define "redepanda.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Labels applied to every object. app.kubernetes.io/version carries the release version rather
than the chart's appVersion: the question this label has to answer in a cluster is "which build
is running", and appVersion is the same string on every revision.

Changing it on an upgrade is safe -- it is not a selector. redepanda.selectorLabels below is
deliberately separate and holds only immutable identity, so a Deployment's selector never moves.
*/}}
{{- define "redepanda.labels" -}}
helm.sh/chart: {{ include "redepanda.chart" . }}
app.kubernetes.io/name: {{ include "redepanda.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ include "redepanda.releaseVersion" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Selector labels for one component. Call as (dict "ctx" . "component" "backend"). */}}
{{- define "redepanda.selectorLabels" -}}
app.kubernetes.io/name: {{ include "redepanda.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}
