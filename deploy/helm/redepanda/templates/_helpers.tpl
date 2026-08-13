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
The configured broker protocol, normalised to the application's spelling. Underscores and dashes
are stripped so the spelling from the broker documentation (SASL_SSL) matches here as well as in
KafkaSecurity, which normalises the same way.

Anything else is a hard error, and that is the point of this helper existing at all. The previous
version compared against two strings and treated everything that did not match as "not SASL", so
`SASL_PLAIN` -- a plausible typo -- rendered perfectly happily, mounted no credentials, and left
the pods failing to authenticate against a broker that was working correctly. A silent no is the
one answer a security setting must never give.
*/}}
{{- define "redepanda.securityProtocol" -}}
{{- $raw := .Values.redpanda.auth.securityProtocol | default "Plaintext" -}}
{{- $key := $raw | replace "_" "" | replace "-" "" | lower -}}
{{- $known := dict "plaintext" "Plaintext" "ssl" "Ssl" "saslplaintext" "SaslPlaintext" "saslssl" "SaslSsl" -}}
{{- if not (hasKey $known $key) -}}
{{- fail (printf "redpanda.auth.securityProtocol is %q, which is not a known value. Accepted: Plaintext, Ssl, SaslPlaintext, SaslSsl (underscores and dashes are ignored, so SASL_SSL also works)." $raw) -}}
{{- end -}}
{{- index $known $key -}}
{{- end -}}

{{/*
Whether the configured protocol authenticates over SASL, i.e. whether the credentials from
redpanda.auth.existingSecret have to be mounted into the pods.
*/}}
{{- define "redepanda.saslEnabled" -}}
{{- $protocol := include "redepanda.securityProtocol" . -}}
{{- if or (eq $protocol "SaslSsl") (eq $protocol "SaslPlaintext") -}}
true
{{- end -}}
{{- end -}}

{{/*
Whether the connection to the broker is encrypted, i.e. whether a private CA bundle is meaningful.
*/}}
{{- define "redepanda.brokerTls" -}}
{{- $protocol := include "redepanda.securityProtocol" . -}}
{{- if or (eq $protocol "Ssl") (eq $protocol "SaslSsl") -}}
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
Where each pod finds its certificate, its key and the release CA. One path for every component,
because the mount is the same everywhere and a per-component path would only be something else
to get wrong in a probe or a config file.
*/}}
{{- define "redepanda.tlsMountPath" -}}
/etc/redepanda/tls
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

Non-emptiness alone was not enough, though. `--set backend.image.tag=latest` satisfied it and
rendered perfectly happily, which is precisely the deploy this guard exists to prevent -- so a
mutable name is rejected by name, and a tag that arrived without the release metadata beside it
is rejected as well. The two checks catch different mistakes: the first a deliberate mutable
tag, the second any tag set by hand instead of by a release file.
*/}}
{{- define "redepanda.image" -}}
{{- $image := index .ctx.Values .component "image" -}}
{{- if not $image.tag -}}
{{- fail (printf "%s.image.tag is empty: no release selected. Run scripts/build-images.sh, then deploy with -f deploy/releases/<version>.yaml" .component) -}}
{{- end -}}
{{- if has (lower (toString $image.tag)) (list "latest" "main" "master" "edge" "stable" "dev" "test") -}}
{{- fail (printf "%s.image.tag is '%s', which is a mutable name: it can point at a different image tomorrow, and a later `helm rollback` would restore the same name rather than the same build. Deploy with -f deploy/releases/<version>.yaml" .component $image.tag) -}}
{{- end -}}
{{- if not .ctx.Values.release.gitSha -}}
{{- fail (printf "%s.image.tag is '%s' but release.gitSha is empty, so nothing records which commit that image was built from. Deploy with -f deploy/releases/<version>.yaml rather than setting the tag by hand." .component $image.tag) -}}
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
