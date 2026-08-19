{{- define "redetim.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "redetim.fullname" -}}
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

{{- define "redetim.brokerService" -}}
{{- .Values.redpanda.serviceName -}}
{{- end -}}

{{- define "redetim.bootstrapServers" -}}
{{- if .Values.redpanda.enabled -}}
{{- printf "%s:9092" (include "redetim.brokerService" .) -}}
{{- else if .Values.redpanda.external.bootstrapServers -}}
{{- .Values.redpanda.external.bootstrapServers -}}
{{- else -}}
{{- fail "redpanda.enabled is false, so redpanda.external.bootstrapServers must name the broker to use." -}}
{{- end -}}
{{- end -}}

{{- define "redetim.securityProtocol" -}}
{{- $raw := .Values.redpanda.auth.securityProtocol | default "Plaintext" -}}
{{- $key := $raw | replace "_" "" | replace "-" "" | lower -}}
{{- $known := dict "plaintext" "Plaintext" "ssl" "Ssl" "saslplaintext" "SaslPlaintext" "saslssl" "SaslSsl" -}}
{{- if not (hasKey $known $key) -}}
{{- fail (printf "redpanda.auth.securityProtocol is %q, which is not a known value. Accepted: Plaintext, Ssl, SaslPlaintext, SaslSsl (underscores and dashes are ignored, so SASL_SSL also works)." $raw) -}}
{{- end -}}
{{- index $known $key -}}
{{- end -}}

{{- define "redetim.saslEnabled" -}}
{{- $protocol := include "redetim.securityProtocol" . -}}
{{- if or (eq $protocol "SaslSsl") (eq $protocol "SaslPlaintext") -}}
true
{{- end -}}
{{- end -}}

{{- define "redetim.brokerTls" -}}
{{- $protocol := include "redetim.securityProtocol" . -}}
{{- if or (eq $protocol "Ssl") (eq $protocol "SaslSsl") -}}
true
{{- end -}}
{{- end -}}

{{- define "redetim.saslEnv" -}}
{{- with .ctx -}}
{{- if include "redetim.saslEnabled" . }}
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

{{- define "redetim.collectorService" -}}
{{- printf "%s-otel-collector" (include "redetim.fullname" .) -}}
{{- end -}}

{{- define "redetim.tlsMountPath" -}}
/etc/redetim/tls
{{- end -}}

{{- define "redetim.releaseVersion" -}}
{{- .Values.release.version | default .Chart.AppVersion -}}
{{- end -}}

{{- define "redetim.image" -}}
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

{{- define "redetim.releaseAnnotations" -}}
{{- with .Values.release.gitSha }}
redetim.dev/git-sha: {{ . | quote }}
{{- end }}
{{- with .Values.release.builtAt }}
redetim.dev/built-at: {{ . | quote }}
{{- end }}
{{- if .Values.release.dirty }}
redetim.dev/dirty-build: "true"
{{- end }}
{{- end -}}

{{- define "redetim.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "redetim.labels" -}}
helm.sh/chart: {{ include "redetim.chart" . }}
app.kubernetes.io/name: {{ include "redetim.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ include "redetim.releaseVersion" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "redetim.selectorLabels" -}}
app.kubernetes.io/name: {{ include "redetim.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}
