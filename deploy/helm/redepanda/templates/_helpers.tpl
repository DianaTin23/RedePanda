{{- define "redepanda.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

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

{{- define "redepanda.brokerService" -}}
{{- .Values.redpanda.serviceName -}}
{{- end -}}

{{- define "redepanda.bootstrapServers" -}}
{{- if .Values.redpanda.enabled -}}
{{- printf "%s:9092" (include "redepanda.brokerService" .) -}}
{{- else if .Values.redpanda.external.bootstrapServers -}}
{{- .Values.redpanda.external.bootstrapServers -}}
{{- else -}}
{{- fail "redpanda.enabled is false, so redpanda.external.bootstrapServers must name the broker to use." -}}
{{- end -}}
{{- end -}}

{{- define "redepanda.securityProtocol" -}}
{{- $raw := .Values.redpanda.auth.securityProtocol | default "Plaintext" -}}
{{- $key := $raw | replace "_" "" | replace "-" "" | lower -}}
{{- $known := dict "plaintext" "Plaintext" "ssl" "Ssl" "saslplaintext" "SaslPlaintext" "saslssl" "SaslSsl" -}}
{{- if not (hasKey $known $key) -}}
{{- fail (printf "redpanda.auth.securityProtocol is %q, which is not a known value. Accepted: Plaintext, Ssl, SaslPlaintext, SaslSsl (underscores and dashes are ignored, so SASL_SSL also works)." $raw) -}}
{{- end -}}
{{- index $known $key -}}
{{- end -}}

{{- define "redepanda.saslEnabled" -}}
{{- $protocol := include "redepanda.securityProtocol" . -}}
{{- if or (eq $protocol "SaslSsl") (eq $protocol "SaslPlaintext") -}}
true
{{- end -}}
{{- end -}}

{{- define "redepanda.brokerTls" -}}
{{- $protocol := include "redepanda.securityProtocol" . -}}
{{- if or (eq $protocol "Ssl") (eq $protocol "SaslSsl") -}}
true
{{- end -}}
{{- end -}}

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

{{- define "redepanda.tlsMountPath" -}}
/etc/redepanda/tls
{{- end -}}

{{- define "redepanda.releaseVersion" -}}
{{- .Values.release.version | default .Chart.AppVersion -}}
{{- end -}}

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

{{- define "redepanda.labels" -}}
helm.sh/chart: {{ include "redepanda.chart" . }}
app.kubernetes.io/name: {{ include "redepanda.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ include "redepanda.releaseVersion" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "redepanda.selectorLabels" -}}
app.kubernetes.io/name: {{ include "redepanda.name" .ctx }}
app.kubernetes.io/instance: {{ .ctx.Release.Name }}
app.kubernetes.io/component: {{ .component }}
{{- end -}}
