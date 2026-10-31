{{/* The release's full name, the prefix of every object. */}}
{{- define "tracekeep.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "tracekeep.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{/* selector labels of one role: (dict "root" $ "role" "api") */}}
{{- define "tracekeep.selector" -}}
app.kubernetes.io/name: {{ .root.Chart.Name }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
app.kubernetes.io/component: {{ .role }}
{{- end -}}

{{- define "tracekeep.serviceAccount" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "tracekeep.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- required "serviceAccount.name names the existing account when serviceAccount.create is false" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "tracekeep.image" -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end -}}

{{/*
The config.yaml every role reads: the values' config, refused when empty,
and refused when it writes a credential the configuration could take by reference instead (architecture.md §3.3).
*/}}
{{- define "tracekeep.config" -}}
{{- $c := .Values.config -}}
{{- if not $c -}}
{{- fail "config is tracekeep's configuration and is required (docs/guides/kubernetes.md)" -}}
{{- end -}}
{{- $catalog := default dict $c.catalog -}}
{{- $bus := default dict $c.bus -}}
{{- $s3 := default dict (default dict $c.store).s3 -}}
{{- range $path, $set := dict "catalog.dsn" (hasKey $catalog "dsn") "bus.password" (hasKey $bus "password") "store.s3.secret_access_key" (hasKey $s3 "secret_access_key") -}}
{{- if $set -}}
{{- fail (printf "config.%s is a credential: name a file under /run/secrets/ by %s_ref instead" $path $path) -}}
{{- end -}}
{{- end -}}
{{- /* A URL's userinfo is a credential too, with a scheme or without (the bus client takes nats:// as the default): any "@" in these addresses is refused. */ -}}
{{- $keys := default dict $c.keys -}}
{{- $oidc := default dict (default dict $c.auth).oidc -}}
{{- range $path, $url := dict "bus.url" $bus.url "store.s3.endpoint" $s3.endpoint "keys.vault.address" (default dict $keys.vault).address "auth.oidc.issuer" $oidc.issuer -}}
{{- if and $url (contains "@" (toString $url)) -}}
{{- fail (printf "config.%s carries a credential in its URL: name it by reference instead (docs/guides/configuration.md)" $path) -}}
{{- end -}}
{{- end -}}
{{ toYaml $c }}
{{- end -}}

{{/* The volumes every tracekeep container mounts: the configuration (configMap names its ConfigMap), the secrets, and /work of size. */}}
{{- define "tracekeep.volumes" -}}
- name: config
  configMap:
    name: {{ .configMap }}
- name: work
  emptyDir:
    sizeLimit: {{ .size }}
{{- range .root.Values.secrets }}
- name: secret-{{ . }}
  secret:
    secretName: {{ . }}
{{- end }}
{{- end -}}

{{- define "tracekeep.volumeMounts" -}}
- name: config
  mountPath: /etc/tracekeep
  readOnly: true
- name: work
  mountPath: /work
{{- range .Values.secrets }}
- name: secret-{{ . }}
  mountPath: /run/secrets/{{ . }}
  readOnly: true
{{- end }}
{{- end -}}

{{/* The environment every tracekeep container has: its work directories on /work, then the values' env. */}}
{{- define "tracekeep.env" -}}
- name: TMPDIR
  value: /work
- name: TRACEKEEP_INGEST_WORK_DIR
  value: /work
- name: TRACEKEEP_EXPORTS_WORK_DIR
  value: /work/exports
{{- with .Values.env }}
{{ toYaml . }}
{{- end }}
{{- end -}}

{{/* A role's resources with an ephemeral-storage request and limit of its work directory's size, unless the values give them. */}}
{{- define "tracekeep.resources" -}}
{{- $r := deepCopy (default dict .role.resources) -}}
{{- range $k := list "requests" "limits" -}}
{{- $m := deepCopy (default dict (get $r $k)) -}}
{{- if not (hasKey $m "ephemeral-storage") -}}
{{- $_ := set $m "ephemeral-storage" $.role.workDirSize -}}
{{- end -}}
{{- $_ := set $r $k $m -}}
{{- end -}}
{{ toYaml $r }}
{{- end -}}
