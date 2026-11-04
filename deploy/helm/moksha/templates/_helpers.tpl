{{- define "moksha.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "moksha.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "moksha.serverSelector" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: server
{{- end -}}

{{/* The Deployment of a pool: <fullname>-<pool name>. */}}
{{- define "moksha.poolName" -}}
{{- printf "%s-%s" (include "moksha.fullname" .root) .pool.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "moksha.serverURL" -}}
{{- printf "http://%s:%d" (include "moksha.fullname" .) (int .Values.server.port) -}}
{{- end -}}
