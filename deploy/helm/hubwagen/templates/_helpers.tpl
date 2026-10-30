{{- define "hubwagen.fullname" -}}
{{- if contains .Chart.Name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "hubwagen.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "hubwagen.serverSelector" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: server
{{- end -}}

{{/* The Deployment of a pool: <fullname>-<pool name>. */}}
{{- define "hubwagen.poolName" -}}
{{- printf "%s-%s" (include "hubwagen.fullname" .root) .pool.name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "hubwagen.serverURL" -}}
{{- printf "http://%s:%d" (include "hubwagen.fullname" .) (int .Values.server.port) -}}
{{- end -}}
