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

{{/* The server's kubernetes.instance: the label on the Deployments it makes. */}}
{{- define "moksha.instance" -}}
{{- .Values.projects.instance | default (include "moksha.fullname" .) -}}
{{- end -}}

{{/* The API as pods in any namespace reach it. */}}
{{- define "moksha.serverURL" -}}
{{- printf "http://%s.%s.svc:%d" (include "moksha.fullname" .) .Release.Namespace (int .Values.server.port) -}}
{{- end -}}
