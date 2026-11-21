{{- define "tessera.name" -}}
{{- .Chart.Name -}}
{{- end -}}

{{- define "tessera.fullname" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "tessera.image" -}}
{{- printf "%s:%s" .Values.image.repository (default .Chart.AppVersion .Values.image.tag) -}}
{{- end -}}

{{- define "tessera.labels" -}}
app.kubernetes.io/name: {{ include "tessera.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/* Non-empty when the pods keep a cache of object reads: with a
     bucket as the root pool and a size above zero. */}}
{{- define "tessera.cached" -}}
{{- if and .Values.rootPool.url (gt (int64 .Values.cache.bytes) 0) -}}true{{- end -}}
{{- end -}}
