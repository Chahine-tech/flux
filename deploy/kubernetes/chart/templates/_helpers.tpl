{{- define "flux.labels" -}}
app.kubernetes.io/name: flux
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "flux.selector" -}}
app.kubernetes.io/name: flux
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
