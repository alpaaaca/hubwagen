package spec

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCheckTemplateAllowsDeclaredVariablesAndFuncs(t *testing.T) {
	sc := scope{
		params:  map[string]bool{"mode": true},
		inputs:  map[string]bool{"orders": true},
		outputs: map[string]bool{"report": true},
		asset:   true,
	}
	for _, text := range []string{
		"plain text",
		"{{ .Pipeline }}/{{ .Tenant }}",
		"{{ .Run.ID }} {{ .Run.Key }} {{ .Run.Generation }} {{ .Run.Date }}",
		`{{ .Interval.Start | date "2006-01-02" }}`,
		`{{ if eq .Params.mode "full" }}--full-refresh{{ else }}--incremental{{ end }}`,
		`{{ default "x" .Params.mode | upper | quote }}`,
		`{{ printf "%s-%s" .Asset.Name .Asset.Partition }}`,
		"{{ .Inputs.orders }} {{ .Outputs.report }}",
		"{{/* comments are fine */}}",
	} {
		assert.NoError(t, checkTemplate(text, sc), text)
	}
}

func TestCheckTemplateRejects(t *testing.T) {
	cases := map[string]string{
		"{{ . }}":                           "bare {{ . }} is not allowed",
		"{{ $x := .Pipeline }}":             "variable declarations are not allowed",
		"{{ $ }}":                           "template variables",
		`{{ define "x" }}y{{ end }}`:        "template definitions",
		`{{ template "x" }}`:                "{{ template }} is not allowed",
		"{{ with .Run }}{{ .ID }}{{ end }}": "{{ with }} is not allowed",
		"{{ index .Params \"mode\" }}":      `function "index" is not allowed`,
		"{{ .Run.Secret }}":                 ".Run.Secret: unknown variable",
		"{{ .Interval }}":                   ".Interval: unknown variable",
		"{{ .Pipeline.Name }}":              ".Pipeline.Name: unknown variable",
		"{{ .Params }}":                     "expected .Params.<name>",
		"{{ .Unclosed ":                     "unclosed action",
	}
	for text, want := range cases {
		err := checkTemplate(text, scope{params: map[string]bool{"mode": true}})
		if assert.Error(t, err, text) {
			assert.Contains(t, err.Error(), want, text)
		}
	}
}

func TestCheckTemplateAssetOnlyScope(t *testing.T) {
	sc := scope{asset: true, assetOnly: true}
	assert.NoError(t, checkTemplate("{{ .Asset.Partition }}", sc))
	assert.ErrorContains(t, checkTemplate("{{ .Run.ID }}", sc), "only .Asset.Name and .Asset.Partition")
}

func TestRender(t *testing.T) {
	ctx := Context{
		Interval: IntervalVars{Start: "2026-10-20T00:00:00+02:00", End: "2026-10-21T00:00:00+02:00"},
		Run:      RunVars{ID: "01J", Key: "t/p/x", Generation: 2, Date: "2026-10-21"},
		Pipeline: "p",
		Params:   map[string]any{"mode": "full", "empty": ""},
		Inputs:   map[string]string{"orders": "s3://b/o/"},
	}
	out, err := Render(`{{ .Interval.Start | date "2006-01-02" }} {{ .Run.Generation }} {{ default "inc" .Params.empty }} {{ .Inputs.orders }}`, ctx)
	require.NoError(t, err)
	assert.Equal(t, "2026-10-20 2 inc s3://b/o/", out)

	_, err = Render(`{{ .Interval.Start | date "2006" }}`, Context{})
	assert.ErrorContains(t, err, "the run has no data interval")

	_, err = Render(`{{ .Params.missing }}`, ctx)
	assert.ErrorContains(t, err, `map has no entry for key "missing"`)
}
