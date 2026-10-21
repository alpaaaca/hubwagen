package spec

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func compileYAML(t *testing.T, src string) (*Spec, error) {
	t.Helper()
	return compileYAMLOpts(t, src, Options{})
}

func compileYAMLOpts(t *testing.T, src string, opts Options) (*Spec, error) {
	t.Helper()
	out, err := CompileFile(context.Background(), "test.yaml", []byte(src), opts)
	if err != nil {
		return nil, err
	}
	require.Len(t, out, 1)
	return out[0].Spec, nil
}

// pipeline wraps a spec body in a valid header.
func pipeline(body string) string {
	return `apiVersion: hubwagen.dev/v1alpha1
kind: Pipeline
metadata: {name: p, tenant: t}
spec:
` + body
}

const minimalTasks = `
  defaults:
    runtime: {kind: agent, pool: local}
  tasks:
    - id: a
      command: ["true"]
`

func TestCompileAppliesDefaults(t *testing.T) {
	s, err := compileYAML(t, pipeline(minimalTasks))
	require.NoError(t, err)

	assert.Equal(t, APIVersion, s.APIVersion)
	assert.Equal(t, Concurrency{MaxActiveRuns: 1, OnOverlap: "queue"}, s.Concurrency)
	require.Len(t, s.Tasks, 1)
	task := s.Tasks[0]
	assert.Equal(t, "all_success", task.TriggerRule)
	assert.Equal(t, "stop", task.OnFailure)
	assert.Equal(t, Runtime{Kind: "agent", Pool: "local"}, task.Runtime)
	assert.Equal(t, Retry{Limit: 0, Backoff: "exponential", InitialSeconds: 30, MaxSeconds: 600}, task.Retry)
	assert.Equal(t, int64(3600), task.TimeoutSeconds)
}

func TestCompileRetryMergesFieldByField(t *testing.T) {
	s, err := compileYAML(t, pipeline(`
  defaults:
    runtime: {kind: agent, pool: local}
    retry: {limit: 3, initial: 10s, max: 1m}
    timeout: 5m
  tasks:
    - id: a
      command: ["true"]
      retry: {limit: 1}
    - id: b
      command: ["true"]
      timeout: 2h
`))
	require.NoError(t, err)
	assert.Equal(t, Retry{Limit: 1, Backoff: "exponential", InitialSeconds: 10, MaxSeconds: 60}, s.Task("a").Retry)
	assert.Equal(t, Retry{Limit: 3, Backoff: "exponential", InitialSeconds: 10, MaxSeconds: 60}, s.Task("b").Retry)
	assert.Equal(t, int64(300), s.Task("a").TimeoutSeconds)
	assert.Equal(t, int64(7200), s.Task("b").TimeoutSeconds)
}

func TestCompileTaskRuntimeReplacesDefault(t *testing.T) {
	s, err := compileYAML(t, pipeline(`
  defaults:
    runtime: {kind: agent, pool: local}
  tasks:
    - id: a
      runtime: {kind: job, image: "alpine@sha256:abc", resources: {cpu: "2", memory: 4Gi}}
      command: ["true"]
`))
	require.NoError(t, err)
	assert.Equal(t, Runtime{Kind: "job", Image: "alpine@sha256:abc", Resources: &Resources{CPU: "2", Memory: "4Gi"}}, s.Tasks[0].Runtime)
}

func TestCompileOrdersTasksTopologically(t *testing.T) {
	s, err := compileYAML(t, pipeline(`
  defaults:
    runtime: {kind: agent, pool: local}
  tasks:
    - {id: load, depends_on: [transform_b, transform_a], command: ["true"]}
    - {id: transform_b, depends_on: [extract], command: ["true"]}
    - {id: transform_a, depends_on: [extract], command: ["true"]}
    - {id: extract, command: ["true"]}
`))
	require.NoError(t, err)
	var ids []string
	for _, task := range s.Tasks {
		ids = append(ids, task.ID)
	}
	assert.Equal(t, []string{"extract", "transform_a", "transform_b", "load"}, ids)
	assert.Equal(t, []string{"transform_a", "transform_b"}, s.Task("load").DependsOn)
}

func TestCompileHashIgnoresFormattingButNotMeaning(t *testing.T) {
	a := pipeline(`
  defaults:
    runtime: {kind: agent, pool: local}
  tasks:
    - id: a
      command: ["echo", "x"]
      env: {B: "2", A: "1"}
`)
	b := `# reformatted, keys reordered, explicit defaults
kind: Pipeline
apiVersion: hubwagen.dev/v1alpha1
metadata:
  tenant: t
  name: p
spec:
  concurrency: {on_overlap: queue, max_active_runs: 1}
  tasks:
    - command:
        - echo
        - x
      id: a
      on_failure: stop
      env:
        A: "1"
        B: "2"
      runtime:
        pool: local
        kind: agent
`
	c := strings.Replace(a, `"x"`, `"y"`, 1)

	hash := func(src string) string {
		out, err := CompileFile(context.Background(), "x.yaml", []byte(src), Options{})
		require.NoError(t, err)
		return out[0].Hash
	}
	assert.Equal(t, hash(a), hash(b))
	assert.NotEqual(t, hash(a), hash(c))
}

func TestCompileErrors(t *testing.T) {
	cases := []struct {
		name string
		src  string
		want []string
	}{
		{
			name: "header",
			src: `apiVersion: hubwagen.dev/v2
kind: Pipelin
metadata: {name: "Bad Name", tenant: ""}
spec:` + minimalTasks,
			want: []string{
				`apiVersion: must be "hubwagen.dev/v1alpha1"`,
				`kind: must be "Pipeline"`,
				`metadata.name: "Bad Name" must match`,
				`metadata.tenant: "" must match`,
			},
		},
		{
			name: "unknown field",
			src:  pipeline(minimalTasks + "      comand: [x]\n"),
			want: []string{"field comand not found"},
		},
		{
			name: "no tasks",
			src:  pipeline("  tasks: []\n"),
			want: []string{"spec.tasks: at least one task is required"},
		},
		{
			name: "ids and dependencies",
			src: pipeline(`
  defaults: {runtime: {kind: agent, pool: local}}
  tasks:
    - {id: a, command: ["true"]}
    - {id: a, command: ["true"]}
    - {id: B, command: ["true"]}
    - {id: c, depends_on: [c, missing], command: ["true"]}
`),
			want: []string{
				`spec.tasks[1].id: duplicate task id "a"`,
				`spec.tasks[2].id: "B" must match`,
				`spec.tasks[3].depends_on[0]: task cannot depend on itself`,
				`spec.tasks[3].depends_on[1]: unknown task "missing"`,
			},
		},
		{
			name: "cycle",
			src: pipeline(`
  defaults: {runtime: {kind: agent, pool: local}}
  tasks:
    - {id: a, depends_on: [c], command: ["true"]}
    - {id: b, depends_on: [a], command: ["true"]}
    - {id: c, depends_on: [b], command: ["true"]}
`),
			want: []string{"spec.tasks: dependency cycle: a → c → b → a"},
		},
		{
			name: "runtime",
			src: pipeline(`
  tasks:
    - {id: a, command: ["true"]}
    - {id: b, runtime: {kind: agent}, command: ["true"]}
    - {id: c, runtime: {kind: agent, pool: p, image: x}, command: ["true"]}
    - {id: d, runtime: {kind: job}, command: ["true"]}
    - {id: e, runtime: {kind: job, image: x, resources: {cpu: lots, memory: 1GB}}, command: ["true"]}
    - {id: f, runtime: {kind: lambda}, command: ["true"]}
`),
			want: []string{
				`spec.tasks[0].runtime: is required`,
				`spec.tasks[1].runtime.pool: is required for kind: agent`,
				`spec.tasks[2].runtime.image: not allowed for kind: agent`,
				`spec.tasks[3].runtime.image: is required for kind: job`,
				`spec.tasks[4].runtime.resources.cpu: "lots" is not a valid CPU quantity`,
				`spec.tasks[4].runtime.resources.memory: "1GB" is not a valid memory quantity`,
				`spec.tasks[5].runtime.kind: must be agent|job, got "lambda"`,
			},
		},
		{
			name: "command, env and secrets",
			src: pipeline(`
  defaults: {runtime: {kind: agent, pool: local}}
  tasks:
    - id: a
    - id: b
      command: ["echo", "{{ .Run.ID }}"]
      env: {HUBWAGEN_RUN_ID: x, "1BAD": y, TOKEN: z}
      secrets:
        - {name: s, key: k, env: TOKEN}
        - {name: s}
`),
			want: []string{
				`spec.tasks[0].command: is required`,
				`spec.tasks[1].command[1]: templates are not allowed in command`,
				`spec.tasks[1].env.HUBWAGEN_RUN_ID: "HUBWAGEN_RUN_ID" is reserved`,
				`spec.tasks[1].env.1BAD: "1BAD" is not a valid environment variable name`,
				`spec.tasks[1].secrets[0].env: "TOKEN" is also set in env`,
				`spec.tasks[1].secrets[1]: name, key and env are all required`,
			},
		},
		{
			name: "inputs and outputs",
			src: pipeline(`
  defaults: {runtime: {kind: agent, pool: local}}
  tasks:
    - id: a
      command: ["true"]
      outputs:
        - {name: out}
        - {name: tbl, asset: raw.orders}
        - {name: x-y}
        - {name: x_y}
    - id: b
      command: ["true"]
      inputs:
        - {name: i1, from: a.out}
        - {name: i2, from: a}
        - {name: i3, from: c.out}
    - id: c
      depends_on: [a]
      command: ["true"]
      outputs: [{name: out}]
      inputs:
        - {name: i1, from: a.nope}
        - {name: i2, from: missing.out}
`),
			want: []string{
				`spec.tasks[0].outputs[1].partition: is required when asset is set`,
				`spec.tasks[0].outputs[3].name: "x_y" collides with output "x-y"`,
				`spec.tasks[1].inputs[0].from: task "a" is not upstream of "b"; add it to depends_on`,
				`spec.tasks[1].inputs[1].from: "a" must be <task-id>.<output-name>`,
				`spec.tasks[1].inputs[2].from: task "c" is not upstream of "b"`,
				`spec.tasks[2].inputs[0].from: task "a" has no output "nope"`,
				`spec.tasks[2].inputs[1].from: unknown task "missing"`,
			},
		},
		{
			name: "templates",
			src: pipeline(`
  params: {mode: {type: string}}
  defaults: {runtime: {kind: agent, pool: local}}
  tasks:
    - id: a
      command: ["true"]
      args:
        - "{{ .Params.mode }} {{ .Interval.Start }}"
        - "{{ .Params.other }}"
        - "{{ .Env.HOME }}"
        - "{{ .Asset.Name }}"
        - "{{ range .Params }}x{{ end }}"
        - "{{ call .Run.ID }}"
        - "{{ .Inputs.nope }}"
`),
			want: []string{
				`spec.tasks[0].args[1]: .Params.other: "other" is not a declared param`,
				`spec.tasks[0].args[2]: .Env.HOME: unknown variable`,
				`spec.tasks[0].args[3]: .Asset.Name: .Asset is only available in pipelines with asset triggers`,
				`spec.tasks[0].args[4]: {{ range }} is not allowed`,
				`spec.tasks[0].args[5]: function "call" is not allowed`,
				`spec.tasks[0].args[6]: .Inputs.nope: "nope" is not a declared input`,
			},
		},
		{
			name: "params",
			src: pipeline(`
  params:
    n: {type: int, default: "three"}
    f: {type: float}
    Bad: {type: string}
` + minimalTasks),
			want: []string{
				`spec.params.Bad: param name must match`,
				`spec.params.f.type: must be string|int|bool, got "float"`,
				`spec.params.n.default: three is not a valid int`,
			},
		},
		{
			name: "schedule",
			src: pipeline(`
  schedule: {cron: "0 * * * *", timezone: Europe/Berlin, interval: daily}
` + minimalTasks),
			want: []string{`spec.schedule: schedule fires more than once per daily interval`},
		},
		{
			name: "schedule gaps and bad values",
			src: pipeline(`
  schedule: {cron: "0 2 * * 1-5", timezone: Mars/Olympus, interval: daily}
` + minimalTasks),
			want: []string{`spec.schedule.timezone: unknown timezone "Mars/Olympus"`},
		},
		{
			name: "weekday schedule with daily interval",
			src: pipeline(`
  schedule: {cron: "0 2 * * 1-5", interval: daily}
` + minimalTasks),
			want: []string{`spec.schedule: schedule skips daily intervals`},
		},
		{
			name: "retry and timeout bounds",
			src: pipeline(`
  defaults: {runtime: {kind: agent, pool: local}}
  tasks:
    - id: a
      command: ["true"]
      retry: {limit: 11, backoff: linear, initial: 5m, max: 1m}
      timeout: 72h
`),
			want: []string{
				`spec.tasks[0].retry.limit: must be between 0 and 10`,
				`spec.tasks[0].retry.backoff: must be exponential|fixed, got "linear"`,
				`spec.tasks[0].retry.max: must not be less than initial`,
				`spec.tasks[0].timeout: must be between 1s and 48h0m0s`,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := compileYAML(t, tc.src)
			require.Error(t, err)
			for _, w := range tc.want {
				assert.Contains(t, err.Error(), w)
			}
		})
	}
}

type fakeResolver struct{ calls []string }

func (f *fakeResolver) Resolve(_ context.Context, ref string) (string, error) {
	f.calls = append(f.calls, ref)
	if strings.Contains(ref, "missing") {
		return "", errors.New("manifest unknown")
	}
	return repoWithoutTag(ref) + "@sha256:" + strings.Repeat("a", 64), nil
}

func TestCompileResolvesJobImagesOnce(t *testing.T) {
	r := &fakeResolver{}
	s, err := compileYAMLOpts(t, pipeline(`
  tasks:
    - {id: a, runtime: {kind: job, image: "ghcr.io/acme/dbt:1.8"}, command: ["true"]}
    - {id: b, runtime: {kind: job, image: "ghcr.io/acme/dbt:1.8"}, command: ["true"]}
    - {id: c, runtime: {kind: job, image: "registry:5000/x@sha256:`+strings.Repeat("b", 64)+`"}, command: ["true"]}
`), Options{Resolver: r})
	require.NoError(t, err)
	assert.Equal(t, []string{"ghcr.io/acme/dbt:1.8"}, r.calls)
	assert.Equal(t, "ghcr.io/acme/dbt@sha256:"+strings.Repeat("a", 64), s.Task("a").Runtime.Image)
	assert.Equal(t, s.Task("a").Runtime.Image, s.Task("b").Runtime.Image)

	_, err = compileYAMLOpts(t, pipeline(`
  tasks:
    - {id: a, runtime: {kind: job, image: "ghcr.io/acme/missing:1"}, command: ["true"]}
`), Options{Resolver: r})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spec.tasks[id=a].runtime.image: manifest unknown")
}

func TestRepoWithoutTag(t *testing.T) {
	for in, want := range map[string]string{
		"alpine:3.20":                "alpine",
		"alpine":                     "alpine",
		"registry:5000/team/img:1.0": "registry:5000/team/img",
		"registry:5000/team/img":     "registry:5000/team/img",
	} {
		assert.Equal(t, want, repoWithoutTag(in), in)
	}
}

func TestCompileFileMultiDocumentErrorsNameTheDocument(t *testing.T) {
	src := pipeline(minimalTasks) + "---\n" + pipeline("  tasks: []\n")
	_, err := CompileFile(context.Background(), "multi.yaml", []byte(src), Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "multi.yaml (document 2): spec.tasks: at least one task is required")
	assert.NotContains(t, err.Error(), "document 1")
}

func TestCompileFileRejectsNonPipelines(t *testing.T) {
	_, err := CompileFile(context.Background(), "x.yaml", []byte("foo: bar\n"), Options{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), fmt.Sprintf("missing apiVersion (expected %q)", APIVersion))
}
