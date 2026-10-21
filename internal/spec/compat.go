package spec

import (
	"fmt"
	"regexp"
	"strings"
)

// v1 DAG format (hubwagen ≤ phase 2), kept only so existing DAG files keep
// working in dev mode. The mapping is documented in
// specs/003-platform-redesign/contracts/pipeline-spec.md.

type v1DAG struct {
	Name        string            `yaml:"name"`
	Description string            `yaml:"description"`
	Schedule    string            `yaml:"schedule"`
	Timezone    string            `yaml:"timezone"`
	Concurrency v1Concurrency     `yaml:"concurrency"`
	Env         map[string]string `yaml:"env"`
	Retry       *v1Retry          `yaml:"retry"`
	Steps       []v1Step          `yaml:"steps"`
}

type v1Concurrency struct {
	MaxRuns   int    `yaml:"max_runs"`
	OnOverlap string `yaml:"on_overlap"`
}

type v1Retry struct {
	Limit       int  `yaml:"limit"`
	IntervalSec int  `yaml:"interval_sec"`
	Backoff     bool `yaml:"backoff"`
}

type v1Step struct {
	ID         string            `yaml:"id"`
	Name       string            `yaml:"name"`
	Executor   string            `yaml:"executor"`
	DependsOn  []string          `yaml:"depends_on"`
	Command    string            `yaml:"command"`
	Env        map[string]string `yaml:"env"`
	TimeoutSec int               `yaml:"timeout_sec"`
	Retry      *v1Retry          `yaml:"retry"`
	OnFailure  string            `yaml:"on_failure"`
	HTTP       any               `yaml:"http"`
	SQL        any               `yaml:"sql"`
}

// V1Tenant is the tenant v1 DAGs are compiled into.
const V1Tenant = "default"

// V1Pool is the agent pool v1 shell steps run in: host processes, dev mode.
const V1Pool = "local"

func convertV1(raw []byte) (*Document, error) {
	var v v1DAG
	if err := strictDecode(raw, &v); err != nil {
		return nil, fmt.Errorf("v1 DAG: %w", err)
	}

	var errs collector
	d := &Document{
		APIVersion: APIVersion,
		Kind:       KindPipeline,
		Metadata:   Metadata{Name: v.Name, Tenant: V1Tenant, Description: v.Description},
	}

	if v.Schedule != "" {
		catchup := false // v1 never caught up missed ticks
		d.Spec.Schedule = &ScheduleDef{Cron: v.Schedule, Timezone: v.Timezone, Interval: IntervalCron, Catchup: &catchup}
	}

	d.Spec.Concurrency = ConcurrencyDef{MaxActiveRuns: v.Concurrency.MaxRuns, OnOverlap: v.Concurrency.OnOverlap}
	if d.Spec.Concurrency.OnOverlap == "" {
		d.Spec.Concurrency.OnOverlap = "skip" // v1's default
	}
	if v.Retry != nil {
		d.Spec.Defaults.Retry = convertV1Retry(v.Retry)
	}

	for i, s := range v.Steps {
		path := fmt.Sprintf("steps[%d]", i)
		t := TaskDef{
			ID:        s.ID,
			DependsOn: s.DependsOn,
			OnFailure: s.OnFailure,
			Runtime:   &RuntimeDef{Kind: RuntimeAgent, Pool: V1Pool},
		}

		switch s.Executor {
		case "", "shell":
			script, err := rewriteV1Template(s.Command)
			if err != nil {
				errs.add(path+".command", "%v", err)
			}
			t.Command = []string{"/bin/sh", "-c"}
			t.Args = []string{script}
		case "noop":
			t.Command = []string{"true"}
		case "http", "sql":
			errs.add(path+".executor", "executor %q is not supported in v1alpha1; run it as a container task "+
				"(for example an image with curl or a SQL client) and convert this file to apiVersion %s", s.Executor, APIVersion)
			continue
		default:
			errs.add(path+".executor", "unknown executor %q", s.Executor)
			continue
		}

		// v1 merged DAG-level env into every step, step values winning.
		if len(v.Env)+len(s.Env) > 0 {
			t.Env = make(map[string]string, len(v.Env)+len(s.Env))
			for k, val := range v.Env {
				t.Env[k] = val
			}
			for k, val := range s.Env {
				t.Env[k] = val
			}
			for k, val := range t.Env {
				rv, err := rewriteV1Template(val)
				if err != nil {
					errs.add(path+".env."+k, "%v", err)
				}
				t.Env[k] = rv
			}
		}

		if s.Retry != nil {
			t.Retry = convertV1Retry(s.Retry)
		}
		if s.TimeoutSec > 0 {
			t.Timeout = fmt.Sprintf("%ds", s.TimeoutSec)
		} else {
			t.Timeout = maxTimeout.String() // v1 steps had no timeout by default
		}
		d.Spec.Tasks = append(d.Spec.Tasks, t)
	}

	if err := errs.err(); err != nil {
		return nil, err
	}
	return d, nil
}

// convertV1Retry spells out every field: in v1 a step's retry block replaced
// the DAG's entirely rather than overriding single fields.
func convertV1Retry(r *v1Retry) *RetryDef {
	limit := r.Limit
	backoff := "fixed"
	if r.Backoff {
		backoff = "exponential"
	}
	initial := fmt.Sprintf("%ds", r.IntervalSec)
	return &RetryDef{Limit: &limit, Backoff: backoff, Initial: initial, Max: maxV1RetryDelay(r.IntervalSec)}
}

// maxV1RetryDelay keeps max ≥ initial; v1 backoff was uncapped, 1h is the
// closest sane equivalent.
func maxV1RetryDelay(intervalSec int) string {
	if intervalSec > 3600 {
		return fmt.Sprintf("%ds", intervalSec)
	}
	return "1h"
}

var (
	v1ActionRe = regexp.MustCompile(`(?s)\{\{.*?\}\}`)
	v1EnvRe    = regexp.MustCompile(`\.Env\.`)
	v1Rewrites = []struct {
		re   *regexp.Regexp
		with string
	}{
		{regexp.MustCompile(`\.RunDate\b`), ".Run.Date"},
		{regexp.MustCompile(`\.RunID\b`), ".Run.ID"},
		{regexp.MustCompile(`\.DAGName\b`), ".Pipeline"},
	}
)

// rewriteV1Template maps v1 template variables onto v1alpha1 ones, touching
// only text inside {{ … }} actions.
func rewriteV1Template(text string) (string, error) {
	var bad error
	out := v1ActionRe.ReplaceAllStringFunc(text, func(action string) string {
		if v1EnvRe.MatchString(action) {
			bad = fmt.Errorf("{{ .Env.X }} is no longer supported: use $X in the command, or env:/secrets:")
			return action
		}
		for _, rw := range v1Rewrites {
			action = rw.re.ReplaceAllString(action, rw.with)
		}
		return action
	})
	return out, bad
}

func init() {
	// Keep the compat mapping honest: every v1 rewrite target must be an
	// allowed template variable.
	for _, rw := range v1Rewrites {
		parts := strings.Split(strings.TrimPrefix(rw.with, "."), ".")
		if err := checkField(parts, scope{}); err != nil {
			panic("compat rewrite to disallowed variable: " + err.Error())
		}
	}
}
