package spec

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	pipelineNameRe = regexp.MustCompile(`^[a-z0-9_-]{1,63}$`)
	taskIDRe       = regexp.MustCompile(`^[a-z0-9_]{1,63}$`)
	poolNameRe     = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$`)
	identRe        = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)
	paramNameRe    = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)
	assetNameRe    = regexp.MustCompile(`^[a-z0-9_]+(\.[a-z0-9_]+)*$`)
	labelKeyRe     = regexp.MustCompile(`^[a-z0-9]([a-z0-9._/-]{0,61}[a-z0-9])?$`)
	envKeyRe       = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	cpuRe          = regexp.MustCompile(`^([0-9]+m|[0-9]+(\.[0-9]+)?)$`)
	memoryRe       = regexp.MustCompile(`^[0-9]+(Ki|Mi|Gi|Ti|K|M|G|T)?$`)
)

var (
	validOnOverlap   = map[string]bool{"queue": true, "skip": true, "cancel_running": true}
	validTriggerRule = map[string]bool{"all_success": true, "all_done": true, "one_failed": true}
	validOnFailure   = map[string]bool{"stop": true, "continue": true, "skip_dependents": true}
	validBackoff     = map[string]bool{"exponential": true, "fixed": true}
	validParamType   = map[string]bool{"string": true, "int": true, "bool": true}
)

const (
	RuntimeAgent = "agent"
	RuntimeJob   = "job"

	defaultTimeout = time.Hour
	minTimeout     = time.Second
	maxTimeout     = 48 * time.Hour
	maxRetryDelay  = 24 * time.Hour
	maxRetryLimit  = 10

	// coverageSamples is how many consecutive ticks checkCoverage inspects.
	coverageSamples = 20
)

// coverageFrom is where the coverage check starts sampling ticks when the
// schedule has no start date — fixed, so compilation stays deterministic.
var coverageFrom = time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)

// Options control compilation.
type Options struct {
	// Resolver pins job images to digests. Nil leaves image references as
	// written (dev mode, or `compile --no-resolve`).
	Resolver ImageResolver
}

// Compile validates doc and turns it into a Spec. On validation failure it
// returns Errors listing every problem found; nothing partial is returned.
func Compile(ctx context.Context, doc *Document, opts Options) (*Spec, error) {
	c := &compiler{doc: doc}
	s := c.compile()
	if err := c.errs.err(); err != nil {
		return nil, err
	}
	if opts.Resolver != nil {
		c.resolveImages(ctx, s, opts.Resolver)
		if err := c.errs.err(); err != nil {
			return nil, err
		}
	}
	return s, nil
}

type compiler struct {
	doc  *Document
	errs collector
}

func (c *compiler) compile() *Spec {
	d := c.doc
	s := &Spec{
		APIVersion:  APIVersion,
		Tenant:      d.Metadata.Tenant,
		Name:        d.Metadata.Name,
		Description: d.Metadata.Description,
	}

	if d.APIVersion != APIVersion {
		c.errs.add("apiVersion", "must be %q, got %q", APIVersion, d.APIVersion)
	}
	if d.Kind != KindPipeline {
		c.errs.add("kind", "must be %q, got %q", KindPipeline, d.Kind)
	}
	if !pipelineNameRe.MatchString(d.Metadata.Name) {
		c.errs.add("metadata.name", "%q must match %s", d.Metadata.Name, pipelineNameRe)
	}
	if !pipelineNameRe.MatchString(d.Metadata.Tenant) {
		c.errs.add("metadata.tenant", "%q must match %s", d.Metadata.Tenant, pipelineNameRe)
	}
	if len(d.Metadata.Labels) > 0 {
		s.Labels = make(map[string]string, len(d.Metadata.Labels))
		for k, v := range d.Metadata.Labels {
			if !labelKeyRe.MatchString(k) {
				c.errs.add("metadata.labels."+k, "label key must match %s", labelKeyRe)
			}
			s.Labels[k] = v
		}
	}

	s.Params = c.params()
	declaredParams := make(map[string]bool, len(s.Params))
	for _, p := range s.Params {
		declaredParams[p.Name] = true
	}

	s.Schedule = c.schedule()
	s.Triggers = c.triggers()
	s.Concurrency = c.concurrency()
	s.Tasks = c.tasks(declaredParams, len(s.Triggers) > 0)
	return s
}

func (c *compiler) params() []Param {
	names := make([]string, 0, len(c.doc.Spec.Params))
	for n := range c.doc.Spec.Params {
		names = append(names, n)
	}
	sort.Strings(names)

	var out []Param
	for _, n := range names {
		def := c.doc.Spec.Params[n]
		path := "spec.params." + n
		if !paramNameRe.MatchString(n) {
			c.errs.add(path, "param name must match %s", paramNameRe)
		}
		if !validParamType[def.Type] {
			c.errs.add(path+".type", "must be string|int|bool, got %q", def.Type)
			continue
		}
		p := Param{Name: n, Type: def.Type}
		if def.Default != nil {
			v, ok := coerceParam(def.Type, def.Default)
			if !ok {
				c.errs.add(path+".default", "%v is not a valid %s", def.Default, def.Type)
			}
			p.Default = v
		}
		out = append(out, p)
	}
	return out
}

// coerceParam normalises a YAML-decoded default to the canonical Go type for
// the param type (string, int64 or bool).
func coerceParam(typ string, v any) (any, bool) {
	switch typ {
	case "string":
		s, ok := v.(string)
		return s, ok
	case "bool":
		b, ok := v.(bool)
		return b, ok
	case "int":
		switch n := v.(type) {
		case int:
			return int64(n), true
		case int64:
			return n, true
		case uint64:
			return int64(n), n <= 1<<63-1
		}
	}
	return nil, false
}

func (c *compiler) schedule() *Schedule {
	def := c.doc.Spec.Schedule
	if def == nil {
		return nil
	}
	s := &Schedule{Cron: def.Cron, Timezone: def.Timezone, Interval: def.Interval, Catchup: true}
	if s.Timezone == "" {
		s.Timezone = "UTC"
	}
	if s.Interval == "" {
		s.Interval = IntervalCron
	}
	if def.Catchup != nil {
		s.Catchup = *def.Catchup
	}
	if def.Cron == "" {
		c.errs.add("spec.schedule.cron", "is required when schedule is set")
		return s
	}

	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		c.errs.add("spec.schedule.timezone", "unknown timezone %q", s.Timezone)
		return s
	}
	if def.Start != "" {
		start, err := parseStart(def.Start, loc)
		if err != nil {
			c.errs.add("spec.schedule.start", "%v", err)
		} else {
			s.Start = start.UTC().Format(time.RFC3339)
		}
	}

	cal, err := NewCalendar(s)
	if err != nil {
		c.errs.add("spec.schedule", "%v", err)
		return s
	}
	from := coverageFrom
	if !cal.Start().IsZero() {
		from = cal.Start()
	}
	if err := cal.checkCoverage(from, coverageSamples); err != nil {
		c.errs.add("spec.schedule", "%v", err)
	}
	return s
}

// parseStart accepts a date (midnight in the pipeline timezone) or RFC 3339.
func parseStart(v string, loc *time.Location) (time.Time, error) {
	if t, err := time.ParseInLocation("2006-01-02", v, loc); err == nil {
		return t, nil
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}, fmt.Errorf("%q must be YYYY-MM-DD or RFC 3339", v)
	}
	return t, nil
}

func (c *compiler) triggers() []Trigger {
	var out []Trigger
	for i, t := range c.doc.Spec.Triggers {
		path := fmt.Sprintf("spec.triggers[%d]", i)
		if !assetNameRe.MatchString(t.Asset) {
			c.errs.add(path+".asset", "%q must match %s", t.Asset, assetNameRe)
		}
		partition := t.Partition
		if partition == "" {
			partition = "{{ .Asset.Partition }}"
		}
		if err := checkTemplate(partition, scope{asset: true, assetOnly: true}); err != nil {
			c.errs.add(path+".partition", "%v", err)
		}
		out = append(out, Trigger{Asset: t.Asset, Partition: partition})
	}
	return out
}

func (c *compiler) concurrency() Concurrency {
	def := c.doc.Spec.Concurrency
	out := Concurrency{MaxActiveRuns: def.MaxActiveRuns, OnOverlap: def.OnOverlap}
	if out.MaxActiveRuns == 0 {
		out.MaxActiveRuns = 1
	}
	if out.MaxActiveRuns < 1 {
		c.errs.add("spec.concurrency.max_active_runs", "must be at least 1")
	}
	if out.OnOverlap == "" {
		out.OnOverlap = "queue"
	}
	if !validOnOverlap[out.OnOverlap] {
		c.errs.add("spec.concurrency.on_overlap", "must be queue|skip|cancel_running, got %q", out.OnOverlap)
	}
	return out
}

func (c *compiler) tasks(params map[string]bool, hasTriggers bool) []Task {
	defs := c.doc.Spec.Tasks
	if len(defs) == 0 {
		c.errs.add("spec.tasks", "at least one task is required")
		return nil
	}

	defaultRetry := c.retry("spec.defaults.retry", Retry{Backoff: "exponential", InitialSeconds: 30, MaxSeconds: 600}, c.doc.Spec.Defaults.Retry)
	defaultTimeoutSec := int64(defaultTimeout / time.Second)
	if t := c.doc.Spec.Defaults.Timeout; t != "" {
		defaultTimeoutSec = c.timeout("spec.defaults.timeout", t)
	}

	// First pass: IDs and declared outputs, which every task's validation
	// below may reference.
	ids := make(map[string]int, len(defs))
	outputs := make(map[string]map[string]bool, len(defs))
	for i, t := range defs {
		path := fmt.Sprintf("spec.tasks[%d].id", i)
		switch {
		case !taskIDRe.MatchString(t.ID):
			c.errs.add(path, "%q must match %s", t.ID, taskIDRe)
		case ids[t.ID] > 0:
			c.errs.add(path, "duplicate task id %q", t.ID)
		}
		ids[t.ID]++
		outputs[t.ID] = make(map[string]bool)
		for _, o := range t.Outputs {
			outputs[t.ID][o.Name] = true
		}
	}

	deps := make(map[string][]string, len(defs))
	depsOK := true
	tasks := make([]Task, 0, len(defs))
	for i, def := range defs {
		path := fmt.Sprintf("spec.tasks[%d]", i)
		t := c.task(path, def, params, hasTriggers, defaultRetry, defaultTimeoutSec)

		seen := make(map[string]bool)
		for j, d := range def.DependsOn {
			dpath := fmt.Sprintf("%s.depends_on[%d]", path, j)
			switch {
			case d == def.ID:
				c.errs.add(dpath, "task cannot depend on itself")
				depsOK = false
			case ids[d] == 0:
				c.errs.add(dpath, "unknown task %q", d)
				depsOK = false
			case seen[d]:
				c.errs.add(dpath, "duplicate dependency %q", d)
			default:
				t.DependsOn = append(t.DependsOn, d)
			}
			seen[d] = true
		}
		sort.Strings(t.DependsOn)
		deps[def.ID] = t.DependsOn
		tasks = append(tasks, t)
	}

	// Duplicate IDs make the graph ambiguous; the duplicate error is enough.
	for _, n := range ids {
		if n > 1 {
			depsOK = false
		}
	}
	if !depsOK {
		return tasks
	}

	order, cycle := topoOrder(deps)
	if cycle != nil {
		c.errs.add("spec.tasks", "dependency cycle: %s", formatCycle(cycle))
		return tasks
	}

	// Inputs can only come from upstream tasks, which needs the graph.
	for i, def := range defs {
		up := upstream(deps, def.ID)
		for j, in := range def.Inputs {
			path := fmt.Sprintf("spec.tasks[%d].inputs[%d].from", i, j)
			task, out, ok := strings.Cut(in.From, ".")
			switch {
			case !ok || task == "" || out == "":
				c.errs.add(path, "%q must be <task-id>.<output-name>", in.From)
			case ids[task] == 0:
				c.errs.add(path, "unknown task %q", task)
			case !up[task]:
				c.errs.add(path, "task %q is not upstream of %q; add it to depends_on", task, def.ID)
			case !outputs[task][out]:
				c.errs.add(path, "task %q has no output %q", task, out)
			}
		}
	}

	byID := make(map[string]Task, len(tasks))
	for _, t := range tasks {
		byID[t.ID] = t
	}
	sorted := make([]Task, 0, len(order))
	for _, id := range order {
		sorted = append(sorted, byID[id])
	}
	return sorted
}

func (c *compiler) task(path string, def TaskDef, params map[string]bool, hasTriggers bool, defaultRetry Retry, defaultTimeoutSec int64) Task {
	t := Task{
		ID:             def.ID,
		TriggerRule:    def.TriggerRule,
		OnFailure:      def.OnFailure,
		Command:        def.Command,
		TimeoutSeconds: defaultTimeoutSec,
	}
	if t.TriggerRule == "" {
		t.TriggerRule = "all_success"
	}
	if !validTriggerRule[t.TriggerRule] {
		c.errs.add(path+".trigger_rule", "must be all_success|all_done|one_failed, got %q", t.TriggerRule)
	}
	if t.OnFailure == "" {
		t.OnFailure = "stop"
	}
	if !validOnFailure[t.OnFailure] {
		c.errs.add(path+".on_failure", "must be stop|continue|skip_dependents, got %q", t.OnFailure)
	}

	rt := def.Runtime
	if rt == nil {
		rt = c.doc.Spec.Defaults.Runtime
	}
	t.Runtime = c.runtime(path+".runtime", rt)

	if len(def.Command) == 0 {
		c.errs.add(path+".command", "is required (the image's own entrypoint is not used)")
	}
	for i, arg := range def.Command {
		if strings.Contains(arg, "{{") {
			c.errs.add(fmt.Sprintf("%s.command[%d]", path, i), "templates are not allowed in command; put them in args")
		}
	}

	inputs := make(map[string]bool)
	inputEnv := make(map[string]string)
	for i, in := range def.Inputs {
		ipath := fmt.Sprintf("%s.inputs[%d]", path, i)
		if !identRe.MatchString(in.Name) {
			c.errs.add(ipath+".name", "%q must match %s", in.Name, identRe)
		} else if prev, dup := inputEnv[envSuffix(in.Name)]; dup {
			c.errs.add(ipath+".name", "%q collides with input %q (same HUBWAGEN_INPUT_ variable)", in.Name, prev)
		}
		inputs[in.Name] = true
		inputEnv[envSuffix(in.Name)] = in.Name
		task, out, _ := strings.Cut(in.From, ".")
		t.Inputs = append(t.Inputs, Input{Name: in.Name, FromTask: task, FromOutput: out})
	}

	outputs := make(map[string]bool)
	outputEnv := make(map[string]string)
	for i, o := range def.Outputs {
		opath := fmt.Sprintf("%s.outputs[%d]", path, i)
		if !identRe.MatchString(o.Name) {
			c.errs.add(opath+".name", "%q must match %s", o.Name, identRe)
		} else if prev, dup := outputEnv[envSuffix(o.Name)]; dup {
			c.errs.add(opath+".name", "%q collides with output %q (same HUBWAGEN_OUTPUT_ variable)", o.Name, prev)
		}
		outputs[o.Name] = true
		outputEnv[envSuffix(o.Name)] = o.Name
		out := Output{Name: o.Name, Asset: o.Asset}
		if o.Asset != "" {
			if !assetNameRe.MatchString(o.Asset) {
				c.errs.add(opath+".asset", "%q must match %s", o.Asset, assetNameRe)
			}
			if o.Partition == "" {
				c.errs.add(opath+".partition", "is required when asset is set")
			}
		}
		if o.Partition != "" {
			if o.Asset == "" {
				c.errs.add(opath+".partition", "only meaningful together with asset")
			}
			if err := checkTemplate(o.Partition, scope{params: params, asset: hasTriggers}); err != nil {
				c.errs.add(opath+".partition", "%v", err)
			}
			out.Partition = o.Partition
		}
		t.Outputs = append(t.Outputs, out)
	}

	sc := scope{params: params, inputs: inputs, outputs: outputs, asset: hasTriggers}
	for i, arg := range def.Args {
		if err := checkTemplate(arg, sc); err != nil {
			c.errs.add(fmt.Sprintf("%s.args[%d]", path, i), "%v", err)
		}
	}
	t.Args = def.Args

	if len(def.Env) > 0 {
		t.Env = make(map[string]string, len(def.Env))
		for k, v := range def.Env {
			epath := path + ".env." + k
			c.checkEnvName(epath, k)
			if err := checkTemplate(v, sc); err != nil {
				c.errs.add(epath, "%v", err)
			}
			t.Env[k] = v
		}
	}

	for i, s := range def.Secrets {
		spath := fmt.Sprintf("%s.secrets[%d]", path, i)
		if s.Name == "" || s.Key == "" || s.Env == "" {
			c.errs.add(spath, "name, key and env are all required")
			continue
		}
		c.checkEnvName(spath+".env", s.Env)
		if _, clash := def.Env[s.Env]; clash {
			c.errs.add(spath+".env", "%q is also set in env", s.Env)
		}
		t.Secrets = append(t.Secrets, Secret{Name: s.Name, Key: s.Key, Env: s.Env})
	}

	t.Retry = c.retry(path+".retry", defaultRetry, def.Retry)
	if def.Timeout != "" {
		t.TimeoutSeconds = c.timeout(path+".timeout", def.Timeout)
	}
	return t
}

func envSuffix(name string) string {
	return strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

func (c *compiler) checkEnvName(path, k string) {
	switch {
	case !envKeyRe.MatchString(k):
		c.errs.add(path, "%q is not a valid environment variable name", k)
	case strings.HasPrefix(strings.ToUpper(k), "HUBWAGEN_") || strings.EqualFold(k, "TRACEPARENT"):
		c.errs.add(path, "%q is reserved by the task contract", k)
	}
}

func (c *compiler) runtime(path string, def *RuntimeDef) Runtime {
	if def == nil {
		c.errs.add(path, "is required (set it on the task or in spec.defaults.runtime)")
		return Runtime{}
	}
	rt := Runtime{Kind: def.Kind, Pool: def.Pool, Image: def.Image}
	switch def.Kind {
	case RuntimeAgent:
		if def.Pool == "" {
			c.errs.add(path+".pool", "is required for kind: agent")
		} else if !poolNameRe.MatchString(def.Pool) {
			c.errs.add(path+".pool", "%q must match %s", def.Pool, poolNameRe)
		}
		if def.Image != "" {
			c.errs.add(path+".image", "not allowed for kind: agent (the pool's image is used)")
		}
		if def.Resources != nil {
			c.errs.add(path+".resources", "not allowed for kind: agent (the pool's resources apply)")
		}
	case RuntimeJob:
		if def.Image == "" {
			c.errs.add(path+".image", "is required for kind: job")
		}
		if def.Pool != "" {
			c.errs.add(path+".pool", "not allowed for kind: job")
		}
		if r := def.Resources; r != nil {
			if r.CPU != "" && !cpuRe.MatchString(r.CPU) {
				c.errs.add(path+".resources.cpu", "%q is not a valid CPU quantity", r.CPU)
			}
			if r.Memory != "" && !memoryRe.MatchString(r.Memory) {
				c.errs.add(path+".resources.memory", "%q is not a valid memory quantity", r.Memory)
			}
			if r.CPU != "" || r.Memory != "" {
				rt.Resources = &Resources{CPU: r.CPU, Memory: r.Memory}
			}
		}
	default:
		c.errs.add(path+".kind", "must be agent|job, got %q", def.Kind)
	}
	return rt
}

// retry overlays def's fields onto base.
func (c *compiler) retry(path string, base Retry, def *RetryDef) Retry {
	if def == nil {
		return base
	}
	r := base
	if def.Limit != nil {
		r.Limit = *def.Limit
		if r.Limit < 0 || r.Limit > maxRetryLimit {
			c.errs.add(path+".limit", "must be between 0 and %d", maxRetryLimit)
		}
	}
	if def.Backoff != "" {
		r.Backoff = def.Backoff
		if !validBackoff[r.Backoff] {
			c.errs.add(path+".backoff", "must be exponential|fixed, got %q", r.Backoff)
		}
	}
	if def.Initial != "" {
		r.InitialSeconds = c.delay(path+".initial", def.Initial)
	}
	if def.Max != "" {
		r.MaxSeconds = c.delay(path+".max", def.Max)
	}
	if r.MaxSeconds < r.InitialSeconds {
		c.errs.add(path+".max", "must not be less than initial")
	}
	return r
}

func (c *compiler) delay(path, v string) int64 {
	d, err := time.ParseDuration(v)
	switch {
	case err != nil:
		c.errs.add(path, "%q is not a duration (e.g. 30s, 5m)", v)
	case d < 0 || d > maxRetryDelay:
		c.errs.add(path, "must be between 0s and %s", maxRetryDelay)
	case d%time.Second != 0:
		c.errs.add(path, "must be a whole number of seconds")
	}
	return int64(d / time.Second)
}

func (c *compiler) timeout(path, v string) int64 {
	d, err := time.ParseDuration(v)
	switch {
	case err != nil:
		c.errs.add(path, "%q is not a duration (e.g. 30m, 2h)", v)
	case d < minTimeout || d > maxTimeout:
		c.errs.add(path, "must be between %s and %s", minTimeout, maxTimeout)
	case d%time.Second != 0:
		c.errs.add(path, "must be a whole number of seconds")
	}
	return int64(d / time.Second)
}

func (c *compiler) resolveImages(ctx context.Context, s *Spec, r ImageResolver) {
	resolved := make(map[string]string)
	for i := range s.Tasks {
		t := &s.Tasks[i]
		if t.Runtime.Kind != RuntimeJob || isDigestRef(t.Runtime.Image) {
			continue
		}
		ref, ok := resolved[t.Runtime.Image]
		if !ok {
			var err error
			ref, err = r.Resolve(ctx, t.Runtime.Image)
			if err != nil {
				c.errs.add(fmt.Sprintf("spec.tasks[id=%s].runtime.image", t.ID), "%v", err)
				continue
			}
			resolved[t.Runtime.Image] = ref
		}
		t.Runtime.Image = ref
	}
}
