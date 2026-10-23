package core

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
)

// --- publish ---

type PublishRequest struct {
	SpecJSON    []byte // canonical bytes from `hubwagen compile`
	PublishedBy string
}

type PublishResult struct {
	Tenant    string
	Pipeline  string
	Hash      string
	Unchanged bool // this version was already current
}

// Publish registers a compiled spec and makes it the pipeline's current
// version. Runs already started keep the version they started with.
func (c *Core) Publish(ctx context.Context, req PublishRequest) (PublishResult, error) {
	return submit(ctx, c, func(c *Core) (PublishResult, error) { return c.publish(req) })
}

func (c *Core) publish(req PublishRequest) (PublishResult, error) {
	s, err := spec.Decode(req.SpecJSON)
	if err != nil {
		return PublishResult{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	canon, err := s.Canonical()
	if err != nil {
		return PublishResult{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if !bytes.Equal(canon, req.SpecJSON) {
		return PublishResult{}, fmt.Errorf("%w: spec is not in canonical form (publish the output of `hubwagen compile`)", ErrInvalid)
	}
	hash := spec.HashBytes(req.SpecJSON)
	res := PublishResult{Tenant: s.Tenant, Pipeline: s.Name, Hash: hash}

	if _, ok := c.tenants[s.Tenant]; !ok {
		if !c.cfg.AutoCreateTenants {
			return res, fmt.Errorf("%w: tenant %q", ErrNotFound, s.Tenant)
		}
		c.upsertTenant(state.Tenant{Name: s.Tenant, Weight: 1, CreatedAt: c.now()})
	}

	key := pipelineKey{s.Tenant, s.Name}
	p := c.pipelines[key]
	if p != nil && p.rec.CurrentVersion == hash {
		res.Unchanged = true
		return res, nil
	}

	ver := c.versions[hash]
	if ver == nil {
		rec := state.PipelineVersion{Hash: hash, Tenant: s.Tenant, Pipeline: s.Name, SpecJSON: req.SpecJSON,
			PublishedAt: c.now(), PublishedBy: req.PublishedBy}
		if ver, err = newVersion(rec); err != nil {
			return res, fmt.Errorf("%w: %v", ErrInvalid, err)
		}
		c.versions[hash] = ver
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutVersion(rec) })
	}

	if p == nil {
		p = &pipelineState{rec: state.Pipeline{Tenant: s.Tenant, Name: s.Name}}
		c.pipelines[key] = p
	}
	p.rec.CurrentVersion = hash
	rec := p.rec
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutPipeline(rec) })
	c.emit(state.EventPipelineVersionPublished, s.Tenant, "", "", "",
		versionPublishedPayload{Pipeline: s.Name, Hash: hash, PublishedBy: req.PublishedBy})

	// A higher max_active_runs may let queued runs start; the schedule may
	// have changed.
	c.startQueued(p)
	c.reschedule(p)
	return res, nil
}

func (c *Core) upsertTenant(t state.Tenant) {
	c.tenants[t.Name] = &t
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutTenant(t) })
	c.emit(state.EventTenantUpserted, t.Name, "", "", "", tenantUpsertedPayload{Weight: t.Weight, MaxRunningTasks: t.MaxRunningTasks})
}

// --- trigger ---

type TriggerRequest struct {
	Tenant   string
	Pipeline string
	// Interval is the run's data interval; nil for an ad-hoc run.
	Interval *spec.Interval
	Params   map[string]any
	// IdempotencyKey identifies a manual run; triggering again with the same
	// key returns the existing run. Scheduled runs are keyed by interval.
	IdempotencyKey string
	Trigger        string // manual (default), cron, backfill, asset, api
	Priority       string // default from Trigger
	// Rerun creates a new generation for a run key whose latest run has
	// finished, instead of returning that run.
	Rerun bool
	By    string
}

type TriggerResult struct {
	RunID      string
	RunKey     string
	Generation int
	Existing   bool   // an existing run was returned
	Skipped    bool   // on_overlap: skip declined to start a run
	Reason     string // why, when Skipped
}

func (c *Core) Trigger(ctx context.Context, req TriggerRequest) (TriggerResult, error) {
	return submit(ctx, c, func(c *Core) (TriggerResult, error) { return c.trigger(ctx, req) })
}

func (c *Core) trigger(ctx context.Context, req TriggerRequest) (TriggerResult, error) {
	key := pipelineKey{req.Tenant, req.Pipeline}
	p := c.pipelines[key]
	if p == nil {
		return TriggerResult{}, fmt.Errorf("%w: pipeline %s", ErrNotFound, key)
	}
	ver := c.versions[p.rec.CurrentVersion]

	params, err := resolveParams(ver.spec.Params, req.Params)
	if err != nil {
		return TriggerResult{}, fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if req.Interval != nil && !req.Interval.Start.Before(req.Interval.End) {
		return TriggerResult{}, fmt.Errorf("%w: interval start must be before end", ErrInvalid)
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	if req.Priority == "" {
		req.Priority = map[string]string{"manual": "interactive", "api": "interactive", "backfill": "backfill"}[req.Trigger]
		if req.Priority == "" {
			req.Priority = "scheduled"
		}
	}

	id := c.cfg.NewID()
	runKey := makeRunKey(key, req, id)
	generation := 1
	latest, err := c.latestRun(ctx, runKey)
	if err != nil {
		return TriggerResult{}, err
	}
	if latest != nil {
		existing := TriggerResult{RunID: latest.ID, RunKey: runKey, Generation: latest.Generation, Existing: true}
		if !req.Rerun || !state.RunTerminal(latest.Status) {
			return existing, nil
		}
		generation = latest.Generation + 1
	}

	if active := p.running + len(p.queued); active >= ver.spec.Concurrency.MaxActiveRuns {
		switch ver.spec.Concurrency.OnOverlap {
		case "skip":
			return TriggerResult{Skipped: true, Reason: fmt.Sprintf("%s already has %d active run(s) (on_overlap: skip)", key, active)}, nil
		case "cancel_running":
			for _, r := range c.activeRuns(key) {
				c.requestCancel(r, "superseded by a newer run (on_overlap: cancel_running)", req.By)
			}
		}
	}

	r := &runState{ver: ver, tasks: map[string]*taskState{}, rec: state.Run{
		ID: id, Tenant: req.Tenant, Pipeline: req.Pipeline, Version: ver.hash, RunKey: runKey,
		Generation: generation, Trigger: req.Trigger, Priority: req.Priority, Params: params,
		Status: state.RunQueued, CreatedAt: c.now(),
	}}
	if req.Interval != nil {
		r.rec.IntervalStart = ptr(req.Interval.Start.UTC())
		r.rec.IntervalEnd = ptr(req.Interval.End.UTC())
	}
	for i := range ver.spec.Tasks {
		def := &ver.spec.Tasks[i]
		t := &taskState{def: def, run: r, rec: state.TaskRun{RunID: id, TaskID: def.ID, Status: state.TaskPending}}
		r.tasks[def.ID] = t
		r.order = append(r.order, def.ID)
		c.dirtyTask(t)
	}
	c.runs[id] = r
	c.runsByKey[runKey] = r
	c.dirtyRun(r)
	c.emitRun(state.EventRunCreated, r, runCreatedPayload{
		Pipeline: r.rec.Pipeline, Version: r.rec.Version, RunKey: runKey, Generation: generation,
		Trigger: r.rec.Trigger, Priority: r.rec.Priority, IntervalStart: r.rec.IntervalStart,
		IntervalEnd: r.rec.IntervalEnd, Params: params,
	})

	p.queued = append(p.queued, r)
	c.startQueued(p)
	return TriggerResult{RunID: id, RunKey: runKey, Generation: generation}, nil
}

// makeRunKey: scheduled/backfill runs are keyed by interval, manual runs by
// the caller's idempotency key, and ad-hoc runs without either are unique.
func makeRunKey(p pipelineKey, req TriggerRequest, runID string) string {
	prefix := p.tenant + "/" + p.name + "/"
	switch {
	case req.IdempotencyKey != "":
		return prefix + "key:" + req.IdempotencyKey
	case req.Interval != nil:
		const f = "20060102T150405Z"
		return prefix + req.Interval.Start.UTC().Format(f) + "_" + req.Interval.End.UTC().Format(f)
	default:
		return prefix + "run:" + runID
	}
}

type runRef struct {
	ID         string
	Generation int
	Status     string
}

// latestRun finds the newest run for a key: in memory first (active runs and
// runs finished in this batch), then in the store.
func (c *Core) latestRun(ctx context.Context, runKey string) (*runRef, error) {
	if r := c.runsByKey[runKey]; r != nil {
		return &runRef{r.rec.ID, r.rec.Generation, r.rec.Status}, nil
	}
	rec, err := c.cfg.Store.LatestRunByKey(ctx, runKey)
	if errors.Is(err, state.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &runRef{rec.ID, rec.Generation, rec.Status}, nil
}

// activeRuns returns a pipeline's running and queued runs, oldest first.
func (c *Core) activeRuns(key pipelineKey) []*runState {
	var out []*runState
	for _, r := range c.runs {
		if r.rec.Tenant == key.tenant && r.rec.Pipeline == key.name && !state.RunTerminal(r.rec.Status) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].rec.ID < out[j].rec.ID })
	return out
}

// resolveParams checks given params against the declarations, applies
// defaults, and normalises values to string, int64 or bool.
func resolveParams(decl []spec.Param, given map[string]any) (map[string]any, error) {
	known := map[string]spec.Param{}
	for _, p := range decl {
		known[p.Name] = p
	}
	var unknown []string
	for name := range given {
		if _, ok := known[name]; !ok {
			unknown = append(unknown, name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nil, fmt.Errorf("unknown param(s): %s", strings.Join(unknown, ", "))
	}

	out := map[string]any{}
	for _, p := range decl {
		v, ok := given[p.Name]
		if !ok {
			if p.Default == nil {
				return nil, fmt.Errorf("param %s is required", p.Name)
			}
			v = p.Default
		}
		nv, err := normalizeParam(p.Type, v)
		if err != nil {
			return nil, fmt.Errorf("param %s: %v", p.Name, err)
		}
		out[p.Name] = nv
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func normalizeParam(typ string, v any) (any, error) {
	switch typ {
	case "string":
		if s, ok := v.(string); ok {
			return s, nil
		}
	case "bool":
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case "int":
		switch n := v.(type) {
		case int:
			return int64(n), nil
		case int64:
			return n, nil
		case float64:
			if n == math.Trunc(n) && math.Abs(n) < 1<<53 {
				return int64(n), nil
			}
		case json.Number:
			if i, err := n.Int64(); err == nil {
				return i, nil
			}
		}
	}
	return nil, fmt.Errorf("%v (%T) is not a valid %s", v, v, typ)
}

// --- cancel ---

// CancelRun cancels a queued or running run. Cancelling a finished run is a
// no-op.
func (c *Core) CancelRun(ctx context.Context, runID, by string) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		if r := c.runs[runID]; r != nil {
			c.requestCancel(r, "cancelled by request", by)
			return struct{}{}, nil
		}
		if _, err := c.cfg.Store.GetRun(ctx, runID); errors.Is(err, state.ErrNotFound) {
			return struct{}{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		} else if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

// --- for tests and callers that need the time used for decisions ---

// Now is the core's clock.
func (c *Core) Now() time.Time { return c.now() }
