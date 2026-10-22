package core

import (
	"encoding/json"
	"fmt"

	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
)

type pipelineKey struct{ tenant, name string }

func (k pipelineKey) String() string { return k.tenant + "/" + k.name }

type pipelineState struct {
	rec     state.Pipeline
	running int         // runs in status running
	queued  []*runState // runs waiting for a slot, oldest first
	cron    cronState
}

type runState struct {
	rec   state.Run
	ver   *version
	tasks map[string]*taskState
	order []string // task IDs in the spec's topological order

	cancelRequested bool // explicit cancel (API, on_overlap: cancel_running)
	stopping        bool // a task failed with on_failure: stop
}

func (r *runState) halting() bool { return r.cancelRequested || r.stopping }

type taskState struct {
	rec     state.TaskRun
	def     *spec.Task
	run     *runState
	current *attemptState     // the non-terminal attempt, if any
	retry   *timerRef         // pending retry, while in retry_wait
	outputs map[string]string // committed output name → URI
}

type attemptState struct {
	rec        state.Attempt
	task       *taskState
	timeout    *timerRef
	cancelSent bool
}

// batch collects everything one loop iteration changes: events, the objects
// whose projections must be rewritten, and what to do after commit.
type batch struct {
	events []state.Event

	catalog  []func(tx *state.Tx) error // tenants, versions, pipelines, cursors, in order
	runs     []*runState
	tasks    []*taskState
	attempts []*attemptState
	outputs  []state.Output
	seen     map[any]bool

	effects []func()
	replies []func(error)
}

func newBatch() *batch { return &batch{seen: map[any]bool{}} }

func (b *batch) empty() bool {
	return len(b.events) == 0 && len(b.catalog) == 0 && len(b.runs) == 0 &&
		len(b.tasks) == 0 && len(b.attempts) == 0 && len(b.outputs) == 0
}

// write is the Store.Append callback. Order follows foreign keys.
func (b *batch) write(tx *state.Tx) error {
	for _, f := range b.catalog {
		if err := f(tx); err != nil {
			return err
		}
	}
	for _, r := range b.runs {
		if err := tx.PutRun(r.rec); err != nil {
			return fmt.Errorf("run %s: %w", r.rec.ID, err)
		}
	}
	for _, t := range b.tasks {
		if err := tx.PutTaskRun(t.rec); err != nil {
			return fmt.Errorf("task %s/%s: %w", t.rec.RunID, t.rec.TaskID, err)
		}
	}
	for _, a := range b.attempts {
		if err := tx.PutAttempt(a.rec); err != nil {
			return fmt.Errorf("attempt %s: %w", a.rec.ID, err)
		}
	}
	for _, o := range b.outputs {
		if err := tx.PutOutput(o); err != nil {
			return fmt.Errorf("output %s/%s/%s: %w", o.RunID, o.TaskID, o.Name, err)
		}
	}
	return nil
}

func (c *Core) dirtyRun(r *runState) {
	if !c.b.seen[r] {
		c.b.seen[r] = true
		c.b.runs = append(c.b.runs, r)
	}
}

func (c *Core) dirtyTask(t *taskState) {
	if !c.b.seen[t] {
		c.b.seen[t] = true
		c.b.tasks = append(c.b.tasks, t)
	}
}

func (c *Core) dirtyAttempt(a *attemptState) {
	if !c.b.seen[a] {
		c.b.seen[a] = true
		c.b.attempts = append(c.b.attempts, a)
	}
}

func (c *Core) effect(f func()) { c.b.effects = append(c.b.effects, f) }

// emit records an event in the current batch.
func (c *Core) emit(typ state.EventType, tenant, runID, taskID, attemptID string, payload any) {
	var raw json.RawMessage
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			panic(fmt.Sprintf("core: marshal %s payload: %v", typ, err)) // payloads are plain structs
		}
		raw = b
	}
	c.b.events = append(c.b.events, state.Event{
		Time: c.now(), Tenant: tenant, Type: typ,
		RunID: runID, TaskID: taskID, AttemptID: attemptID, Payload: raw,
	})
}

func (c *Core) emitRun(typ state.EventType, r *runState, payload any) {
	c.emit(typ, r.rec.Tenant, r.rec.ID, "", "", payload)
}

func (c *Core) emitTask(typ state.EventType, t *taskState, payload any) {
	c.emit(typ, t.run.rec.Tenant, t.run.rec.ID, t.rec.TaskID, "", payload)
}

func (c *Core) emitAttempt(typ state.EventType, a *attemptState, payload any) {
	c.emit(typ, a.task.run.rec.Tenant, a.rec.RunID, a.rec.TaskID, a.rec.ID, payload)
}

func ptr[T any](v T) *T { return &v }
