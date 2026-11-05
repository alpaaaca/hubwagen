package core

import (
	"encoding/json"
	"fmt"

	"github.com/alper/moksha/internal/state"
)

// batch collects everything one loop iteration changes: events, the objects
// whose projections must be rewritten, and what to do after commit.
type batch struct {
	events []state.Event

	catalog []func(tx *state.Tx) error // datasets, builds, data versions and lineage, in order

	runs         []*runState
	runTasks     []*runTaskState
	steps        []*stepState
	stepAttempts []*stepAttemptState
	timers       []*runTimer

	seen map[any]bool

	effects []func()
	replies []func(error)
}

func newBatch() *batch { return &batch{seen: map[any]bool{}} }

func (b *batch) empty() bool {
	return len(b.events) == 0 && len(b.catalog) == 0 &&
		len(b.runs) == 0 && len(b.runTasks) == 0 && len(b.steps) == 0 &&
		len(b.stepAttempts) == 0 && len(b.timers) == 0
}

// write is the Store.Append callback. Order follows foreign keys.
func (b *batch) write(tx *state.Tx) error {
	for _, f := range b.catalog {
		if err := f(tx); err != nil {
			return err
		}
	}
	for _, h := range b.runs {
		if err := tx.PutRun(h.rec); err != nil {
			return fmt.Errorf("run %s: %w", h.rec.ID, err)
		}
	}
	for _, t := range b.runTasks {
		if err := tx.PutRunTask(t.rec); err != nil {
			return fmt.Errorf("run task %s: %w", t.rec.ID, err)
		}
	}
	for _, l := range b.steps {
		if err := tx.PutStep(l.rec); err != nil {
			return fmt.Errorf("step %s/%d: %w", l.rec.RunID, l.rec.Seq, err)
		}
	}
	for _, a := range b.stepAttempts {
		if err := tx.PutStepAttempt(a.rec); err != nil {
			return fmt.Errorf("step attempt %s: %w", a.rec.ID, err)
		}
	}
	for _, t := range b.timers {
		if err := tx.PutTimer(t.rec); err != nil {
			return fmt.Errorf("timer %s/%d: %w", t.rec.RunID, t.rec.Seq, err)
		}
	}
	return nil
}

// dirty marks v to be written at commit, once per batch.
func dirty[T any](b *batch, list *[]*T, v *T) {
	if !b.seen[v] {
		b.seen[v] = true
		*list = append(*list, v)
	}
}

func (c *Core) effect(f func()) { c.b.effects = append(c.b.effects, f) }

// emit records an event that belongs to no run (a leader term, a build, a
// dataset) in the current batch.
func (c *Core) emit(typ state.EventType, tenant string, payload any) {
	c.b.events = append(c.b.events, state.Event{
		Time: c.now(), Tenant: tenant, Type: typ, Payload: marshalPayload(typ, payload),
	})
}

// emitRun records an event in a run's event log. attemptID is a run task or
// step attempt ID, or empty.
func (c *Core) emitRun(typ state.EventType, tenant, runID, attemptID string, payload any) {
	c.b.events = append(c.b.events, state.Event{
		Time: c.now(), Tenant: tenant, Type: typ,
		RunID: runID, AttemptID: attemptID, Payload: marshalPayload(typ, payload),
	})
}

func marshalPayload(typ state.EventType, payload any) json.RawMessage {
	if payload == nil {
		return nil
	}
	b, err := json.Marshal(payload)
	if err != nil {
		panic(fmt.Sprintf("core: marshal %s payload: %v", typ, err)) // payloads are plain structs
	}
	return b
}

func ptr[T any](v T) *T { return &v }
