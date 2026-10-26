package core

import (
	"encoding/json"
	"fmt"

	"github.com/alper/hubwagen/internal/state"
)

// batch collects everything one loop iteration changes: events, the objects
// whose projections must be rewritten, and what to do after commit.
type batch struct {
	events []state.Event

	catalog []func(tx *state.Tx) error // datasets, builds, pallets and lineage, in order

	hauls        []*haulState
	haulTasks    []*haulTaskState
	lifts        []*liftState
	liftAttempts []*liftAttemptState
	timers       []*haulTimer

	seen map[any]bool

	effects []func()
	replies []func(error)
}

func newBatch() *batch { return &batch{seen: map[any]bool{}} }

func (b *batch) empty() bool {
	return len(b.events) == 0 && len(b.catalog) == 0 &&
		len(b.hauls) == 0 && len(b.haulTasks) == 0 && len(b.lifts) == 0 &&
		len(b.liftAttempts) == 0 && len(b.timers) == 0
}

// write is the Store.Append callback. Order follows foreign keys.
func (b *batch) write(tx *state.Tx) error {
	for _, f := range b.catalog {
		if err := f(tx); err != nil {
			return err
		}
	}
	for _, h := range b.hauls {
		if err := tx.PutHaul(h.rec); err != nil {
			return fmt.Errorf("haul %s: %w", h.rec.ID, err)
		}
	}
	for _, t := range b.haulTasks {
		if err := tx.PutHaulTask(t.rec); err != nil {
			return fmt.Errorf("haul task %s: %w", t.rec.ID, err)
		}
	}
	for _, l := range b.lifts {
		if err := tx.PutLift(l.rec); err != nil {
			return fmt.Errorf("lift %s/%d: %w", l.rec.HaulID, l.rec.Seq, err)
		}
	}
	for _, a := range b.liftAttempts {
		if err := tx.PutLiftAttempt(a.rec); err != nil {
			return fmt.Errorf("lift attempt %s: %w", a.rec.ID, err)
		}
	}
	for _, t := range b.timers {
		if err := tx.PutTimer(t.rec); err != nil {
			return fmt.Errorf("timer %s/%d: %w", t.rec.HaulID, t.rec.Seq, err)
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

// emit records an event that belongs to no haul (a leader term, a build, a
// dataset) in the current batch.
func (c *Core) emit(typ state.EventType, tenant string, payload any) {
	c.b.events = append(c.b.events, state.Event{
		Time: c.now(), Tenant: tenant, Type: typ, Payload: marshalPayload(typ, payload),
	})
}

// emitHaul records an event in a haul's logbook. attemptID is a haul task or
// lift attempt ID, or empty.
func (c *Core) emitHaul(typ state.EventType, tenant, haulID, attemptID string, payload any) {
	c.b.events = append(c.b.events, state.Event{
		Time: c.now(), Tenant: tenant, Type: typ,
		HaulID: haulID, AttemptID: attemptID, Payload: marshalPayload(typ, payload),
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
