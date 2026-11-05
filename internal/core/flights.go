package core

import (
	"fmt"

	"github.com/alper/moksha/internal/state"
)

// Single-flight. Two runs that need the same memoized step at the same time
// (a backfill and the daily schedule, say) would both miss the memo lookup,
// since neither has finished. So the first one to run a fingerprint owns the
// flight; a later step with the same fingerprint waits for it instead of
// running. When the owner completes (with its data version committed, if it has an
// output), every waiter reuses what it produced. When
// the owner fails or is cancelled, the first waiter runs itself, under its
// own retry policy, and the rest wait on it.
//
// Flights live in memory; a new leader rebuilds them from the steps it
// restores.

type flight struct {
	owner   *stepState
	waiters []*stepState
}

func flightKey(l *stepState) string { return l.run.rec.Tenant + "\x00" + l.rec.Fingerprint }

// runOrWait runs a step that missed the memo lookup, unless its fingerprint
// is already running.
func (c *Core) runOrWait(l *stepState) {
	if l.rec.Fingerprint == "" {
		c.startAttempt(l)
		return
	}
	if f := c.flights[flightKey(l)]; f != nil {
		c.waitOn(l, f)
		return
	}
	c.flights[flightKey(l)] = &flight{owner: l}
	c.startAttempt(l)
}

func (c *Core) waitOn(l *stepState, f *flight) {
	l.rec.Status = state.StepWaiting
	l.flight = f
	f.waiters = append(f.waiters, l)
	c.dirtyStep(l)
	c.emitH(state.EventStepWaiting, l.run, "", stepWaitingPayload{
		Seq: l.rec.Seq, Fingerprint: l.rec.Fingerprint, Run: f.owner.run.rec.ID, OnSeq: f.owner.rec.Seq})
}

// landFlight settles the flight l owns, once l has finished.
func (c *Core) landFlight(l *stepState) {
	key := flightKey(l)
	f := c.flights[key]
	if f == nil || f.owner != l {
		return
	}
	delete(c.flights, key)
	if l.rec.Status == state.StepCompleted && (l.dataVersion != "" || l.output == nil) {
		for _, w := range f.waiters {
			w.flight = nil
			c.reuseStep(w, memoMatch{dataVersion: l.dataVersion, runID: l.rec.RunID, seq: l.rec.Seq, result: l.rec.Result})
		}
		return
	}
	if len(f.waiters) == 0 {
		return
	}
	next := f.waiters[0]
	next.flight = nil
	next.rec.Status = state.StepScheduled
	next.rec.RunReason = fmt.Sprintf("the run it waited for (run %s, step %d) %s", l.rec.RunID, l.rec.Seq, l.rec.Status)
	c.dirtyStep(next)
	nf := &flight{owner: next}
	c.flights[key] = nf
	for _, w := range f.waiters[1:] {
		w.flight = nil
		c.waitOn(w, nf)
	}
	c.startAttempt(next)
}

// leaveFlight takes a waiting step out of its flight (it was cancelled).
func (c *Core) leaveFlight(l *stepState) {
	f := l.flight
	if f == nil {
		return
	}
	l.flight = nil
	for i, w := range f.waiters {
		if w == l {
			f.waiters = append(f.waiters[:i], f.waiters[i+1:]...)
			return
		}
	}
}

// reuseStep completes l with what a step with the same fingerprint produced,
// earlier or in the flight it waited on, without running anything.
func (c *Core) reuseStep(l *stepState, m memoMatch) {
	l.dataVersion, l.rec.Reused = m.dataVersion, true
	l.rec.RunReason = ""
	c.emitH(state.EventStepReused, l.run, "", stepReusedPayload{
		Seq: l.rec.Seq, DataVersion: m.dataVersion, Run: m.runID, OnSeq: m.seq, Fingerprint: l.rec.Fingerprint})
	c.finishStep(l, state.StepCompleted, m.result, "", "")
	c.wake(l.run)
}

// restoreFlights rebuilds flights from restored steps: every unfinished step
// with a fingerprint owns one (the first per fingerprint), waiting steps
// rejoin it, and a waiting step whose owner is gone runs itself.
func (c *Core) restoreFlights(steps []*stepState) {
	var waiting []*stepState
	for _, l := range steps {
		switch {
		case l.rec.Fingerprint == "":
		case l.rec.Status == state.StepWaiting:
			waiting = append(waiting, l)
		case c.flights[flightKey(l)] == nil:
			c.flights[flightKey(l)] = &flight{owner: l}
		}
	}
	for _, l := range waiting {
		if f := c.flights[flightKey(l)]; f != nil {
			l.flight = f
			f.waiters = append(f.waiters, l)
			continue
		}
		l.rec.Status = state.StepScheduled
		c.dirtyStep(l)
		c.runOrWait(l)
	}
}
