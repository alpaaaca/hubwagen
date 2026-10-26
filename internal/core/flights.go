package core

import (
	"encoding/json"
	"fmt"

	"github.com/alper/hubwagen/internal/state"
)

// Single-flight. Two hauls that need the same memoized lift at the same time
// (a backfill and the daily schedule, say) would both miss the memo lookup,
// since neither has finished. So the first one to run a fingerprint owns the
// flight; a later lift with the same fingerprint waits for it instead of
// running. When the owner commits its pallet, every waiter reuses it. When
// the owner fails or is cancelled, the first waiter runs itself, under its
// own retry policy, and the rest wait on it.
//
// Flights live in memory; a new leader rebuilds them from the lifts it
// restores.

type flight struct {
	owner   *liftState
	waiters []*liftState
}

func flightKey(l *liftState) string { return l.haul.rec.Tenant + "\x00" + l.rec.Fingerprint }

// runOrWait runs a lift that missed the memo lookup, unless its fingerprint
// is already running.
func (c *Core) runOrWait(l *liftState) {
	if l.rec.Fingerprint == "" {
		c.queueLiftAttempt(l)
		return
	}
	if f := c.flights[flightKey(l)]; f != nil {
		c.waitOn(l, f)
		return
	}
	c.flights[flightKey(l)] = &flight{owner: l}
	c.queueLiftAttempt(l)
}

func (c *Core) waitOn(l *liftState, f *flight) {
	l.rec.Status = state.LiftWaiting
	l.flight = f
	f.waiters = append(f.waiters, l)
	c.dirtyLift(l)
	c.emitH(state.EventLiftWaiting, l.haul, "", liftWaitingPayload{
		Seq: l.rec.Seq, Fingerprint: l.rec.Fingerprint, Haul: f.owner.haul.rec.ID, OnSeq: f.owner.rec.Seq})
}

// landFlight settles the flight l owns, once l has finished.
func (c *Core) landFlight(l *liftState) {
	key := flightKey(l)
	f := c.flights[key]
	if f == nil || f.owner != l {
		return
	}
	delete(c.flights, key)
	if l.rec.Status == state.LiftCompleted && l.pallet != "" {
		for _, w := range f.waiters {
			w.flight = nil
			c.reuseLift(w, l.pallet, l.rec.Result)
		}
		return
	}
	if len(f.waiters) == 0 {
		return
	}
	next := f.waiters[0]
	next.flight = nil
	next.rec.Status = state.LiftScheduled
	next.rec.RunReason = fmt.Sprintf("the run it waited for (haul %s, lift %d) %s", l.rec.HaulID, l.rec.Seq, l.rec.Status)
	c.dirtyLift(next)
	nf := &flight{owner: next}
	c.flights[key] = nf
	for _, w := range f.waiters[1:] {
		w.flight = nil
		c.waitOn(w, nf)
	}
	c.queueLiftAttempt(next)
}

// leaveFlight takes a waiting lift out of its flight (it was cancelled).
func (c *Core) leaveFlight(l *liftState) {
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

// reuseLift completes l with a pallet built by the same fingerprint, earlier
// or by the flight it waited on, without running anything.
func (c *Core) reuseLift(l *liftState, palletID string, result json.RawMessage) {
	l.pallet, l.reused = palletID, true
	l.rec.RunReason = ""
	c.emitH(state.EventLiftReused, l.haul, "", liftReusedPayload{Seq: l.rec.Seq, Pallet: palletID, Fingerprint: l.rec.Fingerprint})
	c.finishLift(l, state.LiftCompleted, result, "", "")
	c.wake(l.haul)
}

// restoreFlights rebuilds flights from restored lifts: every unfinished lift
// with a fingerprint owns one (the first per fingerprint), waiting lifts
// rejoin it, and a waiting lift whose owner is gone runs itself.
func (c *Core) restoreFlights(lifts []*liftState) {
	var waiting []*liftState
	for _, l := range lifts {
		switch {
		case l.rec.Fingerprint == "":
		case l.rec.Status == state.LiftWaiting:
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
		l.rec.Status = state.LiftScheduled
		c.dirtyLift(l)
		c.runOrWait(l)
	}
}
