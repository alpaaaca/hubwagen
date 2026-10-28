package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// What a new leader does with the hauls the previous one left open.
// Workers are separate processes that outlive a leader, so:
//
//   - queued haul tasks and lifts go back on their docks;
//   - a dispatched haul task is lost and replaced by a new one: replays are
//     cheap, and a worker that died with the leader would leave the haul
//     hanging;
//   - a dispatched lift attempt stays valid, so its worker can report to the
//     new leader and the lift doesn't run twice. Its timeout gets what is
//     left; finding attempts whose worker died needs heartbeats (#22);
//   - retry waits and timers get what is left of their delay, firing at once
//     if it already passed;
//   - the commands recorded for each haul are rebuilt from its logbook, so
//     replays are still checked against it;
//   - a lift whose pallet was recorded but not promoted gets the promote
//     again, from the manifest in the logbook. Promotes are idempotent;
//   - version markers are rebuilt from the logbook like the commands;
//   - signal waits and signals no wait took yet are rebuilt from the
//     logbook; a wait's timeout is a timer like any other;
//   - single-flight is rebuilt: unfinished lifts with a fingerprint own their
//     flights again, waiting lifts rejoin them.

// restoredHauls is what loadHauls read for settleHauls.
type restoredHauls struct {
	*state.ActiveHauls
	// pending pallets of committing lifts, by "haul/seq", with their
	// manifests from the logbook.
	pending map[string]*promotion
}

// loadHauls rebuilds open hauls, their recorded commands and the known
// datasets. It reads the store and changes nothing; settleHauls finishes the
// job inside a batch.
func (c *Core) loadHauls(ctx context.Context) (*restoredHauls, error) {
	builds, err := c.cfg.Store.LoadBuilds(ctx)
	if err != nil {
		return nil, err
	}
	for id, b := range builds {
		c.addBuild(id, b.Lifts, b.Hauls)
	}
	dss, err := c.cfg.Store.ListDatasets(ctx, "")
	if err != nil {
		return nil, err
	}
	for _, d := range dss {
		c.datasets[datasetKey(d.Tenant, d.Name)] = d.PartitionKey
	}
	act, err := c.cfg.Store.LoadHauls(ctx)
	if err != nil {
		return nil, err
	}
	out := &restoredHauls{ActiveHauls: act, pending: map[string]*promotion{}}
	for _, rec := range act.Hauls {
		h := &haulState{rec: rec, lifts: map[int]*liftState{}, timers: map[int]*haulTimer{}, waits: map[int]*signalWait{}, markers: map[string]int{}}
		evs, err := c.cfg.Store.HaulEvents(ctx, rec.ID)
		if err != nil {
			return nil, err
		}
		manifests := map[string]palletRecordedPayload{}
		for _, e := range evs {
			switch e.Type {
			case state.EventPalletRecorded:
				var p palletRecordedPayload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					return nil, fmt.Errorf("haul %s: event %d: %w", rec.ID, e.Seq, err)
				}
				manifests[p.Manifest.Pallet] = p
			case state.EventHaulTaskQueued:
				h.tasks++
			case state.EventHaulTaskCompleted:
				var p haulTaskCompletedPayload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					return nil, fmt.Errorf("haul %s: event %d: %w", rec.ID, e.Seq, err)
				}
				h.cmds = append(h.cmds, p.Commands...)
				for _, m := range p.Markers {
					h.markers[m.Name] = m.Version
				}
			}
		}
		if len(h.cmds) != rec.Commands {
			return nil, fmt.Errorf("haul %s: logbook has %d commands, the haul record %d", rec.ID, len(h.cmds), rec.Commands)
		}
		if err := restoreSignals(h, evs); err != nil {
			return nil, err
		}
		pending, err := c.cfg.Store.ListPallets(ctx, state.PalletFilter{HaulID: rec.ID, States: []string{state.PalletPending}, Limit: 1 << 20})
		if err != nil {
			return nil, err
		}
		for _, p := range pending {
			rp, ok := manifests[p.ID]
			if !ok {
				return nil, fmt.Errorf("haul %s: pending pallet %s has no PalletRecorded event", rec.ID, p.ID)
			}
			out.pending[fmt.Sprintf("%s/%d", rec.ID, p.LiftSeq)] = &promotion{rec: p, manifest: rp.Manifest}
		}
		c.hauls[rec.ID] = h
	}
	return out, nil
}

// settleHauls re-queues, re-arms and replaces what loadHauls found, in the
// restore batch.
func (c *Core) settleHauls(act *restoredHauls) error {
	var needTask []*haulState
	for _, rec := range act.HaulTasks {
		h := c.hauls[rec.HaulID]
		t := &haulTaskState{rec: rec, haul: h}
		if rec.Status == state.HaulTaskQueued {
			h.task = t
			c.haulTasks[rec.ID] = t
			c.enqueueHaulTask(t)
			continue
		}
		t.rec.Status, t.rec.Reason = state.HaulTaskLost, "leader_restart"
		t.rec.Message = fmt.Sprintf("dispatched when term %d ended", rec.Term)
		t.rec.FinishedAt = ptr(c.now())
		c.dirtyHaulTask(t)
		c.emitH(state.EventHaulTaskFailed, h, t.rec.ID,
			haulTaskFailedPayload{Number: rec.Number, Reason: t.rec.Reason, Message: t.rec.Message})
		needTask = append(needTask, h)
	}

	attempts := map[string]state.LiftAttempt{} // the unfinished attempt per lift
	for _, a := range act.LiftAttempts {
		if !state.AttemptTerminal(a.Status) {
			attempts[fmt.Sprintf("%s/%d", a.HaulID, a.Seq)] = a
		}
	}
	var restored []*liftState
	for _, rec := range act.Lifts {
		h := c.hauls[rec.HaulID]
		l := &liftState{rec: rec, haul: h}
		for _, f := range []struct {
			raw  json.RawMessage
			into any
			what string
		}{{rec.Retry, &l.retry, "retry policy"}, {rec.Output, &l.output, "output"}, {rec.Inputs, &l.inputs, "inputs"}} {
			if len(f.raw) > 0 {
				if err := json.Unmarshal(f.raw, f.into); err != nil {
					return fmt.Errorf("lift %s/%d: %s: %w", rec.HaulID, rec.Seq, f.what, err)
				}
			}
		}
		h.lifts[rec.Seq] = l
		restored = append(restored, l)
		switch rec.Status {
		case state.LiftWaiting: // rejoins its flight below
		case state.LiftCommitting:
			p := act.pending[fmt.Sprintf("%s/%d", rec.HaulID, rec.Seq)]
			if p == nil {
				return fmt.Errorf("lift %s/%d is committing but has no pending pallet", rec.HaulID, rec.Seq)
			}
			p.lift = l
			c.promote(p)
		case state.LiftRetryWait:
			c.armLiftRetry(l, c.timeUntil(rec.RetryAt))
		case state.LiftDispatched, state.LiftRunning:
			ar, ok := attempts[fmt.Sprintf("%s/%d", rec.HaulID, rec.Seq)]
			if !ok {
				c.queueLiftAttempt(l) // no attempt on record: start a new one
				continue
			}
			a := &liftAttemptState{rec: ar, lift: l}
			l.attempt = a
			c.liftAttempts[ar.ID] = a
			c.dock(rec.Dock).runningLifts++
			if rec.Timeout > 0 {
				c.armLiftTimeout(a, max(ar.DispatchedAt.Add(rec.Timeout).Sub(c.now()), 0))
			}
			c.armLiftLease(a) // a fresh lease: heartbeats weren't persisted
		default: // scheduled: the queued attempt was never recorded
			c.queueLiftAttempt(l)
		}
	}
	c.restoreFlights(restored)

	for _, rec := range act.Timers {
		h := c.hauls[rec.HaulID]
		tm := &haulTimer{rec: rec, haul: h}
		h.timers[rec.Seq] = tm
		tm.ref = c.after(max(rec.FireAt.Sub(c.now()), 0), func(c *Core) { c.fireTimer(tm) })
	}

	for _, h := range needTask {
		if h.task == nil {
			c.queueHaulTask(h) // even if stuck: its replay was asked for (ResumeHaul)
		}
	}
	return nil
}

// timeUntil is how long until t, or 0 if it passed (or is unset).
func (c *Core) timeUntil(t *time.Time) time.Duration {
	if t == nil {
		return 0
	}
	return max(t.Sub(c.now()), 0)
}
