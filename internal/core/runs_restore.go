package core

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alper/moksha/internal/state"
)

// What a new leader does with the runs the previous one left open.
// Workers are separate processes that outlive a leader, so:
//
//   - queued run tasks and steps go back on their queues;
//   - a dispatched run task is lost and replaced by a new one: replays are
//     cheap, and a worker that died with the leader would leave the run
//     hanging;
//   - a dispatched step attempt stays valid, so its worker can report to the
//     new leader and the step doesn't run twice. Its timeout gets what is
//     left; finding attempts whose worker died needs heartbeats (#22);
//   - retry waits and timers get what is left of their delay, firing at once
//     if it already passed;
//   - the commands recorded for each run are rebuilt from its event log, so
//     replays are still checked against it;
//   - a step whose data version was recorded but not promoted gets the promote
//     again, from the manifest in the event log. Promotes are idempotent;
//   - version markers are rebuilt from the event log like the commands;
//   - signal waits and signals no wait took yet are rebuilt from the
//     event log; a wait's timeout is a timer like any other;
//   - single-flight is rebuilt: unfinished steps with a fingerprint own their
//     flights again, waiting steps rejoin them.

// restoredRuns is what loadRuns read for settleRuns.
type restoredRuns struct {
	*state.ActiveRuns
	// pending data versions of committing steps, by "run/seq", with their
	// manifests from the event log.
	pending map[string]*promotion
}

// loadRuns rebuilds open runs, their recorded commands and the known
// datasets. It reads the store and changes nothing; settleRuns finishes the
// job inside a batch.
func (c *Core) loadRuns(ctx context.Context) (*restoredRuns, error) {
	builds, err := c.cfg.Store.LoadBuilds(ctx)
	if err != nil {
		return nil, err
	}
	for id, b := range builds {
		c.addBuild(id, b.Steps, b.Pipelines)
	}
	dss, err := c.cfg.Store.ListDatasets(ctx, "")
	if err != nil {
		return nil, err
	}
	for _, d := range dss {
		c.datasets[datasetKey(d.Tenant, d.Name)] = d.PartitionKey
	}
	act, err := c.cfg.Store.LoadRuns(ctx)
	if err != nil {
		return nil, err
	}
	out := &restoredRuns{ActiveRuns: act, pending: map[string]*promotion{}}
	for _, rec := range act.Runs {
		h := &runState{rec: rec, steps: map[int]*stepState{}, timers: map[int]*runTimer{}, waits: map[int]*signalWait{}, markers: map[string]int{}, slots: map[string]int{}}
		evs, err := c.cfg.Store.RunEvents(ctx, rec.ID)
		if err != nil {
			return nil, err
		}
		manifests := map[string]dataVersionRecordedPayload{}
		for _, e := range evs {
			switch e.Type {
			case state.EventDataVersionRecorded:
				var p dataVersionRecordedPayload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					return nil, fmt.Errorf("run %s: event %d: %w", rec.ID, e.Seq, err)
				}
				manifests[p.Manifest.DataVersion] = p
			case state.EventRunTaskQueued:
				h.tasks++
			case state.EventRunTaskCompleted:
				var p runTaskCompletedPayload
				if err := json.Unmarshal(e.Payload, &p); err != nil {
					return nil, fmt.Errorf("run %s: event %d: %w", rec.ID, e.Seq, err)
				}
				h.cmds = append(h.cmds, p.Commands...)
				for _, m := range p.Markers {
					h.markers[m.Name] = m.Version
				}
			}
		}
		if len(h.cmds) != rec.Commands {
			return nil, fmt.Errorf("run %s: event log has %d commands, the run record %d", rec.ID, len(h.cmds), rec.Commands)
		}
		if err := restoreSignals(h, evs); err != nil {
			return nil, err
		}
		// DataVersions whose promote hadn't finished: pending ones, and ones
		// invalidated meanwhile (never committed). A failed promote's data version
		// is invalid too, but its step isn't committing any more. Newest
		// first: a step's current data version comes before an older one's.
		pending, err := c.cfg.Store.ListDataVersions(ctx, state.DataVersionFilter{RunID: rec.ID,
			States: []string{state.DataVersionPending, state.DataVersionInvalid}, Limit: 1 << 20})
		if err != nil {
			return nil, err
		}
		for _, p := range pending {
			key := fmt.Sprintf("%s/%d", rec.ID, p.StepSeq)
			pr := &promotion{rec: p}
			if p.State == state.DataVersionInvalid {
				if p.CommittedAt != nil || out.pending[key] != nil {
					continue
				}
				pr.rec.State, pr.invalidated = state.DataVersionPending, p.Reason
			}
			rp, ok := manifests[p.ID]
			if !ok {
				return nil, fmt.Errorf("run %s: pending data version %s has no DataVersionRecorded event", rec.ID, p.ID)
			}
			pr.manifest = rp.Manifest
			if out.pending[key] == nil {
				out.pending[key] = pr
			}
		}
		c.runs[rec.ID] = h
	}
	return out, nil
}

// settleRuns re-queues, re-arms and replaces what loadRuns found, in the
// restore batch.
func (c *Core) settleRuns(act *restoredRuns) error {
	var needTask []*runState
	for _, rec := range act.RunTasks {
		h := c.runs[rec.RunID]
		t := &runTaskState{rec: rec, run: h}
		if rec.Status == state.RunTaskQueued {
			h.task = t
			c.runTasks[rec.ID] = t
			c.enqueueRunTask(t)
			continue
		}
		t.rec.Status, t.rec.Reason = state.RunTaskLost, "leader_restart"
		t.rec.Message = fmt.Sprintf("dispatched when term %d ended", rec.Term)
		t.rec.FinishedAt = ptr(c.now())
		c.dirtyRunTask(t)
		c.emitH(state.EventRunTaskFailed, h, t.rec.ID,
			runTaskFailedPayload{Number: rec.Number, Reason: t.rec.Reason, Message: t.rec.Message})
		needTask = append(needTask, h)
	}

	attempts := map[string]state.StepAttempt{} // the unfinished attempt per step
	for _, a := range act.StepAttempts {
		if !state.AttemptTerminal(a.Status) {
			attempts[fmt.Sprintf("%s/%d", a.RunID, a.Seq)] = a
		}
	}
	var restored, toStart []*stepState
	for _, rec := range act.Steps {
		h := c.runs[rec.RunID]
		l := &stepState{rec: rec, run: h}
		for _, f := range []struct {
			raw  json.RawMessage
			into any
			what string
		}{{rec.Retry, &l.retry, "retry policy"}, {rec.Output, &l.output, "output"}, {rec.Inputs, &l.inputs, "inputs"}} {
			if len(f.raw) > 0 {
				if err := json.Unmarshal(f.raw, f.into); err != nil {
					return fmt.Errorf("step %s/%d: %s: %w", rec.RunID, rec.Seq, f.what, err)
				}
			}
		}
		if rec.Seq >= 1 && rec.Seq <= len(h.cmds) {
			l.limits = h.cmds[rec.Seq-1].Limits
		}
		h.steps[rec.Seq] = l
		restored = append(restored, l)
		switch rec.Status {
		case state.StepWaiting: // rejoins its flight below
		case state.StepCommitting:
			p := act.pending[fmt.Sprintf("%s/%d", rec.RunID, rec.Seq)]
			if p == nil {
				return fmt.Errorf("step %s/%d is committing but has no pending data version", rec.RunID, rec.Seq)
			}
			p.step = l
			c.promote(p)
		case state.StepRetryWait:
			c.armStepRetry(l, c.timeUntil(rec.RetryAt))
		case state.StepDispatched, state.StepRunning:
			ar, ok := attempts[fmt.Sprintf("%s/%d", rec.RunID, rec.Seq)]
			if !ok {
				toStart = append(toStart, l) // no attempt on record: start a new one
				continue
			}
			c.takeSlotBack(l)
			a := &stepAttemptState{rec: ar, step: l}
			l.attempt = a
			c.stepAttempts[ar.ID] = a
			c.queue(rec.Queue).runningSteps++
			if rec.Timeout > 0 {
				c.armStepTimeout(a, max(ar.DispatchedAt.Add(rec.Timeout).Sub(c.now()), 0))
			}
			c.armStepLease(a) // a fresh lease: heartbeats weren't persisted
		default: // scheduled: the queued attempt was never recorded
			toStart = append(toStart, l)
		}
	}
	// Dispatched attempts took their slots back above; the rest start in
	// command order, or wait for a slot.
	for _, l := range toStart {
		c.startAttempt(l)
	}
	c.restoreFlights(restored)

	for _, rec := range act.Timers {
		h := c.runs[rec.RunID]
		tm := &runTimer{rec: rec, run: h}
		h.timers[rec.Seq] = tm
		tm.ref = c.after(max(rec.FireAt.Sub(c.now()), 0), func(c *Core) { c.fireTimer(tm) })
	}

	for _, h := range needTask {
		if h.task == nil {
			c.queueRunTask(h) // even if stuck: its replay was asked for (ResumeRun)
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
