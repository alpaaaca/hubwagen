package core

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// restore rebuilds the in-memory model from the store, settles attempts left
// in flight by the previous leader, and re-arms timers and dispatches, all in
// one committed batch.
func (c *Core) restore(ctx context.Context) error {
	cat, err := c.cfg.Store.LoadCatalog(ctx)
	if err != nil {
		return err
	}
	for i := range cat.Tenants {
		c.tenants[cat.Tenants[i].Name] = &cat.Tenants[i]
	}
	for hash, v := range cat.Versions {
		if c.versions[hash], err = newVersion(v); err != nil {
			return err
		}
	}
	for _, p := range cat.Pipelines {
		c.pipelines[pipelineKey{p.Tenant, p.Name}] = &pipelineState{rec: p}
	}
	for _, cur := range cat.Cursors {
		if p := c.pipelines[pipelineKey{cur.Tenant, cur.Pipeline}]; p != nil {
			p.cron.cursor, p.cron.hasCursor = cur.LastTick, true
		}
	}

	act, err := c.cfg.Store.LoadActive(ctx)
	if err != nil {
		return err
	}
	tasksByRun := map[string][]state.TaskRun{}
	for _, tr := range act.TaskRuns {
		tasksByRun[tr.RunID] = append(tasksByRun[tr.RunID], tr)
	}
	var inFlight []state.Attempt
	for _, a := range act.Attempts {
		if !state.AttemptTerminal(a.Status) {
			inFlight = append(inFlight, a)
		}
	}

	for _, rec := range act.Runs { // ordered by ID, so queues keep creation order
		r, err := c.restoreRun(ctx, rec, tasksByRun[rec.ID])
		if err != nil {
			return fmt.Errorf("run %s: %w", rec.ID, err)
		}
		p := c.pipelines[pipelineKey{rec.Tenant, rec.Pipeline}]
		if p == nil {
			return fmt.Errorf("run %s: unknown pipeline %s/%s", rec.ID, rec.Tenant, rec.Pipeline)
		}
		if rec.Status == state.RunRunning {
			p.running++
		} else {
			p.queued = append(p.queued, r)
		}
	}
	for _, a := range inFlight {
		r := c.runs[a.RunID]
		t := r.tasks[a.TaskID]
		as := &attemptState{rec: a, task: t}
		t.current = as
		c.attempts[a.ID] = as
	}

	c.b = newBatch()
	c.emit(state.EventLeaderTermStarted, "", "", "", "", leaderTermStartedPayload{
		Term: c.cfg.Term, ActiveRuns: len(act.Runs), LostAttempts: len(inFlight),
	})

	// Attempts the recovery accounts for get its report; the rest are lost.
	var reports []Report
	if c.cfg.Recovery != nil && len(inFlight) > 0 {
		in := make([]InFlight, len(inFlight))
		for i, a := range inFlight {
			r := c.runs[a.RunID].rec
			in[i] = InFlight{Attempt: a, CommitURI: c.cfg.Layout.TaskCommit(r.Tenant, r.Pipeline, a.TaskID, r.RunKey, r.Generation)}
		}
		reports = c.cfg.Recovery.Recover(ctx, in)
	}
	for _, rep := range reports {
		c.handleReport(rep)
	}
	for _, a := range inFlight {
		as := c.attempts[a.ID]
		switch {
		case as == nil: // settled by a recovery report
		case reported(reports, a.ID):
			// Still running somewhere: give it what is left of its timeout
			// (attemptStarted already armed a full one if it was only
			// dispatched before).
			if as.timeout == nil && as.rec.StartedAt != nil {
				left := time.Duration(as.task.def.TimeoutSeconds)*time.Second - c.now().Sub(*as.rec.StartedAt)
				c.armTimeout(as, max(left, 0))
			}
		default:
			c.attemptDone(as, Report{Status: state.AttemptLost, Reason: "leader_restart",
				Message: fmt.Sprintf("in flight when term %d ended", a.Term)})
		}
	}

	for _, rec := range act.Runs {
		r := c.runs[rec.ID]
		if r == nil || state.RunTerminal(r.rec.Status) {
			continue
		}
		if r.halting() {
			c.halt(r, "resumed after leader restart")
			continue
		}
		for _, id := range r.order {
			t := r.tasks[id]
			switch t.rec.Status {
			case state.TaskReady:
				c.dispatch(t) // committed ready, never dispatched
			case state.TaskRetryWait:
				if t.retry == nil {
					c.armRetry(t, max(t.rec.RetryAt.Sub(c.now()), 0))
				}
			case state.TaskPending:
				c.evaluate(t)
			}
		}
		c.maybeFinishRun(r)
	}
	for _, p := range c.pipelines {
		c.startQueued(p)
	}
	// Schedules last: catch-up runs queue behind the restored ones.
	for _, p := range c.sortedPipelines() {
		c.reschedule(p)
	}
	return c.commit(ctx)
}

func (c *Core) restoreRun(ctx context.Context, rec state.Run, taskRecs []state.TaskRun) (*runState, error) {
	ver := c.versions[rec.Version]
	if ver == nil {
		return nil, fmt.Errorf("version %s not loaded", rec.Version)
	}
	r := &runState{rec: rec, ver: ver, tasks: map[string]*taskState{}}
	byID := map[string]state.TaskRun{}
	for _, tr := range taskRecs {
		byID[tr.TaskID] = tr
	}
	for i := range ver.spec.Tasks {
		def := &ver.spec.Tasks[i]
		tr, ok := byID[def.ID]
		if !ok {
			return nil, fmt.Errorf("task run %s missing", def.ID)
		}
		t := &taskState{rec: tr, def: def, run: r}
		r.tasks[def.ID] = t
		r.order = append(r.order, def.ID)
		if tr.Status == state.TaskFailed && def.OnFailure == "stop" {
			r.stopping = true
		}
	}

	outs, err := c.cfg.Store.ListOutputs(ctx, rec.ID)
	if err != nil {
		return nil, err
	}
	for _, o := range outs {
		t := r.tasks[o.TaskID]
		if t.outputs == nil {
			t.outputs = map[string]string{}
		}
		t.outputs[o.Name] = o.URI
	}

	events, err := c.cfg.Store.RunEvents(ctx, rec.ID)
	if err != nil {
		return nil, err
	}
	for _, e := range events {
		if e.Type == state.EventCancelRequested {
			r.cancelRequested = true
		}
	}

	c.runs[rec.ID] = r
	c.runsByKey[rec.RunKey] = r
	return r, nil
}

func reported(reports []Report, attemptID string) bool {
	for _, r := range reports {
		if r.AttemptID == attemptID {
			return true
		}
	}
	return false
}

// sortedPipelines returns pipelines in a stable order, so restore emits the
// same events every time.
func (c *Core) sortedPipelines() []*pipelineState {
	out := make([]*pipelineState, 0, len(c.pipelines))
	for _, p := range c.pipelines {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].rec.Tenant+"/"+out[i].rec.Name < out[j].rec.Tenant+"/"+out[j].rec.Name
	})
	return out
}
