package core

import (
	"context"
	"fmt"
	"sort"

	"github.com/alper/moksha/internal/state"
)

// Leases. With Config.HeartbeatTimeout set, a dispatched step attempt or run
// task stays assigned only while its worker sends heartbeats: without one
// for that long, the worker is presumed dead and the work lost. A lost step
// attempt goes through the step's retry policy; a lost run task is replaced.
// Leases are in memory only: a new leader gives every restored attempt a
// fresh one.

// Heartbeat renews the leases of the tasks a worker is running (run task
// and step attempt IDs) and returns those it should stop: unknown, finished,
// timed out or cancelled.
func (c *Core) Heartbeat(ctx context.Context, workerID string, taskIDs []string) ([]string, error) {
	return submit(ctx, c, func(c *Core) ([]string, error) {
		c.sawHeartbeat(workerID)
		var stop []string
		for _, id := range taskIDs {
			if a := c.stepAttempts[id]; a != nil {
				c.armStepLease(a)
				continue
			}
			if t := c.runTasks[id]; t != nil && t.rec.Status == state.RunTaskDispatched && state.RunOpen(t.run.rec.Status) {
				c.armRunTaskLease(t)
				continue
			}
			stop = append(stop, id)
		}
		return stop, nil
	})
}

func (c *Core) armStepLease(a *stepAttemptState) {
	d := c.cfg.HeartbeatTimeout
	if d <= 0 {
		return
	}
	a.lease.stop()
	a.lease = c.after(d, func(c *Core) {
		if c.stepAttempts[a.rec.ID] != a {
			return
		}
		msg := fmt.Sprintf("no heartbeat from worker %q within %s", a.rec.WorkerID, d)
		c.finishStepAttempt(a, state.AttemptLost, "lost", msg)
		c.stepAttemptFailed(a.step, "lost", msg, false)
	})
}

func (c *Core) armRunTaskLease(t *runTaskState) {
	d := c.cfg.HeartbeatTimeout
	if d <= 0 {
		return
	}
	t.lease.stop()
	t.lease = c.after(d, func(c *Core) {
		if c.runTasks[t.rec.ID] != t || t.rec.Status != state.RunTaskDispatched {
			return
		}
		h := t.run
		delete(c.runTasks, t.rec.ID)
		if h.task == t {
			h.task = nil
		}
		t.rec.Status, t.rec.Reason = state.RunTaskLost, "lost"
		t.rec.Message = fmt.Sprintf("no heartbeat or result from worker %q within %s", t.rec.WorkerID, d)
		t.rec.FinishedAt = ptr(c.now())
		c.dirtyRunTask(t)
		c.emitH(state.EventRunTaskFailed, h, t.rec.ID,
			runTaskFailedPayload{Number: t.rec.Number, Reason: t.rec.Reason, Message: t.rec.Message})
		if state.RunOpen(h.rec.Status) && h.task == nil {
			c.queueRunTask(h)
		}
	})
}

// resumeStuckRuns gives every stuck run a replay, after a new build was
// registered: it may carry the fix.
func (c *Core) resumeStuckRuns() {
	var ids []string
	for id, h := range c.runs {
		if h.rec.Status == state.RunStuck && h.task == nil {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		c.queueRunTask(c.runs[id])
	}
}
