package core

import (
	"context"
	"fmt"
	"sort"

	"github.com/alper/hubwagen/internal/state"
)

// Leases. With Config.HeartbeatTimeout set, a dispatched lift attempt or haul
// task stays assigned only while its worker sends heartbeats: without one
// for that long, the worker is presumed dead and the work lost. A lost lift
// attempt goes through the lift's retry policy; a lost haul task is replaced.
// Leases are in memory only: a new leader gives every restored attempt a
// fresh one.

// Heartbeat renews the leases of the tasks a worker is running (haul task
// and lift attempt IDs) and returns those it should stop: unknown, finished,
// timed out or cancelled.
func (c *Core) Heartbeat(ctx context.Context, workerID string, taskIDs []string) ([]string, error) {
	return submit(ctx, c, func(c *Core) ([]string, error) {
		c.sawHeartbeat(workerID)
		var stop []string
		for _, id := range taskIDs {
			if a := c.liftAttempts[id]; a != nil {
				c.armLiftLease(a)
				continue
			}
			if t := c.haulTasks[id]; t != nil && t.rec.Status == state.HaulTaskDispatched && state.HaulOpen(t.haul.rec.Status) {
				c.armHaulTaskLease(t)
				continue
			}
			stop = append(stop, id)
		}
		return stop, nil
	})
}

func (c *Core) armLiftLease(a *liftAttemptState) {
	d := c.cfg.HeartbeatTimeout
	if d <= 0 {
		return
	}
	a.lease.stop()
	a.lease = c.after(d, func(c *Core) {
		if c.liftAttempts[a.rec.ID] != a {
			return
		}
		msg := fmt.Sprintf("no heartbeat from worker %q within %s", a.rec.WorkerID, d)
		c.finishLiftAttempt(a, state.AttemptLost, "lost", msg)
		c.liftAttemptFailed(a.lift, "lost", msg, false)
	})
}

func (c *Core) armHaulTaskLease(t *haulTaskState) {
	d := c.cfg.HeartbeatTimeout
	if d <= 0 {
		return
	}
	t.lease.stop()
	t.lease = c.after(d, func(c *Core) {
		if c.haulTasks[t.rec.ID] != t || t.rec.Status != state.HaulTaskDispatched {
			return
		}
		h := t.haul
		delete(c.haulTasks, t.rec.ID)
		if h.task == t {
			h.task = nil
		}
		t.rec.Status, t.rec.Reason = state.HaulTaskLost, "lost"
		t.rec.Message = fmt.Sprintf("no heartbeat or result from worker %q within %s", t.rec.WorkerID, d)
		t.rec.FinishedAt = ptr(c.now())
		c.dirtyHaulTask(t)
		c.emitH(state.EventHaulTaskFailed, h, t.rec.ID,
			haulTaskFailedPayload{Number: t.rec.Number, Reason: t.rec.Reason, Message: t.rec.Message})
		if state.HaulOpen(h.rec.Status) && h.task == nil {
			c.queueHaulTask(h)
		}
	})
}

// resumeStuckHauls gives every stuck haul a replay, after a new build was
// registered: it may carry the fix.
func (c *Core) resumeStuckHauls() {
	var ids []string
	for id, h := range c.hauls {
		if h.rec.Status == state.HaulStuck && h.task == nil {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	for _, id := range ids {
		c.queueHaulTask(c.hauls[id])
	}
}
