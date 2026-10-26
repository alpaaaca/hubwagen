package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// Hauls run by replay until blocked. The core queues a haul task; a worker
// replays the haul function against its logbook and reports every command the
// replay issued. Commands beyond those already in the logbook are applied:
// lifts are queued on their dock, sleeps arm timers. When a lift finishes or a
// timer fires, the haul gets another haul task, and so on until the haul
// function returns (completed) or raises (failed).
//
// Not yet: lift retries and timeouts (#15), checking replayed commands against
// the logbook (#16), resuming hauls after a leader restart (#17).

// DefaultDock is where hauls and lifts go when no dock is named.
const DefaultDock = "default"

// --- in-memory model ---

type haulState struct {
	rec    state.Haul
	task   *haulTaskState     // the queued or dispatched haul task, if any
	replay bool               // a result arrived while task was dispatched: replay again after it
	tasks  int                // haul tasks created so far
	lifts  map[int]*liftState // unfinished lifts by seq
	timers map[int]*haulTimer // pending timers by seq
}

type haulTaskState struct {
	rec  state.HaulTask
	haul *haulState
}

type liftState struct {
	rec     state.Lift
	haul    *haulState
	queued  string            // attempt ID waiting on the dock for a worker
	attempt *liftAttemptState // the dispatched attempt
}

type liftAttemptState struct {
	rec  state.LiftAttempt
	lift *liftState
}

type haulTimer struct {
	rec  state.Timer
	haul *haulState
	ref  *timerRef
}

func (c *Core) dirtyHaul(h *haulState)         { dirty(c.b, &c.b.hauls, h) }
func (c *Core) dirtyHaulTask(t *haulTaskState) { dirty(c.b, &c.b.haulTasks, t) }
func (c *Core) dirtyLift(l *liftState)         { dirty(c.b, &c.b.lifts, l) }
func (c *Core) dirtyLiftAttempt(a *liftAttemptState) {
	dirty(c.b, &c.b.liftAttempts, a)
}
func (c *Core) dirtyTimer(t *haulTimer) { dirty(c.b, &c.b.timers, t) }

func (c *Core) emitH(typ state.EventType, h *haulState, attemptID string, payload any) {
	c.emitHaul(typ, h.rec.Tenant, h.rec.ID, attemptID, payload)
}

// --- API ---

type StartHaulRequest struct {
	Tenant  string
	Name    string // registered haul function
	Dock    string // default DefaultDock
	Trigger string // default "manual"
	Params  map[string]any
}

type StartHaulResult struct {
	HaulID string
}

// StartHaul creates a haul and queues its first haul task.
func (c *Core) StartHaul(ctx context.Context, req StartHaulRequest) (StartHaulResult, error) {
	return submit(ctx, c, func(c *Core) (StartHaulResult, error) { return c.startHaul(req) })
}

func (c *Core) startHaul(req StartHaulRequest) (StartHaulResult, error) {
	if req.Tenant == "" || req.Name == "" {
		return StartHaulResult{}, fmt.Errorf("%w: a haul needs a tenant and a name", ErrInvalid)
	}
	if req.Dock == "" {
		req.Dock = DefaultDock
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	h := &haulState{
		rec: state.Haul{
			ID: c.cfg.NewID(), Tenant: req.Tenant, Name: req.Name, Dock: req.Dock, Trigger: req.Trigger,
			Params: req.Params, Status: state.HaulRunning, CreatedAt: c.now(),
		},
		lifts:  map[int]*liftState{},
		timers: map[int]*haulTimer{},
	}
	c.hauls[h.rec.ID] = h
	c.dirtyHaul(h)
	c.emitH(state.EventHaulStarted, h, "", haulStartedPayload{Name: req.Name, Dock: req.Dock, Trigger: req.Trigger, Params: req.Params})
	c.queueHaulTask(h)
	return StartHaulResult{HaulID: h.rec.ID}, nil
}

// CommandKind is what a haul asked for at one point of its replay.
type CommandKind string

const (
	CommandLift  CommandKind = "lift"
	CommandSleep CommandKind = "sleep"
)

// Command is one step a haul's replay issued, numbered from 1 in the order
// the haul function made the calls.
type Command struct {
	Seq  int         `json:"seq"`
	Kind CommandKind `json:"kind"`
	// Lift.
	Name string          `json:"name,omitempty"`
	Dock string          `json:"dock,omitempty"` // default: the haul's dock
	Args json.RawMessage `json:"args,omitempty"`
	// Sleep.
	Duration time.Duration `json:"duration,omitempty"`
}

// HaulOutcome is how the haul function ended: it returned (completed) or
// raised (failed).
type HaulOutcome struct {
	Status  string          `json:"status"` // state.HaulCompleted or state.HaulFailed
	Result  json.RawMessage `json:"result,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
}

// TaskError is a failure of the worker rather than of user code, such as a
// haul function that can't be imported.
type TaskError struct {
	Reason  string
	Message string
}

// HaulTaskResult is what a worker reports after replaying a haul.
type HaulTaskResult struct {
	TaskID  string
	BuildID string
	// Commands is every command the replay issued, in order, starting at
	// seq 1 — including those already in the logbook.
	Commands []Command
	Outcome  *HaulOutcome // the haul function ended
	Error    *TaskError   // the replay itself failed
}

// CompleteHaulTask applies a haul task's result. It returns once the result
// is durable.
func (c *Core) CompleteHaulTask(ctx context.Context, res HaulTaskResult) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) { return struct{}{}, c.completeHaulTask(res) })
	return err
}

// LiftResult is what a worker reports after running a lift attempt.
type LiftResult struct {
	AttemptID string
	Status    string // state.AttemptSucceeded or state.AttemptFailed
	Result    json.RawMessage
	Reason    string
	Message   string
}

// CompleteLift records a lift attempt's result. It returns once the result is
// durable. An attempt that is unknown or already finished (for example
// because its haul was cancelled) gets ErrNotFound; the worker drops it.
func (c *Core) CompleteLift(ctx context.Context, res LiftResult) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) { return struct{}{}, c.completeLift(res) })
	return err
}

// CancelHaul cancels an open haul: queued work is withdrawn, dispatched lift
// attempts are recorded as cancelled and their results ignored, timers stop.
// Cancelling a finished haul is a no-op.
func (c *Core) CancelHaul(ctx context.Context, haulID, by string) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		if h := c.hauls[haulID]; h != nil {
			c.finishHaul(h, state.HaulCancelled, "cancelled", "cancelled by "+by, nil)
			return struct{}{}, nil
		}
		if _, err := c.cfg.Store.GetHaul(ctx, haulID); errors.Is(err, state.ErrNotFound) {
			return struct{}{}, fmt.Errorf("%w: haul %s", ErrNotFound, haulID)
		} else if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

// --- haul tasks ---

func (c *Core) queueHaulTask(h *haulState) {
	h.tasks++
	t := &haulTaskState{
		rec: state.HaulTask{
			ID: c.cfg.NewID(), HaulID: h.rec.ID, Number: h.tasks, Term: c.cfg.Term,
			Status: state.HaulTaskQueued, QueuedAt: c.now(),
		},
		haul: h,
	}
	h.task = t
	c.haulTasks[t.rec.ID] = t
	c.dirtyHaulTask(t)
	c.emitH(state.EventHaulTaskQueued, h, t.rec.ID, haulTaskPayload{Number: t.rec.Number})

	task := Task{Kind: TaskHaul, ID: t.rec.ID, Dock: h.rec.Dock, HaulID: h.rec.ID, Name: h.rec.Name}
	c.enqueue(task, func(c *Core, workerID string) Task {
		t.rec.Status, t.rec.WorkerID, t.rec.DispatchedAt = state.HaulTaskDispatched, workerID, ptr(c.now())
		c.dirtyHaulTask(t)
		c.emitH(state.EventHaulTaskDispatched, h, t.rec.ID, haulTaskPayload{Number: t.rec.Number, Worker: workerID})
		return task
	})
}

// wake gives h a chance to make progress after a result arrived.
func (c *Core) wake(h *haulState) {
	switch {
	case !state.HaulOpen(h.rec.Status):
	case h.task == nil:
		c.queueHaulTask(h)
	case h.task.rec.Status == state.HaulTaskDispatched:
		h.replay = true // its logbook may predate this result
	}
	// A queued haul task reads its logbook when dispatched, so it will see
	// the result.
}

func (c *Core) completeHaulTask(res HaulTaskResult) error {
	t := c.haulTasks[res.TaskID]
	if t == nil {
		return fmt.Errorf("%w: haul task %s is unknown or finished", ErrNotFound, res.TaskID)
	}
	if t.rec.Status != state.HaulTaskDispatched {
		return fmt.Errorf("%w: haul task %s was not dispatched", ErrInvalid, res.TaskID)
	}
	h := t.haul
	delete(c.haulTasks, t.rec.ID)
	if h.task == t {
		h.task = nil
	}
	t.rec.BuildID, t.rec.FinishedAt = res.BuildID, ptr(c.now())
	c.dirtyHaulTask(t)

	if err := validateHaulTaskResult(res); err != nil {
		c.failHaulTask(t, "invalid_result", err.Error())
		if state.HaulOpen(h.rec.Status) {
			c.markStuck(h, "invalid_result", err.Error())
		}
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if res.Error != nil {
		c.failHaulTask(t, res.Error.Reason, res.Error.Message)
		if state.HaulOpen(h.rec.Status) {
			c.finishHaul(h, state.HaulFailed, "haul_task_failed", res.Error.Message, nil)
		}
		return nil
	}

	var fresh []Command
	if len(res.Commands) > h.rec.Commands {
		fresh = res.Commands[h.rec.Commands:]
	}
	t.rec.Status = state.HaulTaskCompleted
	c.emitH(state.EventHaulTaskCompleted, h, t.rec.ID,
		haulTaskCompletedPayload{Number: t.rec.Number, BuildID: res.BuildID, Commands: fresh, Outcome: res.Outcome})
	if !state.HaulOpen(h.rec.Status) {
		return nil // finished while the replay ran; nothing to apply
	}

	h.rec.BuildID = res.BuildID
	if h.rec.Status == state.HaulStuck {
		h.rec.Status, h.rec.Reason, h.rec.Message = state.HaulRunning, "", ""
	}
	c.dirtyHaul(h)
	for _, cmd := range fresh {
		h.rec.Commands = cmd.Seq
		switch cmd.Kind {
		case CommandLift:
			c.scheduleLift(h, cmd)
		case CommandSleep:
			c.startTimer(h, cmd)
		}
	}
	if o := res.Outcome; o != nil {
		c.finishHaul(h, o.Status, o.Reason, o.Message, o.Result)
		return nil
	}
	if h.replay {
		h.replay = false
		c.queueHaulTask(h)
	}
	return nil
}

func validateHaulTaskResult(res HaulTaskResult) error {
	if res.Outcome != nil && res.Error != nil {
		return errors.New("a result has an outcome or an error, not both")
	}
	if o := res.Outcome; o != nil && o.Status != state.HaulCompleted && o.Status != state.HaulFailed {
		return fmt.Errorf("outcome status %q is not %q or %q", o.Status, state.HaulCompleted, state.HaulFailed)
	}
	for i, cmd := range res.Commands {
		if cmd.Seq != i+1 {
			return fmt.Errorf("commands[%d]: seq is %d, want %d", i, cmd.Seq, i+1)
		}
		switch cmd.Kind {
		case CommandLift:
			if cmd.Name == "" {
				return fmt.Errorf("commands[%d]: lift without a name", i)
			}
		case CommandSleep:
			if cmd.Duration <= 0 {
				return fmt.Errorf("commands[%d]: sleep needs a positive duration", i)
			}
		default:
			return fmt.Errorf("commands[%d]: unknown kind %q", i, cmd.Kind)
		}
	}
	return nil
}

func (c *Core) failHaulTask(t *haulTaskState, reason, message string) {
	t.rec.Status, t.rec.Reason, t.rec.Message = state.HaulTaskFailed, reason, message
	if t.rec.FinishedAt == nil {
		t.rec.FinishedAt = ptr(c.now())
	}
	c.dirtyHaulTask(t)
	c.emitH(state.EventHaulTaskFailed, t.haul, t.rec.ID,
		haulTaskFailedPayload{Number: t.rec.Number, Reason: reason, Message: message})
}

// markStuck parks a haul until a replay succeeds again (for example after a
// deploy). Its lifts and timers carry on.
func (c *Core) markStuck(h *haulState, reason, message string) {
	h.rec.Status, h.rec.Reason, h.rec.Message = state.HaulStuck, reason, message
	c.dirtyHaul(h)
	c.emitH(state.EventHaulStuck, h, "", haulStuckPayload{Reason: reason, Message: message})
}

// finishHaul closes h: unfinished lifts are cancelled, timers stop, a queued
// haul task is withdrawn. A dispatched haul task stays known so its result
// is still accepted (and ignored).
func (c *Core) finishHaul(h *haulState, status, reason, message string, result json.RawMessage) {
	for _, seq := range sortedKeys(h.lifts) {
		c.cancelLift(h.lifts[seq], "haul "+status)
	}
	for _, seq := range sortedKeys(h.timers) {
		tm := h.timers[seq]
		tm.ref.stop()
		tm.rec.Status = state.TimerCancelled
		c.dirtyTimer(tm)
		c.emitH(state.EventTimerCancelled, h, "", timerPayload{Seq: seq})
		delete(h.timers, seq)
	}
	if t := h.task; t != nil && t.rec.Status == state.HaulTaskQueued {
		c.unqueue(h.rec.Dock, t.rec.ID)
		delete(c.haulTasks, t.rec.ID)
		c.failHaulTask(t, "cancelled", "haul "+status)
		h.task = nil
	}
	h.rec.Status, h.rec.Reason, h.rec.Message, h.rec.Result = status, reason, message, result
	h.rec.FinishedAt = ptr(c.now())
	c.dirtyHaul(h)
	c.emitH(state.EventHaulFinished, h, "", haulFinishedPayload{Status: status, Reason: reason, Message: message, Result: result})
	delete(c.hauls, h.rec.ID)
}

// --- lifts ---

func (c *Core) scheduleLift(h *haulState, cmd Command) {
	dock := cmd.Dock
	if dock == "" {
		dock = h.rec.Dock
	}
	l := &liftState{
		rec: state.Lift{
			HaulID: h.rec.ID, Seq: cmd.Seq, Name: cmd.Name, Dock: dock, Args: cmd.Args,
			Status: state.LiftScheduled, ScheduledAt: c.now(),
		},
		haul: h,
	}
	h.lifts[cmd.Seq] = l
	c.dirtyLift(l)
	c.emitH(state.EventLiftScheduled, h, "", liftScheduledPayload{Seq: cmd.Seq, Name: cmd.Name, Dock: dock, Args: cmd.Args})
	c.queueLiftAttempt(l)
}

// queueLiftAttempt puts the lift's next attempt on its dock. The attempt is
// recorded when a worker takes it.
func (c *Core) queueLiftAttempt(l *liftState) {
	h := l.haul
	l.queued = c.cfg.NewID()
	task := Task{Kind: TaskLift, ID: l.queued, Dock: l.rec.Dock, HaulID: h.rec.ID, Seq: l.rec.Seq, Name: l.rec.Name, Args: l.rec.Args}
	c.enqueue(task, func(c *Core, workerID string) Task {
		l.queued = ""
		l.rec.Attempts++
		l.rec.Status = state.LiftDispatched
		a := &liftAttemptState{
			rec: state.LiftAttempt{
				ID: task.ID, HaulID: h.rec.ID, Seq: l.rec.Seq, Number: l.rec.Attempts, Term: c.cfg.Term,
				WorkerID: workerID, Status: state.AttemptDispatched, DispatchedAt: c.now(),
			},
			lift: l,
		}
		l.attempt = a
		c.liftAttempts[a.rec.ID] = a
		c.dirtyLift(l)
		c.dirtyLiftAttempt(a)
		c.emitH(state.EventLiftAttemptDispatched, h, a.rec.ID,
			liftAttemptDispatchedPayload{Seq: l.rec.Seq, Number: a.rec.Number, Worker: workerID})
		return task
	})
}

func (c *Core) completeLift(res LiftResult) error {
	if res.Status != state.AttemptSucceeded && res.Status != state.AttemptFailed {
		return fmt.Errorf("%w: lift result status %q is not %q or %q", ErrInvalid, res.Status, state.AttemptSucceeded, state.AttemptFailed)
	}
	a := c.liftAttempts[res.AttemptID]
	if a == nil {
		return fmt.Errorf("%w: lift attempt %s is unknown or finished", ErrNotFound, res.AttemptID)
	}
	c.finishLiftAttempt(a, res.Status, res.Reason, res.Message)

	l := a.lift
	status := state.LiftCompleted
	if res.Status == state.AttemptFailed {
		status = state.LiftFailed // no retries yet (#15): the haul sees the failure
	}
	c.finishLift(l, status, res.Result, res.Reason, res.Message)
	c.wake(l.haul)
	return nil
}

func (c *Core) finishLiftAttempt(a *liftAttemptState, status, reason, message string) {
	l := a.lift
	a.rec.Status, a.rec.Reason, a.rec.Message, a.rec.FinishedAt = status, reason, message, ptr(c.now())
	c.dirtyLiftAttempt(a)
	c.emitH(state.EventLiftAttemptFinished, l.haul, a.rec.ID,
		liftAttemptFinishedPayload{Seq: l.rec.Seq, Number: a.rec.Number, Status: status, Reason: reason, Message: message})
	delete(c.liftAttempts, a.rec.ID)
	l.attempt = nil
	c.releaseLift(l.rec.Dock)
}

func (c *Core) finishLift(l *liftState, status string, result json.RawMessage, reason, message string) {
	l.rec.Status, l.rec.Result, l.rec.Reason, l.rec.Message = status, result, reason, message
	l.rec.FinishedAt = ptr(c.now())
	c.dirtyLift(l)
	c.emitH(state.EventLiftFinished, l.haul, "",
		liftFinishedPayload{Seq: l.rec.Seq, Status: status, Result: result, Reason: reason, Message: message})
	delete(l.haul.lifts, l.rec.Seq)
}

// cancelLift withdraws a queued attempt or records the dispatched one as
// cancelled. Telling the worker to stop needs the worker API (heartbeats);
// until then its result is simply ignored.
func (c *Core) cancelLift(l *liftState, message string) {
	if l.queued != "" {
		c.unqueue(l.rec.Dock, l.queued)
		l.queued = ""
	}
	if a := l.attempt; a != nil {
		c.finishLiftAttempt(a, state.AttemptCancelled, "cancelled", message)
	}
	c.finishLift(l, state.LiftCancelled, nil, "cancelled", message)
}

// --- timers ---

func (c *Core) startTimer(h *haulState, cmd Command) {
	tm := &haulTimer{
		rec: state.Timer{
			HaulID: h.rec.ID, Seq: cmd.Seq, FireAt: c.now().Add(cmd.Duration),
			Status: state.TimerPending, CreatedAt: c.now(),
		},
		haul: h,
	}
	h.timers[cmd.Seq] = tm
	c.dirtyTimer(tm)
	c.emitH(state.EventTimerStarted, h, "", timerStartedPayload{Seq: cmd.Seq, FireAt: tm.rec.FireAt})
	tm.ref = c.after(cmd.Duration, func(c *Core) { c.fireTimer(tm) })
}

func (c *Core) fireTimer(tm *haulTimer) {
	if tm.rec.Status != state.TimerPending {
		return
	}
	h := tm.haul
	tm.rec.Status, tm.rec.FiredAt = state.TimerFired, ptr(c.now())
	c.dirtyTimer(tm)
	c.emitH(state.EventTimerFired, h, "", timerPayload{Seq: tm.rec.Seq})
	delete(h.timers, tm.rec.Seq)
	c.wake(h)
}

func sortedKeys[V any](m map[int]V) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}
