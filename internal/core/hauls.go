package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
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
// A lift attempt that fails or times out is retried under the lift's retry
// policy; only the lift's final result reaches the haul.
//
// A replay must repeat the commands already in the logbook: same kind and
// target, in the same order. One that doesn't (non-deterministic haul code,
// or a deploy that changed it without a version marker) makes the haul
// stuck. Its lifts and timers carry on, but it gets no haul tasks until
// ResumeHaul, typically after a fix is deployed; a consistent replay then
// sets it running again.
//
// Not yet: resuming hauls after a leader restart (#17).

// DefaultDock is where hauls and lifts go when no dock is named.
const DefaultDock = "default"

// --- in-memory model ---

type haulState struct {
	rec    state.Haul
	task   *haulTaskState     // the queued or dispatched haul task, if any
	replay bool               // a result arrived while task was dispatched: replay again after it
	tasks  int                // haul tasks created so far
	cmds   []Command          // commands recorded in the logbook, in order
	lifts  map[int]*liftState // unfinished lifts by seq
	timers map[int]*haulTimer // pending timers by seq
}

type haulTaskState struct {
	rec   state.HaulTask
	haul  *haulState
	lease *timerRef
}

type liftState struct {
	rec     state.Lift
	haul    *haulState
	retry   RetryPolicy
	queued  string            // attempt ID waiting on the dock for a worker
	attempt *liftAttemptState // the dispatched attempt
	wait    *timerRef         // pending retry, while in retry_wait
	output  *OutputSpec       // where its pallet goes; nil = it produces none
	inputs  []InputRef
	memo    memoPolicy
	pallet  string  // the committed (or reused) pallet, once there is one
	flight  *flight // the flight it waits on, while waiting
}

type liftAttemptState struct {
	rec     state.LiftAttempt
	lift    *liftState
	timeout *timerRef
	lease   *timerRef
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
	if !c.haulNames[req.Name] {
		return StartHaulResult{}, fmt.Errorf("%w: no registered build defines haul %q; start a worker whose code has it",
			ErrUnknownHaul, req.Name)
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
	// CommandLatestPallet asks for a dataset partition's newest committed
	// pallet. The server answers at once and records the answer
	// (QueryAnswered), so every replay sees the same one.
	CommandLatestPallet CommandKind = "latest_pallet"
)

// Command is one step a haul's replay issued, numbered from 1 in the order
// the haul function made the calls.
type Command struct {
	Seq  int         `json:"seq"`
	Kind CommandKind `json:"kind"`
	// Lift.
	Name    string          `json:"name,omitempty"`
	Dock    string          `json:"dock,omitempty"` // default: the haul's dock
	Args    json.RawMessage `json:"args,omitempty"`
	Timeout time.Duration   `json:"timeout,omitempty"` // per attempt, enforced by the leader; 0 = none
	Retry   *RetryPolicy    `json:"retry,omitempty"`   // nil = no retries
	Output  *OutputSpec     `json:"output,omitempty"`  // the lift produces a pallet here
	Inputs  []InputRef      `json:"inputs,omitempty"`  // pallets passed to the lift
	// Memo: "" or "never" (default), "fingerprint", or "ttl:<duration>". A
	// lift with an output reuses a pallet; one without reuses a result.
	Memo string `json:"memo,omitempty"`
	// Sleep.
	Duration time.Duration `json:"duration,omitempty"`
	// Latest pallet.
	Dataset   string `json:"dataset,omitempty"`
	Partition string `json:"partition,omitempty"`
	// Source is where the haul code made the call ("app/hauls.py:42"), for
	// error messages.
	Source string `json:"source,omitempty"`
}

// RetryPolicy says how often and when a failed lift attempt is retried.
type RetryPolicy struct {
	Limit   int           `json:"limit"`             // retries after the first attempt
	Backoff string        `json:"backoff,omitempty"` // "fixed" (default) or "exponential"
	Initial time.Duration `json:"initial,omitempty"` // delay before the first retry
	Max     time.Duration `json:"max,omitempty"`     // cap for exponential backoff; default Initial
}

func (r RetryPolicy) validate() error {
	switch {
	case r.Limit < 0:
		return errors.New("retry limit must not be negative")
	case r.Backoff != "" && r.Backoff != "fixed" && r.Backoff != "exponential":
		return fmt.Errorf("retry backoff %q is not fixed or exponential", r.Backoff)
	case r.Initial < 0 || r.Max < 0:
		return errors.New("retry delays must not be negative")
	case r.Max != 0 && r.Max < r.Initial:
		return errors.New("retry max must not be below initial")
	}
	return nil
}

// delay is the wait before the retry that follows attempt n (1-based).
func (r RetryPolicy) delay(n int) time.Duration {
	maxDelay := r.Max
	if maxDelay == 0 {
		maxDelay = r.Initial
	}
	return backoffDelay(r.Backoff == "exponential", r.Initial, maxDelay, n)
}

// HaulOutcome is how the haul function ended: it returned (completed) or
// raised (failed).
type HaulOutcome struct {
	Status  string          `json:"status"` // state.HaulCompleted or state.HaulFailed
	Result  json.RawMessage `json:"result,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
}

// ReasonNondeterminism: the haul's replay contradicted its logbook, or (as
// a TaskError reason) the haul code read the clock or randomness. Either
// makes the haul stuck until a new build resumes it.
const ReasonNondeterminism = "nondeterminism"

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
	// NoRetry: the failure will repeat on every attempt (bad arguments, a
	// non-retryable error raised by the lift), so don't retry it.
	NoRetry bool
	// Output describes what a succeeded attempt of a lift with an output
	// wrote to its staging prefix.
	Output *LiftOutput
}

// CompleteLift records a lift attempt's result. It returns once the result is
// durable. An attempt that is unknown or already finished (for example
// because its haul was cancelled) gets ErrNotFound; the worker drops it.
func (c *Core) CompleteLift(ctx context.Context, res LiftResult) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) { return struct{}{}, c.completeLift(res) })
	return err
}

// ResumeHaul gives a stuck haul a new haul task, typically after a fix was
// deployed. It is a no-op for a haul that isn't stuck or already has one.
func (c *Core) ResumeHaul(ctx context.Context, haulID string) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		if h := c.hauls[haulID]; h != nil {
			if h.rec.Status == state.HaulStuck && h.task == nil {
				c.queueHaulTask(h)
			}
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
	c.enqueueHaulTask(t)
}

// enqueueHaulTask puts a queued haul task on its haul's dock.
func (c *Core) enqueueHaulTask(t *haulTaskState) {
	h := t.haul
	task := Task{Kind: TaskHaul, ID: t.rec.ID, Dock: h.rec.Dock, HaulID: h.rec.ID, Name: h.rec.Name}
	c.enqueue(task, func(c *Core, workerID string) Task {
		t.rec.Status, t.rec.WorkerID, t.rec.DispatchedAt = state.HaulTaskDispatched, workerID, ptr(c.now())
		c.dirtyHaulTask(t)
		c.emitH(state.EventHaulTaskDispatched, h, t.rec.ID, haulTaskPayload{Number: t.rec.Number, Worker: workerID})
		c.armHaulTaskLease(t)
		return task
	})
}

// wake gives h a chance to make progress after a result arrived.
func (c *Core) wake(h *haulState) {
	switch {
	case !state.HaulOpen(h.rec.Status):
	case h.task == nil && h.rec.Status == state.HaulStuck:
		// Replaying the same code would get stuck again; wait for ResumeHaul.
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
	t.lease.stop()
	delete(c.haulTasks, t.rec.ID)
	if h.task == t {
		h.task = nil
	}
	t.rec.BuildID, t.rec.FinishedAt = res.BuildID, ptr(c.now())
	c.dirtyHaulTask(t)

	if err := validateHaulTaskResult(res); err != nil {
		c.failHaulTask(t, "invalid_result", err.Error())
		if state.HaulOpen(h.rec.Status) {
			c.markStuck(h, "invalid_result", err.Error(), 0)
		}
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if res.Error != nil {
		c.failHaulTask(t, res.Error.Reason, res.Error.Message)
		if res.Error.Reason == ReasonNondeterminism {
			if state.HaulOpen(h.rec.Status) {
				c.markStuck(h, ReasonNondeterminism, res.Error.Message, 0)
			}
			return nil
		}
		if state.HaulOpen(h.rec.Status) {
			c.finishHaul(h, state.HaulFailed, "haul_task_failed", res.Error.Message, nil)
		}
		return nil
	}
	if state.HaulOpen(h.rec.Status) {
		if seq, msg := checkReplay(h.cmds, res.Commands); msg != "" {
			c.failHaulTask(t, ReasonNondeterminism, msg)
			c.markStuck(h, ReasonNondeterminism, msg, seq)
			return nil // the worker did its job; the haul code is at fault
		}
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
		h.cmds = append(h.cmds, cmd)
		switch cmd.Kind {
		case CommandLift:
			c.scheduleLift(h, cmd)
		case CommandSleep:
			c.startTimer(h, cmd)
		case CommandLatestPallet:
			c.answerLatestPallet(h, cmd)
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

// checkReplay compares a replay's commands with those in the logbook. It
// returns the first contradicting seq and a message, or "" if the replay
// extends the logbook.
func checkReplay(logged, replayed []Command) (int, string) {
	for i, old := range logged {
		if i >= len(replayed) {
			return old.Seq, fmt.Sprintf("replay issued %d commands but the logbook has %d: missing command %d, %s",
				len(replayed), len(logged), old.Seq, describeCommand(old))
		}
		if got := replayed[i]; got.Kind != old.Kind || got.Name != old.Name || got.Dataset != old.Dataset {
			return old.Seq, fmt.Sprintf("command %d: replay issued %s, the logbook has %s",
				old.Seq, describeCommand(got), describeCommand(old))
		}
	}
	return 0, ""
}

func describeCommand(cmd Command) string {
	s := string(cmd.Kind)
	switch cmd.Kind {
	case CommandLift:
		s += " " + strconv.Quote(cmd.Name)
	case CommandLatestPallet:
		s += " " + strconv.Quote(cmd.Dataset)
	}
	if cmd.Source != "" {
		s += " at " + cmd.Source
	}
	return s
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
			if cmd.Timeout < 0 {
				return fmt.Errorf("commands[%d]: timeout must not be negative", i)
			}
			if cmd.Retry != nil {
				if err := cmd.Retry.validate(); err != nil {
					return fmt.Errorf("commands[%d]: %v", i, err)
				}
			}
			if cmd.Output != nil {
				if err := cmd.Output.validate(); err != nil {
					return fmt.Errorf("commands[%d]: %v", i, err)
				}
			}
			for j, in := range cmd.Inputs {
				if in.Pallet == "" {
					return fmt.Errorf("commands[%d].inputs[%d]: no pallet", i, j)
				}
			}
			if _, err := parseMemo(cmd.Memo); err != nil {
				return fmt.Errorf("commands[%d]: %v", i, err)
			}
		case CommandSleep:
			if cmd.Duration <= 0 {
				return fmt.Errorf("commands[%d]: sleep needs a positive duration", i)
			}
		case CommandLatestPallet:
			if cmd.Dataset == "" {
				return fmt.Errorf("commands[%d]: latest_pallet needs a dataset", i)
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

// markStuck parks a haul until ResumeHaul and a consistent replay. Its lifts
// and timers carry on. seq is the contradicting command, if any.
func (c *Core) markStuck(h *haulState, reason, message string, seq int) {
	h.rec.Status, h.rec.Reason, h.rec.Message = state.HaulStuck, reason, message
	h.replay = false
	c.dirtyHaul(h)
	c.emitH(state.EventHaulStuck, h, "", haulStuckPayload{Reason: reason, Message: message, Seq: seq})
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
			HaulID: h.rec.ID, Seq: cmd.Seq, Name: cmd.Name, Dock: dock, Args: cmd.Args, Timeout: cmd.Timeout,
			Status: state.LiftScheduled, ScheduledAt: c.now(),
		},
		haul: h,
	}
	if cmd.Retry != nil {
		l.retry = *cmd.Retry
		l.rec.Retry = marshalPayload(state.EventLiftScheduled, cmd.Retry)
	}
	if cmd.Output != nil {
		l.output = cmd.Output
		l.rec.Output = marshalPayload(state.EventLiftScheduled, cmd.Output)
	}
	if len(cmd.Inputs) > 0 {
		l.inputs = cmd.Inputs
		l.rec.Inputs = marshalPayload(state.EventLiftScheduled, cmd.Inputs)
	}
	l.memo, _ = parseMemo(cmd.Memo) // validated with the haul task result
	h.lifts[cmd.Seq] = l
	c.dirtyLift(l)
	inputs, reason, msg := c.checkLiftIO(l)
	var match *memoMatch
	if reason == "" {
		match = c.memoize(l, inputs)
	}
	c.emitH(state.EventLiftScheduled, h, "", liftScheduledPayload{
		Seq: cmd.Seq, Name: cmd.Name, Dock: dock, Args: cmd.Args, Timeout: cmd.Timeout, Retry: cmd.Retry,
		Output: cmd.Output, Inputs: cmd.Inputs, Memo: cmd.Memo, Fingerprint: l.rec.Fingerprint, RunReason: l.rec.RunReason})
	switch {
	case reason != "":
		c.finishLift(l, state.LiftFailed, nil, reason, msg)
		c.wake(h)
	case match != nil:
		c.reuseLift(l, *match)
	default:
		c.runOrWait(l)
	}
}

// queueLiftAttempt puts the lift's next attempt on its dock. The attempt is
// recorded when a worker takes it.
func (c *Core) queueLiftAttempt(l *liftState) {
	h := l.haul
	l.queued = c.cfg.NewID()
	task := Task{Kind: TaskLift, ID: l.queued, Dock: l.rec.Dock, HaulID: h.rec.ID, Seq: l.rec.Seq, Name: l.rec.Name, Args: l.rec.Args}
	for _, in := range l.inputs {
		ti := TaskInput{Name: in.Name, Pallet: in.Pallet}
		if p, err := c.cfg.Store.GetPallet(c.ctx, in.Pallet); err == nil {
			ti.Location = p.Location // committed pallets are immutable: read once, here
		}
		task.Inputs = append(task.Inputs, ti)
	}
	task.Output = l.output
	if c.cfg.Pallets != nil {
		task.Staging = c.cfg.Layout.LiftStaging(h.rec.Tenant, h.rec.ID, l.rec.Seq, task.ID)
	}
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
		if l.rec.Timeout > 0 {
			c.armLiftTimeout(a, l.rec.Timeout)
		}
		c.armLiftLease(a)
		return task
	})
}

// armLiftTimeout fails the attempt if no result arrives within d (the lift's
// timeout, or what is left of it after a leader restart). A result that
// arrives later is ignored.
func (c *Core) armLiftTimeout(a *liftAttemptState, d time.Duration) {
	l := a.lift
	a.timeout = c.after(d, func(c *Core) {
		if c.liftAttempts[a.rec.ID] != a {
			return
		}
		msg := fmt.Sprintf("no result within %s", l.rec.Timeout)
		c.finishLiftAttempt(a, state.AttemptFailed, "timeout", msg)
		c.liftAttemptFailed(l, "timeout", msg, false)
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
	l := a.lift
	if res.Output != nil && res.Status != state.AttemptSucceeded {
		return fmt.Errorf("%w: only a succeeded attempt reports an output", ErrInvalid)
	}
	if res.Status == state.AttemptSucceeded && l.output != nil {
		return c.completeLiftWithOutput(a, res, *l.output)
	}
	if res.Status == state.AttemptSucceeded && res.Output != nil {
		return c.completeLiftWithOutput(a, res, OutputSpec{Dataset: ImplicitDataset(l.rec.Name)})
	}
	c.finishLiftAttempt(a, res.Status, res.Reason, res.Message)
	if res.Status == state.AttemptFailed {
		c.liftAttemptFailed(l, res.Reason, res.Message, res.NoRetry)
		return nil
	}
	c.finishLift(l, state.LiftCompleted, res.Result, res.Reason, res.Message)
	c.wake(l.haul)
	return nil
}

// liftAttemptFailed retries the lift if its policy allows, otherwise fails
// it and lets the haul see the failure.
func (c *Core) liftAttemptFailed(l *liftState, reason, message string, noRetry bool) {
	if noRetry || nonRetryable[reason] || l.rec.Attempts > l.retry.Limit {
		c.finishLift(l, state.LiftFailed, nil, reason, message)
		c.wake(l.haul)
		return
	}
	d := l.retry.delay(l.rec.Attempts)
	l.rec.Status, l.rec.RetryAt = state.LiftRetryWait, ptr(c.now().Add(d))
	l.rec.Reason, l.rec.Message = reason, message // the last failure, until the lift finishes
	c.dirtyLift(l)
	c.emitH(state.EventLiftRetryScheduled, l.haul, "",
		liftRetryScheduledPayload{Seq: l.rec.Seq, NextAttempt: l.rec.Attempts + 1, RetryAt: *l.rec.RetryAt})
	c.armLiftRetry(l, d)
}

// armLiftRetry queues the lift's next attempt after d.
func (c *Core) armLiftRetry(l *liftState, d time.Duration) {
	l.wait = c.after(d, func(c *Core) {
		if l.rec.Status != state.LiftRetryWait {
			return // cancelled meanwhile
		}
		l.wait = nil
		l.rec.Status, l.rec.RetryAt = state.LiftScheduled, nil
		c.dirtyLift(l)
		c.queueLiftAttempt(l)
	})
}

func (c *Core) finishLiftAttempt(a *liftAttemptState, status, reason, message string) {
	l := a.lift
	a.timeout.stop()
	a.lease.stop()
	a.rec.Status, a.rec.Reason, a.rec.Message, a.rec.FinishedAt = status, reason, message, ptr(c.now())
	c.dirtyLiftAttempt(a)
	c.emitH(state.EventLiftAttemptFinished, l.haul, a.rec.ID,
		liftAttemptFinishedPayload{Seq: l.rec.Seq, Number: a.rec.Number, Status: status, Reason: reason, Message: message})
	delete(c.liftAttempts, a.rec.ID)
	l.attempt = nil
	c.releaseLift(l.rec.Dock)
	if status != state.AttemptSucceeded {
		c.discardStaging(c.cfg.Layout.LiftStaging(l.haul.rec.Tenant, l.rec.HaulID, l.rec.Seq, a.rec.ID))
	}
}

func (c *Core) finishLift(l *liftState, status string, result json.RawMessage, reason, message string) {
	l.rec.Status, l.rec.Result, l.rec.Reason, l.rec.Message = status, result, reason, message
	l.rec.RetryAt, l.rec.FinishedAt = nil, ptr(c.now())
	c.dirtyLift(l)
	c.emitH(state.EventLiftFinished, l.haul, "",
		liftFinishedPayload{Seq: l.rec.Seq, Status: status, Result: result, Reason: reason, Message: message,
			Pallet: l.pallet, Reused: l.rec.Reused})
	delete(l.haul.lifts, l.rec.Seq)
	c.landFlight(l)
}

// cancelLift withdraws a queued attempt or records the dispatched one as
// cancelled. Telling the worker to stop needs the worker API (heartbeats);
// until then its result is simply ignored.
func (c *Core) cancelLift(l *liftState, message string) {
	l.wait.stop()
	l.wait = nil
	c.leaveFlight(l)
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

// answerLatestPallet answers a latest_pallet query from the catalog and
// records the answer, then lets the haul replay with it.
func (c *Core) answerLatestPallet(h *haulState, cmd Command) {
	var answer *palletAnswer
	p, err := c.cfg.Store.LatestPallet(c.ctx, h.rec.Tenant, cmd.Dataset, cmd.Partition)
	switch {
	case err == nil:
		answer = &palletAnswer{ID: p.ID, Dataset: p.Dataset, Partition: p.Partition, Location: p.Location,
			ContentHash: p.ContentHash, Meta: p.Meta, Stats: p.Stats, CommittedAt: p.CommittedAt}
	case !errors.Is(err, state.ErrNotFound):
		c.log.Error("latest pallet lookup failed", "haul", h.rec.ID, "seq", cmd.Seq, "err", err)
	}
	c.emitH(state.EventQueryAnswered, h, "", queryAnsweredPayload{Seq: cmd.Seq, Pallet: answer})
	h.replay = true
}
