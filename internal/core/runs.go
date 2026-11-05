package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/alper/moksha/internal/state"
)

// A run advances by replay until blocked. The core queues a run task; a worker
// replays the pipeline function against its event log and reports every command the
// replay issued. Commands beyond those already in the event log are applied:
// steps are queued on their queue, sleeps arm timers. When a step finishes or a
// timer fires, the run gets another run task, and so on until the run
// function returns (completed) or raises (failed).
//
// A step attempt that fails or times out is retried under the step's retry
// policy; only the step's final result reaches the run.
//
// A replay must repeat the commands already in the event log: same kind and
// target, in the same order. One that doesn't (non-deterministic pipeline code,
// or a deploy that changed it without a version marker) makes the run
// stuck. Its steps and timers carry on, but it gets no run tasks until
// ResumeRun, typically after a fix is deployed; a consistent replay then
// sets it running again.
//
// Not yet: resuming runs after a leader restart (#17).

// DefaultQueue is where pipelines and steps go when no queue is named.
const DefaultQueue = "default"

// --- in-memory model ---

type runState struct {
	rec     state.Run
	task    *runTaskState       // the queued or dispatched run task, if any
	replay  bool                // a result arrived while task was dispatched: replay again after it
	tasks   int                 // run tasks created so far
	cmds    []Command           // commands recorded in the event log, in order
	markers map[string]int      // version markers decided so far, by name
	steps   map[int]*stepState  // unfinished steps by seq
	timers  map[int]*runTimer   // pending timers by seq
	waits   map[int]*signalWait // wait_signal commands not answered yet, by seq
	signals []keptSignal        // signals received that no wait took yet, oldest first
	// Fan-out limits: steps holding a slot, by group, and steps waiting for
	// one, in the order they asked. See limits.go.
	slots     map[string]int
	throttled []*stepState
	finishing bool // finishRun is ending it: children ending meanwhile don't report to it
}

type runTaskState struct {
	rec   state.RunTask
	run   *runState
	lease *timerRef
}

type stepState struct {
	rec         state.Step
	run         *runState
	retry       RetryPolicy
	queued      string            // attempt ID waiting on the queue for a worker
	attempt     *stepAttemptState // the dispatched attempt
	wait        *timerRef         // pending retry, while in retry_wait
	output      *OutputSpec       // where its data version goes; nil = it produces none
	inputs      []InputRef
	limits      []ParallelLimit
	slot        bool // holds a slot in each of its limits' groups
	memo        memoPolicy
	dataVersion string  // the committed (or reused) data version, once there is one
	flight      *flight // the flight it waits on, while waiting
}

type stepAttemptState struct {
	rec     state.StepAttempt
	step    *stepState
	timeout *timerRef
	lease   *timerRef
}

type runTimer struct {
	rec state.Timer
	run *runState
	ref *timerRef
}

func (c *Core) dirtyRun(h *runState)         { dirty(c.b, &c.b.runs, h) }
func (c *Core) dirtyRunTask(t *runTaskState) { dirty(c.b, &c.b.runTasks, t) }
func (c *Core) dirtyStep(l *stepState)       { dirty(c.b, &c.b.steps, l) }
func (c *Core) dirtyStepAttempt(a *stepAttemptState) {
	dirty(c.b, &c.b.stepAttempts, a)
}
func (c *Core) dirtyTimer(t *runTimer) { dirty(c.b, &c.b.timers, t) }

func (c *Core) emitH(typ state.EventType, h *runState, attemptID string, payload any) {
	c.emitRun(typ, h.rec.Tenant, h.rec.ID, attemptID, payload)
}

// --- API ---

type StartRunRequest struct {
	parent  *parentLink // set for child runs and continue_as_new; see children.go
	Tenant  string
	Name    string // registered pipeline function
	Queue   string // default DefaultQueue
	Trigger string // default "manual"
	Params  map[string]any
}

type StartRunResult struct {
	RunID string
}

// StartRun creates a run and queues its first run task.
func (c *Core) StartRun(ctx context.Context, req StartRunRequest) (StartRunResult, error) {
	return submit(ctx, c, func(c *Core) (StartRunResult, error) { return c.startRun(req) })
}

func (c *Core) startRun(req StartRunRequest) (StartRunResult, error) {
	if req.Tenant == "" || req.Name == "" {
		return StartRunResult{}, fmt.Errorf("%w: a run needs a tenant and a pipeline name", ErrInvalid)
	}
	if !c.pipelineNames[req.Name] {
		return StartRunResult{}, fmt.Errorf("%w: no registered build defines run %q; start a worker whose code has it",
			ErrUnknownPipeline, req.Name)
	}
	if p := c.deletedProjectOf(req.Name); p != "" {
		return StartRunResult{}, fmt.Errorf("%w: pipeline %s belongs to project %s, which was deleted; deploy it again to run it",
			ErrUnknownPipeline, req.Name, p)
	}
	if req.Queue == "" {
		req.Queue = DefaultQueue
	}
	if req.Trigger == "" {
		req.Trigger = "manual"
	}
	h := &runState{
		rec: state.Run{
			ID: c.cfg.NewID(), Tenant: req.Tenant, Name: req.Name, Queue: req.Queue, Trigger: req.Trigger,
			Params: req.Params, Status: state.RunRunning, CreatedAt: c.now(),
		},
		steps:   map[int]*stepState{},
		timers:  map[int]*runTimer{},
		waits:   map[int]*signalWait{},
		markers: map[string]int{},
		slots:   map[string]int{},
	}
	if p := req.parent; p != nil {
		h.rec.ParentID, h.rec.ParentSeq, h.rec.ParentWaits = p.id, p.seq, p.waits
	}
	c.runs[h.rec.ID] = h
	c.dirtyRun(h)
	c.emitH(state.EventRunStarted, h, "", runStartedPayload{
		Name: req.Name, Queue: req.Queue, Trigger: req.Trigger, Params: req.Params, Parent: h.rec.ParentID})
	c.queueRunTask(h)
	return StartRunResult{RunID: h.rec.ID}, nil
}

// CommandKind is what a run asked for at one point of its replay.
type CommandKind string

const (
	CommandStep  CommandKind = "step"
	CommandSleep CommandKind = "sleep"
	// CommandLatestDataVersion asks for a dataset partition's newest committed
	// data version. The server answers at once and records the answer
	// (QueryAnswered), so every replay sees the same one.
	CommandLatestDataVersion CommandKind = "latest_version"
	// CommandWaitSignal waits for a signal named Name; Duration, if set, is
	// its timeout. See signals.go.
	CommandWaitSignal CommandKind = "wait_signal"
	// CommandStartRun starts the run Name with Args as its params, on Queue
	// (default: this run's); with Wait, this run waits for its result. See
	// children.go.
	CommandStartRun CommandKind = "start_run"
)

// Command is one step a run's replay issued, numbered from 1 in the order
// the pipeline function made the calls.
type Command struct {
	Seq  int         `json:"seq"`
	Kind CommandKind `json:"kind"`
	// Step.
	Name    string          `json:"name,omitempty"`
	Queue   string          `json:"queue,omitempty"` // default: the run's queue
	Args    json.RawMessage `json:"args,omitempty"`
	Timeout time.Duration   `json:"timeout,omitempty"` // per attempt, enforced by the leader; 0 = none
	Retry   *RetryPolicy    `json:"retry,omitempty"`   // nil = no retries
	Output  *OutputSpec     `json:"output,omitempty"`  // the step produces a data version here
	Inputs  []InputRef      `json:"inputs,omitempty"`  // data versions passed to the step
	Limits  []ParallelLimit `json:"limits,omitempty"`  // fan-out limits; see limits.go
	// Start run.
	Wait bool `json:"wait,omitempty"`
	// Memo: "" or "never" (default), "fingerprint", or "ttl:<duration>". A
	// step with an output reuses a data version; one without reuses a result.
	Memo string `json:"memo,omitempty"`
	// Sleep.
	Duration time.Duration `json:"duration,omitempty"`
	// Latest data version.
	Dataset   string `json:"dataset,omitempty"`
	Partition string `json:"partition,omitempty"`
	// Source is where the pipeline code made the call ("app/pipelines.py:42"), for
	// error messages.
	Source string `json:"source,omitempty"`
}

// RetryPolicy says how often and when a failed step attempt is retried.
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

// RunOutcome is how the pipeline function ended: it returned (completed) or
// raised (failed).
type RunOutcome struct {
	Status  string          `json:"status"` // state.RunCompleted, state.RunFailed or state.RunContinued
	Result  json.RawMessage `json:"result,omitempty"`
	Params  map[string]any  `json:"params,omitempty"` // continued: the next run's params
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
}

// ReasonNondeterminism: the run's replay contradicted its event log, or (as
// a TaskError reason) the pipeline code read the clock or randomness. Either
// makes the run stuck until a new build resumes it.
const ReasonNondeterminism = "nondeterminism"

// TaskError is a failure of the worker rather than of user code, such as a
// pipeline function that can't be imported.
type TaskError struct {
	Reason  string
	Message string
}

// RunTaskResult is what a worker reports after replaying a run.
type RunTaskResult struct {
	TaskID  string
	BuildID string
	// Commands is every command the replay issued, in order, starting at
	// seq 1 — including those already in the event log.
	Commands []Command
	// Markers is every version marker the replay decided (ctx.version),
	// including those already recorded.
	Markers []Marker
	Outcome *RunOutcome // the pipeline function ended
	Error   *TaskError  // the replay itself failed
}

// Marker is a version marker's decision: which version of a change a run
// takes. Markers are recorded by name, outside the command sequence, so
// adding one to code doesn't contradict the event logs of running pipelines; the
// SDK decides (the old version inside recorded history, the newest at its
// frontier) and the server keeps the first decision per name.
type Marker struct {
	Name    string `json:"name"`
	Version int    `json:"version"`
}

// CompleteRunTask applies a run task's result. It returns once the result
// is durable.
func (c *Core) CompleteRunTask(ctx context.Context, res RunTaskResult) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) { return struct{}{}, c.completeRunTask(res) })
	return err
}

// StepResult is what a worker reports after running a step attempt.
type StepResult struct {
	AttemptID string
	Status    string // state.AttemptSucceeded or state.AttemptFailed
	Result    json.RawMessage
	Reason    string
	Message   string
	// NoRetry: the failure will repeat on every attempt (bad arguments, a
	// non-retryable error raised by the step), so don't retry it.
	NoRetry bool
	// Output describes what a succeeded attempt of a step with an output
	// wrote to its staging prefix.
	Output *StepOutput
}

// CompleteStep records a step attempt's result. It returns once the result is
// durable. An attempt that is unknown or already finished (for example
// because its run was cancelled) gets ErrNotFound; the worker drops it.
func (c *Core) CompleteStep(ctx context.Context, res StepResult) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) { return struct{}{}, c.completeStep(res) })
	return err
}

// ResumeRun gives a stuck run a new run task, typically after a fix was
// deployed. It is a no-op for a run that isn't stuck or already has one.
func (c *Core) ResumeRun(ctx context.Context, runID string) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		if h := c.runs[runID]; h != nil {
			if h.rec.Status == state.RunStuck && h.task == nil {
				c.queueRunTask(h)
			}
			return struct{}{}, nil
		}
		if _, err := c.cfg.Store.GetRun(ctx, runID); errors.Is(err, state.ErrNotFound) {
			return struct{}{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		} else if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

// CancelRun cancels an open run: queued work is withdrawn, dispatched step
// attempts are recorded as cancelled and their results ignored, timers stop.
// Cancelling a finished run is a no-op.
func (c *Core) CancelRun(ctx context.Context, runID, by string) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		if h := c.runs[runID]; h != nil {
			c.finishRun(h, state.RunCancelled, "cancelled", "cancelled by "+by, nil)
			return struct{}{}, nil
		}
		if _, err := c.cfg.Store.GetRun(ctx, runID); errors.Is(err, state.ErrNotFound) {
			return struct{}{}, fmt.Errorf("%w: run %s", ErrNotFound, runID)
		} else if err != nil {
			return struct{}{}, err
		}
		return struct{}{}, nil
	})
	return err
}

// --- run tasks ---

func (c *Core) queueRunTask(h *runState) {
	h.tasks++
	t := &runTaskState{
		rec: state.RunTask{
			ID: c.cfg.NewID(), RunID: h.rec.ID, Number: h.tasks, Term: c.cfg.Term,
			Status: state.RunTaskQueued, QueuedAt: c.now(),
		},
		run: h,
	}
	h.task = t
	c.runTasks[t.rec.ID] = t
	c.dirtyRunTask(t)
	c.emitH(state.EventRunTaskQueued, h, t.rec.ID, runTaskPayload{Number: t.rec.Number})
	c.enqueueRunTask(t)
}

// enqueueRunTask puts a queued run task on its run's queue.
func (c *Core) enqueueRunTask(t *runTaskState) {
	h := t.run
	task := Task{Kind: TaskRun, ID: t.rec.ID, Queue: h.rec.Queue, RunID: h.rec.ID, Name: h.rec.Name}
	c.enqueue(task, func(c *Core, workerID string) Task {
		t.rec.Status, t.rec.WorkerID, t.rec.DispatchedAt = state.RunTaskDispatched, workerID, ptr(c.now())
		c.dirtyRunTask(t)
		c.emitH(state.EventRunTaskDispatched, h, t.rec.ID, runTaskPayload{Number: t.rec.Number, Worker: workerID})
		c.armRunTaskLease(t)
		return task
	})
}

// wake gives h a chance to make progress after a result arrived.
func (c *Core) wake(h *runState) {
	switch {
	case !state.RunOpen(h.rec.Status):
	case h.task == nil && h.rec.Status == state.RunStuck:
		// Replaying the same code would get stuck again; wait for ResumeRun.
	case h.task == nil:
		c.queueRunTask(h)
	case h.task.rec.Status == state.RunTaskDispatched:
		h.replay = true // its event log may predate this result
	}
	// A queued run task reads its event log when dispatched, so it will see
	// the result.
}

func (c *Core) completeRunTask(res RunTaskResult) error {
	t := c.runTasks[res.TaskID]
	if t == nil {
		return fmt.Errorf("%w: run task %s is unknown or finished", ErrNotFound, res.TaskID)
	}
	if t.rec.Status != state.RunTaskDispatched {
		return fmt.Errorf("%w: run task %s was not dispatched", ErrInvalid, res.TaskID)
	}
	h := t.run
	t.lease.stop()
	delete(c.runTasks, t.rec.ID)
	if h.task == t {
		h.task = nil
	}
	t.rec.BuildID, t.rec.FinishedAt = res.BuildID, ptr(c.now())
	c.dirtyRunTask(t)

	if err := validateRunTaskResult(res); err != nil {
		c.failRunTask(t, "invalid_result", err.Error())
		if state.RunOpen(h.rec.Status) {
			c.markStuck(h, "invalid_result", err.Error(), 0)
		}
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	if res.Error != nil {
		c.failRunTask(t, res.Error.Reason, res.Error.Message)
		if res.Error.Reason == ReasonNondeterminism {
			if state.RunOpen(h.rec.Status) {
				c.markStuck(h, ReasonNondeterminism, res.Error.Message, 0)
			}
			return nil
		}
		if state.RunOpen(h.rec.Status) {
			c.finishRun(h, state.RunFailed, "run_task_failed", res.Error.Message, nil)
		}
		return nil
	}
	if state.RunOpen(h.rec.Status) {
		if seq, msg := checkReplay(h.cmds, res.Commands); msg != "" {
			c.failRunTask(t, ReasonNondeterminism, msg)
			c.markStuck(h, ReasonNondeterminism, msg, seq)
			return nil // the worker did its job; the pipeline code is at fault
		}
		if msg := checkMarkers(h.markers, res.Markers); msg != "" {
			c.failRunTask(t, ReasonNondeterminism, msg)
			c.markStuck(h, ReasonNondeterminism, msg, 0)
			return nil
		}
	}

	var fresh []Command
	if len(res.Commands) > h.rec.Commands {
		fresh = res.Commands[h.rec.Commands:]
	}
	var freshMarkers []Marker
	for _, m := range res.Markers {
		if _, ok := h.markers[m.Name]; !ok {
			freshMarkers = append(freshMarkers, m)
		}
	}
	t.rec.Status = state.RunTaskCompleted
	c.emitH(state.EventRunTaskCompleted, h, t.rec.ID, runTaskCompletedPayload{
		Number: t.rec.Number, BuildID: res.BuildID, Commands: fresh, Markers: freshMarkers, Outcome: res.Outcome})
	if !state.RunOpen(h.rec.Status) {
		return nil // finished while the replay ran; nothing to apply
	}

	h.rec.BuildID = res.BuildID
	for _, m := range freshMarkers {
		h.markers[m.Name] = m.Version
	}
	if h.rec.Status == state.RunStuck {
		h.rec.Status, h.rec.Reason, h.rec.Message = state.RunRunning, "", ""
	}
	c.dirtyRun(h)
	for _, cmd := range fresh {
		h.rec.Commands = cmd.Seq
		h.cmds = append(h.cmds, cmd)
		switch cmd.Kind {
		case CommandStep:
			c.scheduleStep(h, cmd)
		case CommandSleep:
			c.startTimer(h, cmd)
		case CommandLatestDataVersion:
			c.answerLatestDataVersion(h, cmd)
		case CommandWaitSignal:
			c.waitSignal(h, cmd)
		case CommandStartRun:
			c.startChild(h, cmd)
		}
	}
	if o := res.Outcome; o != nil && o.Status == state.RunContinued {
		c.continueAsNew(h, o.Params)
		return nil
	}
	if o := res.Outcome; o != nil {
		c.finishRun(h, o.Status, o.Reason, o.Message, o.Result)
		return nil
	}
	if h.replay {
		h.replay = false
		c.queueRunTask(h)
	}
	return nil
}

// checkReplay compares a replay's commands with those in the event log. It
// returns the first contradicting seq and a message, or "" if the replay
// extends the event log.
func checkReplay(logged, replayed []Command) (int, string) {
	for i, old := range logged {
		if i >= len(replayed) {
			return old.Seq, fmt.Sprintf("replay issued %d commands but the event log has %d: missing command %d, %s",
				len(replayed), len(logged), old.Seq, describeCommand(old))
		}
		if got := replayed[i]; got.Kind != old.Kind || got.Name != old.Name || got.Dataset != old.Dataset {
			return old.Seq, fmt.Sprintf("command %d: replay issued %s, the event log has %s",
				old.Seq, describeCommand(got), describeCommand(old))
		}
	}
	return 0, ""
}

// checkMarkers compares a replay's version markers with those recorded: a
// marker decided differently means the replay took another branch.
func checkMarkers(recorded map[string]int, replayed []Marker) string {
	for _, m := range replayed {
		if v, ok := recorded[m.Name]; ok && v != m.Version {
			return fmt.Sprintf("version marker %q: replay took version %d, the event log has %d", m.Name, m.Version, v)
		}
	}
	return ""
}

func describeCommand(cmd Command) string {
	s := string(cmd.Kind)
	switch cmd.Kind {
	case CommandStep:
		s += " " + strconv.Quote(cmd.Name)
	case CommandLatestDataVersion:
		s += " " + strconv.Quote(cmd.Dataset)
	case CommandWaitSignal, CommandStartRun:
		s += " " + strconv.Quote(cmd.Name)
	}
	if cmd.Source != "" {
		s += " at " + cmd.Source
	}
	return s
}

func validateRunTaskResult(res RunTaskResult) error {
	if res.Outcome != nil && res.Error != nil {
		return errors.New("a result has an outcome or an error, not both")
	}
	seen := map[string]int{}
	for i, m := range res.Markers {
		switch v, ok := seen[m.Name]; {
		case m.Name == "":
			return fmt.Errorf("markers[%d]: a version marker needs a name", i)
		case m.Version < 0:
			return fmt.Errorf("markers[%d]: version must not be negative", i)
		case ok && v != m.Version:
			return fmt.Errorf("markers[%d]: %q decided twice, as %d and %d", i, m.Name, v, m.Version)
		}
		seen[m.Name] = m.Version
	}
	if o := res.Outcome; o != nil && o.Status != state.RunCompleted && o.Status != state.RunFailed && o.Status != state.RunContinued {
		return fmt.Errorf("outcome status %q is not %q, %q or %q", o.Status, state.RunCompleted, state.RunFailed, state.RunContinued)
	}
	for i, cmd := range res.Commands {
		if cmd.Seq != i+1 {
			return fmt.Errorf("commands[%d]: seq is %d, want %d", i, cmd.Seq, i+1)
		}
		switch cmd.Kind {
		case CommandStep:
			if cmd.Name == "" {
				return fmt.Errorf("commands[%d]: step without a name", i)
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
			if err := validateLimits(cmd.Limits); err != nil {
				return fmt.Errorf("commands[%d]: %v", i, err)
			}
			for j, in := range cmd.Inputs {
				if in.DataVersion == "" {
					return fmt.Errorf("commands[%d].inputs[%d]: no data version", i, j)
				}
			}
			if _, err := parseMemo(cmd.Memo); err != nil {
				return fmt.Errorf("commands[%d]: %v", i, err)
			}
		case CommandSleep:
			if cmd.Duration <= 0 {
				return fmt.Errorf("commands[%d]: sleep needs a positive duration", i)
			}
		case CommandLatestDataVersion:
			if cmd.Dataset == "" {
				return fmt.Errorf("commands[%d]: latest_version needs a dataset", i)
			}
		case CommandStartRun:
			if cmd.Name == "" {
				return fmt.Errorf("commands[%d]: start_run needs a pipeline name", i)
			}
			if _, err := decodeParams(cmd.Args); err != nil {
				return fmt.Errorf("commands[%d]: %v", i, err)
			}
		case CommandWaitSignal:
			if cmd.Name == "" {
				return fmt.Errorf("commands[%d]: wait_signal needs a signal name", i)
			}
			if cmd.Duration < 0 {
				return fmt.Errorf("commands[%d]: wait_signal timeout must not be negative", i)
			}
		default:
			return fmt.Errorf("commands[%d]: unknown kind %q", i, cmd.Kind)
		}
	}
	return nil
}

func (c *Core) failRunTask(t *runTaskState, reason, message string) {
	t.rec.Status, t.rec.Reason, t.rec.Message = state.RunTaskFailed, reason, message
	if t.rec.FinishedAt == nil {
		t.rec.FinishedAt = ptr(c.now())
	}
	c.dirtyRunTask(t)
	c.emitH(state.EventRunTaskFailed, t.run, t.rec.ID,
		runTaskFailedPayload{Number: t.rec.Number, Reason: reason, Message: message})
}

// markStuck parks a run until ResumeRun and a consistent replay. Its steps
// and timers carry on. seq is the contradicting command, if any.
func (c *Core) markStuck(h *runState, reason, message string, seq int) {
	h.rec.Status, h.rec.Reason, h.rec.Message = state.RunStuck, reason, message
	h.replay = false
	c.dirtyRun(h)
	c.emitH(state.EventRunStuck, h, "", runStuckPayload{Reason: reason, Message: message, Seq: seq})
}

// finishRun closes h: unfinished steps are cancelled, timers stop, a queued
// run task is withdrawn. A dispatched run task stays known so its result
// is still accepted (and ignored).
func (c *Core) finishRun(h *runState, status, reason, message string, result json.RawMessage) {
	h.finishing = true
	c.cancelChildren(h, status)
	h.throttled = nil // cancelled below; slots freed meanwhile must not start them
	for _, seq := range sortedKeys(h.steps) {
		c.cancelStep(h.steps[seq], "run "+status)
	}
	for _, seq := range sortedKeys(h.timers) {
		c.cancelTimer(h, h.timers[seq])
	}
	if t := h.task; t != nil && t.rec.Status == state.RunTaskQueued {
		c.unqueue(h.rec.Queue, t.rec.ID)
		delete(c.runTasks, t.rec.ID)
		c.failRunTask(t, "cancelled", "run "+status)
		h.task = nil
	}
	h.rec.Status, h.rec.Reason, h.rec.Message, h.rec.Result = status, reason, message, result
	h.rec.FinishedAt = ptr(c.now())
	c.dirtyRun(h)
	c.emitH(state.EventRunFinished, h, "", runFinishedPayload{Status: status, Reason: reason, Message: message, Result: result})
	delete(c.runs, h.rec.ID)
	c.childEnded(h)
}

// --- steps ---

func (c *Core) scheduleStep(h *runState, cmd Command) {
	queue := cmd.Queue
	if queue == "" {
		queue = h.rec.Queue
	}
	l := &stepState{
		rec: state.Step{
			RunID: h.rec.ID, Seq: cmd.Seq, Name: cmd.Name, Queue: queue, Args: cmd.Args, Timeout: cmd.Timeout,
			Status: state.StepScheduled, ScheduledAt: c.now(),
		},
		run: h,
	}
	if cmd.Retry != nil {
		l.retry = *cmd.Retry
		l.rec.Retry = marshalPayload(state.EventStepScheduled, cmd.Retry)
	}
	if cmd.Output != nil {
		l.output = cmd.Output
		l.rec.Output = marshalPayload(state.EventStepScheduled, cmd.Output)
	}
	if len(cmd.Inputs) > 0 {
		l.inputs = cmd.Inputs
		l.rec.Inputs = marshalPayload(state.EventStepScheduled, cmd.Inputs)
	}
	l.limits = cmd.Limits
	l.memo, _ = parseMemo(cmd.Memo) // validated with the run task result
	h.steps[cmd.Seq] = l
	c.dirtyStep(l)
	inputs, reason, msg := c.checkStepIO(l)
	var match *memoMatch
	if reason == "" {
		match = c.memoize(l, inputs)
	}
	c.emitH(state.EventStepScheduled, h, "", stepScheduledPayload{
		Seq: cmd.Seq, Name: cmd.Name, Queue: queue, Args: cmd.Args, Timeout: cmd.Timeout, Retry: cmd.Retry,
		Output: cmd.Output, Inputs: cmd.Inputs, Memo: cmd.Memo, Fingerprint: l.rec.Fingerprint, RunReason: l.rec.RunReason})
	switch {
	case reason != "":
		c.finishStep(l, state.StepFailed, nil, reason, msg)
		c.wake(h)
	case match != nil:
		c.reuseStep(l, *match)
	default:
		c.runOrWait(l)
	}
}

// queueStepAttempt puts the step's next attempt on its queue. The attempt is
// recorded when a worker takes it.
func (c *Core) queueStepAttempt(l *stepState) {
	h := l.run
	l.queued = c.cfg.NewID()
	task := Task{Kind: TaskStep, ID: l.queued, Queue: l.rec.Queue, RunID: h.rec.ID, Seq: l.rec.Seq, Name: l.rec.Name, Args: l.rec.Args}
	for _, in := range l.inputs {
		ti := TaskInput{Name: in.Name, DataVersion: in.DataVersion}
		if p, err := c.cfg.Store.GetDataVersion(c.ctx, in.DataVersion); err == nil {
			ti.Location = p.Location // committed data versions are immutable: read once, here
		}
		task.Inputs = append(task.Inputs, ti)
	}
	task.Output = l.output
	if c.cfg.DataVersions != nil {
		o := l.outputSpec()
		task.Staging = c.cfg.Layout.StepStaging(h.rec.Tenant, o.Dataset, o.Partition, task.ID)
	}
	c.enqueue(task, func(c *Core, workerID string) Task {
		l.queued = ""
		l.rec.Attempts++
		l.rec.Status = state.StepDispatched
		a := &stepAttemptState{
			rec: state.StepAttempt{
				ID: task.ID, RunID: h.rec.ID, Seq: l.rec.Seq, Number: l.rec.Attempts, Term: c.cfg.Term,
				WorkerID: workerID, Status: state.AttemptDispatched, DispatchedAt: c.now(), Staging: task.Staging,
			},
			step: l,
		}
		l.attempt = a
		c.stepAttempts[a.rec.ID] = a
		c.dirtyStep(l)
		c.dirtyStepAttempt(a)
		c.emitH(state.EventStepAttemptDispatched, h, a.rec.ID,
			stepAttemptDispatchedPayload{Seq: l.rec.Seq, Number: a.rec.Number, Worker: workerID})
		if l.rec.Timeout > 0 {
			c.armStepTimeout(a, l.rec.Timeout)
		}
		c.armStepLease(a)
		return task
	})
}

// armStepTimeout fails the attempt if no result arrives within d (the step's
// timeout, or what is left of it after a leader restart). A result that
// arrives later is ignored.
func (c *Core) armStepTimeout(a *stepAttemptState, d time.Duration) {
	l := a.step
	a.timeout = c.after(d, func(c *Core) {
		if c.stepAttempts[a.rec.ID] != a {
			return
		}
		msg := fmt.Sprintf("no result within %s", l.rec.Timeout)
		c.finishStepAttempt(a, state.AttemptFailed, "timeout", msg)
		c.stepAttemptFailed(l, "timeout", msg, false)
	})
}

func (c *Core) completeStep(res StepResult) error {
	if res.Status != state.AttemptSucceeded && res.Status != state.AttemptFailed {
		return fmt.Errorf("%w: step result status %q is not %q or %q", ErrInvalid, res.Status, state.AttemptSucceeded, state.AttemptFailed)
	}
	a := c.stepAttempts[res.AttemptID]
	if a == nil {
		return fmt.Errorf("%w: step attempt %s is unknown or finished", ErrNotFound, res.AttemptID)
	}
	l := a.step
	if res.Output != nil && res.Status != state.AttemptSucceeded {
		return fmt.Errorf("%w: only a succeeded attempt reports an output", ErrInvalid)
	}
	if res.Status == state.AttemptSucceeded && l.output != nil {
		return c.completeStepWithOutput(a, res, *l.output)
	}
	if res.Status == state.AttemptSucceeded && res.Output != nil {
		return c.completeStepWithOutput(a, res, OutputSpec{Dataset: ImplicitDataset(l.rec.Name)})
	}
	c.finishStepAttempt(a, res.Status, res.Reason, res.Message)
	if res.Status == state.AttemptFailed {
		c.stepAttemptFailed(l, res.Reason, res.Message, res.NoRetry)
		return nil
	}
	c.finishStep(l, state.StepCompleted, res.Result, res.Reason, res.Message)
	c.wake(l.run)
	return nil
}

// stepAttemptFailed retries the step if its policy allows, otherwise fails
// it and lets the run see the failure.
func (c *Core) stepAttemptFailed(l *stepState, reason, message string, noRetry bool) {
	if noRetry || nonRetryable[reason] || l.rec.Attempts > l.retry.Limit {
		c.finishStep(l, state.StepFailed, nil, reason, message)
		c.wake(l.run)
		return
	}
	d := l.retry.delay(l.rec.Attempts)
	l.rec.Status, l.rec.RetryAt = state.StepRetryWait, ptr(c.now().Add(d))
	l.rec.Reason, l.rec.Message = reason, message // the last failure, until the step finishes
	c.dirtyStep(l)
	c.emitH(state.EventStepRetryScheduled, l.run, "",
		stepRetryScheduledPayload{Seq: l.rec.Seq, NextAttempt: l.rec.Attempts + 1, RetryAt: *l.rec.RetryAt})
	c.armStepRetry(l, d)
}

// armStepRetry queues the step's next attempt after d.
func (c *Core) armStepRetry(l *stepState, d time.Duration) {
	l.wait = c.after(d, func(c *Core) {
		if l.rec.Status != state.StepRetryWait {
			return // cancelled meanwhile
		}
		l.wait = nil
		l.rec.Status, l.rec.RetryAt = state.StepScheduled, nil
		c.dirtyStep(l)
		c.startAttempt(l)
	})
}

func (c *Core) finishStepAttempt(a *stepAttemptState, status, reason, message string) {
	l := a.step
	a.timeout.stop()
	a.lease.stop()
	a.rec.Status, a.rec.Reason, a.rec.Message, a.rec.FinishedAt = status, reason, message, ptr(c.now())
	c.dirtyStepAttempt(a)
	c.emitH(state.EventStepAttemptFinished, l.run, a.rec.ID,
		stepAttemptFinishedPayload{Seq: l.rec.Seq, Number: a.rec.Number, Status: status, Reason: reason, Message: message})
	delete(c.stepAttempts, a.rec.ID)
	l.attempt = nil
	c.releaseStep(l.rec.Queue)
	c.freeSlots(l)
	if status != state.AttemptSucceeded {
		c.discardStaging(c.staging(a))
	}
}

func (c *Core) finishStep(l *stepState, status string, result json.RawMessage, reason, message string) {
	l.rec.Status, l.rec.Result, l.rec.Reason, l.rec.Message = status, result, reason, message
	l.rec.RetryAt, l.rec.FinishedAt = nil, ptr(c.now())
	c.dirtyStep(l)
	c.emitH(state.EventStepFinished, l.run, "",
		stepFinishedPayload{Seq: l.rec.Seq, Status: status, Result: result, Reason: reason, Message: message,
			DataVersion: l.dataVersion, Reused: l.rec.Reused})
	delete(l.run.steps, l.rec.Seq)
	c.landFlight(l)
}

// cancelStep withdraws a queued attempt or records the dispatched one as
// cancelled. Telling the worker to stop needs the worker API (heartbeats);
// until then its result is simply ignored.
func (c *Core) cancelStep(l *stepState, message string) {
	l.wait.stop()
	l.wait = nil
	c.leaveFlight(l)
	c.unthrottle(l)
	if l.queued != "" {
		c.unqueue(l.rec.Queue, l.queued)
		l.queued = ""
		c.freeSlots(l)
	}
	if a := l.attempt; a != nil {
		c.finishStepAttempt(a, state.AttemptCancelled, "cancelled", message)
	}
	c.finishStep(l, state.StepCancelled, nil, "cancelled", message)
}

// --- timers ---

func (c *Core) startTimer(h *runState, cmd Command) {
	tm := &runTimer{
		rec: state.Timer{
			RunID: h.rec.ID, Seq: cmd.Seq, FireAt: c.now().Add(cmd.Duration),
			Status: state.TimerPending, CreatedAt: c.now(),
		},
		run: h,
	}
	h.timers[cmd.Seq] = tm
	c.dirtyTimer(tm)
	c.emitH(state.EventTimerStarted, h, "", timerStartedPayload{Seq: cmd.Seq, FireAt: tm.rec.FireAt})
	tm.ref = c.after(cmd.Duration, func(c *Core) { c.fireTimer(tm) })
}

func (c *Core) fireTimer(tm *runTimer) {
	if tm.rec.Status != state.TimerPending {
		return
	}
	h := tm.run
	tm.rec.Status, tm.rec.FiredAt = state.TimerFired, ptr(c.now())
	c.dirtyTimer(tm)
	delete(h.timers, tm.rec.Seq)
	if w := h.waits[tm.rec.Seq]; w != nil { // a wait_signal's timeout
		c.deliverSignal(h, w, nil)
		return
	}
	c.emitH(state.EventTimerFired, h, "", timerPayload{Seq: tm.rec.Seq})
	c.wake(h)
}

func (c *Core) cancelTimer(h *runState, tm *runTimer) {
	tm.ref.stop()
	tm.rec.Status = state.TimerCancelled
	c.dirtyTimer(tm)
	c.emitH(state.EventTimerCancelled, h, "", timerPayload{Seq: tm.rec.Seq})
	delete(h.timers, tm.rec.Seq)
}

func sortedKeys[V any](m map[int]V) []int {
	keys := make([]int, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Ints(keys)
	return keys
}

// answerLatestDataVersion answers a latest_version query from the catalog and
// records the answer, then lets the run replay with it.
func (c *Core) answerLatestDataVersion(h *runState, cmd Command) {
	var answer *dataVersionAnswer
	p, err := c.cfg.Store.LatestDataVersion(c.ctx, h.rec.Tenant, cmd.Dataset, cmd.Partition)
	switch {
	case err == nil:
		answer = &dataVersionAnswer{ID: p.ID, Dataset: p.Dataset, Partition: p.Partition, Location: p.Location,
			ContentHash: p.ContentHash, Meta: p.Meta, Stats: p.Stats, CommittedAt: p.CommittedAt}
	case !errors.Is(err, state.ErrNotFound):
		c.log.Error("latest data version lookup failed", "run", h.rec.ID, "seq", cmd.Seq, "err", err)
	}
	c.emitH(state.EventQueryAnswered, h, "", queryAnsweredPayload{Seq: cmd.Seq, DataVersion: answer})
	h.replay = true
}
