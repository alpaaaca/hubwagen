package core

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
)

// Assignment is everything a runtime needs to run one attempt under the task
// contract (docs/task-contract.md). Templates are already rendered.
type Assignment struct {
	AttemptID  string
	RunID      string
	TaskID     string
	Attempt    int
	Term       int64
	Tenant     string
	Pipeline   string
	RunKey     string
	Generation int
	Interval   *spec.Interval

	Runtime spec.Runtime
	Command []string
	Args    []string
	Env     map[string]string
	Secrets []spec.Secret
	Timeout time.Duration

	Inputs  map[string]string       // input name → committed output URI
	Outputs map[string]OutputTarget // output name → where to write and commit
	// CommitURI is the task-level create-if-absent marker; whichever attempt
	// creates it wins all outputs.
	CommitURI string

	LogsURI   string
	ResultURI string
	// IdempotencyKey is stable across attempts of the same task in the same
	// run generation; tasks use it for external side effects.
	IdempotencyKey string
}

// OutputTarget is where an attempt writes one output (Staging) and the
// directory whose commit marker decides the winning attempt (Generation).
type OutputTarget struct {
	Staging    string `json:"staging"`
	Generation string `json:"generation"`
}

// Report is a runtime's account of an attempt.
type Report struct {
	AttemptID string
	// Status is AttemptRunning, AttemptSucceeded, AttemptFailed,
	// AttemptCancelled, AttemptSuperseded or AttemptLost.
	Status   string
	AgentID  string
	Reason   string // exit_code, timeout, exec_error, oom, ...
	Message  string
	ExitCode *int
	Result   json.RawMessage
	// Outputs are the committed outputs: this attempt's when it succeeded,
	// the winner's (read from the commit markers) when it was superseded.
	Outputs []OutputRef
	Winner  string // superseded: the attempt whose commit won
}

type OutputRef struct {
	Name     string
	URI      string
	Metadata map[string]any
}

// Report feeds a runtime report into the core. It never blocks.
func (c *Core) Report(r Report) {
	c.inbox.put(funcInput(func(c *Core) { c.handleReport(r) }))
}

// Reasons that will fail the same way on every attempt, so retrying is pointless.
var nonRetryable = map[string]bool{"exec_error": true, "render_error": true, "no_runtime": true, "result_error": true}

// --- run lifecycle ---

// startQueued starts queued runs of p while it has free slots.
func (c *Core) startQueued(p *pipelineState) {
	maxActive := 1
	if v := c.versions[p.rec.CurrentVersion]; v != nil {
		maxActive = v.spec.Concurrency.MaxActiveRuns
	}
	for p.running < maxActive && len(p.queued) > 0 {
		r := p.queued[0]
		p.queued = p.queued[1:]
		c.startRun(r, p)
	}
}

func (c *Core) startRun(r *runState, p *pipelineState) {
	r.rec.Status = state.RunRunning
	r.rec.StartedAt = ptr(c.now())
	p.running++
	c.dirtyRun(r)
	c.emitRun(state.EventRunStarted, r, nil)
	for _, id := range r.order {
		c.evaluate(r.tasks[id])
	}
}

// requestCancel cancels a run: queued runs finish immediately, running runs
// stop their tasks and finish once every attempt has reported.
func (c *Core) requestCancel(r *runState, reason, by string) {
	if r.cancelRequested || state.RunTerminal(r.rec.Status) {
		return
	}
	r.cancelRequested = true
	c.emitRun(state.EventCancelRequested, r, cancelRequestedPayload{Scope: "run", Reason: reason, RequestedBy: by})
	if r.rec.Status == state.RunQueued {
		p := c.pipelines[pipelineKey{r.rec.Tenant, r.rec.Pipeline}]
		for i, q := range p.queued {
			if q == r {
				p.queued = append(p.queued[:i], p.queued[i+1:]...)
				break
			}
		}
	}
	c.halt(r, reason)
}

// halt stops every unfinished task of r. Tasks with an attempt in flight are
// cancelled at the runtime and finish when it reports back.
func (c *Core) halt(r *runState, reason string) {
	for _, id := range r.order {
		t := r.tasks[id]
		switch {
		case state.TaskTerminal(t.rec.Status):
		case t.current != nil:
			c.cancelAttempt(t.current, reason)
		default:
			c.finishTask(t, state.TaskCancelled, "")
		}
	}
	c.maybeFinishRun(r)
}

func (c *Core) maybeFinishRun(r *runState) {
	if state.RunTerminal(r.rec.Status) {
		return
	}
	failed := false
	for _, id := range r.order {
		t := r.tasks[id]
		if !state.TaskTerminal(t.rec.Status) {
			return
		}
		if t.rec.Status == state.TaskFailed && t.def.OnFailure != "continue" || t.rec.Status == state.TaskUpstreamFailed {
			failed = true
		}
	}
	status := state.RunSucceeded
	switch {
	case r.cancelRequested:
		status = state.RunCancelled
	case failed:
		status = state.RunFailed
	}
	c.finishRun(r, status)
}

func (c *Core) finishRun(r *runState, status string) {
	wasRunning := r.rec.Status == state.RunRunning
	r.rec.Status = status
	r.rec.FinishedAt = ptr(c.now())
	c.dirtyRun(r)
	c.emitRun(state.EventRunFinished, r, runFinishedPayload{Status: status})

	p := c.pipelines[pipelineKey{r.rec.Tenant, r.rec.Pipeline}]
	if wasRunning {
		p.running--
	}
	// Keep the run in memory until the batch commits, so later inputs in the
	// same batch (an idempotent re-trigger, say) still find it.
	c.effect(func() {
		delete(c.runs, r.rec.ID)
		if c.runsByKey[r.rec.RunKey] == r {
			delete(c.runsByKey, r.rec.RunKey)
		}
	})
	c.startQueued(p)
}

// --- task lifecycle ---

// evaluate decides whether a pending task can run, applying its trigger rule
// to its dependencies.
func (c *Core) evaluate(t *taskState) {
	r := t.run
	if t.rec.Status != state.TaskPending || r.rec.Status != state.RunRunning || r.halting() {
		return
	}
	switch decision := c.triggerDecision(t); decision {
	case "":
	case state.TaskReady:
		c.markReady(t)
	default:
		c.finishTask(t, decision, "")
	}
}

// triggerDecision returns TaskReady, TaskSkipped, TaskUpstreamFailed, or ""
// to keep waiting.
func (c *Core) triggerDecision(t *taskState) string {
	allDone, anyFailed, anyBlocked, anySkipped := true, false, false, false
	for _, id := range t.def.DependsOn {
		d := t.run.tasks[id]
		switch d.rec.Status {
		case state.TaskSucceeded:
		case state.TaskFailed:
			anyFailed = true
			if d.def.OnFailure != "continue" {
				anyBlocked = true
			}
		case state.TaskUpstreamFailed, state.TaskCancelled:
			anyBlocked = true
		case state.TaskSkipped:
			anySkipped = true
		default:
			allDone = false
		}
	}

	switch t.def.TriggerRule {
	case "all_done":
		if allDone {
			return state.TaskReady
		}
	case "one_failed":
		if anyFailed {
			return state.TaskReady
		}
		if allDone {
			return state.TaskSkipped
		}
	default: // all_success; a failure with on_failure: continue counts as success
		switch {
		case anyBlocked:
			return state.TaskUpstreamFailed
		case !allDone:
		case anySkipped:
			return state.TaskSkipped
		default:
			return state.TaskReady
		}
	}
	return ""
}

func (c *Core) markReady(t *taskState) {
	t.rec.Status = state.TaskReady
	t.rec.ReadyAt = ptr(c.now())
	t.rec.RetryAt = nil
	c.dirtyTask(t)
	c.emitTask(state.EventTaskReady, t, nil)
	c.dispatch(t)
}

func (c *Core) finishTask(t *taskState, status, winner string) {
	if state.TaskTerminal(t.rec.Status) {
		return
	}
	t.retry.stop()
	t.retry = nil
	t.rec.Status = status
	t.rec.RetryAt = nil
	t.rec.WinningAttempt = winner
	t.rec.FinishedAt = ptr(c.now())
	c.dirtyTask(t)
	c.emitTask(state.EventTaskFinished, t, taskFinishedPayload{Status: status, WinningAttempt: winner})

	r := t.run
	if status == state.TaskFailed && t.def.OnFailure == "stop" && !r.halting() {
		r.stopping = true
		c.halt(r, fmt.Sprintf("task %s failed (on_failure: stop)", t.rec.TaskID))
	}
	for _, id := range r.ver.dependents[t.rec.TaskID] {
		c.evaluate(r.tasks[id])
	}
	c.maybeFinishRun(r)
}

// --- attempts ---

func (c *Core) dispatch(t *taskState) {
	r := t.run
	number := t.rec.Attempts + 1
	a := &attemptState{task: t, rec: state.Attempt{
		ID:           c.cfg.NewID(),
		RunID:        r.rec.ID,
		TaskID:       t.rec.TaskID,
		Number:       number,
		Term:         c.cfg.Term,
		Runtime:      t.def.Runtime.Kind,
		Pool:         t.def.Runtime.Pool,
		Status:       state.AttemptDispatched,
		LogsURI:      c.cfg.Layout.Logs(r.rec.Tenant, r.rec.Pipeline, r.rec.ID, t.rec.TaskID, number),
		DispatchedAt: c.now(),
	}}
	t.rec.Attempts = number
	t.rec.Status = state.TaskDispatched
	t.current = a
	c.attempts[a.rec.ID] = a
	c.dirtyTask(t)
	c.dirtyAttempt(a)

	asg, renderErr := c.assignment(a)
	c.emitAttempt(state.EventAttemptDispatched, a, attemptDispatchedPayload{
		Number: number, Runtime: a.rec.Runtime, Pool: a.rec.Pool, Outputs: asg.Outputs,
	})

	rt := c.cfg.Runtimes[t.def.Runtime.Kind]
	switch {
	case rt == nil:
		c.attemptDone(a, Report{Status: state.AttemptFailed, Reason: "no_runtime",
			Message: fmt.Sprintf("no runtime for kind %q in this deployment", t.def.Runtime.Kind)})
	case renderErr != nil:
		c.attemptDone(a, Report{Status: state.AttemptFailed, Reason: "render_error", Message: renderErr.Error()})
	default:
		c.effect(func() { rt.Dispatch(asg) })
	}
}

// assignment renders the task's templates for attempt a.
func (c *Core) assignment(a *attemptState) (Assignment, error) {
	t := a.task
	r := t.run
	def := t.def
	loc := r.ver.loc

	asg := Assignment{
		AttemptID: a.rec.ID, RunID: r.rec.ID, TaskID: def.ID, Attempt: a.rec.Number, Term: c.cfg.Term,
		Tenant: r.rec.Tenant, Pipeline: r.rec.Pipeline, RunKey: r.rec.RunKey, Generation: r.rec.Generation,
		Runtime: def.Runtime, Command: def.Command, Secrets: def.Secrets,
		Timeout: time.Duration(def.TimeoutSeconds) * time.Second,
		Inputs:  map[string]string{}, Outputs: map[string]OutputTarget{},
		LogsURI:        a.rec.LogsURI,
		CommitURI:      c.cfg.Layout.TaskCommit(r.rec.Tenant, r.rec.Pipeline, def.ID, r.rec.RunKey, r.rec.Generation),
		ResultURI:      c.cfg.Layout.Result(r.rec.Tenant, r.rec.ID, def.ID, a.rec.Number),
		IdempotencyKey: fmt.Sprintf("%s/gen-%d/%s", r.rec.RunKey, r.rec.Generation, def.ID),
	}

	tc := spec.Context{
		Run: spec.RunVars{ID: r.rec.ID, Key: r.rec.RunKey, Generation: r.rec.Generation,
			Date: r.rec.CreatedAt.In(loc).Format("2006-01-02")},
		Pipeline: r.rec.Pipeline, Tenant: r.rec.Tenant, Params: r.rec.Params,
		Inputs: asg.Inputs, Outputs: map[string]string{},
	}
	if r.rec.IntervalStart != nil && r.rec.IntervalEnd != nil {
		asg.Interval = &spec.Interval{Start: *r.rec.IntervalStart, End: *r.rec.IntervalEnd}
		tc.Interval = spec.IntervalVars{
			Start: r.rec.IntervalStart.In(loc).Format(time.RFC3339),
			End:   r.rec.IntervalEnd.In(loc).Format(time.RFC3339),
		}
	}
	for _, in := range def.Inputs {
		uri, ok := r.tasks[in.FromTask].outputs[in.FromOutput]
		if !ok {
			return asg, fmt.Errorf("input %s: %s.%s has no committed output", in.Name, in.FromTask, in.FromOutput)
		}
		asg.Inputs[in.Name] = uri
	}
	for _, out := range def.Outputs {
		gen := c.cfg.Layout.OutputGeneration(r.rec.Tenant, r.rec.Pipeline, def.ID, out.Name, r.rec.RunKey, r.rec.Generation)
		target := OutputTarget{Staging: layout.Staging(gen, a.rec.Number), Generation: gen}
		asg.Outputs[out.Name] = target
		tc.Outputs[out.Name] = target.Staging
	}

	for _, arg := range def.Args {
		v, err := spec.Render(arg, tc)
		if err != nil {
			return asg, fmt.Errorf("args: %w", err)
		}
		asg.Args = append(asg.Args, v)
	}
	if len(def.Env) > 0 {
		asg.Env = make(map[string]string, len(def.Env))
		for k, tmpl := range def.Env {
			v, err := spec.Render(tmpl, tc)
			if err != nil {
				return asg, fmt.Errorf("env %s: %w", k, err)
			}
			asg.Env[k] = v
		}
	}
	return asg, nil
}

func (c *Core) cancelAttempt(a *attemptState, reason string) {
	if a.cancelSent {
		return
	}
	a.cancelSent = true
	if rt := c.cfg.Runtimes[a.rec.Runtime]; rt != nil {
		id := a.rec.ID
		c.effect(func() { rt.Cancel(id, reason) })
	}
}

func (c *Core) handleReport(rep Report) {
	a := c.attempts[rep.AttemptID]
	if a == nil {
		// Late report for an attempt already finished (timed out, lost) or
		// from an older term. Its commit marker, if any, is what counts.
		c.log.Debug("ignoring report for unknown or finished attempt", "attempt", rep.AttemptID, "status", rep.Status)
		return
	}
	if rep.Status == state.AttemptRunning {
		c.attemptStarted(a, rep.AgentID)
		return
	}
	c.attemptDone(a, rep)
}

func (c *Core) attemptStarted(a *attemptState, agentID string) {
	if a.rec.Status != state.AttemptDispatched {
		return
	}
	now := c.now()
	a.rec.Status = state.AttemptRunning
	a.rec.AgentID = agentID
	a.rec.StartedAt = &now
	c.dirtyAttempt(a)
	c.emitAttempt(state.EventAttemptStarted, a, attemptStartedPayload{AgentID: agentID})

	t := a.task
	t.rec.Status = state.TaskRunning
	if t.rec.StartedAt == nil {
		t.rec.StartedAt = &now
	}
	c.dirtyTask(t)
	c.armTimeout(a, time.Duration(t.def.TimeoutSeconds)*time.Second)
}

// armTimeout enforces the task timeout from the leader's side. The wrapper
// enforces it locally too; this catches attempts whose runtime went quiet.
func (c *Core) armTimeout(a *attemptState, d time.Duration) {
	id := a.rec.ID
	a.timeout = c.after(d, func(c *Core) {
		a := c.attempts[id]
		if a == nil {
			return
		}
		c.cancelAttempt(a, "timeout")
		c.attemptDone(a, Report{Status: state.AttemptFailed, Reason: "timeout",
			Message: fmt.Sprintf("no result within %s", time.Duration(a.task.def.TimeoutSeconds)*time.Second)})
	})
}

func (c *Core) attemptDone(a *attemptState, rep Report) {
	a.timeout.stop()
	a.rec.Status = rep.Status
	a.rec.Reason = rep.Reason
	a.rec.ExitCode = rep.ExitCode
	a.rec.Result = rep.Result
	a.rec.FinishedAt = ptr(c.now())
	if rep.AgentID != "" {
		a.rec.AgentID = rep.AgentID
	}
	c.dirtyAttempt(a)
	delete(c.attempts, a.rec.ID)

	t := a.task
	t.current = nil
	payload := attemptFinishedPayload{Status: rep.Status, Reason: rep.Reason, Message: rep.Message, ExitCode: rep.ExitCode, Winner: rep.Winner}
	if rep.Status == state.AttemptLost {
		c.emitAttempt(state.EventAttemptLost, a, payload)
	} else {
		c.emitAttempt(state.EventAttemptFinished, a, payload)
	}

	switch rep.Status {
	case state.AttemptSucceeded:
		c.commitOutputs(t, a.rec.ID, rep.Outputs)
		c.finishTask(t, state.TaskSucceeded, a.rec.ID)
	case state.AttemptSuperseded:
		c.commitOutputs(t, rep.Winner, rep.Outputs)
		c.finishTask(t, state.TaskSucceeded, rep.Winner)
	default: // failed, cancelled, lost
		c.attemptFailed(t, rep.Reason)
	}
}

func (c *Core) commitOutputs(t *taskState, attemptID string, outs []OutputRef) {
	if t.outputs == nil {
		t.outputs = map[string]string{}
	}
	for _, o := range outs {
		t.outputs[o.Name] = o.URI
		c.b.outputs = append(c.b.outputs, state.Output{
			RunID: t.rec.RunID, TaskID: t.rec.TaskID, Name: o.Name, URI: o.URI, AttemptID: attemptID, Metadata: o.Metadata,
		})
		c.emit(state.EventOutputCommitted, t.run.rec.Tenant, t.rec.RunID, t.rec.TaskID, attemptID,
			outputCommittedPayload{Output: o.Name, URI: o.URI})
	}
}

func (c *Core) attemptFailed(t *taskState, reason string) {
	if t.run.halting() {
		c.finishTask(t, state.TaskCancelled, "")
		return
	}
	retry := t.def.Retry
	if nonRetryable[reason] || t.rec.Attempts > retry.Limit {
		c.finishTask(t, state.TaskFailed, "")
		return
	}
	delay := backoff(retry, t.rec.Attempts)
	t.rec.Status = state.TaskRetryWait
	t.rec.RetryAt = ptr(c.now().Add(delay))
	c.dirtyTask(t)
	c.emitTask(state.EventTaskRetryScheduled, t, taskRetryPayload{NextAttempt: t.rec.Attempts + 1, RetryAt: *t.rec.RetryAt})
	c.armRetry(t, delay)
}

func (c *Core) armRetry(t *taskState, d time.Duration) {
	t.retry = c.after(d, func(c *Core) {
		if t.rec.Status != state.TaskRetryWait || t.run.halting() || c.runs[t.rec.RunID] != t.run {
			return
		}
		t.retry = nil
		c.markReady(t)
	})
}

// backoff is the delay before the retry following attempt n (1-based).
func backoff(r spec.Retry, n int) time.Duration {
	return backoffDelay(r.Backoff == "exponential", time.Duration(r.InitialSeconds)*time.Second,
		time.Duration(r.MaxSeconds)*time.Second, n)
}

// backoffDelay is initial, doubled per earlier attempt when exponential,
// capped at maxDelay.
func backoffDelay(exponential bool, initial, maxDelay time.Duration, n int) time.Duration {
	d := initial
	if exponential {
		for i := 1; i < n && d < maxDelay; i++ {
			d *= 2
		}
	}
	if d > maxDelay {
		d = maxDelay
	}
	return d
}
