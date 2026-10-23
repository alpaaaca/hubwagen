package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const linear = agentDefaults + `
  tasks:
    - id: extract
      command: [extract]
      outputs: [{name: rows}]
    - id: load
      depends_on: [extract]
      command: [load]
      args: ["--from={{ .Inputs.rows }}", "--day={{ .Interval.Start | date \"2006-01-02\" }}"]
      inputs: [{name: rows, from: extract.rows}]
`

func TestLinearRunPassesOutputsDownstream(t *testing.T) {
	h := newHarness(t)
	h.publish("p", linear)
	res := h.trigger(TriggerRequest{Pipeline: "p", Interval: day(20), Trigger: "cron"})

	assert.Equal(t, "t/p/20261020T000000Z_20261021T000000Z", res.RunKey)
	assert.Equal(t, state.RunRunning, h.run(res.RunID).Status)
	assert.Empty(t, h.dispatched(res.RunID, "load"), "load waits for extract")

	ex := h.last(res.RunID, "extract")
	assert.Equal(t, "file:///bucket/data/t/p/extract/rows/t%2Fp%2F20261020T000000Z_20261021T000000Z/gen-1/attempt-1/", ex.Outputs["rows"].Staging)
	assert.Equal(t, res.RunKey+"/gen-1/extract", ex.IdempotencyKey)
	h.succeed(res.RunID, "extract")

	ld := h.last(res.RunID, "load")
	assert.Equal(t, ex.Outputs["rows"].Staging, ld.Inputs["rows"])
	assert.Equal(t, []string{"--from=" + ex.Outputs["rows"].Staging, "--day=2026-10-20"}, ld.Args)
	h.succeed(res.RunID, "load")

	run := h.run(res.RunID)
	assert.Equal(t, state.RunSucceeded, run.Status)
	assert.Equal(t, "extract=succeeded load=succeeded", h.tasks(res.RunID))
	outs, err := h.store.ListOutputs(h.ctx, res.RunID)
	require.NoError(t, err)
	require.Len(t, outs, 1)
	assert.Equal(t, ex.AttemptID, outs[0].AttemptID)

	assert.Equal(t, []state.EventType{
		state.EventRunCreated, state.EventRunStarted, state.EventTaskReady, state.EventAttemptDispatched,
		state.EventAttemptStarted, state.EventAttemptFinished, state.EventOutputCommitted, state.EventTaskFinished,
		state.EventTaskReady, state.EventAttemptDispatched, state.EventAttemptStarted, state.EventAttemptFinished,
		state.EventTaskFinished, state.EventRunFinished,
	}, h.eventTypes(res.RunID))
}

func TestTasksStartWhenTheirOwnDependenciesFinish(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x]}
    - {id: slow, command: [x]}
    - {id: c, depends_on: [a], command: [x]}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	h.succeed(id, "a")
	assert.Len(t, h.dispatched(id, "c"), 1, "c starts while slow, from the same 'wave', is still running")
}

func TestRetryWithExponentialBackoff(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - id: a
      command: [x]
      retry: {limit: 2, backoff: exponential, initial: 30s, max: 45s}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID

	h.fail(id, "a", "exit_code")
	assert.Equal(t, "a=retry_wait", h.tasks(id))
	h.advance(29 * time.Second)
	assert.Len(t, h.dispatched(id, "a"), 1)
	h.advance(time.Second)
	require.Len(t, h.dispatched(id, "a"), 2)
	assert.Equal(t, 2, h.last(id, "a").Attempt)

	h.fail(id, "a", "exit_code")
	h.advance(44 * time.Second) // 60s doubled, capped at max 45s
	assert.Len(t, h.dispatched(id, "a"), 2)
	h.advance(time.Second)
	require.Len(t, h.dispatched(id, "a"), 3)

	h.fail(id, "a", "exit_code")
	assert.Equal(t, "a=failed", h.tasks(id), "limit 2 means 3 attempts")
	assert.Equal(t, state.RunFailed, h.run(id).Status)

	atts, err := h.store.ListAttempts(h.ctx, id, "a")
	require.NoError(t, err)
	require.Len(t, atts, 3)
	assert.Equal(t, state.AttemptFailed, atts[2].Status)
}

func TestBackoff(t *testing.T) {
	exp := spec.Retry{Backoff: "exponential", InitialSeconds: 10, MaxSeconds: 60}
	for n, want := range map[int]time.Duration{1: 10 * time.Second, 2: 20 * time.Second, 3: 40 * time.Second, 4: 60 * time.Second, 9: 60 * time.Second} {
		assert.Equal(t, want, backoff(exp, n), "attempt %d", n)
	}
	assert.Equal(t, 10*time.Second, backoff(spec.Retry{Backoff: "fixed", InitialSeconds: 10, MaxSeconds: 60}, 5))
}

func TestNonRetryableFailures(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], retry: {limit: 5}}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	h.fail(id, "a", "exec_error")
	assert.Equal(t, "a=failed", h.tasks(id))
	assert.Len(t, h.dispatched(id, "a"), 1)
}

func TestTimeoutCancelsAndRetries(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], timeout: 1m, retry: {limit: 1, initial: 5s}}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	a := h.last(id, "a")
	h.advance(10 * time.Minute)
	_, cancelled := h.cancelReason(a.AttemptID)
	assert.False(t, cancelled, "the timeout runs from start, not dispatch")

	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptRunning})
	h.advance(time.Minute)
	reason, cancelled := h.cancelReason(a.AttemptID)
	assert.True(t, cancelled)
	assert.Equal(t, "timeout", reason)
	assert.Equal(t, "a=retry_wait", h.tasks(id))

	// The runtime's late report about the killed process changes nothing.
	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptCancelled})
	atts, err := h.store.ListAttempts(h.ctx, id, "a")
	require.NoError(t, err)
	assert.Equal(t, "timeout", atts[0].Reason)

	h.advance(5 * time.Second)
	assert.Len(t, h.dispatched(id, "a"), 2)
}

func TestOnFailureStopCancelsTheRest(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x]}
    - {id: b, command: [x]}
    - {id: c, depends_on: [b], command: [x]}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	b := h.last(id, "b")
	h.report(Report{AttemptID: b.AttemptID, Status: state.AttemptRunning})

	h.fail(id, "a", "exit_code")
	reason, ok := h.cancelReason(b.AttemptID)
	require.True(t, ok)
	assert.Contains(t, reason, "task a failed (on_failure: stop)")
	assert.Equal(t, "a=failed b=running c=cancelled", h.tasks(id))
	assert.Equal(t, state.RunRunning, h.run(id).Status, "waits for b to report")

	h.report(Report{AttemptID: b.AttemptID, Status: state.AttemptCancelled, Reason: "cancelled"})
	assert.Equal(t, "a=failed b=cancelled c=cancelled", h.tasks(id))
	assert.Equal(t, state.RunFailed, h.run(id).Status)
}

func TestOnFailureSkipDependentsAndContinue(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], on_failure: skip_dependents}
    - {id: a_child, depends_on: [a], command: [x]}
    - {id: a_grandchild, depends_on: [a_child], command: [x]}
    - {id: b, command: [x], on_failure: continue}
    - {id: b_child, depends_on: [b], command: [x]}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	h.fail(id, "a", "exit_code")
	h.fail(id, "b", "exit_code")
	assert.Equal(t, "a=failed a_child=upstream_failed a_grandchild=upstream_failed b=failed b_child=dispatched", h.tasks(id))
	h.succeed(id, "b_child")
	assert.Equal(t, state.RunFailed, h.run(id).Status, "a's failure fails the run; b's doesn't")
}

func TestOnFailureContinueAloneSucceeds(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: notify, command: [x], on_failure: continue}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	h.fail(id, "notify", "exit_code")
	assert.Equal(t, state.RunSucceeded, h.run(id).Status)
}

func TestTriggerRules(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], on_failure: skip_dependents}
    - {id: b, command: [x]}
    - {id: cleanup, depends_on: [a, b], trigger_rule: all_done, command: [x]}
    - {id: alert, depends_on: [a, b], trigger_rule: one_failed, command: [x]}
    - {id: report, depends_on: [a, b], command: [x]}
    - {id: after_alert, depends_on: [alert], command: [x]}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	h.fail(id, "a", "exit_code")
	assert.Equal(t, "a=failed after_alert=pending alert=dispatched b=dispatched cleanup=pending report=upstream_failed", h.tasks(id),
		"one_failed fires on the first failure; all_success gives up early")
	h.succeed(id, "b")
	h.succeed(id, "alert")
	h.succeed(id, "after_alert")
	h.succeed(id, "cleanup")
	assert.Equal(t, state.RunFailed, h.run(id).Status)

	// No failure: one_failed is skipped, and its all_success dependent too.
	id = h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	h.succeed(id, "a")
	h.succeed(id, "b")
	assert.Equal(t, "a=succeeded after_alert=skipped alert=skipped b=succeeded cleanup=dispatched report=dispatched", h.tasks(id))
	h.succeed(id, "cleanup")
	h.succeed(id, "report")
	assert.Equal(t, state.RunSucceeded, h.run(id).Status, "skipped tasks don't fail a run")
}

func TestIdempotentTriggersAndReruns(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x]}]\n")

	first := h.trigger(TriggerRequest{Pipeline: "p", Interval: day(20)})
	again := h.trigger(TriggerRequest{Pipeline: "p", Interval: day(20)})
	assert.True(t, again.Existing)
	assert.Equal(t, first.RunID, again.RunID)

	rerunWhileActive := h.trigger(TriggerRequest{Pipeline: "p", Interval: day(20), Rerun: true})
	assert.True(t, rerunWhileActive.Existing, "can't rerun an interval that is still running")

	h.succeed(first.RunID, "a")
	afterFinish := h.trigger(TriggerRequest{Pipeline: "p", Interval: day(20)})
	assert.Equal(t, first.RunID, afterFinish.RunID, "found in the store once out of memory")

	rerun := h.trigger(TriggerRequest{Pipeline: "p", Interval: day(20), Rerun: true})
	assert.False(t, rerun.Existing)
	assert.Equal(t, 2, rerun.Generation)
	assert.Equal(t, first.RunKey, rerun.RunKey)
	assert.Contains(t, h.last(rerun.RunID, "a").IdempotencyKey, "/gen-2/")

	k1 := h.trigger(TriggerRequest{Pipeline: "p", IdempotencyKey: "deploy-42"})
	k2 := h.trigger(TriggerRequest{Pipeline: "p", IdempotencyKey: "deploy-42"})
	assert.Equal(t, k1.RunID, k2.RunID)
	adhoc1 := h.trigger(TriggerRequest{Pipeline: "p"})
	adhoc2 := h.trigger(TriggerRequest{Pipeline: "p"})
	assert.NotEqual(t, adhoc1.RunID, adhoc2.RunID)
}

func TestConcurrentDuplicateTriggersCreateOneRun(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+"  concurrency: {max_active_runs: 5}\n  tasks: [{id: a, command: [x]}]\n")

	results := make(chan TriggerResult, 10)
	for i := 0; i < 10; i++ {
		go func() {
			res, err := h.core.Trigger(h.ctx, TriggerRequest{Tenant: "t", Pipeline: "p", Interval: day(20)})
			assert.NoError(t, err)
			results <- res
		}()
	}
	ids := map[string]bool{}
	for i := 0; i < 10; i++ {
		ids[(<-results).RunID] = true
	}
	assert.Len(t, ids, 1)
}

func TestOnOverlap(t *testing.T) {
	t.Run("queue", func(t *testing.T) {
		h := newHarness(t)
		h.publish("p", agentDefaults+"  concurrency: {max_active_runs: 1, on_overlap: queue}\n  tasks: [{id: a, command: [x]}]\n")
		r1 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
		r2 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
		assert.Equal(t, state.RunQueued, h.run(r2).Status)
		assert.Empty(t, h.dispatched(r2, "a"))
		h.succeed(r1, "a")
		assert.Equal(t, state.RunRunning, h.run(r2).Status)
		assert.Len(t, h.dispatched(r2, "a"), 1)
	})
	t.Run("skip", func(t *testing.T) {
		h := newHarness(t)
		h.publish("p", agentDefaults+"  concurrency: {on_overlap: skip}\n  tasks: [{id: a, command: [x]}]\n")
		h.trigger(TriggerRequest{Pipeline: "p"})
		res := h.trigger(TriggerRequest{Pipeline: "p"})
		assert.True(t, res.Skipped)
		assert.Empty(t, res.RunID)
		assert.Contains(t, res.Reason, "on_overlap: skip")
	})
	t.Run("cancel_running", func(t *testing.T) {
		h := newHarness(t)
		h.publish("p", agentDefaults+"  concurrency: {on_overlap: cancel_running}\n  tasks: [{id: a, command: [x]}]\n")
		r1 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
		a1 := h.last(r1, "a")
		r2 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
		_, ok := h.cancelReason(a1.AttemptID)
		assert.True(t, ok)
		assert.Equal(t, state.RunQueued, h.run(r2).Status)
		h.report(Report{AttemptID: a1.AttemptID, Status: state.AttemptCancelled})
		assert.Equal(t, state.RunCancelled, h.run(r1).Status)
		assert.Equal(t, state.RunRunning, h.run(r2).Status)
	})
}

func TestCancelRun(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x]}, {id: b, depends_on: [a], command: [x]}]\n")
	r1 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	r2 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID // queued behind r1

	require.NoError(t, h.core.CancelRun(h.ctx, r2, "alice"))
	assert.Equal(t, state.RunCancelled, h.run(r2).Status, "queued runs cancel immediately")
	assert.Equal(t, "a=cancelled b=cancelled", h.tasks(r2))

	require.NoError(t, h.core.CancelRun(h.ctx, r1, "alice"))
	a := h.last(r1, "a")
	_, ok := h.cancelReason(a.AttemptID)
	assert.True(t, ok)
	assert.Equal(t, "a=dispatched b=cancelled", h.tasks(r1))
	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptFailed, Reason: "exit_code", ExitCode: ptr(143)})
	assert.Equal(t, state.RunCancelled, h.run(r1).Status, "a kill reported as failure still counts as cancelled")

	require.NoError(t, h.core.CancelRun(h.ctx, r1, "alice"), "cancelling a finished run is a no-op")
	assert.ErrorIs(t, h.core.CancelRun(h.ctx, "nope", "alice"), ErrNotFound)
}

func TestPublish(t *testing.T) {
	h := newHarness(t)
	v1 := h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x]}]\n")
	assert.False(t, v1.Unchanged)
	assert.True(t, h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x]}]\n").Unchanged)

	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	v2 := h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x]}, {id: b, depends_on: [a], command: [x]}]\n")
	assert.NotEqual(t, v1.Hash, v2.Hash)
	h.succeed(id, "a")
	assert.Equal(t, state.RunSucceeded, h.run(id).Status, "a running run keeps its version")
	assert.Equal(t, v1.Hash, h.run(id).Version)

	id2 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	assert.Equal(t, v2.Hash, h.run(id2).Version)
	assert.Equal(t, "a=dispatched b=pending", h.tasks(id2))
}

func TestPublishRejects(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.AutoCreateTenants = false })
	good := compile(t, "p", agentDefaults+"  tasks: [{id: a, command: [x]}]\n")

	_, err := h.core.Publish(h.ctx, PublishRequest{SpecJSON: good})
	assert.ErrorIs(t, err, ErrNotFound, "unknown tenant")

	var pretty map[string]any
	require.NoError(t, json.Unmarshal(good, &pretty))
	indented, _ := json.MarshalIndent(pretty, "", "  ")
	_, err = h.core.Publish(h.ctx, PublishRequest{SpecJSON: indented})
	assert.ErrorIs(t, err, ErrInvalid)
	assert.ErrorContains(t, err, "canonical")
}

func TestParams(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  params:
    mode: {type: string, default: incremental}
    limit: {type: int}
  tasks:
    - {id: a, command: [x], args: ["{{ .Params.mode }}", "{{ .Params.limit }}"]}
`)
	_, err := h.core.Trigger(h.ctx, TriggerRequest{Tenant: "t", Pipeline: "p"})
	assert.ErrorContains(t, err, "param limit is required")
	_, err = h.core.Trigger(h.ctx, TriggerRequest{Tenant: "t", Pipeline: "p", Params: map[string]any{"limit": "ten"}})
	assert.ErrorContains(t, err, "is not a valid int")
	_, err = h.core.Trigger(h.ctx, TriggerRequest{Tenant: "t", Pipeline: "p", Params: map[string]any{"limit": 1, "typo": 1}})
	assert.ErrorContains(t, err, "unknown param(s): typo")

	id := h.trigger(TriggerRequest{Pipeline: "p", Params: map[string]any{"limit": 10.0}}).RunID
	assert.Equal(t, []string{"incremental", "10"}, h.last(id, "a").Args)
	assert.Equal(t, map[string]any{"mode": "incremental", "limit": json.Number("10")}, h.run(id).Params)
}

func TestRenderErrorFailsWithoutRetry(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], args: ['{{ .Interval.Start | date "2006" }}'], retry: {limit: 3}}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID // no interval
	assert.Equal(t, "a=failed", h.tasks(id))
	assert.Empty(t, h.dispatched(id, "a"))
	atts, err := h.store.ListAttempts(h.ctx, id, "a")
	require.NoError(t, err)
	assert.Equal(t, "render_error", atts[0].Reason)
}

func TestMissingRuntimeFailsWithoutRetry(t *testing.T) {
	h := newHarness(t, func(c *Config) { delete(c.Runtimes, spec.RuntimeJob) })
	h.publish("p", `
  tasks:
    - {id: a, runtime: {kind: job, image: "x@sha256:abc"}, command: [x], retry: {limit: 3}}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	assert.Equal(t, "a=failed", h.tasks(id))
	atts, err := h.store.ListAttempts(h.ctx, id, "a")
	require.NoError(t, err)
	assert.Equal(t, "no_runtime", atts[0].Reason)
}

func TestSupersededAttemptUsesTheWinnersOutput(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], timeout: 1m, retry: {limit: 1, initial: 0s}, outputs: [{name: o}]}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	first := h.last(id, "a")
	h.report(Report{AttemptID: first.AttemptID, Status: state.AttemptRunning})
	h.advance(time.Minute) // times out; in reality it still committed before dying
	h.advance(0)           // the zero-delay retry timer is armed after that commit
	second := h.last(id, "a")
	require.NotEqual(t, first.AttemptID, second.AttemptID)

	h.report(Report{AttemptID: second.AttemptID, Status: state.AttemptSuperseded, Winner: first.AttemptID,
		Outputs: []OutputRef{{Name: "o", URI: first.Outputs["o"].Staging}}})
	assert.Equal(t, "a=succeeded", h.tasks(id))
	trs, err := h.store.ListTaskRuns(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, first.AttemptID, trs[0].WinningAttempt)
	outs, err := h.store.ListOutputs(h.ctx, id)
	require.NoError(t, err)
	assert.Equal(t, first.Outputs["o"].Staging, outs[0].URI)
}

func TestIntervalRenderedInPipelineTimezone(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  schedule: {cron: "0 2 * * *", timezone: Europe/Berlin, interval: daily}
  tasks:
    - {id: a, command: [x], args: ["{{ .Interval.Start }}", "{{ .Run.Date }}"]}
`)
	berlin, _ := time.LoadLocation("Europe/Berlin")
	start := time.Date(2026, 10, 20, 0, 0, 0, 0, berlin)
	id := h.trigger(TriggerRequest{Pipeline: "p", Interval: &spec.Interval{Start: start, End: start.AddDate(0, 0, 1)}}).RunID
	assert.Equal(t, []string{"2026-10-20T00:00:00+02:00", "2026-10-21"}, h.last(id, "a").Args)
}
