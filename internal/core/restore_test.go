package core

import (
	"context"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestartLosesInFlightAttemptsAndRetries(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], retry: {limit: 1, initial: 10s}}
    - {id: b, depends_on: [a], command: [x]}
`)
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	a := h.last(id, "a")
	h.report(Report{AttemptID: a.AttemptID, Status: state.AttemptRunning})

	h.restart()

	atts, err := h.store.ListAttempts(h.ctx, id, "a")
	require.NoError(t, err)
	assert.Equal(t, state.AttemptLost, atts[0].Status)
	assert.Equal(t, "leader_restart", atts[0].Reason)
	assert.Equal(t, "a=retry_wait b=pending", h.tasks(id))

	h.advance(10 * time.Second)
	retried := h.last(id, "a")
	assert.Equal(t, 2, retried.Attempt)
	assert.Equal(t, int64(2), retried.Term, "dispatched by the new term")

	h.succeed(id, "a")
	h.succeed(id, "b")
	assert.Equal(t, state.RunSucceeded, h.run(id).Status)
}

func TestRestartRearmsRetryTimersAndKeepsQueues(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  tasks:
    - {id: a, command: [x], retry: {limit: 1, initial: 1m}}
`)
	r1 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	r2 := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	h.fail(r1, "a", "exit_code")
	h.advance(20 * time.Second)

	h.restart()
	assert.Equal(t, state.RunQueued, h.run(r2).Status)
	h.advance(39 * time.Second)
	assert.Len(t, h.dispatched(r1, "a"), 1, "retry_at is kept, not reset")
	h.advance(time.Second)
	assert.Len(t, h.dispatched(r1, "a"), 2)

	h.succeed(r1, "a")
	assert.Equal(t, state.RunRunning, h.run(r2).Status, "queued run starts after restart too")
}

// recoverAll reports every in-flight attempt as having succeeded and
// committed (as the commit-marker check will in step 3).
type recoverAll struct{}

func (recoverAll) Recover(_ context.Context, inFlight []InFlight) []Report {
	var out []Report
	for _, f := range inFlight {
		out = append(out, Report{AttemptID: f.Attempt.ID, Status: state.AttemptSucceeded, Reason: "recovered"})
	}
	return out
}

func TestRestartUsesRecovery(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x], retry: {limit: 3}}]\n")
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID

	h.restart(func(c *Config) { c.Recovery = recoverAll{} })
	assert.Equal(t, state.RunSucceeded, h.run(id).Status)
	assert.Len(t, h.dispatched(id, "a"), 1, "not retried")
}

func TestRestartResumesCancellation(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x]}, {id: b, command: [x]}]\n")
	id := h.trigger(TriggerRequest{Pipeline: "p"}).RunID
	require.NoError(t, h.core.CancelRun(h.ctx, id, "alice"))
	b := h.last(id, "b")
	h.report(Report{AttemptID: b.AttemptID, Status: state.AttemptCancelled})
	assert.Equal(t, state.RunRunning, h.run(id).Status, "a hasn't confirmed yet")

	h.restart()
	assert.Equal(t, state.RunCancelled, h.run(id).Status, "a is lost on restart; the run finishes as cancelled")
}

func TestLeaderTermStartedEvent(t *testing.T) {
	h := newHarness(t)
	h.restart()
	evs, err := h.store.EventsSince(h.ctx, 0, "", 0)
	require.NoError(t, err)
	var terms []int64
	for _, e := range evs {
		if e.Type == state.EventLeaderTermStarted {
			terms = append(terms, e.Term)
		}
	}
	assert.Equal(t, []int64{1, 2}, terms)
}

func TestStaleTermStopsTheCore(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+"  tasks: [{id: a, command: [x]}]\n")

	_, err := h.store.StartTerm(h.ctx, 0) // another leader took over
	require.NoError(t, err)

	_, err = h.core.Trigger(h.ctx, TriggerRequest{Tenant: "t", Pipeline: "p"})
	assert.ErrorIs(t, err, state.ErrStaleTerm)

	h.expectRunErr = true
	assert.ErrorIs(t, h.stop(), state.ErrStaleTerm, "Run stops on a failed commit")
}
