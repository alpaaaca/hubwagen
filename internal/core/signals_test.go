package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func waitSignal(name string, timeout time.Duration) Command {
	return Command{Kind: CommandWaitSignal, Name: name, Duration: timeout}
}

func (h *harness) signal(runID, name, payload string) string {
	h.t.Helper()
	id, err := h.core.SignalRun(h.ctx, SignalRequest{RunID: runID, Name: name, Payload: json.RawMessage(payload)})
	require.NoError(h.t, err)
	return id
}

// deliveries returns the run's SignalDelivered events by wait seq.
func (h *harness) deliveries(runID string) map[int]signalDeliveredPayload {
	h.t.Helper()
	evs, err := h.store.RunEvents(h.ctx, runID)
	require.NoError(h.t, err)
	out := map[int]signalDeliveredPayload{}
	for _, e := range evs {
		if e.Type == state.EventSignalDelivered {
			var p signalDeliveredPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			_, dup := out[p.Seq]
			require.False(h.t, dup, "wait %d answered twice", p.Seq)
			out[p.Seq] = p
		}
	}
	return out
}

func TestSignalReachesAWaitingRun(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(waitSignal("approve", 0)), nil)
	h.pollNothing() // waiting: no replay until something happens
	assert.Empty(t, h.deliveries(id))

	sig := h.signal(id, "approve", `{"by": "ana"}`)
	d := h.deliveries(id)[1]
	assert.Equal(t, sig, d.Signal)
	assert.Equal(t, "approve", d.Name)
	assert.JSONEq(t, `{"by":"ana"}`, string(d.Payload))
	assert.False(t, d.TimedOut)

	h.replay(seqs(waitSignal("approve", 0)), &RunOutcome{Status: state.RunCompleted}) // woken by it
	assert.Equal(t, state.RunCompleted, h.run(id).Status)
}

func TestSignalSentBeforeTheWaitIsKept(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(stepCmd("a")), nil)
	early := h.signal(id, "file_arrived", `"a.csv"`)
	h.signal(id, "other", `1`)
	assert.Empty(t, h.deliveries(id), "nothing waits yet")

	h.succeedStep(h.poll(TaskStep))
	h.replay(seqs(stepCmd("a"), waitSignal("file_arrived", time.Hour)), nil)
	d := h.deliveries(id)[2]
	assert.Equal(t, early, d.Signal, "answered at once by the kept signal")
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	assert.Empty(t, tms, "no timeout needed")
}

func TestEachSignalGoesToOneWaitInOrder(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	first := h.signal(id, "batch", `1`)
	second := h.signal(id, "batch", `2`)
	h.replay(seqs(waitSignal("batch", 0)), nil)
	h.replay(seqs(waitSignal("batch", 0), waitSignal("batch", 0)), nil)
	h.replay(seqs(waitSignal("batch", 0), waitSignal("batch", 0), waitSignal("batch", 0)), nil)
	d := h.deliveries(id)
	assert.Equal(t, first, d[1].Signal)
	assert.Equal(t, second, d[2].Signal)
	_, third := d[3]
	assert.False(t, third, "two signals, two waits answered")

	h.signal(id, "batch", `3`)
	assert.JSONEq(t, `3`, string(h.deliveries(id)[3].Payload))
}

func TestWaitTimesOut(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(waitSignal("approve", time.Hour)), nil)
	h.advance(59 * time.Minute)
	assert.Empty(t, h.deliveries(id))
	h.advance(time.Minute)
	d := h.deliveries(id)[1]
	assert.True(t, d.TimedOut)
	assert.Empty(t, d.Signal)

	evs, err := h.store.RunEvents(h.ctx, id)
	require.NoError(t, err)
	for _, e := range evs {
		assert.NotEqual(t, state.EventTimerFired, e.Type, "a wait's timeout isn't a sleep's result")
	}

	late := h.signal(id, "approve", `true`)
	h.replay(seqs(waitSignal("approve", time.Hour), waitSignal("approve", 0)), nil)
	assert.Equal(t, late, h.deliveries(id)[2].Signal, "a late signal is kept for the next wait")
}

func TestSignalBeatsTheTimeout(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(waitSignal("approve", time.Hour)), nil)
	h.signal(id, "approve", `true`)
	h.advance(2 * time.Hour)
	d := h.deliveries(id)
	require.Len(t, d, 1)
	assert.False(t, d[1].TimedOut)
	tms, err := h.store.ListTimers(h.ctx, id)
	require.NoError(t, err)
	require.Len(t, tms, 1)
	assert.Equal(t, state.TimerCancelled, tms[0].Status)
}

func TestSignalErrors(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.SignalRun(h.ctx, SignalRequest{RunID: "nope", Name: "x"})
	assert.ErrorIs(t, err, ErrNotFound)

	id := h.startRun("x")
	_, err = h.core.SignalRun(h.ctx, SignalRequest{RunID: id, Name: ""})
	assert.ErrorIs(t, err, ErrInvalid)
	_, err = h.core.SignalRun(h.ctx, SignalRequest{RunID: id, Name: "x", Payload: json.RawMessage(`{`)})
	assert.ErrorIs(t, err, ErrInvalid)
	h.signal(id, "x", "") // no payload: null

	h.replay(nil, &RunOutcome{Status: state.RunCompleted})
	_, err = h.core.SignalRun(h.ctx, SignalRequest{RunID: id, Name: "x"})
	assert.ErrorIs(t, err, ErrRunFinished)
	assert.ErrorContains(t, err, "is completed")

	assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Commands: seqs(waitSignal("", 0))}), "needs a signal name")
	assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Commands: seqs(waitSignal("x", -time.Second))}), "negative")
}

func TestReplayMustWaitForTheSameSignal(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(waitSignal("approve", 0)), nil)
	h.signal(id, "approve", `true`)
	h.replay(seqs(waitSignal("reject", 0)), nil)
	got := h.run(id)
	assert.Equal(t, state.RunStuck, got.Status)
	assert.Contains(t, got.Message, `wait_signal "reject"`)
}

func TestSignalsSurviveRestart(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(waitSignal("approve", time.Hour), waitSignal("never", 0)), nil) // two open waits
	kept := h.signal(id, "later", `"kept"`)

	h.restart()
	h.signal(id, "never", `1`) // the restored wait for "never" takes it
	assert.Equal(t, 1, len(h.deliveries(id)))
	h.advance(time.Hour) // the restored timeout fires
	assert.True(t, h.deliveries(id)[1].TimedOut)

	// The kept signal answers a wait issued after the restart.
	h.replay(seqs(waitSignal("approve", time.Hour), waitSignal("never", 0), waitSignal("later", 0)), nil)
	assert.Equal(t, kept, h.deliveries(id)[3].Signal)
}
