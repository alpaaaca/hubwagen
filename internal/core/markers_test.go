package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replayWith completes the next run task with cmds and markers.
func (h *harness) replayWith(cmds []Command, markers []Marker, outcome *RunOutcome) {
	h.t.Helper()
	t := h.poll(TaskRun)
	require.NoError(h.t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t.ID, BuildID: "b", Commands: cmds, Markers: markers, Outcome: outcome}))
}

// recordedMarkers reads the markers each RunTaskCompleted recorded.
func (h *harness) recordedMarkers(runID string) [][]Marker {
	h.t.Helper()
	evs, err := h.store.RunEvents(h.ctx, runID)
	require.NoError(h.t, err)
	var out [][]Marker
	for _, e := range evs {
		if e.Type == state.EventRunTaskCompleted {
			var p runTaskCompletedPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			out = append(out, p.Markers)
		}
	}
	return out
}

func TestMarkersAreRecordedOnce(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	gate := []Marker{{Name: "quality-gate", Version: 2}}
	h.replayWith(seqs(stepCmd("a")), gate, nil)
	h.succeedStep(h.poll(TaskStep))
	h.replayWith(seqs(stepCmd("a"), stepCmd("b")), append(gate, Marker{Name: "report", Version: 1}), nil)
	assert.Equal(t, [][]Marker{gate, {{Name: "report", Version: 1}}}, h.recordedMarkers(id), "only new markers are recorded")
	assert.Equal(t, state.RunRunning, h.run(id).Status)
}

func TestAMarkerDecidedDifferentlyMakesTheRunStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replayWith(seqs(stepCmd("a")), []Marker{{Name: "quality-gate", Version: 1}}, nil)
	h.succeedStep(h.poll(TaskStep))

	// After a restart, so the recorded marker comes from the event log.
	h.restart()
	h.replayWith(seqs(stepCmd("a")), []Marker{{Name: "quality-gate", Version: 2}}, nil)
	got := h.run(id)
	assert.Equal(t, state.RunStuck, got.Status)
	assert.Equal(t, `version marker "quality-gate": replay took version 2, the event log has 1`, got.Message)

	require.NoError(t, h.core.ResumeRun(h.ctx, id))
	h.replayWith(seqs(stepCmd("a")), []Marker{{Name: "quality-gate", Version: 1}}, &RunOutcome{Status: state.RunCompleted})
	assert.Equal(t, state.RunCompleted, h.run(id).Status)
}

func TestMarkerValidation(t *testing.T) {
	for want, markers := range map[string][]Marker{
		"needs a name":         {{Version: 1}},
		"must not be negative": {{Name: "x", Version: -1}},
		`"x" decided twice`:    {{Name: "x", Version: 1}, {Name: "x", Version: 2}},
	} {
		assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Markers: markers}), want)
	}
	assert.NoError(t, validateRunTaskResult(RunTaskResult{Markers: []Marker{{Name: "x", Version: 1}, {Name: "x", Version: 1}}}))
}
