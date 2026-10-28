package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// replayWith completes the next haul task with cmds and markers.
func (h *harness) replayWith(cmds []Command, markers []Marker, outcome *HaulOutcome) {
	h.t.Helper()
	t := h.poll(TaskHaul)
	require.NoError(h.t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t.ID, BuildID: "b", Commands: cmds, Markers: markers, Outcome: outcome}))
}

// recordedMarkers reads the markers each HaulTaskCompleted recorded.
func (h *harness) recordedMarkers(haulID string) [][]Marker {
	h.t.Helper()
	evs, err := h.store.HaulEvents(h.ctx, haulID)
	require.NoError(h.t, err)
	var out [][]Marker
	for _, e := range evs {
		if e.Type == state.EventHaulTaskCompleted {
			var p haulTaskCompletedPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			out = append(out, p.Markers)
		}
	}
	return out
}

func TestMarkersAreRecordedOnce(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	gate := []Marker{{Name: "quality-gate", Version: 2}}
	h.replayWith(seqs(lift("a")), gate, nil)
	h.succeedLift(h.poll(TaskLift))
	h.replayWith(seqs(lift("a"), lift("b")), append(gate, Marker{Name: "report", Version: 1}), nil)
	assert.Equal(t, [][]Marker{gate, {{Name: "report", Version: 1}}}, h.recordedMarkers(id), "only new markers are recorded")
	assert.Equal(t, state.HaulRunning, h.haul(id).Status)
}

func TestAMarkerDecidedDifferentlyMakesTheHaulStuck(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replayWith(seqs(lift("a")), []Marker{{Name: "quality-gate", Version: 1}}, nil)
	h.succeedLift(h.poll(TaskLift))

	// After a restart, so the recorded marker comes from the logbook.
	h.restart()
	h.replayWith(seqs(lift("a")), []Marker{{Name: "quality-gate", Version: 2}}, nil)
	got := h.haul(id)
	assert.Equal(t, state.HaulStuck, got.Status)
	assert.Equal(t, `version marker "quality-gate": replay took version 2, the logbook has 1`, got.Message)

	require.NoError(t, h.core.ResumeHaul(h.ctx, id))
	h.replayWith(seqs(lift("a")), []Marker{{Name: "quality-gate", Version: 1}}, &HaulOutcome{Status: state.HaulCompleted})
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)
}

func TestMarkerValidation(t *testing.T) {
	for want, markers := range map[string][]Marker{
		"needs a name":         {{Version: 1}},
		"must not be negative": {{Name: "x", Version: -1}},
		`"x" decided twice`:    {{Name: "x", Version: 1}, {Name: "x", Version: 2}},
	} {
		assert.ErrorContains(t, validateHaulTaskResult(HaulTaskResult{Markers: markers}), want)
	}
	assert.NoError(t, validateHaulTaskResult(HaulTaskResult{Markers: []Marker{{Name: "x", Version: 1}, {Name: "x", Version: 1}}}))
}
