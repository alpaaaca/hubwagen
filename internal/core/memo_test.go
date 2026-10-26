package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// step is one lift of a sequential test haul; from names earlier steps whose
// pallets it takes as inputs.
type step struct {
	cmd  Command
	from []int
}

func memoLift(name, dataset string, from ...int) step {
	cmd := output(name, dataset, "")
	cmd.Memo = "fingerprint"
	return step{cmd: cmd, from: from}
}

// runHaul runs a sequential haul to its end under build, writing content[name]
// as each lift's single file. It returns the lifts that actually ran.
func (h *harness) runHaul(b blob.Bucket, build string, steps []step, content map[string]string) (haulID string, ran []string) {
	h.t.Helper()
	haulID = h.startHaul("pipeline")
	deadline := time.Now().Add(5 * time.Second)
	for {
		require.True(h.t, time.Now().Before(deadline), "haul %s didn't finish", haulID)
		task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Docks: []string{DefaultDock}})
		require.NoError(h.t, err)
		if !ok {
			if !state.HaulOpen(h.haul(haulID).Status) {
				return haulID, ran
			}
			time.Sleep(time.Millisecond) // a promote is running
			continue
		}
		switch task.Kind {
		case TaskHaul:
			cmds, outcome := replaySteps(steps, task.Logbook)
			require.NoError(h.t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: task.ID, BuildID: build, Commands: cmds, Outcome: outcome}))
		case TaskLift:
			ran = append(ran, task.Name)
			text, ok := content[task.Name]
			if !ok {
				text = task.Name
			}
			require.NoError(h.t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: task.ID, Status: state.AttemptSucceeded,
				Result: json.RawMessage(`{"by":"` + task.Name + `"}`), Output: writeOutput(h.t, b, task, map[string]string{"data.csv": text})}))
		}
	}
}

func replaySteps(steps []step, logbook []state.Event) ([]Command, *HaulOutcome) {
	results := map[int]liftFinishedPayload{}
	for _, e := range logbook {
		if e.Type == state.EventLiftFinished {
			var p liftFinishedPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				panic(err)
			}
			results[p.Seq] = p
		}
	}
	var cmds []Command
	for i, st := range steps {
		cmd := st.cmd
		cmd.Seq = i + 1
		for _, f := range st.from {
			cmd.Inputs = append(cmd.Inputs, InputRef{Name: steps[f].cmd.Name, Pallet: results[f+1].Pallet})
		}
		cmds = append(cmds, cmd)
		r, ok := results[cmd.Seq]
		if !ok {
			return cmds, nil
		}
		if r.Status != state.LiftCompleted {
			return cmds, &HaulOutcome{Status: state.HaulFailed}
		}
	}
	return cmds, &HaulOutcome{Status: state.HaulCompleted}
}

// reasons maps each lift of a haul to why it ran ("reused" if it didn't).
func (h *harness) reasons(haulID string) map[string]string {
	h.t.Helper()
	ls, err := h.store.ListLifts(h.ctx, haulID)
	require.NoError(h.t, err)
	out := map[string]string{}
	for _, l := range ls {
		if l.RunReason == "" {
			out[l.Name] = "reused"
		} else {
			out[l.Name] = l.RunReason
		}
	}
	return out
}

func (h *harness) build(id string, lifts map[string]string) {
	h.t.Helper()
	require.NoError(h.t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: id, Lifts: lifts}))
}

func TestMemoizedRerunSkipsUnchangedLifts(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	report := output("report", "reports", "") // not memoized
	steps := []step{memoLift("extract", "raw"), memoLift("transform", "mart", 0), {cmd: report, from: []int{1}}}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1", "report": "v1"})

	id, ran := h.runHaul(b, "b1", steps, map[string]string{"extract": "orders v1"})
	assert.Equal(t, []string{"extract", "transform", "report"}, ran)
	assert.Equal(t, map[string]string{"extract": "no previous run", "transform": "no previous run", "report": "not memoized"}, h.reasons(id))

	// Same build, same data: only the lift without memo runs.
	id, ran = h.runHaul(b, "b1", steps, map[string]string{"extract": "orders v1"})
	assert.Equal(t, []string{"report"}, ran)
	assert.Equal(t, map[string]string{"extract": "reused", "transform": "reused", "report": "not memoized"}, h.reasons(id))
	assert.Contains(t, h.haulEventTypes(id), state.EventLiftReused)
	assert.Equal(t, state.HaulCompleted, h.haul(id).Status)

	// extract's code changed and so did its data: it and its downstream run.
	h.build("b2", map[string]string{"extract": "v2", "transform": "v1", "report": "v1"})
	id, ran = h.runHaul(b, "b2", steps, map[string]string{"extract": "orders v2"})
	assert.Equal(t, []string{"extract", "transform", "report"}, ran)
	assert.Equal(t, map[string]string{"extract": "code changed (v1 → v2)", "transform": "input changed", "report": "not memoized"}, h.reasons(id))

	// extract's code changed but its output is identical: the content hash
	// is the same, so transform is reused.
	h.build("b3", map[string]string{"extract": "v3", "transform": "v1", "report": "v1"})
	id, ran = h.runHaul(b, "b3", steps, map[string]string{"extract": "orders v2"})
	assert.Equal(t, []string{"extract", "report"}, ran)
	assert.Equal(t, "reused", h.reasons(id)["transform"])
}

func TestReusedLiftCarriesThePalletAndResult(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoLift("extract", "raw")}
	h.build("b1", map[string]string{"extract": "v1"})
	first, _ := h.runHaul(b, "b1", steps, nil)
	second, ran := h.runHaul(b, "b1", steps, nil)
	assert.Empty(t, ran)

	built := h.pallets(first)
	require.Len(t, built, 1)
	assert.NotEmpty(t, built[0].Fingerprint)
	assert.Empty(t, h.pallets(second), "nothing new was built")

	evs, err := h.store.HaulEvents(h.ctx, second)
	require.NoError(t, err)
	var finished liftFinishedPayload
	for _, e := range evs {
		if e.Type == state.EventLiftFinished {
			require.NoError(t, json.Unmarshal(e.Payload, &finished))
		}
	}
	assert.Equal(t, built[0].ID, finished.Pallet)
	assert.True(t, finished.Reused)
	assert.JSONEq(t, `{"by":"extract"}`, string(finished.Result), "the original lift's result")
	l := h.lift(second)
	assert.Equal(t, built[0].Fingerprint, l.Fingerprint)
	assert.Equal(t, 0, l.Attempts)
}

func TestMemoTTL(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	st := memoLift("fetch", "api")
	st.cmd.Memo = "ttl:1h"
	h.build("b1", map[string]string{"fetch": "v1"})
	h.runHaul(b, "b1", []step{st}, nil)

	h.advance(30 * time.Minute)
	_, ran := h.runHaul(b, "b1", []step{st}, nil)
	assert.Empty(t, ran, "younger than the TTL")

	h.advance(time.Hour)
	id, ran := h.runHaul(b, "b1", []step{st}, nil)
	assert.Equal(t, []string{"fetch"}, ran)
	assert.Equal(t, "expired (older than 1h0m0s)", h.reasons(id)["fetch"])
}

func TestParamsChangeAndUnknownCodeVersion(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	st := memoLift("extract", "raw")
	st.cmd.Args = json.RawMessage(`{"day":"2026-10-24","full":false}`)
	h.build("b1", map[string]string{"extract": "v1"})
	h.runHaul(b, "b1", []step{st}, nil)

	// The same arguments in another key order are the same parameters.
	same := st
	same.cmd.Args = json.RawMessage(`{"full":false, "day":"2026-10-24"}`)
	_, ran := h.runHaul(b, "b1", []step{same}, nil)
	assert.Empty(t, ran)

	other := st
	other.cmd.Args = json.RawMessage(`{"day":"2026-10-25","full":false}`)
	id, _ := h.runHaul(b, "b1", []step{other}, nil)
	assert.Equal(t, "params changed", h.reasons(id)["extract"])

	// A build that doesn't know the lift never reuses.
	h.build("b2", map[string]string{"other": "v1"})
	id, ran = h.runHaul(b, "b2", []step{st}, nil)
	assert.Equal(t, []string{"extract"}, ran)
	assert.Equal(t, "unknown code version", h.reasons(id)["extract"])
}

func TestInvalidPalletIsNotReused(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoLift("extract", "raw")}
	h.build("b1", map[string]string{"extract": "v1"})
	first, _ := h.runHaul(b, "b1", steps, nil)

	p := h.pallets(first)[0]
	p.State, p.Reason = state.PalletInvalid, "bad source data"
	_, err := h.store.Append(h.ctx, h.store.Term(), nil, func(tx *state.Tx) error { return tx.PutPallet(p) })
	require.NoError(t, err)

	_, ran := h.runHaul(b, "b1", steps, nil)
	assert.Equal(t, []string{"extract"}, ran)
}

func TestMemoSurvivesRestart(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoLift("extract", "raw")}
	h.build("b1", map[string]string{"extract": "v1"})
	h.runHaul(b, "b1", steps, nil)

	h.restart(withBucket(b, nil))
	_, ran := h.runHaul(b, "b1", steps, nil)
	assert.Empty(t, ran, "the build and the pallet are known to the new leader")
}

func TestRegisterBuild(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"a": "v1"})
	h.build("b1", map[string]string{"a": "v1"}) // again: a no-op
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Lifts: map[string]string{"a": "v2"}}), ErrInvalid)
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{Lifts: map[string]string{"a": "v1"}}), ErrInvalid)
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b2", Lifts: map[string]string{"a": ""}}), ErrInvalid)

	builds, err := h.store.LoadBuilds(h.ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "v1"}, builds["b1"].Lifts)
}

func TestMemoParsingAndValidation(t *testing.T) {
	for in, want := range map[string]memoPolicy{
		"": {}, "never": {}, "fingerprint": {on: true}, "ttl:90m": {on: true, ttl: 90 * time.Minute},
	} {
		got, err := parseMemo(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"always", "ttl:", "ttl:-1h", "ttl:soon"} {
		_, err := parseMemo(bad)
		assert.Error(t, err, bad)
	}
	noOutput := lift("x")
	noOutput.Memo = "fingerprint"
	assert.ErrorContains(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(noOutput)}), "only a lift with an output")
}

func TestCanonicalJSON(t *testing.T) {
	got, err := canonicalJSON(json.RawMessage(` {"b": [1, {"y":2,"x":1}], "a": 1.50} `))
	require.NoError(t, err)
	assert.Equal(t, `{"a":1.50,"b":[1,{"x":1,"y":2}]}`, string(got), "keys sorted, numbers as written")
	got, err = canonicalJSON(nil)
	require.NoError(t, err)
	assert.Nil(t, got)
	_, err = canonicalJSON(json.RawMessage(`{`))
	assert.Error(t, err)
}
