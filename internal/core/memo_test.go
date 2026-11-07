package core

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/alper/moksha/internal/blob"
	"github.com/alper/moksha/internal/history"
	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// step is one step of a sequential test run; from names earlier steps whose
// data versions it takes as inputs.
type step struct {
	cmd  Command
	from []int
}

func memoStep(name, dataset string, from ...int) step {
	cmd := output(name, dataset, "")
	cmd.Memo = "fingerprint"
	return step{cmd: cmd, from: from}
}

// runPipeline runs a sequential run to its end under build, writing content[name]
// as the single file of each step with an output. Steps return h.results[name],
// else {"by":name}. It returns the steps that actually ran.
func (h *harness) runPipeline(b blob.Bucket, build string, steps []step, content map[string]string) (runID string, ran []string) {
	h.t.Helper()
	runID = h.startRun("pipeline")
	deadline := time.Now().Add(5 * time.Second)
	for {
		require.True(h.t, time.Now().Before(deadline), "run %s didn't finish", runID)
		task, ok, err := h.core.Poll(h.ctx, PollRequest{WorkerID: "w1", Queues: []string{DefaultQueue}})
		require.NoError(h.t, err)
		if !ok {
			if !state.RunOpen(h.run(runID).Status) {
				return runID, ran
			}
			time.Sleep(time.Millisecond) // a promote is running
			continue
		}
		switch task.Kind {
		case TaskRun:
			cmds, outcome := replaySteps(steps, task.EventLog)
			require.NoError(h.t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: task.ID, BuildID: build, Commands: cmds, Outcome: outcome}))
		case TaskStep:
			ran = append(ran, task.Name)
			text, ok := content[task.Name]
			if !ok {
				text = task.Name
			}
			res := StepResult{AttemptID: task.ID, Status: state.AttemptSucceeded, Result: json.RawMessage(`{"by":"` + task.Name + `"}`)}
			if r, ok := h.results[task.Name]; ok {
				res.Result = json.RawMessage(r)
			}
			if task.Output != nil {
				res.Output = writeOutput(h.t, b, task, map[string]string{"data.csv": text})
			}
			require.NoError(h.t, h.core.CompleteStep(h.ctx, res))
		}
	}
}

func replaySteps(steps []step, eventLog []state.Event) ([]Command, *RunOutcome) {
	results := map[int]stepFinishedPayload{}
	for _, e := range eventLog {
		if e.Type == state.EventStepFinished {
			var p stepFinishedPayload
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
			cmd.Inputs = append(cmd.Inputs, InputRef{Name: steps[f].cmd.Name, DataVersion: results[f+1].DataVersion})
		}
		cmds = append(cmds, cmd)
		r, ok := results[cmd.Seq]
		if !ok {
			return cmds, nil
		}
		if r.Status != state.StepCompleted {
			return cmds, &RunOutcome{Status: state.RunFailed}
		}
	}
	return cmds, &RunOutcome{Status: state.RunCompleted}
}

// reasons maps each step of a run to why it ran ("reused" if it didn't).
func (h *harness) reasons(runID string) map[string]string {
	h.t.Helper()
	ls, err := h.store.ListSteps(h.ctx, runID)
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

func (h *harness) build(id string, steps map[string]string) {
	h.t.Helper()
	require.NoError(h.t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: id, Steps: steps}))
}

func TestMemoizedRerunSkipsUnchangedSteps(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	report := output("report", "reports", "") // not memoized
	steps := []step{memoStep("extract", "raw"), memoStep("transform", "mart", 0), {cmd: report, from: []int{1}}}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1", "report": "v1"})

	id, ran := h.runPipeline(b, "b1", steps, map[string]string{"extract": "orders v1"})
	assert.Equal(t, []string{"extract", "transform", "report"}, ran)
	assert.Equal(t, map[string]string{"extract": "no previous run", "transform": "no previous run", "report": "not memoized"}, h.reasons(id))

	// Same build, same data: only the step without memo runs.
	id, ran = h.runPipeline(b, "b1", steps, map[string]string{"extract": "orders v1"})
	assert.Equal(t, []string{"report"}, ran)
	assert.Equal(t, map[string]string{"extract": "reused", "transform": "reused", "report": "not memoized"}, h.reasons(id))
	assert.Contains(t, h.runEventTypes(id), state.EventStepReused)
	assert.Equal(t, state.RunCompleted, h.run(id).Status)

	// extract's code changed and so did its data: it and its downstream run.
	h.build("b2", map[string]string{"extract": "v2", "transform": "v1", "report": "v1"})
	id, ran = h.runPipeline(b, "b2", steps, map[string]string{"extract": "orders v2"})
	assert.Equal(t, []string{"extract", "transform", "report"}, ran)
	assert.Equal(t, map[string]string{"extract": "code changed (v1 → v2)", "transform": "input changed", "report": "not memoized"}, h.reasons(id))

	// extract's code changed but its output is identical: the content hash
	// is the same, so transform is reused.
	h.build("b3", map[string]string{"extract": "v3", "transform": "v1", "report": "v1"})
	id, ran = h.runPipeline(b, "b3", steps, map[string]string{"extract": "orders v2"})
	assert.Equal(t, []string{"extract", "report"}, ran)
	assert.Equal(t, "reused", h.reasons(id)["transform"])
}

func TestReusedStepCarriesTheDataVersionAndResult(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoStep("extract", "raw")}
	h.build("b1", map[string]string{"extract": "v1"})
	first, _ := h.runPipeline(b, "b1", steps, nil)
	second, ran := h.runPipeline(b, "b1", steps, nil)
	assert.Empty(t, ran)

	built := h.dataVersions(first)
	require.Len(t, built, 1)
	assert.NotEmpty(t, built[0].Fingerprint)
	assert.Empty(t, h.dataVersions(second), "nothing new was built")

	evs, err := h.store.RunEvents(h.ctx, second)
	require.NoError(t, err)
	var finished stepFinishedPayload
	for _, e := range evs {
		if e.Type == state.EventStepFinished {
			require.NoError(t, json.Unmarshal(e.Payload, &finished))
		}
	}
	assert.Equal(t, built[0].ID, finished.DataVersion)
	assert.True(t, finished.Reused)
	assert.JSONEq(t, `{"by":"extract"}`, string(finished.Result), "the original step's result")
	l := h.step(second)
	assert.Equal(t, built[0].Fingerprint, l.Fingerprint)
	assert.Equal(t, 0, l.Attempts)
}

// A run started with reuse none runs every step, steps with memo too, and
// still records their fingerprints: the runs after it reuse what it made.
func TestARunThatReusesNothingRunsEveryStep(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoStep("extract", "raw"), memoStep("transform", "mart", 0)}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1"})
	h.runPipeline(b, "b1", steps, nil)

	h.reuse = state.ReuseNone
	id, ran := h.runPipeline(b, "b1", steps, nil)
	assert.Equal(t, []string{"extract", "transform"}, ran)
	assert.Equal(t, map[string]string{"extract": "run reuses nothing", "transform": "run reuses nothing"}, h.reasons(id))
	assert.Equal(t, state.ReuseNone, h.run(id).Reuse)
	assert.Equal(t, "its run was started with --reuse=none", history.WhyItRan(reasonReuseNone))

	h.restart()
	assert.Equal(t, state.ReuseNone, h.run(id).Reuse, "kept with the run")
	h.reuse = ""
	_, ran = h.runPipeline(b, "b1", steps, nil)
	assert.Empty(t, ran, "the next run reuses")

	_, err := h.core.StartRun(h.ctx, StartRunRequest{Tenant: "t", Name: "pipeline", Reuse: "some"})
	assert.ErrorIs(t, err, ErrInvalid)
	assert.ErrorContains(t, err, `reuse "some": want auto or none`)
}

func TestMemoTTL(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	st := memoStep("fetch", "api")
	st.cmd.Memo = "ttl:1h"
	h.build("b1", map[string]string{"fetch": "v1"})
	h.runPipeline(b, "b1", []step{st}, nil)

	h.advance(30 * time.Minute)
	_, ran := h.runPipeline(b, "b1", []step{st}, nil)
	assert.Empty(t, ran, "younger than the TTL")

	h.advance(time.Hour)
	id, ran := h.runPipeline(b, "b1", []step{st}, nil)
	assert.Equal(t, []string{"fetch"}, ran)
	assert.Equal(t, "expired (older than 1h0m0s)", h.reasons(id)["fetch"])
}

func TestParamsChangeAndUnknownCodeVersion(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	st := memoStep("extract", "raw")
	st.cmd.Args = json.RawMessage(`{"day":"2026-10-24","full":false}`)
	h.build("b1", map[string]string{"extract": "v1"})
	h.runPipeline(b, "b1", []step{st}, nil)

	// The same arguments in another key order are the same parameters.
	same := st
	same.cmd.Args = json.RawMessage(`{"full":false, "day":"2026-10-24"}`)
	_, ran := h.runPipeline(b, "b1", []step{same}, nil)
	assert.Empty(t, ran)

	other := st
	other.cmd.Args = json.RawMessage(`{"day":"2026-10-25","full":false}`)
	id, _ := h.runPipeline(b, "b1", []step{other}, nil)
	assert.Equal(t, "params changed", h.reasons(id)["extract"])

	// A build that doesn't know the step never reuses.
	h.build("b2", map[string]string{"other": "v1"})
	id, ran = h.runPipeline(b, "b2", []step{st}, nil)
	assert.Equal(t, []string{"extract"}, ran)
	assert.Equal(t, "unknown code version", h.reasons(id)["extract"])
}

func TestInvalidDataVersionIsNotReused(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoStep("extract", "raw")}
	h.build("b1", map[string]string{"extract": "v1"})
	first, _ := h.runPipeline(b, "b1", steps, nil)

	p := h.dataVersions(first)[0]
	p.State, p.Reason = state.DataVersionInvalid, "bad source data"
	_, err := h.store.Append(h.ctx, h.store.Term(), nil, func(tx *state.Tx) error { return tx.PutDataVersion(p) })
	require.NoError(t, err)

	_, ran := h.runPipeline(b, "b1", steps, nil)
	assert.Equal(t, []string{"extract"}, ran)
}

func TestDataVersionWhoseFilesAreGoneIsNotReused(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoStep("extract", "raw"), memoStep("report", "reports", 0)}
	h.build("b1", map[string]string{"extract": "v1", "report": "v1"})
	first, _ := h.runPipeline(b, "b1", steps, nil)

	// Someone deletes extract's manifest by hand: the catalog still says
	// committed, but the data version can't be read, so it runs again.
	var raw state.DataVersion
	for _, p := range h.dataVersions(first) {
		if p.Dataset == "raw" {
			raw = p
		}
	}
	path, ok := b.LocalPath(raw.Location)
	require.True(t, ok)
	require.NoError(t, os.Remove(path))
	second, ran := h.runPipeline(b, "b1", steps, nil)
	assert.Equal(t, []string{"extract"}, ran, "report's input has the same content, so it is reused")
	assert.Equal(t, "the earlier run's output can't be read: its files are gone", h.reasons(second)["extract"])

	// The new data version stands in from now on.
	_, ran = h.runPipeline(b, "b1", steps, nil)
	assert.Empty(t, ran)
}

func TestMemoSurvivesRestart(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	steps := []step{memoStep("extract", "raw")}
	h.build("b1", map[string]string{"extract": "v1"})
	h.runPipeline(b, "b1", steps, nil)

	h.restart(withBucket(b, nil))
	_, ran := h.runPipeline(b, "b1", steps, nil)
	assert.Empty(t, ran, "the build and the data version are known to the new leader")
}

func TestRegisterBuild(t *testing.T) {
	h := newHarness(t)
	h.build("b1", map[string]string{"a": "v1"})
	h.build("b1", map[string]string{"a": "v1"}) // again: a no-op
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Steps: map[string]string{"a": "v2"}}), ErrInvalid)
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{Steps: map[string]string{"a": "v1"}}), ErrInvalid)
	assert.ErrorIs(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b2", Steps: map[string]string{"a": ""}}), ErrInvalid)

	builds, err := h.store.LoadBuilds(h.ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "v1"}, builds["b1"].Steps)
}

func TestMemoParsingAndValidation(t *testing.T) {
	for in, want := range map[string]memoPolicy{
		"": {}, "never": {off: true}, "fingerprint": {on: true}, "ttl:90m": {on: true, ttl: 90 * time.Minute},
	} {
		got, err := parseMemo(in)
		require.NoError(t, err, in)
		assert.Equal(t, want, got, in)
	}
	for _, bad := range []string{"always", "ttl:", "ttl:-1h", "ttl:soon"} {
		_, err := parseMemo(bad)
		assert.Error(t, err, bad)
	}
	noOutput := stepCmd("x")
	noOutput.Memo = "fingerprint"
	assert.NoError(t, validateRunTaskResult(RunTaskResult{Commands: seqs(noOutput)}), "results are memoized too")
	noOutput.Memo = "always"
	assert.Error(t, validateRunTaskResult(RunTaskResult{Commands: seqs(noOutput)}))
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

// Steps without reuse= are fingerprinted too, never looked up, and
// StepRepeats tells whether they keep getting the same result: the reuse
// hint.
func TestStepsWithoutReuseAreFingerprintedForTheHint(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	plain := Command{Kind: CommandStep, Name: "rate", Args: json.RawMessage(`{"day":"2026-10-24"}`)} // no output, no reuse=
	table := output("load", "raw", "")                                                               // an output, no reuse=
	steps := []step{{cmd: plain}, {cmd: table}}
	h.build("b1", map[string]string{"rate": "v1", "load": "v1"})
	repeats := func(runID string) map[string]state.Repeats {
		t.Helper()
		ls, err := h.store.ListSteps(h.ctx, runID)
		require.NoError(t, err)
		out := map[string]state.Repeats{}
		for _, l := range ls {
			require.NotEmpty(t, l.Fingerprint, "every step has a fingerprint")
			dv := ""
			if l.Name == "load" {
				v, err := h.store.StepDataVersion(h.ctx, runID, l.Seq)
				require.NoError(t, err)
				dv = v.ID
			}
			r, err := h.store.StepRepeats(h.ctx, "t", l, dv)
			require.NoError(t, err)
			out[l.Name] = r
		}
		return out
	}

	first, _ := h.runPipeline(b, "b1", steps, map[string]string{"load": "a"})
	assert.Equal(t, map[string]state.Repeats{"rate": {}, "load": {}}, repeats(first), "nothing to compare with yet")
	id, ran := h.runPipeline(b, "b1", steps, map[string]string{"load": "a"})
	assert.Equal(t, []string{"rate", "load"}, ran, "without reuse= nothing is reused, fingerprint or not")
	assert.Equal(t, map[string]string{"rate": ReasonNoReuse, "load": ReasonNoReuse}, h.reasons(id))
	assert.Equal(t, map[string]state.Repeats{"rate": {Others: 1, Same: 1}, "load": {Others: 1, Same: 1}}, repeats(id))

	h.results = map[string]string{"rate": `1.09`}
	id, _ = h.runPipeline(b, "b1", steps, map[string]string{"load": "b"})
	assert.Equal(t, map[string]state.Repeats{"rate": {Others: 2, Same: 0}, "load": {Others: 2, Same: 0}}, repeats(id), "a new result: no hint")
}

// reuse=False (memo never): not reused on purpose, so its run reason says
// so and the reuse hint leaves it alone; the fingerprint is still there.
func TestReuseFalseIsNotReusedOnPurpose(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	live := output("extract", "raw", "")
	live.Memo = "never"
	h.build("b1", map[string]string{"extract": "v1"})
	h.runPipeline(b, "b1", []step{{cmd: live}}, nil)
	id, ran := h.runPipeline(b, "b1", []step{{cmd: live}}, nil)
	assert.Equal(t, []string{"extract"}, ran)
	assert.Equal(t, map[string]string{"extract": ReasonReuseOff}, h.reasons(id))
	assert.NotEmpty(t, h.step(id).Fingerprint)
	assert.Equal(t, "", history.WhyItRan(ReasonReuseOff), "nothing to explain: it said so")
}
