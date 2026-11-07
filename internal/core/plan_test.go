package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/moksha/internal/state"
)

func TestPlanSaysWhatTheLatestEarlierRunsStepsWouldDo(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	report := output("report", "reports", "") // no reuse
	steps := []step{memoStep("extract", "raw"), memoStep("transform", "mart", 0), {cmd: report, from: []int{1}}}
	h.build("b1", map[string]string{"extract": "v1", "transform": "v1", "report": "v1"})
	day := map[string]any{"day": "2026-10-24"} // what the harness's runs take
	plan := func(params map[string]any, reuse string) RunPlan {
		t.Helper()
		p, err := h.core.PlanRun(h.ctx, "t", "pipeline", params, reuse)
		require.NoError(t, err)
		return p
	}
	type want struct {
		reuse bool
		why   string
	}
	got := func(p RunPlan) map[string]want {
		out := map[string]want{}
		for _, s := range p.Steps {
			out[s.Name] = want{s.Reuse, s.Why}
		}
		return out
	}

	assert.Nil(t, plan(day, "").Earlier, "no earlier run: every step runs")
	id, _ := h.runPipeline(b, "b1", steps, nil)

	p := plan(day, "")
	require.NotNil(t, p.Earlier)
	assert.Equal(t, id, p.Earlier.ID)
	assert.Equal(t, map[string]want{"extract": {true, ""}, "transform": {true, ""}, "report": {false, "it has no reuse="}}, got(p))
	assert.Nil(t, plan(map[string]any{"day": "2026-10-25"}, "").Earlier, "other inputs: no earlier run")

	h.build("b2", map[string]string{"extract": "v2", "transform": "v1", "report": "v1"})
	assert.Equal(t, map[string]want{
		"extract":   {false, "its code changed (v1 → v2)"},
		"transform": {true, "unless a step before it returns something new"},
		"report":    {false, "it has no reuse="},
	}, got(plan(day, "")))

	none := got(plan(day, state.ReuseNone))
	assert.Equal(t, want{false, "the run would be started with --reuse=none"}, none["transform"])

	_, err := h.core.PlanRun(h.ctx, "t", "pipeline", day, "maybe")
	assert.ErrorIs(t, err, ErrInvalid)

	// The current code is the deployed build's, not the newest registered:
	// deploying v1's build again after v3 runs v1's code.
	deploy := func(build, extract string) {
		t.Helper()
		_, err := h.core.Deploy(h.ctx, DeployRequest{Project: "revenue", Image: "acme/revenue:" + build, By: "ana",
			Build: BuildRequest{ID: build, Pipelines: []string{"pipeline"}, Steps: map[string]string{"extract": extract, "transform": "v1", "report": "v1"}},
			Pools: []state.DeployedPool{{Name: "default", Queues: []string{DefaultQueue}, Max: 1, Steps: 1, IdleAfter: time.Minute}}})
		require.NoError(t, err)
	}
	deploy("b3", "v3")
	assert.Equal(t, want{false, "its code changed (v1 → v3)"}, got(plan(day, ""))["extract"])
	deploy("b1", "v1")
	assert.Equal(t, want{true, ""}, got(plan(day, ""))["extract"], "b1 runs again, though b2 and b3 were registered after it")
}
