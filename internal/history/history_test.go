package history

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/alper/hubwagen/internal/state"
)

var t0 = time.Date(2026, 10, 29, 3, 0, 0, 0, time.UTC)

func ev(at time.Duration, typ state.EventType, payload map[string]any) state.Event {
	b, _ := json.Marshal(payload)
	return state.Event{Time: t0.Add(at), Type: typ, HaulID: "run2", Payload: b}
}

func TestARunInPlainWords(t *testing.T) {
	evs := []state.Event{
		ev(0, state.EventHaulStarted, map[string]any{"name": "daily", "trigger": "schedule",
			"params": map[string]any{"interval": map[string]any{"$interval": map[string]any{
				"start": "2026-10-28T00:00:00+01:00", "end": "2026-10-29T00:00:00+01:00"}}}}),
		ev(10*time.Millisecond, state.EventHaulTaskQueued, map[string]any{"number": 1}),
		ev(20*time.Millisecond, state.EventLiftScheduled, map[string]any{"seq": 1, "name": "extract_orders", "run_reason": "no previous run"}),
		ev(30*time.Millisecond, state.EventLiftAttemptDispatched, map[string]any{"seq": 1, "number": 1, "worker": "worker-1"}),
		ev(830*time.Millisecond, state.EventLiftAttemptFinished, map[string]any{"seq": 1, "number": 1, "status": "succeeded"}),
		ev(830*time.Millisecond, state.EventLiftFinished, map[string]any{"seq": 1, "status": "completed", "pallet": "p1"}),
		ev(time.Second, state.EventLiftScheduled, map[string]any{"seq": 2, "name": "daily_revenue"}),
		ev(time.Second, state.EventLiftReused, map[string]any{"seq": 2, "pallet": "p0"}),
		ev(time.Second, state.EventLiftFinished, map[string]any{"seq": 2, "status": "completed", "pallet": "p0", "reused": true}),
		ev(time.Second, state.EventLiftScheduled, map[string]any{"seq": 3, "name": "revenue_report", "run_reason": "code changed (a1 → b2), input changed"}),
		ev(time.Second, state.EventLiftAttemptDispatched, map[string]any{"seq": 3, "number": 1, "worker": "worker-2"}),
		ev(2*time.Second, state.EventLiftAttemptFinished, map[string]any{"seq": 3, "number": 1, "status": "failed", "reason": "KeyError", "message": "'region'"}),
		ev(2*time.Second, state.EventLiftRetryScheduled, map[string]any{"seq": 3, "next_attempt": 2, "retry_at": t0.Add(12 * time.Second)}),
		ev(12*time.Second, state.EventLiftAttemptDispatched, map[string]any{"seq": 3, "number": 2, "worker": "worker-2"}),
		ev(14*time.Second, state.EventLiftFinished, map[string]any{"seq": 3, "status": "failed", "reason": "KeyError", "message": "'region'"}),
		ev(14*time.Second, state.EventHaulFinished, map[string]any{"status": "failed", "reason": "LiftFailed", "message": "revenue_report failed"}),
	}
	look := Lookup{
		Pallet: func(id string) (state.Pallet, bool) {
			return map[string]state.Pallet{
				"p1": {Dataset: "raw.orders", Partition: "2026-10-28", HaulID: "run2"},
				"p0": {Dataset: "mart.revenue", Partition: "2026-10-28", HaulID: "run1"},
			}[id], true
		},
		Haul: func(id string) (state.Haul, bool) {
			return state.Haul{Name: "daily", Params: map[string]any{"day": "2026-10-28"}}, id == "run1"
		},
	}
	type line struct{ Kind, Step, Text, Detail, Run string }
	var got []line
	for _, e := range Tell(evs, look) {
		got = append(got, line{e.Kind, e.Step, e.Text, e.Detail, e.Run})
	}
	assert.Equal(t, []line{
		{KindStarted, "", "Started by its schedule, for 2026-10-28", "", ""},
		{KindEngine, "", "Waits for a worker to run the pipeline's code", "", ""},
		{KindStep, "extract_orders", "queued; it runs because it hasn't run with these inputs before", "", ""},
		{KindStep, "extract_orders", "started on worker-1", "", ""},
		{KindEngine, "extract_orders", "attempt 1 succeeded", "", ""},
		{KindStep, "extract_orders", "done in 800 ms, wrote a new version of raw.orders for 2026-10-28", "", ""},
		{KindEngine, "daily_revenue", "queued", "", ""},
		{KindReused, "daily_revenue", "reused the existing version of mart.revenue for 2026-10-28: same inputs and same code, so it didn't run again (from the run of daily for day=2026-10-28)", "", "run1"},
		{KindEngine, "daily_revenue", "done (reused)", "", ""},
		{KindStep, "revenue_report", "queued; it runs because its code changed (a1 → b2), an input changed", "", ""},
		{KindStep, "revenue_report", "started on worker-2", "", ""},
		{KindProblem, "revenue_report", "attempt 1 failed", "KeyError: 'region'", ""},
		{KindRetry, "revenue_report", "tries again in 10 s (attempt 2)", "", ""},
		{KindStep, "revenue_report", "attempt 2 started on worker-2", "", ""},
		{KindProblem, "revenue_report", "failed in 2.0 s", "KeyError: 'region'", ""},
		{KindFinished, "", "Failed in 14 s", "LiftFailed: revenue_report failed", ""},
	}, got)
}

func TestReusedFromARunWithTheSameInputs(t *testing.T) {
	evs := []state.Event{
		ev(0, state.EventHaulStarted, map[string]any{"name": "daily", "params": map[string]any{"day": "2026-10-28"}}),
		ev(time.Second, state.EventLiftScheduled, map[string]any{"seq": 1, "name": "build"}),
		ev(time.Second, state.EventLiftReused, map[string]any{"seq": 1, "haul": "run1", "on_seq": 1}),
	}
	look := Lookup{Haul: func(id string) (state.Haul, bool) {
		return state.Haul{Name: "daily", Params: map[string]any{"day": "2026-10-28"}}, true
	}}
	e := Tell(evs, look)[2]
	assert.Equal(t, "reused: same inputs and same code as before, so it didn't run again (from an earlier run with the same inputs)", e.Text)
	assert.Equal(t, "run1", e.Run)
}

func TestInputs(t *testing.T) {
	iv := func(s, e string) map[string]any {
		return map[string]any{"$interval": map[string]any{"start": s, "end": e}}
	}
	assert.Equal(t, "day=2026-10-01, region=eu", Inputs(map[string]any{"region": "eu", "day": "2026-10-01"}))
	assert.Equal(t, "2026-10-01 – 2026-10-07", Inputs(map[string]any{"interval": iv("2026-10-01T00:00:00Z", "2026-10-08T00:00:00Z")}))
	assert.Equal(t, "interval=2026-10-01 13:00 – 14:00, n=1", Inputs(map[string]any{"n": 1, "interval": iv("2026-10-01T13:00:00Z", "2026-10-01T14:00:00Z")}))
	assert.Equal(t, "n=3, on=2026-10-01", Inputs(map[string]any{"n": 3, "on": map[string]any{"$date": "2026-10-01"}}))
	assert.Equal(t, "", Inputs(nil))
}

func TestTook(t *testing.T) {
	assert.Equal(t, "45 ms", Took(45*time.Millisecond))
	assert.Equal(t, "3.2 s", Took(3200*time.Millisecond))
	assert.Equal(t, "42 s", Took(42*time.Second))
	assert.Equal(t, "4 min 5 s", Took(4*time.Minute+5*time.Second))
	assert.Equal(t, "2 h 10 min", Took(2*time.Hour+10*time.Minute))
}
