package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// t0 is 2026-10-21 02:00 UTC = 04:00 in Berlin.
const berlinDaily = agentDefaults + `
  schedule: {cron: "0 2 * * *", timezone: Europe/Berlin, interval: daily}
  tasks: [{id: a, command: [x]}]
`

func (h *harness) runs(pipeline string) []state.Run {
	h.t.Helper()
	runs, err := h.store.ListRuns(h.ctx, state.RunFilter{Pipeline: pipeline, Limit: 1000})
	require.NoError(h.t, err)
	// ListRuns is newest first; tests read oldest first.
	for i, j := 0, len(runs)-1; i < j; i, j = i+1, j-1 {
		runs[i], runs[j] = runs[j], runs[i]
	}
	return runs
}

func (h *harness) cronTicks() []cronTickedPayload {
	h.t.Helper()
	evs, err := h.store.EventsSince(h.ctx, 0, "", 10000)
	require.NoError(h.t, err)
	var out []cronTickedPayload
	for _, e := range evs {
		if e.Type == state.EventCronTicked {
			var p cronTickedPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			out = append(out, p)
		}
	}
	return out
}

func berlin(t *testing.T, s string) time.Time {
	t.Helper()
	loc, err := time.LoadLocation("Europe/Berlin")
	require.NoError(t, err)
	v, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
	require.NoError(t, err)
	return v
}

func TestCronCreatesOneRunPerTick(t *testing.T) {
	h := newHarness(t)
	h.publish("p", berlinDaily)
	assert.Empty(t, h.runs("p"), "publishing doesn't backfill without a start date")

	h.advance(20 * time.Hour) // 2026-10-21 22:00 UTC = 2026-10-22 00:00 Berlin
	assert.Empty(t, h.runs("p"))
	h.advance(2 * time.Hour) // 02:00 Berlin
	runs := h.runs("p")
	require.Len(t, runs, 1)
	assert.Equal(t, "cron", runs[0].Trigger)
	assert.Equal(t, "scheduled", runs[0].Priority)
	assert.Equal(t, berlin(t, "2026-10-21 00:00"), runs[0].IntervalStart.In(berlin(t, "2026-01-01 00:00").Location()))
	assert.Equal(t, berlin(t, "2026-10-22 00:00").UTC(), runs[0].IntervalEnd.UTC())

	h.succeed(runs[0].ID, "a")
	h.advance(24 * time.Hour)
	assert.Len(t, h.runs("p"), 2)
}

func TestCronCatchupFromStartDate(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  schedule: {cron: "0 2 * * *", timezone: Europe/Berlin, interval: daily, start: 2026-10-17}
  tasks: [{id: a, command: [x]}]
`)
	runs := h.runs("p")
	require.Len(t, runs, 4, "intervals 10-17, 10-18, 10-19, 10-20 are complete by 10-21 04:00 Berlin")
	for i, r := range runs {
		assert.Equal(t, berlin(t, "2026-10-17 00:00").AddDate(0, 0, i).UTC(), r.IntervalStart.UTC())
	}
	assert.Equal(t, state.RunRunning, runs[0].Status)
	assert.Equal(t, state.RunQueued, runs[3].Status, "catch-up runs queue behind max_active_runs")
}

func TestCronWithoutCatchupRunsOnlyTheLatestInterval(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  schedule: {cron: "0 2 * * *", timezone: Europe/Berlin, interval: daily, start: 2026-10-01, catchup: false}
  tasks: [{id: a, command: [x]}]
`)
	runs := h.runs("p")
	require.Len(t, runs, 1)
	assert.Equal(t, berlin(t, "2026-10-20 00:00").UTC(), runs[0].IntervalStart.UTC())
}

func TestCronCatchesUpAfterDowntimeExactlyOnce(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  concurrency: {max_active_runs: 10}
  schedule: {cron: "0 * * * *", interval: hourly}
  tasks: [{id: a, command: [x]}]
`)
	h.advance(time.Hour)
	require.Len(t, h.runs("p"), 1)

	// The leader is down for 5 hours, then a new one starts.
	h.stop()
	h.clock.Advance(5 * time.Hour)
	h.start()
	require.Len(t, h.runs("p"), 6, "five missed ticks caught up")

	h.restart()
	assert.Len(t, h.runs("p"), 6, "a restart without missed ticks creates nothing")

	keys := map[string]bool{}
	for _, r := range h.runs("p") {
		assert.False(t, keys[r.RunKey], "duplicate run key %s", r.RunKey)
		keys[r.RunKey] = true
	}
}

func TestCronLargeCatchupIsBatched(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  concurrency: {max_active_runs: 1000}
  schedule: {cron: "*/5 * * * *", interval: cron, start: 2026-10-20}
  tasks: [{id: a, command: [x]}]
`)
	h.advance(0) // let the follow-up batches run
	h.advance(0)
	h.advance(0)
	runs := h.runs("p")
	assert.Len(t, runs, 26*12, "every 5 minutes from 10-20 00:00 to 10-21 02:00")
	assert.Equal(t, 5*time.Minute, runs[1].IntervalEnd.Sub(*runs[1].IntervalStart), "cron intervals run tick to tick")
}

func TestCronSkipIsRecorded(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  concurrency: {on_overlap: skip}
  schedule: {cron: "0 * * * *", interval: hourly}
  tasks: [{id: a, command: [x]}]
`)
	h.advance(time.Hour)
	h.advance(time.Hour) // first run still running
	ticks := h.cronTicks()
	require.Len(t, ticks, 2)
	assert.NotEmpty(t, ticks[0].RunID)
	assert.True(t, ticks[1].Skipped)
	assert.Contains(t, ticks[1].Reason, "on_overlap: skip")
	assert.Len(t, h.runs("p"), 1)
}

func TestCronFollowsNewVersions(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  schedule: {cron: "0 * * * *", interval: hourly}
  concurrency: {max_active_runs: 10}
  tasks: [{id: a, command: [x]}]
`)
	h.advance(time.Hour)
	require.Len(t, h.runs("p"), 1)

	h.publish("p", agentDefaults+"  concurrency: {max_active_runs: 10}\n  tasks: [{id: a, command: [x]}]\n") // schedule removed
	h.advance(3 * time.Hour)
	assert.Len(t, h.runs("p"), 1)

	h.publish("p", agentDefaults+`
  schedule: {cron: "30 * * * *", interval: cron}
  concurrency: {max_active_runs: 10}
  tasks: [{id: a, command: [x]}]
`)
	assert.Len(t, h.runs("p"), 4, "re-added schedule catches up from the old cursor")
}

func TestPauseAndResume(t *testing.T) {
	h := newHarness(t)
	h.publish("p", agentDefaults+`
  schedule: {cron: "0 * * * *", interval: hourly, catchup: false}
  concurrency: {max_active_runs: 10}
  tasks: [{id: a, command: [x]}]
`)
	require.NoError(t, h.core.SetPaused(h.ctx, "t", "p", true, "alice"))
	h.advance(3 * time.Hour)
	assert.Empty(t, h.runs("p"))
	assert.False(t, h.trigger(TriggerRequest{Pipeline: "p"}).Skipped, "manual triggers still work")

	require.NoError(t, h.core.SetPaused(h.ctx, "t", "p", false, "alice"))
	ticks := h.cronTicks()
	require.Len(t, ticks, 1, "catchup: false resumes with only the latest interval")

	p, err := h.store.GetPipeline(h.ctx, "t", "p")
	require.NoError(t, err)
	assert.False(t, p.Paused)
	assert.ErrorIs(t, h.core.SetPaused(h.ctx, "t", "nope", true, "alice"), ErrNotFound)
}

// The cron library skips 02:30 on the night clocks go forward and fires twice
// at 02:30 when they go back. Every daily interval must still get exactly one
// run.
func TestCronDailyIntervalsSurviveDSTChanges(t *testing.T) {
	for name, tc := range map[string]struct{ start, from string }{
		"spring forward": {"2026-03-26", "2026-03-26 12:00"},
		"fall back":      {"2026-10-22", "2026-10-22 12:00"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarnessAt(t, berlin(t, tc.from))
			h.publish("p", agentDefaults+`
  concurrency: {max_active_runs: 100}
  schedule: {cron: "30 2 * * *", timezone: Europe/Berlin, interval: daily, start: `+tc.start+`}
  tasks: [{id: a, command: [x]}]
`)
			for i := 0; i < 7*24; i++ {
				h.advance(time.Hour)
			}
			runs := h.runs("p")
			require.Len(t, runs, 7, "one run per day, none missing, none doubled")
			for i, r := range runs {
				want := berlin(t, tc.start+" 00:00").AddDate(0, 0, i)
				assert.Equal(t, want.UTC(), r.IntervalStart.UTC(), "run %d", i)
				assert.Equal(t, want.AddDate(0, 0, 1).UTC(), r.IntervalEnd.UTC(), "run %d", i)
			}
		})
	}
}
