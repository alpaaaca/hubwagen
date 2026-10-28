package core

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// t0 is 2026-10-21 02:00 UTC. A daily schedule at 03:00 starts with the
// interval that begins at the next midnight: its first run is 2026-10-22
// 03:00, for 2026-10-21.
var daily = ScheduleSpec{Haul: "daily_revenue", Cron: "0 3 * * *", Interval: "daily"}

func (h *harness) scheduleBuild(id string, specs ...ScheduleSpec) {
	h.t.Helper()
	require.NoError(h.t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: id, Hauls: []string{"daily_revenue", "x"}, Schedules: specs}))
}

// scheduled returns the ScheduleFired payloads so far.
func (h *harness) scheduled() []scheduleFiredPayload {
	h.t.Helper()
	evs, err := h.store.EventsSince(h.ctx, 0, "", 1<<20)
	require.NoError(h.t, err)
	var out []scheduleFiredPayload
	for _, e := range evs {
		if e.Type == state.EventScheduleFired {
			var p scheduleFiredPayload
			require.NoError(h.t, json.Unmarshal(e.Payload, &p))
			out = append(out, p)
		}
	}
	return out
}

func day(d int) time.Time { return time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC) }

func TestScheduleStartsAHaulPerInterval(t *testing.T) {
	h := newHarness(t)
	h.scheduleBuild("b1", daily)
	h.advance(25 * time.Hour) // 2026-10-22 03:00
	fired := h.scheduled()
	require.Len(t, fired, 1)
	f := fired[0]
	assert.Equal(t, day(21), f.Start.UTC())
	assert.Equal(t, day(22), f.End.UTC())
	got := h.haul(f.HaulID)
	assert.Equal(t, "daily_revenue", got.Name)
	assert.Equal(t, "schedule", got.Trigger)
	assert.Equal(t, ScheduleTenant, got.Tenant)
	assert.Equal(t, map[string]any{"interval": map[string]any{"$interval": map[string]any{
		"start": "2026-10-21T00:00:00Z", "end": "2026-10-22T00:00:00Z"}}}, got.Params)

	h.advance(24 * time.Hour)
	assert.Len(t, h.scheduled(), 2, "the next day")
	scheds, err := h.store.ListSchedules(h.ctx)
	require.NoError(t, err)
	require.Len(t, scheds, 1)
	assert.Equal(t, day(23), scheds[0].Cursor.UTC(), "the cursor is persisted")
}

func TestScheduleInItsTimezone(t *testing.T) {
	h := newHarness(t)
	berlin := daily
	berlin.Timezone = "Europe/Berlin" // UTC+2 until 2026-10-25, then +1
	h.scheduleBuild("b1", berlin)
	h.advance(23 * time.Hour) // 2026-10-22 01:00 UTC
	fired := h.scheduled()
	require.NotEmpty(t, fired)
	loc, _ := time.LoadLocation("Europe/Berlin")
	assert.Equal(t, time.Date(2026, 10, 21, 0, 0, 0, 0, loc).UTC(), fired[0].Start.UTC(), "midnight in Berlin")
	assert.Equal(t, time.Date(2026, 10, 22, 1, 0, 0, 0, time.UTC), fired[0].Tick.UTC(), "03:00 Berlin")
}

func TestNewestBuildDecidesTheSchedule(t *testing.T) {
	h := newHarness(t)
	h.scheduleBuild("b1", daily)
	hourly := daily
	hourly.Cron, hourly.Interval = "0 * * * *", "hourly"
	h.scheduleBuild("b2", hourly)
	for range 3 {
		h.advance(time.Hour) // the fake clock jumps: one tick at a time, so each is on time
	}
	assert.Len(t, h.scheduled(), 3, "hourly now")

	h.scheduleBuild("b3") // defines the haul without a schedule: removed
	h.advance(48 * time.Hour)
	assert.Len(t, h.scheduled(), 3)
	scheds, err := h.store.ListSchedules(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, scheds)

	// Registering a known build again changes nothing.
	h.scheduleBuild("b2", hourly)
	h.advance(time.Hour)
	assert.Len(t, h.scheduled(), 3)
}

func TestCatchUpAfterDowntime(t *testing.T) {
	for policy, want := range map[string]int{CatchUpAll: 3, CatchUpLatest: 1, CatchUpSkip: 0} {
		t.Run(policy, func(t *testing.T) {
			h := newHarness(t)
			spec := daily
			spec.CatchUp = policy
			h.scheduleBuild("b1", spec)
			require.NoError(t, h.stop())
			h.clock.Advance(3*24*time.Hour + 2*time.Hour) // no leader from 10-21 02:00 to 10-24 04:00
			h.start()

			fired := h.scheduled()
			started := 0
			for _, f := range fired {
				if f.HaulID != "" {
					started++
				}
			}
			assert.Equal(t, want, started)
			if policy == CatchUpAll {
				assert.Equal(t, []time.Time{day(21), day(22), day(23)},
					[]time.Time{fired[0].Start.UTC(), fired[1].Start.UTC(), fired[2].Start.UTC()}, "oldest first")
			}
			if policy == CatchUpSkip {
				require.Len(t, fired, 1)
				assert.Contains(t, fired[0].Skipped, "catch_up: skip")
			}
			scheds, err := h.store.ListSchedules(h.ctx)
			require.NoError(t, err)
			assert.Equal(t, day(24), scheds[0].Cursor.UTC(), "everything up to now is handled")

			h.advance(24 * time.Hour) // back on time: runs as usual
			assert.Len(t, h.scheduled(), len(fired)+1)
		})
	}
}

func TestScheduleValidation(t *testing.T) {
	h := newHarness(t)
	err := h.core.RegisterBuild(h.ctx, BuildRequest{ID: "bad", Hauls: []string{"x"}, Schedules: []ScheduleSpec{
		{Haul: "nope", Cron: "0 3 * * *"},
		{Haul: "x", Cron: "every day"},
		{Haul: "x", Cron: "0 3 * * *", CatchUp: "some"},
	}})
	require.ErrorIs(t, err, ErrInvalid)
	for _, want := range []string{`schedules[0]: haul "nope" isn't one of the build's hauls`, `schedules[1] (x): cron "every day"`,
		`schedules[2]: haul "x" has two schedules`, `catch_up "some" must be skip, latest or all`} {
		assert.ErrorContains(t, err, want)
	}
}
