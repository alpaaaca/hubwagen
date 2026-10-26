package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func days(fires []Fire) []string {
	var out []string
	for _, f := range fires {
		out = append(out, f.Interval.Start.Format("01-02")+"→"+f.Interval.End.Format("01-02"))
	}
	return out
}

func TestDueCatchesUpContiguousDays(t *testing.T) {
	c := calendar(t, "0 2 * * *", "Europe/Berlin", IntervalDaily)
	loc := c.Location()
	cursor := at(t, loc, "2026-03-27 00:00") // the end of the 26th's interval

	// Down from the 27th to the 31st, 03:00: four intervals are due,
	// including the 23-hour 29th (DST starts).
	fires, next, more := c.Due(cursor, at(t, loc, "2026-03-31 03:00"), true, 0)
	assert.Equal(t, []string{"03-27→03-28", "03-28→03-29", "03-29→03-30", "03-30→03-31"}, days(fires))
	assert.False(t, more)
	assert.Equal(t, at(t, loc, "2026-03-31 00:00"), next)
	assert.Equal(t, 23*time.Hour, fires[2].Interval.End.Sub(fires[2].Interval.Start))

	// Nothing more until the next tick.
	fires, same, _ := c.Due(next, at(t, loc, "2026-03-31 23:00"), true, 0)
	assert.Empty(t, fires)
	assert.Equal(t, next, same)
}

func TestDueSurvivesSkippedTick(t *testing.T) {
	// 02:30 doesn't exist on 2026-03-29 in Berlin: cron skips that tick, but
	// the 28th's interval is still scheduled (with the next tick).
	c := calendar(t, "30 2 * * *", "Europe/Berlin", IntervalDaily)
	loc := c.Location()
	fires, _, _ := c.Due(at(t, loc, "2026-03-28 00:00"), at(t, loc, "2026-03-30 03:00"), true, 0)
	assert.Equal(t, []string{"03-28→03-29", "03-29→03-30"}, days(fires))
}

func TestDueWithoutCatchupAndInBatches(t *testing.T) {
	c := calendar(t, "0 1 * * *", "UTC", IntervalDaily)
	cursor := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 10, 11, 2, 0, 0, 0, time.UTC)

	fires, next, more := c.Due(cursor, now, false, 0)
	assert.Equal(t, []string{"10-10→10-11"}, days(fires), "only the latest")
	assert.False(t, more)
	assert.Equal(t, time.Date(2026, 10, 11, 0, 0, 0, 0, time.UTC), next)

	fires, next, more = c.Due(cursor, now, true, 4)
	assert.Equal(t, []string{"10-01→10-02", "10-02→10-03", "10-03→10-04", "10-04→10-05"}, days(fires))
	assert.True(t, more)
	fires, _, more = c.Due(next, now, true, 0)
	assert.Len(t, fires, 6)
	assert.False(t, more)
}

func TestDueCronIntervals(t *testing.T) {
	start := time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC)
	c, err := NewCalendar(Schedule{Cron: "0 */6 * * *", Timezone: "UTC", Interval: IntervalCron, Start: start})
	require.NoError(t, err)

	cursor := c.InitialCursor(time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC))
	assert.Equal(t, start, cursor, "a start date anchors the cursor")
	fires, next, more := c.Due(time.Date(2026, 10, 1, 5, 0, 0, 0, time.UTC), time.Date(2026, 10, 1, 19, 0, 0, 0, time.UTC), true, 0)
	require.Len(t, fires, 3) // 06:00, 12:00, 18:00
	assert.True(t, fires[0].Skipped, "06:00 covers 00:00-06:00, before the start date")
	assert.Equal(t, "interval starts before the schedule's start date", fires[0].Reason)
	assert.True(t, fires[1].Skipped, "12:00 covers 06:00-12:00, still before it")
	assert.False(t, fires[2].Skipped)
	assert.Equal(t, time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC), fires[2].Interval.Start)
	assert.Equal(t, fires[2].Tick, next)
	assert.False(t, more)
}

func TestInitialCursor(t *testing.T) {
	c := calendar(t, "0 2 * * *", "Europe/Berlin", IntervalDaily)
	loc := c.Location()
	cursor := c.InitialCursor(at(t, loc, "2026-10-25 15:00"))
	assert.Equal(t, at(t, loc, "2026-10-25 00:00"), cursor, "the day it is now is the first interval")
	fires, _, _ := c.Due(cursor, at(t, loc, "2026-10-26 01:00"), true, 0)
	assert.Empty(t, fires, "due only once the day is over and the 02:00 tick came")
	fires, _, _ = c.Due(cursor, at(t, loc, "2026-10-26 02:00"), true, 0)
	assert.Equal(t, []string{"10-25→10-26"}, days(fires))
}
