package schedule

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func calendar(t *testing.T, cronExpr, tz, interval string) *Calendar {
	t.Helper()
	c, err := NewCalendar(Schedule{Cron: cronExpr, Timezone: tz, Interval: interval})
	require.NoError(t, err)
	return c
}

func at(t *testing.T, loc *time.Location, s string) time.Time {
	t.Helper()
	v, err := time.ParseInLocation("2006-01-02 15:04", s, loc)
	require.NoError(t, err)
	return v
}

func TestIntervalDailyAcrossDST(t *testing.T) {
	c := calendar(t, "0 2 * * *", "Europe/Berlin", IntervalDaily)
	loc := c.Location()

	// 2026-03-29: clocks go forward, the day has 23 hours.
	iv := c.IntervalFor(time.Time{}, at(t, loc, "2026-03-30 02:00"))
	assert.Equal(t, at(t, loc, "2026-03-29 00:00"), iv.Start)
	assert.Equal(t, at(t, loc, "2026-03-30 00:00"), iv.End)
	assert.Equal(t, 23*time.Hour, iv.End.Sub(iv.Start))

	// 2026-10-25: clocks go back, the day has 25 hours.
	iv = c.IntervalFor(time.Time{}, at(t, loc, "2026-10-26 02:00"))
	assert.Equal(t, 25*time.Hour, iv.End.Sub(iv.Start))
}

func TestIntervalKinds(t *testing.T) {
	utc := time.UTC
	tick := at(t, utc, "2026-10-21 14:30") // a Wednesday

	cases := []struct {
		interval   string
		start, end string
	}{
		{IntervalHourly, "2026-10-21 13:00", "2026-10-21 14:00"},
		{IntervalDaily, "2026-10-20 00:00", "2026-10-21 00:00"},
		{IntervalWeekly, "2026-10-12 00:00", "2026-10-19 00:00"},
		{IntervalMonthly, "2026-09-01 00:00", "2026-10-01 00:00"},
	}
	for _, tc := range cases {
		c := calendar(t, "30 14 * * *", "UTC", tc.interval)
		iv := c.IntervalFor(time.Time{}, tick)
		assert.Equal(t, at(t, utc, tc.start), iv.Start, tc.interval)
		assert.Equal(t, at(t, utc, tc.end), iv.End, tc.interval)
	}

	c := calendar(t, "0 2 * * 1-5", "UTC", IntervalCron)
	prev := at(t, utc, "2026-10-16 02:00") // Friday
	next := c.Next(prev)
	assert.Equal(t, at(t, utc, "2026-10-19 02:00"), next) // Monday
	assert.Equal(t, Interval{Start: prev, End: next}, c.IntervalFor(prev, next))
}

func TestIntervalMonthlyAcrossYearBoundary(t *testing.T) {
	c := calendar(t, "0 3 1 * *", "America/New_York", IntervalMonthly)
	loc := c.Location()
	iv := c.IntervalFor(time.Time{}, at(t, loc, "2027-01-01 03:00"))
	assert.Equal(t, at(t, loc, "2026-12-01 00:00"), iv.Start)
	assert.Equal(t, at(t, loc, "2027-01-01 00:00"), iv.End)
}

func TestCheckCoverage(t *testing.T) {
	from := time.Date(2026, 1, 5, 0, 0, 0, 0, time.UTC)
	ok := []struct{ cron, interval string }{
		{"0 2 * * *", IntervalDaily},
		{"15 * * * *", IntervalHourly},
		{"0 6 * * 1", IntervalWeekly},
		{"0 0 1 * *", IntervalMonthly},
		{"*/5 * * * *", IntervalCron},
	}
	for _, tc := range ok {
		assert.NoError(t, calendar(t, tc.cron, "UTC", tc.interval).CheckCoverage(from, 20), tc.cron)
	}
	assert.ErrorContains(t, calendar(t, "0 */6 * * *", "UTC", IntervalDaily).CheckCoverage(from, 20), "more than once")
	assert.ErrorContains(t, calendar(t, "0 2 * * 1", "UTC", IntervalDaily).CheckCoverage(from, 20), "skips")

	// DST changes skip or repeat one local time; that alone is not an error.
	for _, cron := range []string{"0 2 * * *", "30 2 * * *"} {
		c := calendar(t, cron, "Europe/Berlin", IntervalDaily)
		for _, from := range []string{"2026-03-20 00:00", "2026-10-15 00:00"} {
			assert.NoError(t, c.CheckCoverage(at(t, c.Location(), from), 20), cron+" from "+from)
		}
	}
}

func TestIntervalStarting(t *testing.T) {
	c := calendar(t, "0 2 1 * *", "Europe/Berlin", IntervalMonthly)
	loc := c.Location()
	iv := c.IntervalStarting(at(t, loc, "2026-02-01 00:00"))
	assert.Equal(t, at(t, loc, "2026-03-01 00:00"), iv.End)

	d := calendar(t, "0 2 * * *", "Europe/Berlin", IntervalDaily)
	iv = d.IntervalStarting(at(t, loc, "2026-03-29 00:00"))
	assert.Equal(t, 23*time.Hour, iv.End.Sub(iv.Start))
	assert.True(t, d.Calendar())
	assert.False(t, calendar(t, "0 2 * * *", "UTC", IntervalCron).Calendar())
}

func TestPrev(t *testing.T) {
	utc := time.UTC
	cases := []struct{ cron, at, want string }{
		{"0 2 * * *", "2026-10-21 02:00", "2026-10-20 02:00"},
		{"0 2 * * *", "2026-10-21 01:59", "2026-10-20 02:00"},
		{"0 2 * * 1-5", "2026-10-19 02:00", "2026-10-16 02:00"}, // Monday → Friday
		{"*/5 * * * *", "2026-10-21 10:07", "2026-10-21 10:05"},
		{"0 0 1 1 *", "2026-06-01 00:00", "2026-01-01 00:00"},
	}
	for _, tc := range cases {
		c := calendar(t, tc.cron, "UTC", IntervalCron)
		assert.Equal(t, at(t, utc, tc.want), c.Prev(at(t, utc, tc.at)), tc.cron+" before "+tc.at)
	}

	c := calendar(t, "0 2 * * 1-5", "UTC", IntervalCron)
	monday := at(t, utc, "2026-10-19 02:00")
	assert.Equal(t, Interval{Start: at(t, utc, "2026-10-16 02:00"), End: monday}, c.Interval(monday))
}
