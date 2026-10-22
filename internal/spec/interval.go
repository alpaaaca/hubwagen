package spec

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
)

// Interval kinds. A run's data interval is computed from the schedule tick
// that created it, in the pipeline's timezone, so a "daily" interval is
// midnight to midnight local time (23 or 25 hours across DST changes).
const (
	IntervalHourly  = "hourly"  // previous full hour
	IntervalDaily   = "daily"   // previous full day
	IntervalWeekly  = "weekly"  // previous full ISO week (Monday 00:00 to Monday 00:00)
	IntervalMonthly = "monthly" // previous full calendar month
	IntervalCron    = "cron"    // previous tick to this tick
)

var validIntervals = map[string]bool{
	IntervalHourly: true, IntervalDaily: true, IntervalWeekly: true,
	IntervalMonthly: true, IntervalCron: true,
}

// Interval is a half-open data interval [Start, End).
type Interval struct {
	Start, End time.Time
}

func (iv Interval) IsZero() bool { return iv.Start.IsZero() && iv.End.IsZero() }

// Calendar computes schedule ticks and their data intervals for a Schedule.
type Calendar struct {
	sched    cron.Schedule
	loc      *time.Location
	interval string
	start    time.Time
}

func NewCalendar(s *Schedule) (*Calendar, error) {
	loc, err := time.LoadLocation(s.Timezone)
	if err != nil {
		return nil, fmt.Errorf("timezone %q: %w", s.Timezone, err)
	}
	sched, err := cron.ParseStandard(s.Cron)
	if err != nil {
		return nil, fmt.Errorf("cron %q: %w", s.Cron, err)
	}
	if !validIntervals[s.Interval] {
		return nil, fmt.Errorf("interval %q must be hourly|daily|weekly|monthly|cron", s.Interval)
	}
	c := &Calendar{sched: sched, loc: loc, interval: s.Interval}
	if s.Start != "" {
		if c.start, err = time.Parse(time.RFC3339, s.Start); err != nil {
			return nil, fmt.Errorf("start %q: %w", s.Start, err)
		}
	}
	return c, nil
}

// Location is the pipeline's timezone.
func (c *Calendar) Location() *time.Location { return c.loc }

// Start is the earliest interval start the pipeline schedules (zero if unset).
func (c *Calendar) Start() time.Time { return c.start }

// Next returns the first tick strictly after t.
func (c *Calendar) Next(t time.Time) time.Time {
	return c.sched.Next(t.In(c.loc))
}

// Prev returns the last tick strictly before t. The cron library only
// computes forward, so this widens a window backwards until it contains a
// tick, then walks forward to the last one before t. It returns the zero
// time if there is no tick in the 5 years before t.
func (c *Calendar) Prev(t time.Time) time.Time {
	t = t.In(c.loc)
	for w := time.Minute; w <= 5*366*24*time.Hour; w *= 2 {
		p := c.sched.Next(t.Add(-w))
		if !p.Before(t) {
			continue
		}
		for {
			n := c.sched.Next(p)
			if !n.Before(t) {
				return p
			}
			p = n
		}
	}
	return time.Time{}
}

// Calendar reports whether intervals are calendar periods (hourly, daily,
// weekly, monthly) rather than tick to tick. Calendar intervals are generated
// contiguously; ticks only decide when they are due. That matters around DST
// changes, where cron skips local times that don't exist and repeats ones
// that occur twice.
func (c *Calendar) Calendar() bool { return c.interval != IntervalCron }

// IntervalStarting returns the calendar interval that starts at start (which
// should be the End of the previous one). Only for calendar kinds.
func (c *Calendar) IntervalStarting(start time.Time) Interval {
	s := start.In(c.loc)
	y, m, d := s.Date()
	var end time.Time
	switch c.interval {
	case IntervalHourly:
		end = s.Add(time.Hour)
	case IntervalDaily:
		end = time.Date(y, m, d+1, 0, 0, 0, 0, c.loc)
	case IntervalWeekly:
		end = time.Date(y, m, d+7, 0, 0, 0, 0, c.loc)
	case IntervalMonthly:
		end = time.Date(y, m+1, 1, 0, 0, 0, 0, c.loc)
	}
	return Interval{Start: s, End: end}
}

// Interval returns the data interval for tick, computing the previous tick
// itself when the interval kind needs it.
func (c *Calendar) Interval(tick time.Time) Interval {
	var prev time.Time
	if c.interval == IntervalCron {
		prev = c.Prev(tick)
	}
	return c.IntervalFor(prev, tick)
}

// IntervalFor returns the data interval for tick. prevTick is only used by
// the "cron" interval kind, where the interval is [prevTick, tick).
func (c *Calendar) IntervalFor(prevTick, tick time.Time) Interval {
	t := tick.In(c.loc)
	y, m, d := t.Date()
	switch c.interval {
	case IntervalHourly:
		end := time.Date(y, m, d, t.Hour(), 0, 0, 0, c.loc)
		return Interval{Start: end.Add(-time.Hour), End: end}
	case IntervalDaily:
		end := time.Date(y, m, d, 0, 0, 0, 0, c.loc)
		return Interval{Start: time.Date(y, m, d-1, 0, 0, 0, 0, c.loc), End: end}
	case IntervalWeekly:
		offset := (int(t.Weekday()) + 6) % 7 // days since Monday
		end := time.Date(y, m, d-offset, 0, 0, 0, 0, c.loc)
		return Interval{Start: time.Date(y, m, d-offset-7, 0, 0, 0, 0, c.loc), End: end}
	case IntervalMonthly:
		end := time.Date(y, m, 1, 0, 0, 0, 0, c.loc)
		return Interval{Start: time.Date(y, m-1, 1, 0, 0, 0, 0, c.loc), End: end}
	default: // IntervalCron
		return Interval{Start: prevTick.In(c.loc), End: t}
	}
}

// checkCoverage samples consecutive ticks and reports a schedule whose ticks
// don't line up with its interval kind: firing more often than the interval
// (several runs for one interval) or less often (intervals with no tick of
// their own). A single anomaly is tolerated: that is what a DST change looks
// like (a skipped or repeated local time), and the scheduler fills such gaps
// itself. A schedule that doesn't fit its interval shows the anomaly again
// and again.
func (c *Calendar) checkCoverage(from time.Time, samples int) error {
	if c.interval == IntervalCron {
		return nil
	}
	var dupErr, gapErr error
	dups, gaps := 0, 0
	prev := c.Next(from)
	prevIv := c.IntervalFor(time.Time{}, prev)
	for i := 0; i < samples; i++ {
		tick := c.Next(prev)
		iv := c.IntervalFor(prev, tick)
		switch {
		case iv.Start.Equal(prevIv.Start):
			dups++
			dupErr = fmt.Errorf("schedule fires more than once per %s interval (ticks %s and %s both cover %s); "+
				"use a less frequent cron or interval: cron",
				c.interval, prev.Format(time.RFC3339), tick.Format(time.RFC3339), iv.Start.Format(time.RFC3339))
		case !iv.Start.Equal(prevIv.End):
			gaps++
			gapErr = fmt.Errorf("schedule skips %s intervals (no tick for %s to %s); "+
				"use interval: cron to make each run cover the time since the previous tick",
				c.interval, prevIv.End.Format(time.RFC3339), iv.Start.Format(time.RFC3339))
		}
		prev, prevIv = tick, iv
	}
	switch {
	case dups > 1:
		return dupErr
	case gaps > 1:
		return gapErr
	}
	return nil
}
