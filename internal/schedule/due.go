package schedule

import "time"

// What is due. A schedule keeps a cursor, persisted with whatever it
// started, so nothing is scheduled twice and what was missed while no
// leader ran is caught up:
//
//   - calendar kinds (hourly, daily, weekly, monthly): the cursor is the end
//     of the last scheduled interval. Intervals are generated contiguously
//     from there; ticks only decide when they are due. A tick skipped by a
//     DST change therefore can't lose an interval, and a repeated one can't
//     schedule one twice.
//   - interval: cron: the cursor is the last processed tick; each tick
//     covers the time since the previous one.
//
// Without catch-up only the most recent due interval is returned.

// Fire is one interval that is due.
type Fire struct {
	Tick     time.Time // the tick that made it due
	Interval Interval
	// Skipped: due, but not to be started (it begins before the schedule's
	// start date); still advances the cursor.
	Skipped bool
	Reason  string
}

// InitialCursor is the cursor of a schedule that has none yet: its start
// date, or now; for calendar kinds, the end of the interval containing that
// moment, so the first interval scheduled is the one that starts there.
func (c *Calendar) InitialCursor(now time.Time) time.Time {
	anchor := now
	if !c.start.IsZero() {
		anchor = c.start
	}
	if c.Calendar() {
		return c.IntervalFor(time.Time{}, anchor).End
	}
	return anchor
}

// Due returns what is due after cursor up to now, oldest first and at most
// max of them (0: no limit), the cursor to persist with them, and whether
// more remain beyond max. When nothing is due, it returns the cursor as is.
func (c *Calendar) Due(cursor, now time.Time, catchup bool, max int) (fires []Fire, next time.Time, more bool) {
	latest := c.Prev(now.Add(time.Nanosecond)) // the last tick at or before now
	if latest.IsZero() {
		return nil, cursor, false
	}
	if c.Calendar() {
		target := c.IntervalFor(time.Time{}, latest).End
		for iv := c.IntervalStarting(cursor); !iv.End.After(target); iv = c.IntervalStarting(iv.End) {
			fires = append(fires, Fire{Tick: latest, Interval: iv})
		}
	} else if latest.After(cursor) {
		if !catchup {
			fires = []Fire{{Tick: latest}}
		} else {
			for t := c.Next(cursor); !t.After(now); t = c.Next(t) {
				fires = append(fires, Fire{Tick: t})
				if max > 0 && len(fires) > max {
					break // one beyond max is enough to know there are more
				}
			}
		}
		for i := range fires {
			fires[i].Interval = c.Interval(fires[i].Tick)
			if !c.start.IsZero() && fires[i].Interval.Start.Before(c.start) {
				fires[i].Skipped, fires[i].Reason = true, "interval starts before the schedule's start date"
			}
		}
	}
	if len(fires) == 0 {
		return nil, cursor, false
	}
	switch {
	case !catchup:
		fires = fires[len(fires)-1:]
	case max > 0 && len(fires) > max:
		fires, more = fires[:max], true
	}
	last := fires[len(fires)-1]
	if c.Calendar() {
		return fires, last.Interval.End, more
	}
	return fires, last.Tick, more
}
