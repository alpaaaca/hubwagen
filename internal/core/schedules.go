package core

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/alper/moksha/internal/schedule"
	"github.com/alper/moksha/internal/state"
)

// Schedules. A build declares schedules for the pipelines it defines; the newest
// build that defines a pipeline decides its schedule (or, declaring none,
// removes it). The leader keeps a cursor per schedule (internal/schedule:
// the end of the last interval handled, or the last tick for interval
// "cron") and a timer for the next tick. When a tick comes, it starts a run
// per due interval with the param {"interval": {"$interval": {start, end}}}
// and moves the cursor, in the same batch.
//
// After downtime (no leader, or a long one), catch_up decides what happens
// to the intervals that came due meanwhile: all starts every one, oldest
// first; latest (default) starts the newest only; skip starts none of them.
// Scheduled runs start in ScheduleTenant, on the default queue. Runs of one
// schedule may overlap.

// ScheduleTenant is the tenant scheduled runs start in.
const ScheduleTenant = "default"

// Catch-up policies.
const (
	CatchUpSkip   = "skip"
	CatchUpLatest = "latest"
	CatchUpAll    = "all"
)

// maxFiresPerRound bounds the runs one evaluation starts (catch_up: all);
// the next round follows at once.
const maxFiresPerRound = 100

// skipGrace: with catch_up skip, a tick this late still counts as on time.
const skipGrace = time.Minute

// ScheduleSpec is a schedule a build declares for one of its runs.
type ScheduleSpec struct {
	Pipeline string
	Cron     string
	Timezone string // IANA; "" is UTC
	Interval string // hourly|daily|weekly|monthly|cron; "" is cron
	CatchUp  string // skip|latest|all; "" is latest
}

func (s ScheduleSpec) normalized() ScheduleSpec {
	if s.Interval == "" {
		s.Interval = schedule.IntervalCron
	}
	if s.CatchUp == "" {
		s.CatchUp = CatchUpLatest
	}
	return s
}

func (s ScheduleSpec) calendar() (*schedule.Calendar, error) {
	switch s.CatchUp {
	case CatchUpSkip, CatchUpLatest, CatchUpAll:
	default:
		return nil, fmt.Errorf("catch_up %q must be skip, latest or all", s.CatchUp)
	}
	return schedule.NewCalendar(schedule.Schedule{Cron: s.Cron, Timezone: s.Timezone, Interval: s.Interval})
}

type scheduleState struct {
	rec   state.Schedule
	cal   *schedule.Calendar
	timer *timerRef
}

func (s *scheduleState) spec() ScheduleSpec {
	return ScheduleSpec{Pipeline: s.rec.Pipeline, Cron: s.rec.Cron, Timezone: s.rec.Timezone, Interval: s.rec.Interval, CatchUp: s.rec.CatchUp}
}

// validateSchedules checks a build's schedules: each for a pipeline it defines,
// one per run, and well-formed. It reports every problem at once.
func validateSchedules(runs []string, specs []ScheduleSpec) ([]ScheduleSpec, error) {
	var problems []string
	seen := map[string]bool{}
	out := make([]ScheduleSpec, 0, len(specs))
	for i, s := range specs {
		s = s.normalized()
		switch {
		case !slices.Contains(runs, s.Pipeline):
			problems = append(problems, fmt.Sprintf("schedules[%d]: pipeline %q isn't one of the build's pipelines", i, s.Pipeline))
		case seen[s.Pipeline]:
			problems = append(problems, fmt.Sprintf("schedules[%d]: pipeline %q has two schedules", i, s.Pipeline))
		}
		seen[s.Pipeline] = true
		if _, err := s.calendar(); err != nil {
			problems = append(problems, fmt.Sprintf("schedules[%d] (%s): %v", i, s.Pipeline, err))
		}
		out = append(out, s)
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalid, strings.Join(problems, "; "))
	}
	return out, nil
}

// applySchedules makes a newly registered build's schedules the current ones
// for the pipelines it defines.
func (c *Core) applySchedules(build string, runs []string, specs []ScheduleSpec) {
	byRun := map[string]ScheduleSpec{}
	for _, s := range specs {
		byRun[s.Pipeline] = s
	}
	sorted := slices.Sorted(slices.Values(runs))
	for _, run := range sorted {
		cur := c.schedules[run]
		spec, ok := byRun[run]
		switch {
		case ok && cur != nil && cur.spec() == spec:
		case ok:
			c.setSchedule(build, spec, cur)
		case cur != nil:
			cur.timer.stop()
			delete(c.schedules, run)
			c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.DeleteSchedule(ScheduleTenant, run) })
			c.emit(state.EventScheduleRemoved, ScheduleTenant, scheduleRemovedPayload{Pipeline: run, Build: build})
		}
	}
}

func (c *Core) setSchedule(build string, spec ScheduleSpec, cur *scheduleState) {
	cal, _ := spec.calendar() // validated
	now := c.now()
	s := &scheduleState{cal: cal, rec: state.Schedule{
		Tenant: ScheduleTenant, Pipeline: spec.Pipeline, Cron: spec.Cron, Timezone: spec.Timezone, Interval: spec.Interval,
		CatchUp: spec.CatchUp, BuildID: build, Cursor: cal.InitialCursor(now), CreatedAt: now, UpdatedAt: now,
	}}
	if cur != nil {
		cur.timer.stop()
		s.rec.CreatedAt = cur.rec.CreatedAt
		if cur.rec.Interval == spec.Interval {
			s.rec.Cursor = cur.rec.Cursor // what was handled stays handled
		}
	}
	c.schedules[spec.Pipeline] = s
	c.putSchedule(s)
	c.emit(state.EventScheduleSet, ScheduleTenant, scheduleSetPayload{
		Pipeline: spec.Pipeline, Cron: spec.Cron, Timezone: spec.Timezone, Interval: spec.Interval, CatchUp: spec.CatchUp,
		Build: build, Cursor: s.rec.Cursor})
	c.armSchedule(s)
}

func (c *Core) putSchedule(s *scheduleState) {
	rec := s.rec
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutSchedule(rec) })
}

// armSchedule sets the timer for the schedule's next tick.
func (c *Core) armSchedule(s *scheduleState) {
	s.timer.stop()
	next := s.cal.Next(c.now())
	s.timer = c.after(next.Sub(c.now()), func(c *Core) { c.runSchedule(s) })
}

// runSchedule starts what is due, moves the cursor and arms the next tick.
func (c *Core) runSchedule(s *scheduleState) {
	if c.schedules[s.rec.Pipeline] != s {
		return // replaced or removed meanwhile
	}
	now := c.now()
	fires, cursor, more := s.cal.Due(s.rec.Cursor, now, s.rec.CatchUp == CatchUpAll, maxFiresPerRound)
	for _, f := range fires {
		p := scheduleFiredPayload{Pipeline: s.rec.Pipeline, Tick: f.Tick, Start: f.Interval.Start, End: f.Interval.End}
		switch {
		case f.Skipped:
			p.Skipped = f.Reason
		case s.rec.CatchUp == CatchUpSkip && f.Tick.Before(now.Add(-skipGrace)):
			p.Skipped = "due while no leader ran (catch_up: skip)"
		default:
			res, err := c.startRun(StartRunRequest{
				Tenant: ScheduleTenant, Name: s.rec.Pipeline, Trigger: "schedule",
				Params: map[string]any{"interval": IntervalParam(f.Interval)},
			})
			if err != nil {
				p.Error = err.Error()
			}
			p.RunID = res.RunID
		}
		c.emit(state.EventScheduleFired, ScheduleTenant, p)
	}
	if !cursor.Equal(s.rec.Cursor) {
		s.rec.Cursor, s.rec.UpdatedAt = cursor, now
		c.putSchedule(s)
	}
	if more {
		s.timer = c.after(0, func(c *Core) { c.runSchedule(s) })
		return
	}
	c.armSchedule(s)
}

// IntervalParam is how a run receives its data interval; the Python SDK
// decodes it as moksha.Interval.
func IntervalParam(iv schedule.Interval) map[string]any {
	return map[string]any{"$interval": map[string]any{
		"start": iv.Start.Format(time.RFC3339Nano), "end": iv.End.Format(time.RFC3339Nano)}}
}

// restoreSchedules loads the schedules and catches up on what came due
// while no leader ran, in the restore batch.
func (c *Core) restoreSchedules(recs []state.Schedule) {
	sort.Slice(recs, func(i, j int) bool { return recs[i].Pipeline < recs[j].Pipeline })
	for _, rec := range recs {
		if rec.Tenant != ScheduleTenant {
			continue
		}
		s := &scheduleState{rec: rec}
		cal, err := s.spec().calendar()
		if err != nil {
			c.log.Error("schedule can't be restored", "run", rec.Pipeline, "err", err)
			continue
		}
		s.cal = cal
		c.schedules[rec.Pipeline] = s
		c.runSchedule(s)
	}
}
