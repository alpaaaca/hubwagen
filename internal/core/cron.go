package core

import (
	"context"
	"fmt"
	"time"

	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
)

// Cron scheduling. Each scheduled pipeline has a cursor, committed in the
// same batch as the runs it led to, so nothing is scheduled twice and
// whatever was missed while no leader ran is caught up on restart:
//
//   - calendar intervals (hourly, daily, weekly, monthly): the cursor is the
//     end of the last scheduled interval. Intervals are generated
//     contiguously from there; cron ticks only decide when they are due. A
//     tick skipped by a DST change therefore can't lose an interval, and a
//     repeated one can't create a second run.
//   - interval: cron: the cursor is the last processed tick, and each run
//     covers the time since the previous tick.
//
// With catchup: false only the most recent due interval gets a run.

// maxCatchupPerBatch bounds how many runs one batch creates for a pipeline;
// the rest follow in the next batches.
const maxCatchupPerBatch = 100

type cronState struct {
	cal       *spec.Calendar
	catchup   bool
	cursor    time.Time
	hasCursor bool
	timer     *timerRef
	timerGen  int // invalidates timers armed before the last reschedule
}

type cronTickedPayload struct {
	Tick          time.Time `json:"tick"`
	IntervalStart time.Time `json:"interval_start"`
	IntervalEnd   time.Time `json:"interval_end"`
	RunID         string    `json:"run_id,omitempty"`
	Existing      bool      `json:"existing,omitempty"`
	Skipped       bool      `json:"skipped,omitempty"`
	Reason        string    `json:"reason,omitempty"`
}

// reschedule (re)arms a pipeline's schedule from its current version. Called
// on publish, pause/resume and leader start.
func (c *Core) reschedule(p *pipelineState) {
	cs := &p.cron
	cs.timer.stop()
	cs.timer = nil
	cs.timerGen++
	cs.cal = nil

	ver := c.versions[p.rec.CurrentVersion]
	if ver == nil || ver.spec.Schedule == nil || p.rec.Paused || c.cfg.DisableSchedules {
		return
	}
	cal, err := spec.NewCalendar(ver.spec.Schedule)
	if err != nil {
		// Compile validated the schedule, so this means a bad spec slipped in.
		c.log.Error("pipeline schedule disabled", "pipeline", p.rec.Tenant+"/"+p.rec.Name, "err", err)
		return
	}
	cs.cal = cal
	cs.catchup = ver.spec.Schedule.Catchup

	if !cs.hasCursor {
		// First schedule for this pipeline: start at the configured start
		// date (catching up from there), or now.
		anchor := c.now()
		if !cal.Start().IsZero() {
			anchor = cal.Start()
		}
		if cal.Calendar() {
			// The interval anchor falls in starts the schedule; with a start
			// date at a period boundary that is exactly the start date.
			anchor = cal.IntervalFor(time.Time{}, anchor).End
		}
		c.setCursor(p, anchor)
	}
	c.tickDue(p)
}

// tickDue creates runs for everything due up to now and arms the next timer.
func (c *Core) tickDue(p *pipelineState) {
	cs := &p.cron
	if cs.cal == nil {
		return
	}
	now := c.now()
	latest := cs.cal.Prev(now.Add(time.Nanosecond)) // last tick at or before now

	var more bool
	if cs.cal.Calendar() {
		more = c.dueIntervals(p, latest)
	} else {
		more = c.dueTicks(p, now, latest)
	}

	gen := cs.timerGen
	wait := time.Duration(0)
	if !more {
		wait = cs.cal.Next(now).Sub(now)
	}
	cs.timer = c.after(wait, func(c *Core) {
		if p.cron.timerGen == gen {
			c.tickDue(p)
		}
	})
}

// dueIntervals schedules calendar intervals from the cursor up to the one
// the latest tick covers. It reports whether more remain for another batch.
func (c *Core) dueIntervals(p *pipelineState, latest time.Time) bool {
	cs := &p.cron
	if latest.IsZero() {
		return false
	}
	target := cs.cal.IntervalFor(time.Time{}, latest).End
	var due []spec.Interval
	for iv := cs.cal.IntervalStarting(cs.cursor); !iv.End.After(target); iv = cs.cal.IntervalStarting(iv.End) {
		due = append(due, iv)
	}
	if len(due) == 0 {
		return false
	}
	more := false
	switch {
	case !cs.catchup:
		due = due[len(due)-1:]
	case len(due) > maxCatchupPerBatch:
		due, more = due[:maxCatchupPerBatch], true
	}
	for _, iv := range due {
		c.cronRun(p, latest, iv)
	}
	c.setCursor(p, due[len(due)-1].End)
	return more
}

// dueTicks schedules tick-to-tick intervals for ticks after the cursor.
func (c *Core) dueTicks(p *pipelineState, now, latest time.Time) bool {
	cs := &p.cron
	var due []time.Time
	more := false
	switch {
	case latest.IsZero() || !latest.After(cs.cursor):
	case !cs.catchup:
		due = []time.Time{latest}
	default:
		for t := cs.cal.Next(cs.cursor); !t.After(now); t = cs.cal.Next(t) {
			if len(due) == maxCatchupPerBatch {
				more = true
				break
			}
			due = append(due, t)
		}
	}
	for _, tick := range due {
		iv := cs.cal.Interval(tick)
		if start := cs.cal.Start(); !start.IsZero() && iv.Start.Before(start) {
			c.emit(state.EventCronTicked, p.rec.Tenant, "", "", "", cronTickedPayload{
				Tick: tick.UTC(), IntervalStart: iv.Start.UTC(), IntervalEnd: iv.End.UTC(),
				Skipped: true, Reason: "interval starts before the schedule's start date",
			})
			continue
		}
		c.cronRun(p, tick, iv)
	}
	if len(due) > 0 {
		c.setCursor(p, due[len(due)-1])
	}
	return more
}

func (c *Core) cronRun(p *pipelineState, tick time.Time, iv spec.Interval) {
	payload := cronTickedPayload{Tick: tick.UTC(), IntervalStart: iv.Start.UTC(), IntervalEnd: iv.End.UTC()}
	res, err := c.trigger(c.ctx, TriggerRequest{
		Tenant: p.rec.Tenant, Pipeline: p.rec.Name, Interval: &iv, Trigger: "cron", By: "schedule",
	})
	if err != nil {
		payload.Skipped, payload.Reason = true, err.Error()
		c.log.Error("scheduled run not created", "pipeline", p.rec.Tenant+"/"+p.rec.Name, "interval_start", iv.Start, "err", err)
	} else {
		payload.RunID, payload.Existing, payload.Skipped, payload.Reason = res.RunID, res.Existing, res.Skipped, res.Reason
	}
	c.emit(state.EventCronTicked, p.rec.Tenant, payload.RunID, "", "", payload)
}

func (c *Core) setCursor(p *pipelineState, tick time.Time) {
	p.cron.cursor = tick
	p.cron.hasCursor = true
	rec := state.ScheduleCursor{Tenant: p.rec.Tenant, Pipeline: p.rec.Name, LastTick: tick}
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutScheduleCursor(rec) })
}

// SetPaused pauses or resumes a pipeline's schedule. Manual triggers still
// work while paused. On resume, missed ticks are handled per the schedule's
// catchup setting.
func (c *Core) SetPaused(ctx context.Context, tenant, pipeline string, paused bool, by string) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		p := c.pipelines[pipelineKey{tenant, pipeline}]
		if p == nil {
			return struct{}{}, fmt.Errorf("%w: pipeline %s/%s", ErrNotFound, tenant, pipeline)
		}
		if p.rec.Paused == paused {
			return struct{}{}, nil
		}
		p.rec.Paused = paused
		rec := p.rec
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutPipeline(rec) })
		typ := state.EventPipelineResumed
		if paused {
			typ = state.EventPipelinePaused
		}
		c.emit(typ, tenant, "", "", "", cancelRequestedPayload{Scope: "schedule", RequestedBy: by})
		c.reschedule(p)
		return struct{}{}, nil
	})
	return err
}
