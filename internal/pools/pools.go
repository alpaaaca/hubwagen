// Package pools scales worker pools: the config's worker groups with min
// below max. Every few seconds the scaler looks at the work queued on each
// pool's docks that waits for a worker (core.Demand) and at the workers
// the core sees, then starts workers up to max when work waits and stops
// those that had nothing to run for idle_after, down to min. A launcher
// does the starting and stopping (processes today; containers later).
//
// It runs on the leader, outside the core's loop, and writes no events:
// like the core's record of workers, scaling is not state. A new leader
// starts from min and scales from what queues. Design:
// docs/worker-pools.md.
package pools

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/core"
)

// Pool is one worker group as the scaler sees it.
type Pool struct {
	Name      string
	Docks     []string
	Lifts     int // steps one worker runs at once
	Min, Max  int
	IdleAfter time.Duration
}

// Launcher starts and stops a pool's workers by worker ID (<pool>-<n>).
type Launcher interface {
	Start(pool, workerID string)
	Stop(workerID string)
}

// Source is what the scaler reads and reports to: the core.
type Source interface {
	Workers(ctx context.Context) ([]core.Worker, error)
	Demand(ctx context.Context) (map[string]core.DockDemand, error)
	SetPools(ctx context.Context, pools []core.PoolStatus) error
}

// StartGrace is how long a started worker counts as starting: until it
// polls, or this long. Meanwhile its pool starts no more, so a burst of
// queued work doesn't start max workers before the first ones poll.
const StartGrace = time.Minute

// Every is how often Run scales.
const Every = 2 * time.Second

type Scaler struct {
	pools  []Pool
	launch Launcher
	src    Source
	clock  clock.Clock
	log    *slog.Logger
	state  map[string]*poolState
}

type poolState struct {
	workers   map[string]*worker // by ID
	decision  string
	decidedAt time.Time
}

type worker struct {
	started   time.Time
	seen      bool      // it has polled
	idleSince time.Time // zero: running something, or not seen lately
}

func New(pools []Pool, launch Launcher, src Source, clk clock.Clock, log *slog.Logger) *Scaler {
	s := &Scaler{pools: pools, launch: launch, src: src, clock: clk, log: log, state: map[string]*poolState{}}
	for _, p := range pools {
		s.state[p.Name] = &poolState{workers: map[string]*worker{}}
	}
	return s
}

// Run scales every Every until ctx ends.
func (s *Scaler) Run(ctx context.Context) {
	t := time.NewTicker(Every)
	defer t.Stop()
	for {
		if err := s.Tick(ctx); err != nil && ctx.Err() == nil {
			s.log.Warn("scaling worker pools", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// Tick scales each pool once, in the config's order.
func (s *Scaler) Tick(ctx context.Context) error {
	seen, err := s.src.Workers(ctx)
	if err != nil {
		return err
	}
	demand, err := s.src.Demand(ctx)
	if err != nil {
		return err
	}
	byID := map[string]core.Worker{}
	for _, w := range seen {
		byID[w.ID] = w
	}
	now := s.clock.Now()
	claimed := map[string]bool{} // docks whose demand a pool took on
	var status []core.PoolStatus
	for _, p := range s.pools {
		st := s.state[p.Name]
		starting := 0
		for id, w := range st.workers {
			cw, ok := byID[id]
			if ok {
				w.seen = true
			}
			switch {
			case ok && cw.Polling && cw.Running == 0:
				if w.idleSince.IsZero() {
					w.idleSince = now
				}
			default:
				w.idleSince = time.Time{}
			}
			if !w.seen && now.Sub(w.started) < StartGrace {
				starting++
			}
		}

		// The work waiting on its docks that no pool before it took on.
		var haulTasks, lifts int
		var docks []string
		for _, d := range p.Docks {
			dd := demand[d]
			if claimed[d] || dd.HaulTasks+dd.Lifts == 0 {
				continue
			}
			haulTasks += dd.HaulTasks
			lifts += dd.Lifts
			docks = append(docks, d)
		}
		n := len(st.workers)
		waits := haulTasks+lifts > 0
		if waits && n < p.Max {
			for _, d := range docks {
				claimed[d] = true
			}
		}

		switch {
		case n < p.Min:
			s.start(p, st, p.Min-n, now, fmt.Sprintf("below min %d", p.Min))
		case waits && starting == 0 && n < p.Max:
			need := (lifts + p.Lifts - 1) / p.Lifts
			if need == 0 {
				need = 1 // haul tasks: any worker takes them
			}
			s.start(p, st, min(need, p.Max-n), now, waitsText(haulTasks, lifts, docks))
		case !waits && n > p.Min:
			s.stopIdle(p, st, now)
		}

		status = append(status, core.PoolStatus{Name: p.Name, Docks: p.Docks, Min: p.Min, Max: p.Max,
			Workers: slices.Sorted(maps.Keys(st.workers)), Starting: s.starting(st, now),
			Decision: st.decision, DecidedAt: st.decidedAt})
	}
	return s.src.SetPools(ctx, status)
}

func (s *Scaler) starting(st *poolState, now time.Time) int {
	n := 0
	for _, w := range st.workers {
		if !w.seen && now.Sub(w.started) < StartGrace {
			n++
		}
	}
	return n
}

// start starts k workers, numbered from the lowest free number.
func (s *Scaler) start(p Pool, st *poolState, k int, now time.Time, why string) {
	var ids []string
	for i := 1; len(ids) < k; i++ {
		id := p.Name + "-" + strconv.Itoa(i)
		if _, ok := st.workers[id]; ok {
			continue
		}
		st.workers[id] = &worker{started: now}
		s.launch.Start(p.Name, id)
		ids = append(ids, id)
	}
	s.decide(p, st, now, fmt.Sprintf("started %s: %s", strings.Join(ids, ", "), why))
}

// stopIdle stops workers idle for IdleAfter, highest number first, down to
// Min.
func (s *Scaler) stopIdle(p Pool, st *poolState, now time.Time) {
	ids := slices.Sorted(maps.Keys(st.workers))
	slices.SortStableFunc(ids, func(a, b string) int { return number(b) - number(a) })
	var stopped []string
	for _, id := range ids {
		if len(st.workers)-len(stopped) <= p.Min {
			break
		}
		w := st.workers[id]
		if !w.idleSince.IsZero() && now.Sub(w.idleSince) >= p.IdleAfter {
			stopped = append(stopped, id)
		}
	}
	if len(stopped) == 0 {
		return
	}
	for _, id := range stopped {
		s.launch.Stop(id)
		delete(st.workers, id)
	}
	slices.SortFunc(stopped, func(a, b string) int { return number(a) - number(b) })
	s.decide(p, st, now, fmt.Sprintf("stopped %s: nothing to run for %s", strings.Join(stopped, ", "), p.IdleAfter))
}

func (s *Scaler) decide(p Pool, st *poolState, now time.Time, text string) {
	st.decision, st.decidedAt = text, now
	s.log.Info("worker pool "+p.Name+": "+text, "pool", p.Name)
}

func number(id string) int {
	n, _ := strconv.Atoi(id[strings.LastIndexByte(id, '-')+1:])
	return n
}

func waitsText(haulTasks, lifts int, docks []string) string {
	var parts []string
	if lifts > 0 {
		parts = append(parts, plural(lifts, "step"))
	}
	if haulTasks > 0 {
		parts = append(parts, plural(haulTasks, "run"))
	}
	verb := "wait"
	if haulTasks+lifts == 1 {
		verb = "waits"
	}
	return fmt.Sprintf("%s %s on %s", strings.Join(parts, " and "), verb, strings.Join(docks, ", "))
}

func plural(n int, word string) string {
	if n == 1 {
		return "1 " + word
	}
	return fmt.Sprintf("%d %ss", n, word)
}
