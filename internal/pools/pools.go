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
	"sync"
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
	// External: something else starts its workers (KEDA, an HPA); the
	// scaler only works out how many it wants (Desired), from the workers
	// that poll its docks and the work waiting there.
	External bool
	// Apply, if set (an external pool), makes the desired count so: it
	// sets a Kubernetes Deployment's replicas. It is called when the count
	// changes, and again on the next pass while it fails.
	Apply func(ctx context.Context, n int) error
}

// Launcher starts and stops a pool's workers by worker ID (<pool>-<n>).
type Launcher interface {
	// Start starts a worker; an error (it can't, e.g. a secret is
	// missing) leaves it unstarted, to try again on a later pass.
	Start(pool, workerID string) error
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
	mu     sync.Mutex // Tick and SetPools
	pools  []Pool
	launch Launcher
	src    Source
	clock  clock.Clock
	log    *slog.Logger
	state  map[string]*poolState
}

type poolState struct {
	workers   map[string]*worker // by ID: started by the launcher
	decision  string
	decidedAt time.Time
	// External pools: when each worker polling its docks last ran
	// something (or was first seen), and the desired count.
	active  map[string]time.Time
	desired int
	applied int // what Apply last made so; -1 before the first
}

type worker struct {
	started   time.Time
	seen      bool      // it has polled
	idleSince time.Time // zero: running something, or not seen lately
}

func New(pools []Pool, launch Launcher, src Source, clk clock.Clock, log *slog.Logger) *Scaler {
	s := &Scaler{launch: launch, src: src, clock: clk, log: log, state: map[string]*poolState{}}
	s.SetPools(pools)
	return s
}

// SetPools changes the set of pools: new ones start empty (the next pass
// starts their min), changed ones keep their workers, and those no longer
// in the set have their workers stopped (SIGTERM: they finish what they
// run). A project's new version is a new pool and its old one goes: that
// is the rollout.
func (s *Scaler) SetPools(pools []Pool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keep := map[string]bool{}
	for _, p := range pools {
		keep[p.Name] = true
		if s.state[p.Name] == nil {
			s.state[p.Name] = &poolState{workers: map[string]*worker{}, active: map[string]time.Time{}, desired: p.Min, applied: -1}
		}
	}
	for name, st := range s.state {
		if keep[name] {
			continue
		}
		ids := slices.Sorted(maps.Keys(st.workers))
		for _, id := range ids {
			s.launch.Stop(id)
		}
		if len(ids) > 0 {
			s.log.Info(fmt.Sprintf("worker pool %s: gone; stopped %s", name, strings.Join(ids, ", ")), "pool", name)
		}
		delete(s.state, name)
	}
	s.pools = slices.Clone(pools)
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	// Workers an external pool counts: those polling its docks that no
	// process pool started and no external pool before it counted.
	counted := map[string]bool{}
	for _, st := range s.state {
		for id := range st.workers {
			counted[id] = true
		}
	}
	var status []core.PoolStatus
	for _, p := range s.pools {
		st := s.state[p.Name]
		if p.External {
			status = append(status, s.external(ctx, p, st, seen, demand, claimed, counted, now))
			continue
		}
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

		n := len(st.workers)
		haulTasks, lifts, need, docks := waiting(p, demand, claimed, n)
		waits := need > 0

		switch {
		case n < p.Min:
			s.start(p, st, p.Min-n, now, fmt.Sprintf("below min %d", p.Min))
		case waits && starting == 0 && n < p.Max:
			s.start(p, st, min(need, p.Max-n), now, waitsText(haulTasks, lifts, docks))
		case !waits && n > p.Min:
			s.stopIdle(p, st, now)
		}

		status = append(status, core.PoolStatus{Name: p.Name, Docks: p.Docks, Min: p.Min, Max: p.Max,
			Workers: slices.Sorted(maps.Keys(st.workers)), Starting: s.starting(st, now), Desired: len(st.workers),
			Decision: st.decision, DecidedAt: st.decidedAt})
	}
	return s.src.SetPools(ctx, status)
}

// waiting is the work waiting on p's docks that no pool before it took on,
// and how many workers it takes; it marks those docks claimed if p has
// room for more than n workers.
func waiting(p Pool, demand map[string]core.DockDemand, claimed map[string]bool, n int) (haulTasks, lifts, need int, docks []string) {
	for _, d := range p.Docks {
		dd := demand[d]
		if claimed[d] || dd.HaulTasks+dd.Lifts == 0 {
			continue
		}
		haulTasks += dd.HaulTasks
		lifts += dd.Lifts
		docks = append(docks, d)
	}
	if haulTasks+lifts == 0 {
		return 0, 0, 0, nil
	}
	if n < p.Max {
		for _, d := range docks {
			claimed[d] = true
		}
	}
	need = (lifts + p.Lifts - 1) / p.Lifts
	if need == 0 {
		need = 1 // haul tasks: any worker takes them
	}
	return haulTasks, lifts, need, docks
}

// external works out how many workers an external pool wants: those
// polling its docks that ran something within IdleAfter, plus what the
// work waiting there takes, between Min and Max. Whatever starts its
// workers reads it (GET /v1/pools/<name>).
func (s *Scaler) external(ctx context.Context, p Pool, st *poolState, seen []core.Worker, demand map[string]core.DockDemand,
	claimed, counted map[string]bool, now time.Time) core.PoolStatus {
	var ids []string
	active := 0
	for _, w := range seen {
		if counted[w.ID] || !overlaps(w.Docks, p.Docks) {
			continue
		}
		counted[w.ID] = true
		ids = append(ids, w.ID)
		last, ok := st.active[w.ID]
		if !ok || w.Running > 0 || !w.Polling {
			last = now
			st.active[w.ID] = now
		}
		if now.Sub(last) < p.IdleAfter {
			active++
		}
	}
	for id := range st.active {
		if !slices.Contains(ids, id) {
			delete(st.active, id)
		}
	}
	haulTasks, lifts, need, docks := waiting(p, demand, claimed, active)
	desired := min(max(active+need, p.Min), p.Max)
	if desired != st.desired {
		why := "workers had nothing to run for " + p.IdleAfter.String()
		switch {
		case need > 0:
			why = waitsText(haulTasks, lifts, docks)
		case desired > st.desired:
			why = "workers are busy"
		}
		s.decide(p, st, now, fmt.Sprintf("wants %s (was %d): %s", plural(desired, "worker"), st.desired, why))
		st.desired = desired
	}
	if p.Apply != nil && st.applied != desired {
		if err := p.Apply(ctx, desired); err != nil {
			s.log.Warn("worker pool "+p.Name+": can't apply its desired count", "pool", p.Name, "desired", desired, "err", err)
		} else {
			st.applied = desired
		}
	}
	slices.Sort(ids)
	return core.PoolStatus{Name: p.Name, Docks: p.Docks, Min: p.Min, Max: p.Max, Workers: ids, Desired: desired,
		External: true, Decision: st.decision, DecidedAt: st.decidedAt}
}

func overlaps(a, b []string) bool {
	for _, x := range a {
		if slices.Contains(b, x) {
			return true
		}
	}
	return false
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
	for i, tried := 1, 0; tried < k; i++ {
		id := p.Name + "-" + strconv.Itoa(i)
		if _, ok := st.workers[id]; ok {
			continue
		}
		tried++
		if err := s.launch.Start(p.Name, id); err != nil {
			s.decide(p, st, now, fmt.Sprintf("can't start %s: %v", id, err))
			return
		}
		st.workers[id] = &worker{started: now}
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
