package core

import (
	"context"
	"maps"
	"slices"
	"time"

	"github.com/alper/moksha/internal/state"
)

// Workers. The core remembers which workers polled lately, in memory only:
// like leases, it is not state, and a new leader learns it again from the
// next polls. It lets queued work say why it waits: no worker polls its
// queue, none of those that do has a build that defines its code, or they
// are all busy.

// DefaultWorkerExpiry is how long a worker running tasks counts as
// connected after its last poll or heartbeat: twice the API's longest poll
// wait. A worker with nothing to run polls again at once, so one that
// neither polls nor runs anything counts only for workerIdleGrace.
const DefaultWorkerExpiry = time.Minute

const workerIdleGrace = 5 * time.Second

// Worker is a worker the core has seen lately.
type Worker struct {
	ID       string
	BuildID  string // "" if it polls without one
	Queues   []string
	Kinds    []TaskKind
	LastSeen time.Time
	// Polling: it waits for work right now; otherwise it is busy (or gone,
	// until it expires).
	Polling bool
	// Running is how many tasks it runs: step attempts and run tasks
	// dispatched to it and not finished.
	Running int
}

type seenWorker struct {
	build  string
	queues map[string]bool
	kinds  map[TaskKind]bool
	last   time.Time
}

// sawPoll records a worker's poll. Only the core goroutine calls it.
func (c *Core) sawPoll(req PollRequest) {
	w := c.workers[req.WorkerID]
	if w == nil || w.build != req.BuildID {
		w = &seenWorker{build: req.BuildID, queues: map[string]bool{}, kinds: map[TaskKind]bool{}}
		c.workers[req.WorkerID] = w
	}
	for _, d := range req.Queues {
		w.queues[d] = true
	}
	kinds := req.Kinds
	if len(kinds) == 0 {
		kinds = []TaskKind{TaskRun, TaskStep}
	}
	for _, k := range kinds {
		w.kinds[k] = true
	}
	w.last = c.now()
}

// sawHeartbeat keeps a known worker connected while it runs long tasks.
func (c *Core) sawHeartbeat(workerID string) {
	if w := c.workers[workerID]; w != nil {
		w.last = c.now()
	}
}

// sawHangUp forgets a worker that gave up a poll (it stopped, or its
// connection broke) unless it still polls on another: a stopped worker
// shouldn't look busy until it expires. Its next poll brings it back.
func (c *Core) sawHangUp(workerID string) {
	for _, p := range c.pollers {
		if !p.done && p.req.WorkerID == workerID {
			return
		}
	}
	delete(c.workers, workerID)
}

func (c *Core) workerExpiry() time.Duration {
	if c.cfg.WorkerExpiry > 0 {
		return c.cfg.WorkerExpiry
	}
	return DefaultWorkerExpiry
}

// liveWorkers forgets workers not seen within the expiry and returns the
// rest, by ID.
func (c *Core) liveWorkers() []Worker {
	polling, running := map[string]bool{}, map[string]int{}
	for _, p := range c.pollers {
		if !p.done {
			polling[p.req.WorkerID] = true
		}
	}
	for _, a := range c.stepAttempts {
		running[a.rec.WorkerID]++
	}
	for _, t := range c.runTasks {
		if t.rec.Status == state.RunTaskDispatched {
			running[t.rec.WorkerID]++
		}
	}
	now := c.now()
	var out []Worker
	for _, id := range slices.Sorted(maps.Keys(c.workers)) {
		w := c.workers[id]
		keep := c.workerExpiry()
		if running[id] == 0 {
			keep = min(keep, workerIdleGrace)
		}
		if !polling[id] && now.Sub(w.last) > keep {
			delete(c.workers, id)
			continue
		}
		kinds := slices.Collect(maps.Keys(w.kinds))
		slices.Sort(kinds)
		out = append(out, Worker{ID: id, BuildID: w.build, Queues: slices.Sorted(maps.Keys(w.queues)), Kinds: kinds,
			LastSeen: w.last, Polling: polling[id], Running: running[id]})
	}
	return out
}

// Workers returns the workers seen lately, by ID.
func (c *Core) Workers(ctx context.Context) ([]Worker, error) {
	return submit(ctx, c, func(c *Core) ([]Worker, error) { return c.liveWorkers(), nil })
}

// WaitReason says why queued work isn't running.
type WaitReason string

const (
	// WaitNoWorker: no worker polls the queue for this kind of task.
	WaitNoWorker WaitReason = "no_worker"
	// WaitNoBuild: workers poll the queue, but none has a build that
	// defines the run or step (its code changed, or it was never there).
	WaitNoBuild WaitReason = "no_build"
	// WaitBusy: workers that can run it are all busy.
	WaitBusy WaitReason = "busy"
	// WaitQueueLimit: the queue runs as many steps as it may (MaxRunningSteps).
	WaitQueueLimit WaitReason = "queue_limit"
	// WaitStarting: no worker (or no free one) yet, but a pool is starting
	// one for the queue (pools.go).
	WaitStarting WaitReason = "starting"
)

// Waiting is a run's queued work and why no worker took it yet.
type Waiting struct {
	Reason WaitReason
	Kind   TaskKind
	Name   string // the run (run task) or step function
	Seq    int    // step: its command's sequence number
	Queue  string
	Since  time.Time // queued at; the oldest when several are
	Queued int       // tasks of the run queued
	// Workers that poll the queue for this kind of task, and their builds
	// (WaitNoBuild: none of them defines Name).
	Workers int
	Builds  []string
	// Pool (WaitStarting): the pool starting a worker.
	Pool string
}

// Waiting returns, for each of runIDs that has queued work, why it waits:
// its run task if it has one queued, else its oldest queued step. Runs
// with nothing queued (running, waiting on a timer, a signal or a child)
// are left out.
func (c *Core) Waiting(ctx context.Context, runIDs []string) (map[string]Waiting, error) {
	return submit(ctx, c, func(c *Core) (map[string]Waiting, error) {
		want := map[string]bool{}
		for _, id := range runIDs {
			want[id] = true
		}
		first := map[string]*queuedTask{}
		queued := map[string]int{}
		for _, d := range c.queues {
			for _, list := range [][]*queuedTask{d.runTasks, d.steps} {
				for _, q := range list {
					id := q.task.RunID
					if !want[id] {
						continue
					}
					queued[id]++
					f := first[id]
					if f == nil || waitsFirst(q, f) {
						first[id] = q
					}
				}
			}
		}
		if len(first) == 0 {
			return nil, nil
		}
		workers := c.liveWorkers()
		out := map[string]Waiting{}
		for id, q := range first {
			w := c.waitReason(q.task, workers)
			w.Since, w.Queued = q.queuedAt, queued[id]
			out[id] = w
		}
		return out, nil
	})
}

// waitsFirst: run tasks before steps, then oldest first, as queues hand
// them out.
func waitsFirst(q, than *queuedTask) bool {
	if q.task.Kind != than.task.Kind {
		return q.task.Kind == TaskRun
	}
	return q.order < than.order
}

// waitReason tells why t, queued, has no worker yet.
func (c *Core) waitReason(t Task, workers []Worker) Waiting {
	w := Waiting{Kind: t.Kind, Name: t.Name, Seq: t.Seq, Queue: t.Queue}
	capable := false
	for _, wk := range workers {
		if !slices.Contains(wk.Queues, t.Queue) || !slices.Contains(wk.Kinds, t.Kind) {
			continue
		}
		if c.rolledOver(wk.BuildID, t) {
			continue // a replaced version's: not there for new work, so the new version's pool counts it as demand
		}
		w.Workers++
		if !slices.Contains(w.Builds, wk.BuildID) {
			w.Builds = append(w.Builds, wk.BuildID)
		}
		if c.canRun(wk.BuildID, t) {
			capable = true
		}
	}
	slices.Sort(w.Builds)
	d := c.queues[t.Queue]
	switch {
	case w.Workers == 0:
		w.Reason = WaitNoWorker
	case !capable:
		w.Reason = WaitNoBuild
	case t.Kind == TaskStep && d != nil && d.limit > 0 && d.runningSteps >= d.limit:
		w.Reason = WaitQueueLimit
	default:
		w.Reason = WaitBusy
	}
	if w.Reason == WaitNoWorker || w.Reason == WaitBusy {
		if p := c.startingFor(t.Queue); p != "" {
			w.Reason, w.Pool = WaitStarting, p
		}
	}
	return w
}
