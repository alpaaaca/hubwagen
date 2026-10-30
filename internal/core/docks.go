package core

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// Docks are the queues workers pull haul tasks and lift tasks from. They live
// in the core goroutine like the rest of the scheduling state: the haul
// engine queues work while processing a batch, and at the end of the batch
// queued work is matched to waiting pollers. A task is handed to a worker
// only after the batch that records its dispatch has committed (write before
// act).
//
// Haul tasks always go before lifts: they are short replays and should never
// wait behind work. A dock's concurrency limit applies to lifts only.

// TaskKind says what a worker is asked to do.
type TaskKind string

const (
	TaskHaul TaskKind = "haul" // replay a haul until it blocks
	TaskLift TaskKind = "lift" // run one lift attempt
)

// Task is one unit of work handed to a worker.
type Task struct {
	Kind   TaskKind
	ID     string // haul task ID or lift attempt ID
	Dock   string
	HaulID string
	Seq    int    // lift: sequence number of the command that scheduled it
	Name   string // haul or lift function
	Args   json.RawMessage
	// Output (lifts with an output): the pallet to produce. Staging (every
	// lift, when the server has a bucket): the prefix this attempt writes its
	// files to. A lift without an output that writes files there (a
	// dataframe it returned) produces a pallet in its implicit dataset.
	Output  *OutputSpec
	Staging string
	// Inputs (lifts): the pallets passed to it, by argument name, with
	// where their manifests are.
	Inputs []TaskInput
	// Logbook is the haul's events up to at least this task's dispatch
	// (haul tasks only). The replay reads results from it.
	Logbook []state.Event
}

// TaskInput is a pallet passed to a lift, as the worker gets it.
type TaskInput struct {
	Name     string
	Pallet   string
	Location string // the pallet's manifest
}

// DockConfig limits one dock.
type DockConfig struct {
	MaxRunningLifts int // 0 = unlimited
}

// PollRequest asks for the next task from any of Docks.
type PollRequest struct {
	WorkerID string
	// BuildID, if set, is the worker's registered build: it gets only the
	// haul tasks and lifts its build defines.
	BuildID string
	Docks   []string
	Kinds   []TaskKind    // empty: any
	Wait    time.Duration // how long to wait for work; 0 = answer at once
}

// Poll returns the next task for a worker, waiting up to req.Wait (measured
// on the core's clock). ok is false when there was no work in time. If ctx
// ends first, Poll withdraws; a task handed out in the meantime is still
// returned, so it is never silently lost. Poll needs a running core.
func (c *Core) Poll(ctx context.Context, req PollRequest) (task Task, ok bool, err error) {
	if req.WorkerID == "" || len(req.Docks) == 0 {
		return Task{}, false, fmt.Errorf("%w: poll needs a worker ID and at least one dock", ErrInvalid)
	}
	p := &poller{req: req, reply: make(chan pollResult, 1)}
	if !c.inbox.put(pollInput{p: p}) {
		return Task{}, false, ErrStopped
	}
	var r pollResult
	select {
	case r = <-p.reply:
	case <-ctx.Done():
		c.inbox.put(pollInput{p: p, withdraw: ctx.Err()}) // if the inbox is closed, shutdown answers p
		r = <-p.reply
	}
	if r.ok && r.task.Kind == TaskHaul {
		// Read after the dispatch committed, so the logbook has every result
		// recorded up to then (and possibly later ones).
		r.task.Logbook, r.err = c.cfg.Store.HaulEvents(context.WithoutCancel(ctx), r.task.HaulID)
	}
	return r.task, r.ok, r.err
}

type pollResult struct {
	task Task
	ok   bool
	err  error
}

type poller struct {
	req   PollRequest
	reply chan pollResult // buffered; written exactly once
	timer *timerRef
	done  bool
}

// finish answers p. Only the core goroutine calls it.
func (p *poller) finish(r pollResult) {
	if p.done {
		return
	}
	p.done = true
	p.timer.stop()
	p.reply <- r
}

func (p *poller) wants(kind TaskKind) bool {
	if len(p.req.Kinds) == 0 {
		return true
	}
	for _, k := range p.req.Kinds {
		if k == kind {
			return true
		}
	}
	return false
}

// pollInput registers a poller or, with withdraw set, takes it back.
type pollInput struct {
	p        *poller
	withdraw error
}

func (in pollInput) apply(c *Core) {
	if in.withdraw != nil {
		c.dropPoller(in.p)
		c.sawHangUp(in.p.req.WorkerID)
		in.p.finish(pollResult{err: in.withdraw})
		return
	}
	if b := in.p.req.BuildID; b != "" {
		if _, ok := c.builds[b]; !ok {
			in.p.finish(pollResult{err: fmt.Errorf("%w: build %s is not registered", ErrUnknownBuild, b)})
			return
		}
	}
	c.sawPoll(in.p.req)
	if in.p.req.Wait > 0 {
		p := in.p
		p.timer = c.after(p.req.Wait, func(c *Core) {
			c.dropPoller(p)
			p.finish(pollResult{})
		})
	}
	c.pollers = append(c.pollers, in.p)
}

func (in pollInput) reject(err error) { in.p.finish(pollResult{err: err}) }

type queuedTask struct {
	task     Task
	order    uint64
	queuedAt time.Time
	// dispatch runs in the batch that hands the task to workerID: it records
	// the dispatch and returns the task as the worker receives it.
	dispatch func(c *Core, workerID string) Task
}

type dockState struct {
	limit        int
	haulTasks    []*queuedTask
	lifts        []*queuedTask
	runningLifts int
}

func (c *Core) dock(name string) *dockState {
	d := c.docks[name]
	if d == nil {
		d = &dockState{limit: c.cfg.Docks[name].MaxRunningLifts}
		c.docks[name] = d
	}
	return d
}

// enqueue queues a task on its dock. dispatch is called when a worker takes
// it; nil hands the task out as is.
func (c *Core) enqueue(t Task, dispatch func(c *Core, workerID string) Task) {
	if dispatch == nil {
		dispatch = func(*Core, string) Task { return t }
	}
	c.queueOrder++
	q := &queuedTask{task: t, order: c.queueOrder, queuedAt: c.now(), dispatch: dispatch}
	d := c.dock(t.Dock)
	if t.Kind == TaskHaul {
		d.haulTasks = append(d.haulTasks, q)
	} else {
		d.lifts = append(d.lifts, q)
	}
}

// unqueue removes a task that no worker has taken yet, reporting whether it
// was still queued.
func (c *Core) unqueue(dock, id string) bool {
	d := c.docks[dock]
	if d == nil {
		return false
	}
	for _, list := range []*[]*queuedTask{&d.haulTasks, &d.lifts} {
		for i, q := range *list {
			if q.task.ID == id {
				*list = append((*list)[:i], (*list)[i+1:]...)
				return true
			}
		}
	}
	return false
}

// releaseLift frees a lift slot on a dock when a dispatched lift attempt
// finishes.
func (c *Core) releaseLift(dock string) {
	if d := c.docks[dock]; d != nil && d.runningLifts > 0 {
		d.runningLifts--
	}
}

// matchDocks hands queued tasks to waiting pollers, oldest poller first, and
// answers pollers that asked not to wait. Called at the end of every batch,
// before commit; replies go out after the commit.
func (c *Core) matchDocks() {
	kept := c.pollers[:0]
	for _, p := range c.pollers {
		if p.done {
			continue
		}
		if q := c.nextTask(p); q != nil {
			c.handOut(p, q)
			continue
		}
		if p.req.Wait <= 0 {
			p.finish(pollResult{})
			continue
		}
		kept = append(kept, p)
	}
	for i := len(kept); i < len(c.pollers); i++ {
		c.pollers[i] = nil
	}
	c.pollers = kept
}

// nextTask picks the oldest haul task on p's docks that p's build can run,
// else the oldest such lift on a dock with a free slot, and removes it from
// its queue.
func (c *Core) nextTask(p *poller) *queuedTask {
	for _, kind := range []TaskKind{TaskHaul, TaskLift} {
		if !p.wants(kind) {
			continue
		}
		var (
			best     *queuedTask
			bestDock *dockState
			bestAt   int
		)
		for _, name := range p.req.Docks {
			d := c.docks[name]
			if d == nil {
				continue
			}
			list := d.haulTasks
			if kind == TaskLift {
				if d.limit > 0 && d.runningLifts >= d.limit {
					continue
				}
				list = d.lifts
			}
			for i, q := range list {
				if best != nil && q.order > best.order {
					break // queues are oldest first
				}
				if c.canRun(p.req.BuildID, q.task) {
					best, bestDock, bestAt = q, d, i
					break
				}
			}
		}
		if best != nil {
			if kind == TaskHaul {
				bestDock.haulTasks = slices.Delete(bestDock.haulTasks, bestAt, bestAt+1)
			} else {
				bestDock.lifts = slices.Delete(bestDock.lifts, bestAt, bestAt+1)
				bestDock.runningLifts++
			}
			return best
		}
	}
	return nil
}

func (c *Core) handOut(p *poller, q *queuedTask) {
	t := q.dispatch(c, p.req.WorkerID)
	p.done = true // answered after commit
	p.timer.stop()
	c.b.replies = append(c.b.replies, func(commitErr error) {
		if commitErr != nil {
			p.reply <- pollResult{err: commitErr}
			return
		}
		p.reply <- pollResult{task: t, ok: true}
	})
}

// stopPollers answers every waiting poller when the core stops.
func (c *Core) stopPollers() {
	for _, p := range c.pollers {
		p.finish(pollResult{err: ErrStopped})
	}
	c.pollers = nil
}

func (c *Core) dropPoller(p *poller) {
	for i, q := range c.pollers {
		if q == p {
			c.pollers = append(c.pollers[:i], c.pollers[i+1:]...)
			return
		}
	}
}
