package core

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/alper/moksha/internal/state"
)

// Queues are the queues workers pull run tasks and step tasks from. They live
// in the core goroutine like the rest of the scheduling state: the run
// engine queues work while processing a batch, and at the end of the batch
// queued work is matched to waiting pollers. A task is handed to a worker
// only after the batch that records its dispatch has committed (write before
// act).
//
// Run tasks always go before steps: they are short replays and should never
// wait behind work. A queue's concurrency limit applies to steps only.

// TaskKind says what a worker is asked to do.
type TaskKind string

const (
	TaskRun  TaskKind = "run"  // replay a run until it blocks
	TaskStep TaskKind = "step" // run one step attempt
)

// Task is one unit of work handed to a worker.
type Task struct {
	Kind  TaskKind
	ID    string // run task ID or step attempt ID
	Queue string
	RunID string
	Seq   int    // step: sequence number of the command that scheduled it
	Name  string // run or step function
	Args  json.RawMessage
	// Output (steps with an output): the data version to produce. Staging (every
	// step, when the server has a bucket): the prefix this attempt writes its
	// files to. A step without an output that writes files there (a
	// dataframe it returned) produces a data version in its implicit dataset.
	Output  *OutputSpec
	Staging string
	// Inputs (steps): the data versions passed to it, by argument name, with
	// where their manifests are.
	Inputs []TaskInput
	// EventLog is the run's events up to at least this task's dispatch
	// (run tasks only). The replay reads results from it.
	EventLog []state.Event
}

// TaskInput is a data version passed to a step, as the worker gets it.
type TaskInput struct {
	Name        string
	DataVersion string
	Location    string // the data version's manifest
}

// QueueConfig limits one queue.
type QueueConfig struct {
	MaxRunningSteps int // 0 = unlimited
}

// PollRequest asks for the next task from any of Queues.
type PollRequest struct {
	WorkerID string
	// BuildID, if set, is the worker's registered build: it gets only the
	// run tasks and steps its build defines.
	BuildID string
	Queues  []string
	Kinds   []TaskKind    // empty: any
	Wait    time.Duration // how long to wait for work; 0 = answer at once
}

// Poll returns the next task for a worker, waiting up to req.Wait (measured
// on the core's clock). ok is false when there was no work in time. If ctx
// ends first, Poll withdraws; a task handed out in the meantime is still
// returned, so it is never silently lost. Poll needs a running core.
func (c *Core) Poll(ctx context.Context, req PollRequest) (task Task, ok bool, err error) {
	if req.WorkerID == "" || len(req.Queues) == 0 {
		return Task{}, false, fmt.Errorf("%w: poll needs a worker ID and at least one queue", ErrInvalid)
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
	if r.ok && r.task.Kind == TaskRun {
		// Read after the dispatch committed, so the event log has every result
		// recorded up to then (and possibly later ones).
		r.task.EventLog, r.err = c.cfg.Store.RunEvents(context.WithoutCancel(ctx), r.task.RunID)
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

type queueState struct {
	limit        int
	runTasks     []*queuedTask
	steps        []*queuedTask
	runningSteps int
}

func (c *Core) queue(name string) *queueState {
	d := c.queues[name]
	if d == nil {
		d = &queueState{limit: c.cfg.Queues[name].MaxRunningSteps}
		c.queues[name] = d
	}
	return d
}

// enqueue queues a task on its queue. dispatch is called when a worker takes
// it; nil hands the task out as is.
func (c *Core) enqueue(t Task, dispatch func(c *Core, workerID string) Task) {
	if dispatch == nil {
		dispatch = func(*Core, string) Task { return t }
	}
	c.queueOrder++
	q := &queuedTask{task: t, order: c.queueOrder, queuedAt: c.now(), dispatch: dispatch}
	d := c.queue(t.Queue)
	if t.Kind == TaskRun {
		d.runTasks = append(d.runTasks, q)
	} else {
		d.steps = append(d.steps, q)
	}
}

// unqueue removes a task that no worker has taken yet, reporting whether it
// was still queued.
func (c *Core) unqueue(queue, id string) bool {
	d := c.queues[queue]
	if d == nil {
		return false
	}
	for _, list := range []*[]*queuedTask{&d.runTasks, &d.steps} {
		for i, q := range *list {
			if q.task.ID == id {
				*list = append((*list)[:i], (*list)[i+1:]...)
				return true
			}
		}
	}
	return false
}

// releaseStep frees a step slot on a queue when a dispatched step attempt
// finishes.
func (c *Core) releaseStep(queue string) {
	if d := c.queues[queue]; d != nil && d.runningSteps > 0 {
		d.runningSteps--
	}
}

// matchQueues hands queued tasks to waiting pollers, oldest poller first, and
// answers pollers that asked not to wait. Called at the end of every batch,
// before commit; replies go out after the commit.
func (c *Core) matchQueues() {
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

// nextTask picks the oldest run task on p's queues that p's build can run,
// else the oldest such step on a queue with a free slot, and removes it from
// its queue.
func (c *Core) nextTask(p *poller) *queuedTask {
	for _, kind := range []TaskKind{TaskRun, TaskStep} {
		if !p.wants(kind) {
			continue
		}
		var (
			best      *queuedTask
			bestQueue *queueState
			bestAt    int
		)
		for _, name := range p.req.Queues {
			d := c.queues[name]
			if d == nil {
				continue
			}
			list := d.runTasks
			if kind == TaskStep {
				if d.limit > 0 && d.runningSteps >= d.limit {
					continue
				}
				list = d.steps
			}
			for i, q := range list {
				if best != nil && q.order > best.order {
					break // queues are oldest first
				}
				if c.canRun(p.req.BuildID, q.task) {
					best, bestQueue, bestAt = q, d, i
					break
				}
			}
		}
		if best != nil {
			if kind == TaskRun {
				bestQueue.runTasks = slices.Delete(bestQueue.runTasks, bestAt, bestAt+1)
			} else {
				bestQueue.steps = slices.Delete(bestQueue.steps, bestAt, bestAt+1)
				bestQueue.runningSteps++
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
