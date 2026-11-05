package core

import (
	"context"
	"slices"
	"time"
)

// Pools. The server scales worker pools (internal/pools) from the work
// queued on their queues: the core says how much waits for a worker
// (Demand), and keeps what the scaler reports (SetPools), in memory like
// workers: for ListPools, and so a run that waits while a pool starts a
// worker says so (WaitStarting).

// PoolStatus is one pool as the scaler last reported it.
type PoolStatus struct {
	Name     string
	Queues   []string
	Min, Max int
	// Workers are the processes it runs, by worker ID; Starting of them
	// haven't polled yet.
	Workers  []string
	Starting int
	// Desired is how many workers it wants: what it runs, for a pool
	// moksha starts; for an external one, what its launcher should run.
	Desired  int
	External bool
	// Decision is its last scaling decision in words ("started py-3: 4
	// steps wait on default"), at DecidedAt; "" before the first.
	Decision  string
	DecidedAt time.Time
}

// SetPools replaces the pools' status.
func (c *Core) SetPools(ctx context.Context, pools []PoolStatus) error {
	_, err := submit(ctx, c, func(c *Core) (struct{}, error) {
		c.pools = slices.Clone(pools)
		return struct{}{}, nil
	})
	return err
}

// Pools returns the pools' status as last set.
func (c *Core) Pools(ctx context.Context) ([]PoolStatus, error) {
	return submit(ctx, c, func(c *Core) ([]PoolStatus, error) { return slices.Clone(c.pools), nil })
}

// startingFor is the first pool starting a worker for queue, or "".
func (c *Core) startingFor(queue string) string {
	for _, p := range c.pools {
		if p.Starting > 0 && slices.Contains(p.Queues, queue) {
			return p.Name
		}
	}
	return ""
}

// QueueDemand is the work queued on a queue that waits for a worker: none
// polls the queue, or those that can run it are busy. Work no worker's
// code defines, or held by the queue's limit, isn't demand: more workers
// wouldn't run it.
type QueueDemand struct {
	RunTasks int
	Steps    int
}

// Demand returns the queued work waiting for a worker, by queue.
func (c *Core) Demand(ctx context.Context) (map[string]QueueDemand, error) {
	return submit(ctx, c, func(c *Core) (map[string]QueueDemand, error) {
		workers := c.liveWorkers()
		out := map[string]QueueDemand{}
		for name, d := range c.queues {
			for _, list := range [][]*queuedTask{d.runTasks, d.steps} {
				for _, q := range list {
					switch c.waitReason(q.task, workers).Reason {
					case WaitNoWorker, WaitBusy, WaitStarting:
					default:
						continue
					}
					dd := out[name]
					if q.task.Kind == TaskRun {
						dd.RunTasks++
					} else {
						dd.Steps++
					}
					out[name] = dd
				}
			}
		}
		return out, nil
	})
}
