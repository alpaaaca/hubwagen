package core

import (
	"context"
	"slices"
	"time"
)

// Pools. The server scales worker pools (internal/pools) from the work
// queued on their docks: the core says how much waits for a worker
// (Demand), and keeps what the scaler reports (SetPools), in memory like
// workers: for ListPools, and so a run that waits while a pool starts a
// worker says so (WaitStarting).

// PoolStatus is one pool as the scaler last reported it.
type PoolStatus struct {
	Name     string
	Docks    []string
	Min, Max int
	// Workers are the processes it runs, by worker ID; Starting of them
	// haven't polled yet.
	Workers  []string
	Starting int
	// Desired is how many workers it wants: what it runs, for a pool
	// hubwagen starts; for an external one, what its launcher should run.
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

// startingFor is the first pool starting a worker for dock, or "".
func (c *Core) startingFor(dock string) string {
	for _, p := range c.pools {
		if p.Starting > 0 && slices.Contains(p.Docks, dock) {
			return p.Name
		}
	}
	return ""
}

// DockDemand is the work queued on a dock that waits for a worker: none
// polls the dock, or those that can run it are busy. Work no worker's
// code defines, or held by the dock's limit, isn't demand: more workers
// wouldn't run it.
type DockDemand struct {
	HaulTasks int
	Lifts     int
}

// Demand returns the queued work waiting for a worker, by dock.
func (c *Core) Demand(ctx context.Context) (map[string]DockDemand, error) {
	return submit(ctx, c, func(c *Core) (map[string]DockDemand, error) {
		workers := c.liveWorkers()
		out := map[string]DockDemand{}
		for name, d := range c.docks {
			for _, list := range [][]*queuedTask{d.haulTasks, d.lifts} {
				for _, q := range list {
					switch c.waitReason(q.task, workers).Reason {
					case WaitNoWorker, WaitBusy, WaitStarting:
					default:
						continue
					}
					dd := out[name]
					if q.task.Kind == TaskHaul {
						dd.HaulTasks++
					} else {
						dd.Lifts++
					}
					out[name] = dd
				}
			}
		}
		return out, nil
	})
}
