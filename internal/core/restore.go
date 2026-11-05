package core

import (
	"context"

	"github.com/alper/moksha/internal/state"
)

// restore rebuilds the in-memory model from the store and settles what the
// previous leader left in flight (see runs_restore.go), in one committed
// batch.
func (c *Core) restore(ctx context.Context) error {
	runs, err := c.loadRuns(ctx)
	if err != nil {
		return err
	}
	lost := 0
	for _, t := range runs.RunTasks {
		if t.Status == state.RunTaskDispatched {
			lost++
		}
	}

	c.b = newBatch()
	c.emit(state.EventLeaderTermStarted, "", leaderTermStartedPayload{
		Term: c.cfg.Term, ActiveRuns: len(runs.Runs), LostRunTasks: lost,
	})
	if err := c.settleRuns(runs); err != nil {
		return err
	}
	scheds, err := c.cfg.Store.ListSchedules(ctx)
	if err != nil {
		return err
	}
	c.restoreSchedules(scheds)
	if err := c.restoreProjects(ctx); err != nil {
		return err
	}
	subs, err := c.cfg.Store.ListSubscriptions(ctx)
	if err != nil {
		return err
	}
	waiting, err := c.cfg.Store.ListSubscriptionDataVersions(ctx)
	if err != nil {
		return err
	}
	if err := c.restoreSubscriptions(subs, waiting); err != nil {
		return err
	}
	keep, err := c.cfg.Store.ListRetention(ctx)
	if err != nil {
		return err
	}
	for _, r := range keep {
		c.retention[r.Dataset] = r
	}
	c.armRetention(0)
	return c.commit(ctx)
}
