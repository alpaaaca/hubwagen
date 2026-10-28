package core

import (
	"context"

	"github.com/alper/hubwagen/internal/state"
)

// restore rebuilds the in-memory model from the store and settles what the
// previous leader left in flight (see hauls_restore.go), in one committed
// batch.
func (c *Core) restore(ctx context.Context) error {
	hauls, err := c.loadHauls(ctx)
	if err != nil {
		return err
	}
	lost := 0
	for _, t := range hauls.HaulTasks {
		if t.Status == state.HaulTaskDispatched {
			lost++
		}
	}

	c.b = newBatch()
	c.emit(state.EventLeaderTermStarted, "", leaderTermStartedPayload{
		Term: c.cfg.Term, ActiveHauls: len(hauls.Hauls), LostHaulTasks: lost,
	})
	if err := c.settleHauls(hauls); err != nil {
		return err
	}
	scheds, err := c.cfg.Store.ListSchedules(ctx)
	if err != nil {
		return err
	}
	c.restoreSchedules(scheds)
	subs, err := c.cfg.Store.ListSubscriptions(ctx)
	if err != nil {
		return err
	}
	waiting, err := c.cfg.Store.ListSubscriptionPallets(ctx)
	if err != nil {
		return err
	}
	if err := c.restoreSubscriptions(subs, waiting); err != nil {
		return err
	}
	return c.commit(ctx)
}
