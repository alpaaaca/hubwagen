package core

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/alper/moksha/internal/state"
)

// Invalidation marks bad data. InvalidateDataVersion marks a data version and every
// data version built from it, directly or transitively, invalid: they are never
// reused by memoization, never a dataset's latest, and can't be a step's
// input, so replaying the runs that built them recomputes exactly that set.
// Each one gets a DataVersionInvalidated event naming the cause; the events
// belong to the tenant, not to a run's event log, so no replay changes.
//
// DataVersions still being promoted are marked too: their promote then records
// them as invalid. Steps already running with an invalidated input finish
// as they are.

// InvalidateRequest: Reason is required; DryRun only reports.
type InvalidateRequest struct {
	DataVersionID string
	Reason        string
	By            string
	DryRun        bool
}

// InvalidateResult is what was (or, dry, would be) marked invalid, the root
// first, and the runs that built those data versions.
type InvalidateResult struct {
	DataVersions []state.DataVersion
	Runs         []state.Run
}

func (c *Core) InvalidateDataVersion(ctx context.Context, req InvalidateRequest) (InvalidateResult, error) {
	return submit(ctx, c, func(c *Core) (InvalidateResult, error) {
		if req.Reason == "" {
			return InvalidateResult{}, fmt.Errorf("%w: invalidating needs a reason", ErrInvalid)
		}
		down, err := c.cfg.Store.Downstream(ctx, req.DataVersionID)
		if errors.Is(err, state.ErrNotFound) {
			return InvalidateResult{}, fmt.Errorf("%w: data version %s", ErrNotFound, req.DataVersionID)
		}
		if err != nil {
			return InvalidateResult{}, err
		}
		var res InvalidateResult
		for _, p := range down.DataVersions {
			if p.State == state.DataVersionCommitted || p.State == state.DataVersionPending {
				res.DataVersions = append(res.DataVersions, p)
			}
		}
		sort.SliceStable(res.DataVersions, func(i, j int) bool {
			return res.DataVersions[i].ID == req.DataVersionID && res.DataVersions[j].ID != req.DataVersionID
		})
		seen := map[string]bool{}
		for _, p := range res.DataVersions {
			if p.RunID == "" || seen[p.RunID] {
				continue
			}
			seen[p.RunID] = true
			if h, err := c.cfg.Store.GetRun(ctx, p.RunID); err == nil {
				res.Runs = append(res.Runs, h)
			}
		}
		if req.DryRun {
			return res, nil
		}
		now := c.now()
		for i := range res.DataVersions {
			p := &res.DataVersions[i]
			reason := req.Reason
			if p.ID != req.DataVersionID {
				reason = fmt.Sprintf("built from invalidated version %s: %s", req.DataVersionID, req.Reason)
			}
			if pr := c.promotions[p.ID]; pr != nil {
				pr.invalidated = reason // its promote records it as invalid
			}
			p.State, p.Reason, p.RetiredAt = state.DataVersionInvalid, reason, ptr(now)
			c.putDataVersion(*p)
			c.emit(state.EventDataVersionInvalidated, p.Tenant, dataVersionInvalidatedPayload{
				DataVersion: p.ID, Dataset: p.Dataset, Partition: p.Partition, Reason: reason, Cause: req.DataVersionID, By: req.By})
		}
		return res, nil
	})
}
