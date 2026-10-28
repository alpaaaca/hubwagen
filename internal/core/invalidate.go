package core

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/alper/hubwagen/internal/state"
)

// Invalidation marks bad data. InvalidatePallet marks a pallet and every
// pallet built from it, directly or transitively, invalid: they are never
// reused by memoization, never a dataset's latest, and can't be a lift's
// input, so replaying the hauls that built them recomputes exactly that set.
// Each one gets a PalletInvalidated event naming the cause; the events
// belong to the tenant, not to a haul's logbook, so no replay changes.
//
// Pallets still being promoted are marked too: their promote then records
// them as invalid. Lifts already running with an invalidated input finish
// as they are.

// InvalidateRequest: Reason is required; DryRun only reports.
type InvalidateRequest struct {
	PalletID string
	Reason   string
	By       string
	DryRun   bool
}

// InvalidateResult is what was (or, dry, would be) marked invalid, the root
// first, and the hauls that built those pallets.
type InvalidateResult struct {
	Pallets []state.Pallet
	Hauls   []state.Haul
}

func (c *Core) InvalidatePallet(ctx context.Context, req InvalidateRequest) (InvalidateResult, error) {
	return submit(ctx, c, func(c *Core) (InvalidateResult, error) {
		if req.Reason == "" {
			return InvalidateResult{}, fmt.Errorf("%w: invalidating needs a reason", ErrInvalid)
		}
		down, err := c.cfg.Store.Downstream(ctx, req.PalletID)
		if errors.Is(err, state.ErrNotFound) {
			return InvalidateResult{}, fmt.Errorf("%w: pallet %s", ErrNotFound, req.PalletID)
		}
		if err != nil {
			return InvalidateResult{}, err
		}
		var res InvalidateResult
		for _, p := range down.Pallets {
			if p.State == state.PalletCommitted || p.State == state.PalletPending {
				res.Pallets = append(res.Pallets, p)
			}
		}
		sort.SliceStable(res.Pallets, func(i, j int) bool { return res.Pallets[i].ID == req.PalletID && res.Pallets[j].ID != req.PalletID })
		seen := map[string]bool{}
		for _, p := range res.Pallets {
			if p.HaulID == "" || seen[p.HaulID] {
				continue
			}
			seen[p.HaulID] = true
			if h, err := c.cfg.Store.GetHaul(ctx, p.HaulID); err == nil {
				res.Hauls = append(res.Hauls, h)
			}
		}
		if req.DryRun {
			return res, nil
		}
		now := c.now()
		for i := range res.Pallets {
			p := &res.Pallets[i]
			reason := req.Reason
			if p.ID != req.PalletID {
				reason = fmt.Sprintf("built from invalidated pallet %s: %s", req.PalletID, req.Reason)
			}
			if pr := c.promotions[p.ID]; pr != nil {
				pr.invalidated = reason // its promote records it as invalid
			}
			p.State, p.Reason, p.RetiredAt = state.PalletInvalid, reason, ptr(now)
			c.putPallet(*p)
			c.emit(state.EventPalletInvalidated, p.Tenant, palletInvalidatedPayload{
				Pallet: p.ID, Dataset: p.Dataset, Partition: p.Partition, Reason: reason, Cause: req.PalletID, By: req.By})
		}
		return res, nil
	})
}
