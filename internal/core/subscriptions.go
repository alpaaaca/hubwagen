package core

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// Dataset subscriptions. A build declares, for a haul it defines, datasets
// whose new pallets start it; the newest build that defines a haul decides
// (or, declaring none, removes it), as for schedules. When a pallet is
// committed to one of those datasets, it joins a batch for the haul in the
// pallet's tenant (recorded, so a new leader keeps it). The haul starts with
// the batch, as the param {"pallets": [pallet refs]}, once no new pallet has
// come for Debounce, once MaxBatch pallets wait, or once the first one has
// waited MaxWait, whichever is first; the batch is cleared in the same
// commit. A haul's own pallets don't start it again.

// Subscription defaults.
const (
	defaultMaxBatch     = 100
	maxWaitPerDebounce  = 10 // MaxWait defaults to this many Debounces
	maxSubscriptionSets = 32 // datasets per subscription
)

// SubscriptionSpec is what a build declares for one of its hauls.
type SubscriptionSpec struct {
	Haul     string
	Datasets []string
	Debounce time.Duration
	MaxWait  time.Duration // 0: maxWaitPerDebounce × Debounce
	MaxBatch int           // 0: defaultMaxBatch
}

func (s SubscriptionSpec) normalized() SubscriptionSpec {
	if s.MaxBatch == 0 {
		s.MaxBatch = defaultMaxBatch
	}
	if s.MaxWait == 0 {
		s.MaxWait = maxWaitPerDebounce * s.Debounce
	}
	s.Datasets = slices.Compact(slices.Sorted(slices.Values(s.Datasets)))
	return s
}

func (s SubscriptionSpec) equal(o SubscriptionSpec) bool {
	return s.Haul == o.Haul && slices.Equal(s.Datasets, o.Datasets) && s.Debounce == o.Debounce &&
		s.MaxWait == o.MaxWait && s.MaxBatch == o.MaxBatch
}

func validateSubscriptions(hauls []string, specs []SubscriptionSpec) ([]SubscriptionSpec, error) {
	var problems []string
	seen := map[string]bool{}
	out := make([]SubscriptionSpec, 0, len(specs))
	for i, s := range specs {
		s = s.normalized()
		where := fmt.Sprintf("subscriptions[%d]", i)
		switch {
		case !slices.Contains(hauls, s.Haul):
			problems = append(problems, fmt.Sprintf("%s: haul %q isn't one of the build's hauls", where, s.Haul))
		case seen[s.Haul]:
			problems = append(problems, fmt.Sprintf("%s: haul %q has two subscriptions", where, s.Haul))
		}
		seen[s.Haul] = true
		switch {
		case len(s.Datasets) == 0 || slices.Contains(s.Datasets, ""):
			problems = append(problems, fmt.Sprintf("%s (%s): needs dataset names", where, s.Haul))
		case len(s.Datasets) > maxSubscriptionSets:
			problems = append(problems, fmt.Sprintf("%s (%s): at most %d datasets", where, s.Haul, maxSubscriptionSets))
		}
		if s.Debounce < 0 || s.MaxWait < s.Debounce || s.MaxBatch < 1 {
			problems = append(problems, fmt.Sprintf("%s (%s): want debounce ≥ 0, max_wait ≥ debounce and max_batch ≥ 1", where, s.Haul))
		}
		out = append(out, s)
	}
	if len(problems) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrInvalid, strings.Join(problems, "; "))
	}
	return out, nil
}

type subscriptionState struct {
	rec     state.Subscription
	batches map[string]*pendingBatch // by tenant
}

func (s *subscriptionState) spec() SubscriptionSpec {
	return SubscriptionSpec{Haul: s.rec.Haul, Datasets: s.rec.Datasets, Debounce: s.rec.Debounce, MaxWait: s.rec.MaxWait, MaxBatch: s.rec.MaxBatch}
}

// pendingBatch is the pallets waiting to start a subscription's haul in
// one tenant.
type pendingBatch struct {
	pallets     []pendingPallet
	first, last time.Time
	timer       *timerRef
}

type pendingPallet struct {
	ID, Dataset, Partition string
}

// applySubscriptions makes a newly registered build's subscriptions the
// current ones for the hauls it defines.
func (c *Core) applySubscriptions(build string, hauls []string, specs []SubscriptionSpec) {
	byHaul := map[string]SubscriptionSpec{}
	for _, s := range specs {
		byHaul[s.Haul] = s
	}
	for _, haul := range slices.Sorted(slices.Values(hauls)) {
		cur := c.subscriptions[haul]
		spec, ok := byHaul[haul]
		switch {
		case ok && cur != nil && cur.spec().equal(spec):
		case ok:
			now := c.now()
			s := &subscriptionState{batches: map[string]*pendingBatch{}, rec: state.Subscription{
				Haul: haul, Datasets: spec.Datasets, Debounce: spec.Debounce, MaxWait: spec.MaxWait, MaxBatch: spec.MaxBatch,
				BuildID: build, CreatedAt: now, UpdatedAt: now}}
			if cur != nil { // waiting pallets stay; the new timing applies to them
				s.rec.CreatedAt, s.batches = cur.rec.CreatedAt, cur.batches
			}
			c.subscriptions[haul] = s
			rec := s.rec
			c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutSubscription(rec) })
			c.emit(state.EventSubscriptionSet, "", subscriptionSetPayload{Haul: haul, Datasets: spec.Datasets,
				Debounce: spec.Debounce, MaxWait: spec.MaxWait, MaxBatch: spec.MaxBatch, Build: build})
			for tenant := range s.batches {
				c.armBatch(s, tenant)
			}
		case cur != nil:
			for _, b := range cur.batches {
				b.timer.stop()
			}
			delete(c.subscriptions, haul)
			c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.DeleteSubscription(haul) })
			c.emit(state.EventSubscriptionRemoved, "", subscriptionRemovedPayload{Haul: haul, Build: build})
		}
	}
}

// palletCommitted adds a newly committed pallet to the batches of the
// subscriptions on its dataset. producer is the haul whose lift built it.
func (c *Core) palletCommitted(rec state.Pallet, producer string) {
	for _, haul := range slices.Sorted(maps.Keys(c.subscriptions)) {
		s := c.subscriptions[haul]
		if haul == producer || !slices.Contains(s.rec.Datasets, rec.Dataset) {
			continue
		}
		c.addToBatch(s, rec.Tenant, pendingPallet{ID: rec.ID, Dataset: rec.Dataset, Partition: rec.Partition}, c.now(), true)
	}
}

func (c *Core) addToBatch(s *subscriptionState, tenant string, p pendingPallet, at time.Time, record bool) {
	b := s.batches[tenant]
	if b == nil {
		b = &pendingBatch{first: at}
		s.batches[tenant] = b
	}
	b.pallets = append(b.pallets, p)
	b.last = at
	if record {
		row := state.SubscriptionPallet{Haul: s.rec.Haul, Tenant: tenant, PalletID: p.ID, AddedAt: at}
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.AddSubscriptionPallet(row) })
	}
	c.armBatch(s, tenant)
}

// armBatch sets the batch's timer: quiet for Debounce, or MaxWait since
// its first pallet, or at once when it is full.
func (c *Core) armBatch(s *subscriptionState, tenant string) {
	b := s.batches[tenant]
	b.timer.stop()
	due := b.last.Add(s.rec.Debounce)
	if limit := b.first.Add(s.rec.MaxWait); limit.Before(due) {
		due = limit
	}
	if len(b.pallets) >= s.rec.MaxBatch {
		c.startBatch(s, tenant, b)
		return
	}
	b.timer = c.after(max(due.Sub(c.now()), 0), func(c *Core) { c.startBatch(s, tenant, b) })
}

// startBatch starts the subscription's haul with the batch and clears it.
func (c *Core) startBatch(s *subscriptionState, tenant string, b *pendingBatch) {
	if c.subscriptions[s.rec.Haul] != s || s.batches[tenant] != b {
		return // replaced, removed or started meanwhile
	}
	batch := b.pallets
	if len(batch) > s.rec.MaxBatch {
		batch = batch[:s.rec.MaxBatch]
	}
	refs := make([]any, len(batch))
	ids := make([]string, len(batch))
	for i, p := range batch {
		refs[i] = map[string]any{"$pallet": p.ID, "dataset": p.Dataset, "partition": p.Partition}
		ids[i] = p.ID
	}
	res, err := c.startHaul(StartHaulRequest{Tenant: tenant, Name: s.rec.Haul, Trigger: "subscription",
		Params: map[string]any{"pallets": refs}})
	payload := subscriptionStartedPayload{Haul: s.rec.Haul, Pallets: ids, HaulID: res.HaulID}
	if err != nil {
		payload.Error = err.Error()
	}
	c.emit(state.EventSubscriptionStarted, tenant, payload)

	haul := s.rec.Haul
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.ClearSubscriptionPallets(haul, tenant) })
	delete(s.batches, tenant)
	// What didn't fit in this batch starts the next one.
	now := c.now()
	for _, p := range b.pallets[len(batch):] {
		c.addToBatch(s, tenant, p, now, true)
	}
}

// restoreSubscriptions loads subscriptions and their waiting pallets, in the
// restore batch; batches start when their timing says, counted from when
// their pallets came.
func (c *Core) restoreSubscriptions(subs []state.Subscription, waiting []state.SubscriptionPallet) error {
	for _, rec := range subs {
		c.subscriptions[rec.Haul] = &subscriptionState{rec: rec, batches: map[string]*pendingBatch{}}
	}
	for _, w := range waiting {
		s := c.subscriptions[w.Haul]
		if s == nil {
			continue
		}
		p, err := c.cfg.Store.GetPallet(c.ctx, w.PalletID)
		if err != nil {
			return fmt.Errorf("subscription %s: pallet %s: %w", w.Haul, w.PalletID, err)
		}
		c.addToBatch(s, w.Tenant, pendingPallet{ID: p.ID, Dataset: p.Dataset, Partition: p.Partition}, w.AddedAt, false)
	}
	return nil
}
