package core

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/alper/moksha/internal/state"
)

// Dataset subscriptions. A build declares, for a pipeline it defines, datasets
// whose new data versions start it; the newest build that defines a pipeline decides
// (or, declaring none, removes it), as for schedules. When a data version is
// committed to one of those datasets, it joins a batch for the run in the
// data version's tenant (recorded, so a new leader keeps it). The run starts with
// the batch, as the param {"versions": [data version refs]}, once no new data version has
// come for Debounce, once MaxBatch data versions wait, or once the first one has
// waited MaxWait, whichever is first; the batch is cleared in the same
// commit. A run's own data versions don't start it again.

// Subscription defaults.
const (
	defaultMaxBatch     = 100
	maxWaitPerDebounce  = 10 // MaxWait defaults to this many Debounces
	maxSubscriptionSets = 32 // datasets per subscription
)

// SubscriptionSpec is what a build declares for one of its runs.
type SubscriptionSpec struct {
	Pipeline string
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
	return s.Pipeline == o.Pipeline && slices.Equal(s.Datasets, o.Datasets) && s.Debounce == o.Debounce &&
		s.MaxWait == o.MaxWait && s.MaxBatch == o.MaxBatch
}

func validateSubscriptions(runs []string, specs []SubscriptionSpec) ([]SubscriptionSpec, error) {
	var problems []string
	seen := map[string]bool{}
	out := make([]SubscriptionSpec, 0, len(specs))
	for i, s := range specs {
		s = s.normalized()
		where := fmt.Sprintf("subscriptions[%d]", i)
		switch {
		case !slices.Contains(runs, s.Pipeline):
			problems = append(problems, fmt.Sprintf("%s: pipeline %q isn't one of the build's pipelines", where, s.Pipeline))
		case seen[s.Pipeline]:
			problems = append(problems, fmt.Sprintf("%s: pipeline %q has two subscriptions", where, s.Pipeline))
		}
		seen[s.Pipeline] = true
		switch {
		case len(s.Datasets) == 0 || slices.Contains(s.Datasets, ""):
			problems = append(problems, fmt.Sprintf("%s (%s): needs dataset names", where, s.Pipeline))
		case len(s.Datasets) > maxSubscriptionSets:
			problems = append(problems, fmt.Sprintf("%s (%s): at most %d datasets", where, s.Pipeline, maxSubscriptionSets))
		}
		if s.Debounce < 0 || s.MaxWait < s.Debounce || s.MaxBatch < 1 {
			problems = append(problems, fmt.Sprintf("%s (%s): want debounce ≥ 0, max_wait ≥ debounce and max_batch ≥ 1", where, s.Pipeline))
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
	return SubscriptionSpec{Pipeline: s.rec.Pipeline, Datasets: s.rec.Datasets, Debounce: s.rec.Debounce, MaxWait: s.rec.MaxWait, MaxBatch: s.rec.MaxBatch}
}

// pendingBatch is the data versions waiting to start a subscription's run in
// one tenant.
type pendingBatch struct {
	dataVersions []pendingDataVersion
	first, last  time.Time
	timer        *timerRef
}

type pendingDataVersion struct {
	ID, Dataset, Partition string
}

// applySubscriptions makes a newly registered build's subscriptions the
// current ones for the pipelines it defines.
func (c *Core) applySubscriptions(build string, runs []string, specs []SubscriptionSpec) {
	byRun := map[string]SubscriptionSpec{}
	for _, s := range specs {
		byRun[s.Pipeline] = s
	}
	for _, run := range slices.Sorted(slices.Values(runs)) {
		cur := c.subscriptions[run]
		spec, ok := byRun[run]
		switch {
		case ok && cur != nil && cur.spec().equal(spec):
		case ok:
			now := c.now()
			s := &subscriptionState{batches: map[string]*pendingBatch{}, rec: state.Subscription{
				Pipeline: run, Datasets: spec.Datasets, Debounce: spec.Debounce, MaxWait: spec.MaxWait, MaxBatch: spec.MaxBatch,
				BuildID: build, CreatedAt: now, UpdatedAt: now}}
			if cur != nil { // waiting data versions stay; the new timing applies to them
				s.rec.CreatedAt, s.batches = cur.rec.CreatedAt, cur.batches
			}
			c.subscriptions[run] = s
			rec := s.rec
			c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutSubscription(rec) })
			c.emit(state.EventSubscriptionSet, "", subscriptionSetPayload{Pipeline: run, Datasets: spec.Datasets,
				Debounce: spec.Debounce, MaxWait: spec.MaxWait, MaxBatch: spec.MaxBatch, Build: build})
			for tenant := range s.batches {
				c.armBatch(s, tenant)
			}
		case cur != nil:
			for _, b := range cur.batches {
				b.timer.stop()
			}
			delete(c.subscriptions, run)
			c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.DeleteSubscription(run) })
			c.emit(state.EventSubscriptionRemoved, "", subscriptionRemovedPayload{Pipeline: run, Build: build})
		}
	}
}

// dataVersionCommitted adds a newly committed data version to the batches of the
// subscriptions on its dataset. producer is the run whose step built it.
func (c *Core) dataVersionCommitted(rec state.DataVersion, producer string) {
	for _, run := range slices.Sorted(maps.Keys(c.subscriptions)) {
		s := c.subscriptions[run]
		if run == producer || !slices.Contains(s.rec.Datasets, rec.Dataset) {
			continue
		}
		c.addToBatch(s, rec.Tenant, pendingDataVersion{ID: rec.ID, Dataset: rec.Dataset, Partition: rec.Partition}, c.now(), true)
	}
}

func (c *Core) addToBatch(s *subscriptionState, tenant string, p pendingDataVersion, at time.Time, record bool) {
	b := s.batches[tenant]
	if b == nil {
		b = &pendingBatch{first: at}
		s.batches[tenant] = b
	}
	b.dataVersions = append(b.dataVersions, p)
	b.last = at
	if record {
		row := state.SubscriptionDataVersion{Pipeline: s.rec.Pipeline, Tenant: tenant, DataVersionID: p.ID, AddedAt: at}
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.AddSubscriptionDataVersion(row) })
	}
	c.armBatch(s, tenant)
}

// armBatch sets the batch's timer: quiet for Debounce, or MaxWait since
// its first data version, or at once when it is full.
func (c *Core) armBatch(s *subscriptionState, tenant string) {
	b := s.batches[tenant]
	b.timer.stop()
	due := b.last.Add(s.rec.Debounce)
	if limit := b.first.Add(s.rec.MaxWait); limit.Before(due) {
		due = limit
	}
	if len(b.dataVersions) >= s.rec.MaxBatch {
		c.startBatch(s, tenant, b)
		return
	}
	b.timer = c.after(max(due.Sub(c.now()), 0), func(c *Core) { c.startBatch(s, tenant, b) })
}

// startBatch starts the subscription's run with the batch and clears it.
func (c *Core) startBatch(s *subscriptionState, tenant string, b *pendingBatch) {
	if c.subscriptions[s.rec.Pipeline] != s || s.batches[tenant] != b {
		return // replaced, removed or started meanwhile
	}
	batch := b.dataVersions
	if len(batch) > s.rec.MaxBatch {
		batch = batch[:s.rec.MaxBatch]
	}
	refs := make([]any, len(batch))
	ids := make([]string, len(batch))
	for i, p := range batch {
		refs[i] = map[string]any{"$data_version": p.ID, "dataset": p.Dataset, "partition": p.Partition}
		ids[i] = p.ID
	}
	res, err := c.startRun(StartRunRequest{Tenant: tenant, Name: s.rec.Pipeline, Trigger: "subscription",
		Params: map[string]any{"versions": refs}})
	payload := subscriptionStartedPayload{Pipeline: s.rec.Pipeline, DataVersions: ids, RunID: res.RunID}
	if err != nil {
		payload.Error = err.Error()
	}
	c.emit(state.EventSubscriptionStarted, tenant, payload)

	run := s.rec.Pipeline
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.ClearSubscriptionDataVersions(run, tenant) })
	delete(s.batches, tenant)
	// What didn't fit in this batch starts the next one.
	now := c.now()
	for _, p := range b.dataVersions[len(batch):] {
		c.addToBatch(s, tenant, p, now, true)
	}
}

// restoreSubscriptions loads subscriptions and their waiting data versions, in the
// restore batch; batches start when their timing says, counted from when
// their data versions came.
func (c *Core) restoreSubscriptions(subs []state.Subscription, waiting []state.SubscriptionDataVersion) error {
	for _, rec := range subs {
		c.subscriptions[rec.Pipeline] = &subscriptionState{rec: rec, batches: map[string]*pendingBatch{}}
	}
	for _, w := range waiting {
		s := c.subscriptions[w.Pipeline]
		if s == nil {
			continue
		}
		p, err := c.cfg.Store.GetDataVersion(c.ctx, w.DataVersionID)
		if err != nil {
			return fmt.Errorf("subscription %s: data version %s: %w", w.Pipeline, w.DataVersionID, err)
		}
		c.addToBatch(s, w.Tenant, pendingDataVersion{ID: p.ID, Dataset: p.Dataset, Partition: p.Partition}, w.AddedAt, false)
	}
	return nil
}
