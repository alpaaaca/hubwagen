package core

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// Retention. A build declares, for the datasets its lifts write, how many
// versions to keep (hw.Dataset(keep=...)); the newest build that declares a
// dataset decides, and one that declares it without keep removes its
// retention (everything stays). Per partition, the latest committed pallet
// always stays, and so do those among the newest Versions or committed less
// than Age ago; the rest expire, unless something running may read them
// (see state.ExpiryCandidates).
//
// Expiring is write before act: the pallet is recorded as expired with a
// PalletExpired event first, then its data is deleted off the loop. Its
// manifest and lineage stay, as a record. Expired pallets are never reused
// or read; a replay that needs one reruns its lift.
//
// The leader expires on a timer (Config.RetentionEvery) and on request
// (Expire, `hubwagen gc`).

// DefaultRetentionEvery is how often the leader expires by default.
const DefaultRetentionEvery = time.Hour

// maxExpiredPerRun bounds one run; the next run continues.
const maxExpiredPerRun = 1000

// DatasetSpec is what a build declares for a dataset its lifts write.
type DatasetSpec struct {
	Name     string
	Versions int           // keep the newest N per partition; 0: unset
	Age      time.Duration // keep those younger than this; 0: unset
}

func validateDatasets(specs []DatasetSpec) error {
	var problems []string
	seen := map[string]bool{}
	for i, d := range specs {
		switch {
		case d.Name == "":
			problems = append(problems, fmt.Sprintf("datasets[%d]: needs a name", i))
		case seen[d.Name]:
			problems = append(problems, fmt.Sprintf("datasets[%d]: %s declared twice", i, d.Name))
		}
		seen[d.Name] = true
		if d.Versions < 0 || d.Age < 0 {
			problems = append(problems, fmt.Sprintf("datasets[%d] (%s): keep must not be negative", i, d.Name))
		}
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalid, strings.Join(problems, "; "))
	}
	return nil
}

// applyRetention makes a newly registered build's retention the current one
// for the datasets it declares.
func (c *Core) applyRetention(build string, specs []DatasetSpec) {
	for _, d := range specs {
		cur, ok := c.retention[d.Name]
		switch {
		case d.Versions == 0 && d.Age == 0:
			if ok {
				delete(c.retention, d.Name)
				name := d.Name
				c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.DeleteRetention(name) })
				c.emit(state.EventRetentionSet, "", retentionSetPayload{Dataset: d.Name, Build: build})
			}
		case ok && cur.Versions == d.Versions && cur.Age == d.Age:
		default:
			r := state.Retention{Dataset: d.Name, Versions: d.Versions, Age: d.Age, BuildID: build, UpdatedAt: c.now()}
			c.retention[d.Name] = r
			c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutRetention(r) })
			c.emit(state.EventRetentionSet, "", retentionSetPayload{Dataset: d.Name, Versions: d.Versions, Age: d.Age, Build: build})
		}
	}
}

// ExpiredPallet is a pallet retention expired (or, dry, would expire).
type ExpiredPallet struct {
	Pallet state.Pallet
	Reason string
}

// Expire expires what retention no longer keeps, at most maxExpiredPerRun
// pallets, oldest first per dataset. DryRun only reports.
func (c *Core) Expire(ctx context.Context, dryRun bool) ([]ExpiredPallet, error) {
	return submit(ctx, c, func(c *Core) ([]ExpiredPallet, error) { return c.expire(dryRun) })
}

func (c *Core) expire(dryRun bool) ([]ExpiredPallet, error) {
	var out []ExpiredPallet
	for _, name := range slices.Sorted(maps.Keys(c.retention)) {
		r := c.retention[name]
		left := maxExpiredPerRun - len(out)
		if left <= 0 {
			break
		}
		ps, err := c.cfg.Store.ExpiryCandidates(c.ctx, r, c.now(), left)
		if err != nil {
			return out, err
		}
		reason := retentionReason(r)
		for _, p := range ps {
			out = append(out, ExpiredPallet{Pallet: p, Reason: reason})
		}
	}
	if dryRun {
		return out, nil
	}
	for i := range out {
		p := &out[i].Pallet
		p.State, p.Reason, p.RetiredAt = state.PalletExpired, out[i].Reason, ptr(c.now())
		c.putPallet(*p)
		c.emit(state.EventPalletExpired, p.Tenant, palletExpiredPayload{
			Pallet: p.ID, Dataset: p.Dataset, Partition: p.Partition, Reason: out[i].Reason})
		c.deleteData(p.Location)
	}
	return out, nil
}

func retentionReason(r state.Retention) string {
	var keep []string
	if r.Versions > 0 {
		keep = append(keep, fmt.Sprintf("the newest %d", r.Versions))
	}
	if r.Age > 0 {
		keep = append(keep, "those younger than "+r.Age.String())
	}
	return fmt.Sprintf("retention: %s keeps each partition's latest and %s", r.Dataset, strings.Join(keep, " and "))
}

// deleteData removes an expired pallet's files, off the loop, once its
// expiry is committed. Its manifest stays.
func (c *Core) deleteData(manifest string) {
	if c.cfg.Pallets == nil {
		return
	}
	ctx, store := c.ctx, c.cfg.Pallets
	c.background(func() {
		m, err := store.Read(ctx, manifest)
		if err == nil {
			err = store.Discard(ctx, m.Data)
		}
		if err != nil {
			c.log.Warn("deleting an expired pallet's data failed", "manifest", manifest, "err", err)
		}
	})
}

// armRetention runs retention every Config.RetentionEvery, the first time
// right after the leader starts.
func (c *Core) armRetention(d time.Duration) {
	every := c.cfg.RetentionEvery
	if every < 0 {
		return // off
	}
	if every == 0 {
		every = DefaultRetentionEvery
	}
	c.after(d, func(c *Core) {
		if len(c.retention) > 0 {
			if _, err := c.expire(false); err != nil {
				c.log.Warn("retention failed", "err", err)
			}
		}
		c.armRetention(every)
	})
}
