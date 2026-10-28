package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/state"
)

// Lifts with an output produce a pallet, and so does a lift without one
// that reports files (the SDK writes a returned dataframe as Parquet): its
// pallet goes to the lift's implicit dataset, ImplicitDataset(name),
// unpartitioned. The worker writes files to the
// attempt's staging prefix and reports them; the core records the pallet as
// pending (with its manifest in the logbook), then promotes it off the loop
// and, once the promote succeeded, commits it: only then is the lift
// completed and the haul woken. A promote that fails for good invalidates
// the pallet and fails the attempt, which the lift's retry policy may
// retry. Staging of attempts that will never be promoted is discarded.

// OutputSpec is where a lift's pallet goes.
type OutputSpec struct {
	Dataset      string `json:"dataset"`
	Partition    string `json:"partition,omitempty"`
	PartitionKey string `json:"partition_key,omitempty"` // e.g. "day"; set when the dataset is created
}

// ImplicitDatasetPrefix starts the names of implicit datasets; outputs a
// lift declares can't use it.
const ImplicitDatasetPrefix = "lift:"

// ImplicitDataset is where the pallets of a lift without an output go.
func ImplicitDataset(lift string) string { return ImplicitDatasetPrefix + lift }

func (o OutputSpec) validate() error {
	switch {
	case o.Dataset == "":
		return errors.New("output without a dataset")
	case strings.HasPrefix(o.Dataset, ImplicitDatasetPrefix):
		return fmt.Errorf("output dataset %q: names starting with %q are for lifts without an output", o.Dataset, ImplicitDatasetPrefix)
	case o.PartitionKey == "" && o.Partition != "":
		return fmt.Errorf("output partition %q for a dataset without a partition key", o.Partition)
	case o.PartitionKey != "" && o.Partition == "":
		return fmt.Errorf("output to a dataset partitioned by %q needs a partition", o.PartitionKey)
	}
	return nil
}

// InputRef is a pallet passed to a lift.
type InputRef struct {
	Name   string `json:"name,omitempty"` // the lift argument
	Pallet string `json:"pallet"`
}

// LiftOutput is what a succeeded attempt wrote.
type LiftOutput struct {
	Files []pallet.File
	Stats map[string]any
	Meta  map[string]any
}

// PalletStore promotes and discards pallets in the bucket; *pallet.Store
// implements it. Its methods are called off the core goroutine.
type PalletStore interface {
	Promote(ctx context.Context, m pallet.Manifest) (string, error)
	Discard(ctx context.Context, prefix string) error
	Read(ctx context.Context, uri string) (pallet.Manifest, error)
}

// promoteRetryDelay is how long to wait before retrying a promote that
// failed for a reason that may go away (an unreachable bucket).
const promoteRetryDelay = 5 * time.Second

type promotion struct {
	lift     *liftState
	rec      state.Pallet
	manifest pallet.Manifest
	// invalidated: InvalidatePallet marked it while its promote ran; it is
	// recorded as invalid with this reason once promoted.
	invalidated string
}

func datasetKey(tenant, name string) string { return tenant + "\x00" + name }

// checkLiftIO returns the lift's input pallets, or why it can't run.
func (c *Core) checkLiftIO(l *liftState) (inputs []state.Pallet, reason, message string) {
	tenant := l.haul.rec.Tenant
	if o := l.output; o != nil {
		if c.cfg.Pallets == nil {
			return nil, "no_pallet_store", "this server has no bucket for pallets"
		}
		if key, ok := c.datasets[datasetKey(tenant, o.Dataset)]; ok && key != o.PartitionKey {
			return nil, "bad_output", fmt.Sprintf("dataset %s is partitioned by %q, the lift says %q", o.Dataset, key, o.PartitionKey)
		}
	}
	for _, in := range l.inputs {
		p, err := c.cfg.Store.GetPallet(c.ctx, in.Pallet)
		switch {
		case errors.Is(err, state.ErrNotFound):
			return nil, "bad_input", fmt.Sprintf("input %s: pallet %s does not exist", in.Name, in.Pallet)
		case err != nil:
			return nil, "bad_input", fmt.Sprintf("input %s: %v", in.Name, err)
		case p.Tenant != tenant:
			return nil, "bad_input", fmt.Sprintf("input %s: pallet %s belongs to another tenant", in.Name, in.Pallet)
		case p.State != state.PalletCommitted:
			return nil, "bad_input", fmt.Sprintf("input %s: pallet %s is %s", in.Name, in.Pallet, p.State)
		}
		inputs = append(inputs, p)
	}
	return inputs, "", ""
}

// completeLiftWithOutput completes a succeeded attempt that produces a
// pallet in o: the lift's output, or for a lift without one that reported
// files, its implicit dataset.
func (c *Core) completeLiftWithOutput(a *liftAttemptState, res LiftResult, o OutputSpec) error {
	l, h := a.lift, a.lift.haul
	if c.cfg.Pallets == nil {
		msg := "this server has no bucket for pallets"
		c.finishLiftAttempt(a, state.AttemptFailed, "no_pallet_store", msg)
		c.liftAttemptFailed(l, "no_pallet_store", msg, true)
		return nil
	}
	if res.Output == nil {
		msg := "the lift succeeded but reported no output"
		c.finishLiftAttempt(a, state.AttemptFailed, "no_output", msg)
		c.liftAttemptFailed(l, "no_output", msg, true)
		return nil
	}
	staging := c.staging(a)
	id := c.cfg.NewID()
	m, err := newManifest(id, h.rec.Tenant, o, staging, res)
	if err != nil {
		c.finishLiftAttempt(a, state.AttemptFailed, "bad_output", err.Error())
		c.liftAttemptFailed(l, "bad_output", err.Error(), true)
		return nil
	}
	return c.recordPallet(a, res, o, id, m)
}

// newManifest describes what the attempt wrote and returned.
func newManifest(id, tenant string, o OutputSpec, staging string, res LiftResult) (pallet.Manifest, error) {
	result, err := canonicalJSON(res.Result)
	if err != nil {
		return pallet.Manifest{}, fmt.Errorf("the result is not JSON: %v", err)
	}
	return pallet.New(id, tenant, o.Dataset, o.Partition, staging, res.Output.Files, result)
}

// recordPallet records a succeeded attempt's pallet as pending and starts
// its promote.
func (c *Core) recordPallet(a *liftAttemptState, res LiftResult, o OutputSpec, id string, m pallet.Manifest) error {
	l, h := a.lift, a.lift.haul
	c.finishLiftAttempt(a, state.AttemptSucceeded, res.Reason, res.Message)
	l.rec.Status, l.rec.Result = state.LiftCommitting, res.Result
	c.dirtyLift(l)

	c.ensureDataset(h.rec.Tenant, o)
	rec := state.Pallet{
		ID: id, Tenant: h.rec.Tenant, Dataset: o.Dataset, Partition: o.Partition,
		Location:    c.cfg.Layout.PalletManifest(h.rec.Tenant, o.Dataset, o.Partition, id),
		ContentHash: m.ContentHash, Fingerprint: l.rec.Fingerprint, Stats: res.Output.Stats, Meta: res.Output.Meta,
		HaulID: h.rec.ID, LiftSeq: l.rec.Seq, State: state.PalletPending, CreatedAt: c.now(),
	}
	c.putPallet(rec)
	for _, in := range l.inputs {
		edge := state.PalletInput{PalletID: id, InputID: in.Pallet, Name: in.Name}
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutPalletInput(edge) })
	}
	c.emitH(state.EventPalletRecorded, h, "", palletRecordedPayload{Seq: l.rec.Seq, Manifest: m, Location: rec.Location})
	c.promote(&promotion{lift: l, rec: rec, manifest: m})
	return nil
}

// outputSpec is where the lift's pallet goes: its output, or for a lift
// without one, its implicit dataset (if it builds a pallet at all).
func (l *liftState) outputSpec() OutputSpec {
	if l.output != nil {
		return *l.output
	}
	return OutputSpec{Dataset: ImplicitDataset(l.rec.Name)}
}

// staging is where attempt a writes: what it was handed, recorded with it.
func (c *Core) staging(a *liftAttemptState) string {
	if a.rec.Staging != "" {
		return a.rec.Staging
	}
	l := a.lift // dispatched before staging was recorded
	return c.cfg.Layout.LegacyLiftStaging(l.haul.rec.Tenant, l.rec.HaulID, l.rec.Seq, a.rec.ID)
}

func (c *Core) ensureDataset(tenant string, o OutputSpec) {
	key := datasetKey(tenant, o.Dataset)
	if _, ok := c.datasets[key]; ok {
		return
	}
	c.datasets[key] = o.PartitionKey
	d := state.Dataset{Tenant: tenant, Name: o.Dataset, PartitionKey: o.PartitionKey, CreatedAt: c.now()}
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutDataset(d) })
	c.emit(state.EventDatasetCreated, tenant, datasetCreatedPayload{Name: o.Dataset, PartitionKey: o.PartitionKey})
}

func (c *Core) putPallet(rec state.Pallet) {
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutPallet(rec) })
}

// promote writes the pallet's manifest off the loop, once the batch that
// recorded the pallet has committed, and feeds the outcome back.
func (c *Core) promote(p *promotion) {
	c.promotions[p.rec.ID] = p
	ctx, store, m := c.ctx, c.cfg.Pallets, p.manifest
	c.background(func() {
		_, err := store.Promote(ctx, m)
		c.inbox.put(funcInput(func(c *Core) { c.promoted(m.Pallet, err) }))
	})
}

func (c *Core) promoted(id string, err error) {
	p := c.promotions[id]
	if p == nil {
		return
	}
	delete(c.promotions, id)
	l, h := p.lift, p.lift.haul
	payload := palletPayload{Pallet: id, Seq: l.rec.Seq}
	switch {
	case err == nil:
		p.rec.State, p.rec.CommittedAt = state.PalletCommitted, ptr(c.now())
		if p.invalidated != "" {
			p.rec.State, p.rec.Reason, p.rec.RetiredAt = state.PalletInvalid, p.invalidated, p.rec.CommittedAt
		}
		c.putPallet(p.rec)
		c.emitH(state.EventPalletCommitted, h, "", payload)
		if p.invalidated == "" {
			c.palletCommitted(p.rec, h.rec.Name)
		}
		if l.rec.Status == state.LiftCommitting { // not cancelled meanwhile
			l.pallet = id
			c.finishLift(l, state.LiftCompleted, l.rec.Result, "", "")
			c.wake(h)
		}
	case errors.Is(err, pallet.ErrMissingFiles) || errors.Is(err, pallet.ErrConflict):
		p.rec.State, p.rec.Reason, p.rec.RetiredAt = state.PalletInvalid, "promote_failed", ptr(c.now())
		c.putPallet(p.rec)
		payload.Reason, payload.Message = "promote_failed", err.Error()
		c.emitH(state.EventPalletInvalidated, h, "", payload)
		c.discardStaging(p.manifest.Data)
		if l.rec.Status == state.LiftCommitting {
			c.liftAttemptFailed(l, "promote_failed", err.Error(), false)
		}
	default:
		if c.ctx.Err() != nil {
			return // shutting down; the next leader promotes it
		}
		c.log.Warn("promote failed, retrying", "pallet", id, "err", err)
		c.after(promoteRetryDelay, func(c *Core) { c.promote(p) })
	}
}

// discardStaging removes an attempt's staging prefix, off the loop.
func (c *Core) discardStaging(prefix string) {
	if c.cfg.Pallets == nil {
		return
	}
	ctx, store := c.ctx, c.cfg.Pallets
	c.background(func() {
		if err := store.Discard(ctx, prefix); err != nil {
			c.log.Warn("discarding staging failed", "prefix", prefix, "err", err)
		}
	})
}

// background runs f on its own goroutine once the current batch commits.
// Run waits for it before returning.
func (c *Core) background(f func()) {
	c.effect(func() {
		c.bg.Add(1)
		go func() {
			defer c.bg.Done()
			f()
		}()
	})
}
