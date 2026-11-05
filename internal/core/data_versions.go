package core

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/state"
)

// Steps with an output produce a data version, and so does a step without one
// that reports files (the SDK writes a returned dataframe as Parquet): its
// data version goes to the step's implicit dataset, ImplicitDataset(name),
// unpartitioned. The worker writes files to the
// attempt's staging prefix and reports them; the core records the data version as
// pending (with its manifest in the event log), then promotes it off the loop
// and, once the promote succeeded, commits it: only then is the step
// completed and the run woken. A promote that fails for good invalidates
// the data version and fails the attempt, which the step's retry policy may
// retry. Staging of attempts that will never be promoted is discarded.

// OutputSpec is where a step's data version goes.
type OutputSpec struct {
	Dataset      string `json:"dataset"`
	Partition    string `json:"partition,omitempty"`
	PartitionKey string `json:"partition_key,omitempty"` // e.g. "day"; set when the dataset is created
}

// ImplicitDatasetPrefix starts the names of implicit datasets; outputs a
// step declares can't use it.
const ImplicitDatasetPrefix = "step:"

// ImplicitDataset is where the data versions of a step without an output go.
func ImplicitDataset(step string) string { return ImplicitDatasetPrefix + step }

func (o OutputSpec) validate() error {
	switch {
	case o.Dataset == "":
		return errors.New("output without a dataset")
	case strings.HasPrefix(o.Dataset, ImplicitDatasetPrefix):
		return fmt.Errorf("output dataset %q: names starting with %q are for steps without an output", o.Dataset, ImplicitDatasetPrefix)
	case o.PartitionKey == "" && o.Partition != "":
		return fmt.Errorf("output partition %q for a dataset without a partition key", o.Partition)
	case o.PartitionKey != "" && o.Partition == "":
		return fmt.Errorf("output to a dataset partitioned by %q needs a partition", o.PartitionKey)
	}
	return nil
}

// InputRef is a data version passed to a step.
type InputRef struct {
	Name        string `json:"name,omitempty"` // the step argument
	DataVersion string `json:"data_version"`
}

// StepOutput is what a succeeded attempt wrote.
type StepOutput struct {
	Files []dataversion.File
	Stats map[string]any
	Meta  map[string]any
}

// DataVersionStore promotes and discards data versions in the bucket; *dataversion.Store
// implements it. Its methods are called off the core goroutine.
type DataVersionStore interface {
	Promote(ctx context.Context, m dataversion.Manifest) (string, error)
	Discard(ctx context.Context, prefix string) error
	Read(ctx context.Context, uri string) (dataversion.Manifest, error)
	// Check reports whether the data version whose manifest is at uri can still
	// be read: its manifest and files are there.
	Check(ctx context.Context, uri string) error
}

// promoteRetryDelay is how long to wait before retrying a promote that
// failed for a reason that may go away (an unreachable bucket).
const promoteRetryDelay = 5 * time.Second

type promotion struct {
	step     *stepState
	rec      state.DataVersion
	manifest dataversion.Manifest
	// invalidated: InvalidateDataVersion marked it while its promote ran; it is
	// recorded as invalid with this reason once promoted.
	invalidated string
}

func datasetKey(tenant, name string) string { return tenant + "\x00" + name }

// checkStepIO returns the step's input data versions, or why it can't run.
func (c *Core) checkStepIO(l *stepState) (inputs []state.DataVersion, reason, message string) {
	tenant := l.run.rec.Tenant
	if o := l.output; o != nil {
		if c.cfg.DataVersions == nil {
			return nil, "no_data_version_store", "this server has no bucket for data versions"
		}
		if key, ok := c.datasets[datasetKey(tenant, o.Dataset)]; ok && key != o.PartitionKey {
			return nil, "bad_output", fmt.Sprintf("dataset %s is partitioned by %q, the step says %q", o.Dataset, key, o.PartitionKey)
		}
	}
	for _, in := range l.inputs {
		p, err := c.cfg.Store.GetDataVersion(c.ctx, in.DataVersion)
		switch {
		case errors.Is(err, state.ErrNotFound):
			return nil, "bad_input", fmt.Sprintf("input %s: data version %s does not exist", in.Name, in.DataVersion)
		case err != nil:
			return nil, "bad_input", fmt.Sprintf("input %s: %v", in.Name, err)
		case p.Tenant != tenant:
			return nil, "bad_input", fmt.Sprintf("input %s: data version %s belongs to another tenant", in.Name, in.DataVersion)
		case p.State != state.DataVersionCommitted:
			return nil, "bad_input", fmt.Sprintf("input %s: data version %s is %s", in.Name, in.DataVersion, p.State)
		}
		inputs = append(inputs, p)
	}
	return inputs, "", ""
}

// completeStepWithOutput completes a succeeded attempt that produces a
// data version in o: the step's output, or for a step without one that reported
// files, its implicit dataset.
func (c *Core) completeStepWithOutput(a *stepAttemptState, res StepResult, o OutputSpec) error {
	l, h := a.step, a.step.run
	if c.cfg.DataVersions == nil {
		msg := "this server has no bucket for data versions"
		c.finishStepAttempt(a, state.AttemptFailed, "no_data_version_store", msg)
		c.stepAttemptFailed(l, "no_data_version_store", msg, true)
		return nil
	}
	if res.Output == nil {
		msg := "the step succeeded but reported no output"
		c.finishStepAttempt(a, state.AttemptFailed, "no_output", msg)
		c.stepAttemptFailed(l, "no_output", msg, true)
		return nil
	}
	staging := c.staging(a)
	id := c.cfg.NewID()
	m, err := newManifest(id, h.rec.Tenant, o, staging, res)
	if err != nil {
		c.finishStepAttempt(a, state.AttemptFailed, "bad_output", err.Error())
		c.stepAttemptFailed(l, "bad_output", err.Error(), true)
		return nil
	}
	return c.recordDataVersion(a, res, o, id, m)
}

// newManifest describes what the attempt wrote and returned.
func newManifest(id, tenant string, o OutputSpec, staging string, res StepResult) (dataversion.Manifest, error) {
	result, err := canonicalJSON(res.Result)
	if err != nil {
		return dataversion.Manifest{}, fmt.Errorf("the result is not JSON: %v", err)
	}
	return dataversion.New(id, tenant, o.Dataset, o.Partition, staging, res.Output.Files, result)
}

// recordDataVersion records a succeeded attempt's data version as pending and starts
// its promote.
func (c *Core) recordDataVersion(a *stepAttemptState, res StepResult, o OutputSpec, id string, m dataversion.Manifest) error {
	l, h := a.step, a.step.run
	c.finishStepAttempt(a, state.AttemptSucceeded, res.Reason, res.Message)
	l.rec.Status, l.rec.Result = state.StepCommitting, res.Result
	c.dirtyStep(l)

	c.ensureDataset(h.rec.Tenant, o)
	rec := state.DataVersion{
		ID: id, Tenant: h.rec.Tenant, Dataset: o.Dataset, Partition: o.Partition,
		Location:    c.cfg.Layout.DataVersionManifest(h.rec.Tenant, o.Dataset, o.Partition, id),
		ContentHash: m.ContentHash, Fingerprint: l.rec.Fingerprint, Stats: res.Output.Stats, Meta: res.Output.Meta,
		RunID: h.rec.ID, StepSeq: l.rec.Seq, State: state.DataVersionPending, CreatedAt: c.now(),
	}
	c.putDataVersion(rec)
	for _, in := range l.inputs {
		edge := state.DataVersionInput{DataVersionID: id, InputID: in.DataVersion, Name: in.Name}
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutDataVersionInput(edge) })
	}
	c.emitH(state.EventDataVersionRecorded, h, "", dataVersionRecordedPayload{Seq: l.rec.Seq, Manifest: m, Location: rec.Location})
	c.promote(&promotion{step: l, rec: rec, manifest: m})
	return nil
}

// outputSpec is where the step's data version goes: its output, or for a step
// without one, its implicit dataset (if it builds a data version at all).
func (l *stepState) outputSpec() OutputSpec {
	if l.output != nil {
		return *l.output
	}
	return OutputSpec{Dataset: ImplicitDataset(l.rec.Name)}
}

// staging is where attempt a writes: what it was handed, recorded with it.
func (c *Core) staging(a *stepAttemptState) string {
	if a.rec.Staging != "" {
		return a.rec.Staging
	}
	l := a.step // dispatched before staging was recorded
	return c.cfg.Layout.LegacyStepStaging(l.run.rec.Tenant, l.rec.RunID, l.rec.Seq, a.rec.ID)
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

func (c *Core) putDataVersion(rec state.DataVersion) {
	c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutDataVersion(rec) })
}

// promote writes the data version's manifest off the loop, once the batch that
// recorded the data version has committed, and feeds the outcome back.
func (c *Core) promote(p *promotion) {
	c.promotions[p.rec.ID] = p
	ctx, store, m := c.ctx, c.cfg.DataVersions, p.manifest
	c.background(func() {
		_, err := store.Promote(ctx, m)
		c.inbox.put(funcInput(func(c *Core) { c.promoted(m.DataVersion, err) }))
	})
}

func (c *Core) promoted(id string, err error) {
	p := c.promotions[id]
	if p == nil {
		return
	}
	delete(c.promotions, id)
	l, h := p.step, p.step.run
	payload := dataVersionPayload{DataVersion: id, Seq: l.rec.Seq}
	switch {
	case err == nil:
		p.rec.State, p.rec.CommittedAt = state.DataVersionCommitted, ptr(c.now())
		if p.invalidated != "" {
			p.rec.State, p.rec.Reason, p.rec.RetiredAt = state.DataVersionInvalid, p.invalidated, p.rec.CommittedAt
		}
		c.putDataVersion(p.rec)
		c.emitH(state.EventDataVersionCommitted, h, "", payload)
		if p.invalidated == "" {
			c.dataVersionCommitted(p.rec, h.rec.Name)
		}
		if l.rec.Status == state.StepCommitting { // not cancelled meanwhile
			l.dataVersion = id
			c.finishStep(l, state.StepCompleted, l.rec.Result, "", "")
			c.wake(h)
		}
	case errors.Is(err, dataversion.ErrMissingFiles) || errors.Is(err, dataversion.ErrConflict):
		p.rec.State, p.rec.Reason, p.rec.RetiredAt = state.DataVersionInvalid, "promote_failed", ptr(c.now())
		c.putDataVersion(p.rec)
		payload.Reason, payload.Message = "promote_failed", err.Error()
		c.emitH(state.EventDataVersionInvalidated, h, "", payload)
		c.discardStaging(p.manifest.Data)
		if l.rec.Status == state.StepCommitting {
			c.stepAttemptFailed(l, "promote_failed", err.Error(), false)
		}
	default:
		if c.ctx.Err() != nil {
			return // shutting down; the next leader promotes it
		}
		c.log.Warn("promote failed, retrying", "data_version", id, "err", err)
		c.after(promoteRetryDelay, func(c *Core) { c.promote(p) })
	}
}

// discardStaging removes an attempt's staging prefix, off the loop.
func (c *Core) discardStaging(prefix string) {
	if c.cfg.DataVersions == nil {
		return
	}
	ctx, store := c.ctx, c.cfg.DataVersions
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

// AttemptIO is what a running step attempt may write and read in the
// bucket: its staging prefix and its input data versions.
type AttemptIO struct {
	Staging string
	Inputs  []TaskInput
}

// AttemptIO returns what attempt id may write and read, while it runs; an
// attempt that isn't running gets ErrNotFound. The API hands workers URLs
// for these and nothing else.
func (c *Core) AttemptIO(ctx context.Context, id string) (AttemptIO, error) {
	return submit(ctx, c, func(c *Core) (AttemptIO, error) {
		a := c.stepAttempts[id]
		if a == nil {
			return AttemptIO{}, fmt.Errorf("%w: step attempt %s is not running", ErrNotFound, id)
		}
		io := AttemptIO{Staging: c.staging(a)}
		for _, in := range a.step.inputs {
			ti := TaskInput{Name: in.Name, DataVersion: in.DataVersion}
			if p, err := c.cfg.Store.GetDataVersion(c.ctx, in.DataVersion); err == nil {
				ti.Location = p.Location
			}
			io.Inputs = append(io.Inputs, ti)
		}
		return io, nil
	})
}
