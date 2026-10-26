package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withBucket gives the core a file:// bucket for pallets. wrap, if set,
// sits between the core and the real pallet store.
func withBucket(b blob.Bucket, wrap func(PalletStore) PalletStore) option {
	return func(c *Config) {
		c.Layout = layout.New(b.Base())
		var ps PalletStore = pallet.NewStore(b, c.Layout)
		if wrap != nil {
			ps = wrap(ps)
		}
		c.Pallets = ps
	}
}

func newBucket(t *testing.T) blob.Bucket {
	t.Helper()
	b, err := blob.Open(blob.FileURI(filepath.Join(t.TempDir(), "bucket")))
	require.NoError(t, err)
	return b
}

// writeOutput writes files into an attempt's staging prefix, as a worker
// would, and describes them.
func writeOutput(t *testing.T, b blob.Bucket, task Task, files map[string]string) *LiftOutput {
	t.Helper()
	require.NotEmpty(t, task.Staging, "the lift has no output")
	out := &LiftOutput{Stats: map[string]any{"rows": len(files)}, Meta: map[string]any{"max_ts": "2026-10-24T23:00:00Z"}}
	for path, content := range files {
		require.NoError(t, b.Put(context.Background(), task.Staging+path, []byte(content)))
		sum := sha256.Sum256([]byte(content))
		out.Files = append(out.Files, pallet.File{Path: path, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])})
	}
	return out
}

func output(name, dataset, partition string) Command {
	cmd := lift(name)
	cmd.Output = &OutputSpec{Dataset: dataset, Partition: partition}
	if partition != "" {
		cmd.Output.PartitionKey = "day"
	}
	return cmd
}

func (h *harness) waitLift(haulID, status string) state.Lift {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.lift(haulID).Status == status }, 5*time.Second, time.Millisecond,
		"lift never reached %s", status)
	return h.lift(haulID)
}

func (h *harness) pallets(haulID string) []state.Pallet {
	h.t.Helper()
	ps, err := h.store.ListPallets(h.ctx, state.PalletFilter{HaulID: haulID})
	require.NoError(h.t, err)
	return ps
}

func TestLiftOutputIsPromotedAndCommitted(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id, a := h.startLift(output("extract", "raw.orders", "2026-10-24"))
	assert.Equal(t, &OutputSpec{Dataset: "raw.orders", Partition: "2026-10-24", PartitionKey: "day"}, a.Output)
	assert.Equal(t, b.Base()+"/lifts/t/"+id+"/1/"+a.ID+"/", a.Staging)

	out := writeOutput(t, b, a, map[string]string{"part-0.csv": "1,2\n", "part-1.csv": "3,4\n"})
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":2}`), Output: out}))

	l := h.waitLift(id, state.LiftCompleted)
	assert.JSONEq(t, `{"rows":2}`, string(l.Result))
	ps := h.pallets(id)
	require.Len(t, ps, 1)
	p := ps[0]
	assert.Equal(t, state.PalletCommitted, p.State)
	assert.NotNil(t, p.CommittedAt)
	assert.Equal(t, 1, p.LiftSeq)
	assert.Equal(t, "2026-10-24T23:00:00Z", p.Meta["max_ts"])

	m, err := pallet.NewStore(b, layout.New(b.Base())).Read(h.ctx, p.Location)
	require.NoError(t, err)
	assert.Equal(t, a.Staging, m.Data, "the files stay where the attempt wrote them")
	assert.Equal(t, p.ContentHash, m.ContentHash)
	assert.Equal(t, []string{"part-0.csv", "part-1.csv"}, []string{m.Files[0].Path, m.Files[1].Path})

	latest, err := h.store.LatestPallet(h.ctx, "t", "raw.orders", "2026-10-24")
	require.NoError(t, err)
	assert.Equal(t, p.ID, latest.ID)
	ds, err := h.store.GetDataset(h.ctx, "t", "raw.orders")
	require.NoError(t, err)
	assert.Equal(t, "day", ds.PartitionKey)

	// The haul's replay learns the pallet from the lift's result.
	evs, err := h.store.HaulEvents(h.ctx, id)
	require.NoError(t, err)
	var finished liftFinishedPayload
	var types []state.EventType
	for _, e := range evs {
		types = append(types, e.Type)
		if e.Type == state.EventLiftFinished {
			require.NoError(t, json.Unmarshal(e.Payload, &finished))
		}
	}
	assert.Equal(t, p.ID, finished.Pallet)
	assert.Subset(t, types, []state.EventType{state.EventPalletRecorded, state.EventPalletCommitted})
	assert.Equal(t, TaskHaul, h.poll().Kind)
}

func TestLiftInputsAreRecordedAsLineage(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id := h.startHaul("x")
	h.replay(seqs(output("extract", "raw", "")), nil)
	a := h.poll(TaskLift)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	h.waitLift(id, state.LiftCompleted)
	raw := h.pallets(id)[0].ID

	build := output("build", "mart", "")
	build.Inputs = []InputRef{{Name: "orders", Pallet: raw}}
	h.replay(seqs(output("extract", "raw", ""), build), nil)
	a2 := h.poll(TaskLift)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a2.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a2, map[string]string{"m.csv": "y"})}))
	require.Eventually(t, func() bool { return len(h.pallets(id)) == 2 && h.pallets(id)[0].State == state.PalletCommitted },
		5*time.Second, time.Millisecond)

	mart := h.pallets(id)[0]
	up, err := h.store.Upstream(h.ctx, mart.ID)
	require.NoError(t, err)
	assert.Equal(t, []state.PalletInput{{PalletID: mart.ID, InputID: raw, Name: "orders"}}, up.Edges)
}

// gate blocks promotes until released, or until the leader stops. With
// writeFirst, the promote has already written the manifest when it blocks:
// only its report gets lost.
type gate struct {
	PalletStore
	release    chan struct{}
	writeFirst bool
	started    chan string
}

func (g *gate) Promote(ctx context.Context, m pallet.Manifest) (string, error) {
	var uri string
	if g.writeFirst {
		var err error
		if uri, err = g.PalletStore.Promote(ctx, m); err != nil {
			return "", err
		}
	}
	g.started <- m.Pallet
	select {
	case <-g.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if g.writeFirst {
		return uri, nil
	}
	return g.PalletStore.Promote(ctx, m)
}

func TestCrashBetweenCompletionAndPromote(t *testing.T) {
	for _, writeFirst := range []bool{false, true} {
		name := "promote never ran"
		if writeFirst {
			name = "promote ran, its report was lost"
		}
		t.Run(name, func(t *testing.T) {
			b := newBucket(t)
			g := &gate{release: make(chan struct{}), writeFirst: writeFirst, started: make(chan string, 1)}
			h := newHarness(t, withBucket(b, func(ps PalletStore) PalletStore { g.PalletStore = ps; return g }))
			id, a := h.startLift(output("extract", "raw", ""))
			require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
				Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
			<-g.started

			// The leader dies while the promote is in flight.
			require.NoError(t, h.stop())
			assert.Equal(t, state.LiftCommitting, h.lift(id).Status)
			assert.Equal(t, state.PalletPending, h.pallets(id)[0].State, "not visible before the promote")
			_, err := h.store.LatestPallet(h.ctx, "t", "raw", "")
			assert.ErrorIs(t, err, state.ErrNotFound)

			h.restart(withBucket(b, nil))
			h.waitLift(id, state.LiftCompleted)
			ps := h.pallets(id)
			require.Len(t, ps, 1, "the same pallet, promoted once")
			assert.Equal(t, state.PalletCommitted, ps[0].State)
			l := h.lift(id)
			assert.Equal(t, 1, l.Attempts, "the lift didn't run again")
		})
	}
}

func TestLateResultOfTimedOutAttemptIsDiscarded(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	cmd := output("extract", "raw", "")
	cmd.Timeout = time.Minute
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Second}
	id, a1 := h.startLift(cmd)
	out1 := writeOutput(t, b, a1, map[string]string{"a.csv": "slow"})

	h.advance(time.Minute) // a1 times out; its staging is discarded
	require.Eventually(t, func() bool {
		left, err := b.List(h.ctx, a1.Staging)
		return err == nil && len(left) == 0
	}, 5*time.Second, time.Millisecond)
	assert.ErrorIs(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a1.ID, Status: state.AttemptSucceeded, Output: out1}), ErrNotFound)

	h.advance(time.Second)
	a2 := h.poll(TaskLift)
	assert.NotEqual(t, a1.Staging, a2.Staging, "every attempt writes to its own prefix")
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a2.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a2, map[string]string{"a.csv": "fast"})}))
	h.waitLift(id, state.LiftCompleted)

	ps := h.pallets(id)
	require.Len(t, ps, 1)
	m, err := pallet.NewStore(b, layout.New(b.Base())).Read(h.ctx, ps[0].Location)
	require.NoError(t, err)
	assert.Equal(t, a2.Staging, m.Data, "the winning attempt's files")
}

func TestPromoteWithMissingFilesFailsTheAttempt(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	cmd := output("extract", "raw", "")
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Second}
	id, a1 := h.startLift(cmd)
	out := writeOutput(t, b, a1, map[string]string{"a.csv": "x"})
	out.Files = append(out.Files, pallet.File{Path: "never-written.csv", Size: 1, SHA256: out.Files[0].SHA256})
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a1.ID, Status: state.AttemptSucceeded, Output: out}))

	l := h.waitLift(id, state.LiftRetryWait)
	assert.Equal(t, "promote_failed", l.Reason)
	ps := h.pallets(id)
	assert.Equal(t, state.PalletInvalid, ps[0].State)
	assert.Equal(t, "promote_failed", ps[0].Reason)

	h.advance(time.Second)
	a2 := h.poll(TaskLift)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a2.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a2, map[string]string{"a.csv": "x"})}))
	h.waitLift(id, state.LiftCompleted)
}

func TestFailedAttemptStagingIsDiscarded(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	_, a := h.startLift(output("extract", "raw", ""))
	writeOutput(t, b, a, map[string]string{"half.csv": "x"})
	h.failLift(a, "error")
	require.Eventually(t, func() bool {
		left, err := b.List(h.ctx, a.Staging)
		return err == nil && len(left) == 0
	}, 5*time.Second, time.Millisecond)
}

func TestLiftSucceedingWithoutOutputFails(t *testing.T) {
	h := newHarness(t, withBucket(newBucket(t), nil))
	cmd := output("extract", "raw", "")
	cmd.Retry = &RetryPolicy{Limit: 3, Initial: time.Second}
	id, a := h.startLift(cmd)
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded}))
	l := h.lift(id)
	assert.Equal(t, state.LiftFailed, l.Status)
	assert.Equal(t, "no_output", l.Reason, "no retry either")
}

func TestOutputForLiftWithoutOneIsRejected(t *testing.T) {
	h := newHarness(t, withBucket(newBucket(t), nil))
	_, plain := h.startLift(lift("plain"))
	assert.ErrorIs(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: plain.ID, Status: state.AttemptSucceeded,
		Output: &LiftOutput{}}), ErrInvalid)
}

func TestLiftInputsMustBeCommittedPallets(t *testing.T) {
	h := newHarness(t, withBucket(newBucket(t), nil))
	bad := lift("build")
	bad.Inputs = []InputRef{{Name: "orders", Pallet: "nope"}}
	id := h.startHaul("x")
	h.replay(seqs(bad), nil)
	l := h.lift(id)
	assert.Equal(t, state.LiftFailed, l.Status)
	assert.Equal(t, "bad_input", l.Reason)
	assert.Equal(t, "input orders: pallet nope does not exist", l.Message)
	assert.Equal(t, TaskHaul, h.poll().Kind, "the haul sees the failure")

	assert.Error(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(Command{Kind: CommandLift, Name: "x",
		Output: &OutputSpec{Dataset: "d", Partition: "p"}})}), "partition without a key")
	assert.Error(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(Command{Kind: CommandLift, Name: "x",
		Output: &OutputSpec{}})}), "no dataset")
}

func TestDatasetPartitionKeyMustMatch(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id, a := h.startLift(output("extract", "raw", "2026-10-24"))
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	h.waitLift(id, state.LiftCompleted)
	h.replay(seqs(output("extract", "raw", "2026-10-24")), &HaulOutcome{Status: state.HaulCompleted})

	// The dataset is known after a restart too.
	h.restart(withBucket(b, nil))
	id2 := h.startHaul("y")
	h.replay(seqs(output("other", "raw", "")), nil)
	l := h.lift(id2)
	assert.Equal(t, "bad_output", l.Reason)
	assert.Equal(t, `dataset raw is partitioned by "day", the lift says ""`, l.Message)
}

func TestLiftOutputNeedsAPalletStore(t *testing.T) {
	h := newHarness(t)
	id := h.startHaul("x")
	h.replay(seqs(output("extract", "raw", "")), nil)
	assert.Equal(t, "no_pallet_store", h.lift(id).Reason)
}
