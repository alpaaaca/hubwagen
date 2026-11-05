package core

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/alper/moksha/internal/blob"
	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/layout"
	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withBucket gives the core a file:// bucket for data versions. wrap, if set,
// sits between the core and the real data version store.
func withBucket(b blob.Bucket, wrap func(DataVersionStore) DataVersionStore) option {
	return func(c *Config) {
		c.Layout = layout.New(b.Base())
		var ps DataVersionStore = dataversion.NewStore(b, c.Layout)
		if wrap != nil {
			ps = wrap(ps)
		}
		c.DataVersions = ps
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
func writeOutput(t *testing.T, b blob.Bucket, task Task, files map[string]string) *StepOutput {
	t.Helper()
	require.NotEmpty(t, task.Staging, "the step has no output")
	out := &StepOutput{Stats: map[string]any{"rows": len(files)}, Meta: map[string]any{"max_ts": "2026-10-24T23:00:00Z"}}
	for path, content := range files {
		require.NoError(t, b.Put(context.Background(), task.Staging+path, []byte(content)))
		sum := sha256.Sum256([]byte(content))
		out.Files = append(out.Files, dataversion.File{Path: path, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])})
	}
	return out
}

func output(name, dataset, partition string) Command {
	cmd := stepCmd(name)
	cmd.Output = &OutputSpec{Dataset: dataset, Partition: partition}
	if partition != "" {
		cmd.Output.PartitionKey = "day"
	}
	return cmd
}

func (h *harness) waitStep(runID, status string) state.Step {
	h.t.Helper()
	require.Eventually(h.t, func() bool { return h.step(runID).Status == status }, 5*time.Second, time.Millisecond,
		"step never reached %s", status)
	return h.step(runID)
}

func (h *harness) dataVersions(runID string) []state.DataVersion {
	h.t.Helper()
	ps, err := h.store.ListDataVersions(h.ctx, state.DataVersionFilter{RunID: runID})
	require.NoError(h.t, err)
	return ps
}

func TestStepOutputIsPromotedAndCommitted(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id, a := h.startStep(output("extract", "raw.orders", "2026-10-24"))
	assert.Equal(t, &OutputSpec{Dataset: "raw.orders", Partition: "2026-10-24", PartitionKey: "day"}, a.Output)
	assert.Equal(t, b.Base()+"/data/t/raw.orders/2026-10-24/"+a.ID+"/", a.Staging, "under the dataset and partition")
	att, err := h.store.ListStepAttempts(h.ctx, id, 1)
	require.NoError(t, err)
	assert.Equal(t, a.Staging, att[0].Staging, "recorded with the attempt")

	out := writeOutput(t, b, a, map[string]string{"part-0.csv": "1,2\n", "part-1.csv": "3,4\n"})
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"rows":2}`), Output: out}))

	l := h.waitStep(id, state.StepCompleted)
	assert.JSONEq(t, `{"rows":2}`, string(l.Result))
	ps := h.dataVersions(id)
	require.Len(t, ps, 1)
	p := ps[0]
	assert.Equal(t, state.DataVersionCommitted, p.State)
	assert.NotNil(t, p.CommittedAt)
	assert.Equal(t, 1, p.StepSeq)
	assert.Equal(t, "2026-10-24T23:00:00Z", p.Meta["max_ts"])

	m, err := dataversion.NewStore(b, layout.New(b.Base())).Read(h.ctx, p.Location)
	require.NoError(t, err)
	assert.Equal(t, a.Staging, m.Data, "the files stay where the attempt wrote them")
	assert.Equal(t, p.ContentHash, m.ContentHash)
	assert.Equal(t, []string{"part-0.csv", "part-1.csv"}, []string{m.Files[0].Path, m.Files[1].Path})

	latest, err := h.store.LatestDataVersion(h.ctx, "t", "raw.orders", "2026-10-24")
	require.NoError(t, err)
	assert.Equal(t, p.ID, latest.ID)
	ds, err := h.store.GetDataset(h.ctx, "t", "raw.orders")
	require.NoError(t, err)
	assert.Equal(t, "day", ds.PartitionKey)

	// The run's replay learns the data version from the step's result.
	evs, err := h.store.RunEvents(h.ctx, id)
	require.NoError(t, err)
	var finished stepFinishedPayload
	var types []state.EventType
	for _, e := range evs {
		types = append(types, e.Type)
		if e.Type == state.EventStepFinished {
			require.NoError(t, json.Unmarshal(e.Payload, &finished))
		}
	}
	assert.Equal(t, p.ID, finished.DataVersion)
	assert.Subset(t, types, []state.EventType{state.EventDataVersionRecorded, state.EventDataVersionCommitted})
	assert.Equal(t, TaskRun, h.poll().Kind)
}

func TestStepInputsAreRecordedAsLineage(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id := h.startRun("x")
	h.replay(seqs(output("extract", "raw", "")), nil)
	a := h.poll(TaskStep)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	h.waitStep(id, state.StepCompleted)
	raw := h.dataVersions(id)[0].ID

	build := output("build", "mart", "")
	build.Inputs = []InputRef{{Name: "orders", DataVersion: raw}}
	h.replay(seqs(output("extract", "raw", ""), build), nil)
	a2 := h.poll(TaskStep)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a2.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a2, map[string]string{"m.csv": "y"})}))
	require.Eventually(t, func() bool {
		return len(h.dataVersions(id)) == 2 && h.dataVersions(id)[0].State == state.DataVersionCommitted
	},
		5*time.Second, time.Millisecond)

	mart := h.dataVersions(id)[0]
	up, err := h.store.Upstream(h.ctx, mart.ID)
	require.NoError(t, err)
	assert.Equal(t, []state.DataVersionInput{{DataVersionID: mart.ID, InputID: raw, Name: "orders"}}, up.Edges)
}

// gate blocks promotes until released, or until the leader stops. With
// writeFirst, the promote has already written the manifest when it blocks:
// only its report gets lost.
type gate struct {
	DataVersionStore
	release    chan struct{}
	writeFirst bool
	started    chan string
}

func (g *gate) Promote(ctx context.Context, m dataversion.Manifest) (string, error) {
	var uri string
	if g.writeFirst {
		var err error
		if uri, err = g.DataVersionStore.Promote(ctx, m); err != nil {
			return "", err
		}
	}
	g.started <- m.DataVersion
	select {
	case <-g.release:
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if g.writeFirst {
		return uri, nil
	}
	return g.DataVersionStore.Promote(ctx, m)
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
			h := newHarness(t, withBucket(b, func(ps DataVersionStore) DataVersionStore { g.DataVersionStore = ps; return g }))
			id, a := h.startStep(output("extract", "raw", ""))
			require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
				Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
			<-g.started

			// The leader dies while the promote is in flight.
			require.NoError(t, h.stop())
			assert.Equal(t, state.StepCommitting, h.step(id).Status)
			assert.Equal(t, state.DataVersionPending, h.dataVersions(id)[0].State, "not visible before the promote")
			_, err := h.store.LatestDataVersion(h.ctx, "t", "raw", "")
			assert.ErrorIs(t, err, state.ErrNotFound)

			h.restart(withBucket(b, nil))
			h.waitStep(id, state.StepCompleted)
			ps := h.dataVersions(id)
			require.Len(t, ps, 1, "the same data version, promoted once")
			assert.Equal(t, state.DataVersionCommitted, ps[0].State)
			l := h.step(id)
			assert.Equal(t, 1, l.Attempts, "the step didn't run again")
		})
	}
}

func TestLateResultOfTimedOutAttemptIsDiscarded(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	cmd := output("extract", "raw", "")
	cmd.Timeout = time.Minute
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Second}
	id, a1 := h.startStep(cmd)
	out1 := writeOutput(t, b, a1, map[string]string{"a.csv": "slow"})

	h.advance(time.Minute) // a1 times out; its staging is discarded
	require.Eventually(t, func() bool {
		left, err := b.List(h.ctx, a1.Staging)
		return err == nil && len(left) == 0
	}, 5*time.Second, time.Millisecond)
	assert.ErrorIs(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a1.ID, Status: state.AttemptSucceeded, Output: out1}), ErrNotFound)

	h.advance(time.Second)
	a2 := h.poll(TaskStep)
	assert.NotEqual(t, a1.Staging, a2.Staging, "every attempt writes to its own prefix")
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a2.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a2, map[string]string{"a.csv": "fast"})}))
	h.waitStep(id, state.StepCompleted)

	ps := h.dataVersions(id)
	require.Len(t, ps, 1)
	m, err := dataversion.NewStore(b, layout.New(b.Base())).Read(h.ctx, ps[0].Location)
	require.NoError(t, err)
	assert.Equal(t, a2.Staging, m.Data, "the winning attempt's files")
}

func TestPromoteWithMissingFilesFailsTheAttempt(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	cmd := output("extract", "raw", "")
	cmd.Retry = &RetryPolicy{Limit: 1, Initial: time.Second}
	id, a1 := h.startStep(cmd)
	out := writeOutput(t, b, a1, map[string]string{"a.csv": "x"})
	out.Files = append(out.Files, dataversion.File{Path: "never-written.csv", Size: 1, SHA256: out.Files[0].SHA256})
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a1.ID, Status: state.AttemptSucceeded, Output: out}))

	l := h.waitStep(id, state.StepRetryWait)
	assert.Equal(t, "promote_failed", l.Reason)
	ps := h.dataVersions(id)
	assert.Equal(t, state.DataVersionInvalid, ps[0].State)
	assert.Equal(t, "promote_failed", ps[0].Reason)

	h.advance(time.Second)
	a2 := h.poll(TaskStep)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a2.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a2, map[string]string{"a.csv": "x"})}))
	h.waitStep(id, state.StepCompleted)
}

func TestFailedAttemptStagingIsDiscarded(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	_, a := h.startStep(output("extract", "raw", ""))
	writeOutput(t, b, a, map[string]string{"half.csv": "x"})
	h.failStep(a, "error")
	require.Eventually(t, func() bool {
		left, err := b.List(h.ctx, a.Staging)
		return err == nil && len(left) == 0
	}, 5*time.Second, time.Millisecond)
}

func TestStepSucceedingWithoutOutputFails(t *testing.T) {
	h := newHarness(t, withBucket(newBucket(t), nil))
	cmd := output("extract", "raw", "")
	cmd.Retry = &RetryPolicy{Limit: 3, Initial: time.Second}
	id, a := h.startStep(cmd)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded}))
	l := h.step(id)
	assert.Equal(t, state.StepFailed, l.Status)
	assert.Equal(t, "no_output", l.Reason, "no retry either")
}

func TestOutputOfAFailedAttemptIsRejected(t *testing.T) {
	h := newHarness(t, withBucket(newBucket(t), nil))
	_, plain := h.startStep(stepCmd("plain"))
	assert.ErrorIs(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: plain.ID, Status: state.AttemptFailed,
		Output: &StepOutput{}}), ErrInvalid)
}

func TestStepWithoutOutputCanBuildADataVersion(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id, a := h.startStep(stepCmd("frame"))
	require.NotEmpty(t, a.Staging, "every step gets a staging prefix")
	assert.Nil(t, a.Output)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Result: json.RawMessage(`{"$frame":"polars","rows":1}`), Output: writeOutput(t, b, a, map[string]string{"data.parquet": "x"})}))
	l := h.waitStep(id, state.StepCompleted)
	ps := h.dataVersions(id)
	require.Len(t, ps, 1)
	assert.Equal(t, "step:frame", ps[0].Dataset, "its implicit dataset")
	assert.Equal(t, "", ps[0].Partition)
	assert.Equal(t, state.DataVersionCommitted, ps[0].State)
	assert.JSONEq(t, `{"$frame":"polars","rows":1}`, string(l.Result))
	assert.Empty(t, l.Output, "no output was declared")
}

func TestImplicitDatasetsAreReserved(t *testing.T) {
	cmd := output("x", "step:x", "")
	assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Commands: seqs(cmd)}), "for steps without an output")
}

func TestStepWithoutOutputNeedsABucketForFiles(t *testing.T) {
	h := newHarness(t)
	id, a := h.startStep(stepCmd("frame"))
	assert.Empty(t, a.Staging, "no bucket, no staging")
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: &StepOutput{}}))
	assert.Equal(t, "no_data_version_store", h.waitStep(id, state.StepFailed).Reason)
}

func TestStepInputsMustBeCommittedDataVersions(t *testing.T) {
	h := newHarness(t, withBucket(newBucket(t), nil))
	bad := stepCmd("build")
	bad.Inputs = []InputRef{{Name: "orders", DataVersion: "nope"}}
	id := h.startRun("x")
	h.replay(seqs(bad), nil)
	l := h.step(id)
	assert.Equal(t, state.StepFailed, l.Status)
	assert.Equal(t, "bad_input", l.Reason)
	assert.Equal(t, "input orders: data version nope does not exist", l.Message)
	assert.Equal(t, TaskRun, h.poll().Kind, "the run sees the failure")

	assert.Error(t, validateRunTaskResult(RunTaskResult{Commands: seqs(Command{Kind: CommandStep, Name: "x",
		Output: &OutputSpec{Dataset: "d", Partition: "p"}})}), "partition without a key")
	assert.Error(t, validateRunTaskResult(RunTaskResult{Commands: seqs(Command{Kind: CommandStep, Name: "x",
		Output: &OutputSpec{}})}), "no dataset")
}

func TestDatasetPartitionKeyMustMatch(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id, a := h.startStep(output("extract", "raw", "2026-10-24"))
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	h.waitStep(id, state.StepCompleted)
	h.replay(seqs(output("extract", "raw", "2026-10-24")), &RunOutcome{Status: state.RunCompleted})

	// The dataset is known after a restart too.
	h.restart(withBucket(b, nil))
	id2 := h.startRun("y")
	h.replay(seqs(output("other", "raw", "")), nil)
	l := h.step(id2)
	assert.Equal(t, "bad_output", l.Reason)
	assert.Equal(t, `dataset raw is partitioned by "day", the step says ""`, l.Message)
}

func TestStepOutputNeedsADataVersionStore(t *testing.T) {
	h := newHarness(t)
	id := h.startRun("x")
	h.replay(seqs(output("extract", "raw", "")), nil)
	assert.Equal(t, "no_data_version_store", h.step(id).Reason)
}

func TestStepTaskCarriesInputLocations(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	id := h.startRun("x")
	h.replay(seqs(output("extract", "raw", "")), nil)
	a := h.poll(TaskStep)
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
		Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
	h.waitStep(id, state.StepCompleted)
	raw := h.dataVersions(id)[0]

	build := stepCmd("build")
	build.Inputs = []InputRef{{Name: "orders", DataVersion: raw.ID}}
	h.replay(seqs(output("extract", "raw", ""), build), nil)
	task := h.poll(TaskStep)
	assert.Equal(t, []TaskInput{{Name: "orders", DataVersion: raw.ID, Location: raw.Location}}, task.Inputs)
}

func TestStagingOfALiftWithoutOutput(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))
	_, a := h.startStep(stepCmd("frame"))
	assert.Equal(t, b.Base()+"/data/t/step:frame/_/"+a.ID+"/", a.Staging, "its implicit dataset")
}

func TestAttemptsKeepTheirStagingAcrossRestarts(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "recorded"
		if legacy {
			name = "dispatched before staging was recorded"
		}
		t.Run(name, func(t *testing.T) {
			b := newBucket(t)
			h := newHarness(t, withBucket(b, nil))
			id, a := h.startStep(output("extract", "raw", ""))
			require.NoError(t, h.stop())
			if legacy { // an attempt from before the layout moved: no staging recorded, the old path
				att, err := h.store.ListStepAttempts(h.ctx, id, 1)
				require.NoError(t, err)
				att[0].Staging = ""
				_, err = h.store.Append(h.ctx, h.store.Term(), nil, func(tx *state.Tx) error { return tx.PutStepAttempt(att[0]) })
				require.NoError(t, err)
				a.Staging = layout.New(b.Base()).LegacyStepStaging("t", id, 1, a.ID)
			}
			h.restart(withBucket(b, nil))
			require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded,
				Output: writeOutput(t, b, a, map[string]string{"a.csv": "x"})}))
			h.waitStep(id, state.StepCompleted)
			p := h.dataVersions(id)[0]
			assert.Equal(t, state.DataVersionCommitted, p.State, "its files were found where it wrote them")
			m, err := dataversion.NewStore(b, layout.New(b.Base())).Read(h.ctx, p.Location)
			require.NoError(t, err)
			assert.Equal(t, a.Staging, m.Data)
		})
	}
}
