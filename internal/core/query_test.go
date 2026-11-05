package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func latest(dataset, partition string) Command {
	return Command{Kind: CommandLatestDataVersion, Dataset: dataset, Partition: partition}
}

func answers(t *testing.T, eventLog []state.Event) map[int]queryAnsweredPayload {
	t.Helper()
	out := map[int]queryAnsweredPayload{}
	for _, e := range eventLog {
		if e.Type == state.EventQueryAnswered {
			var p queryAnsweredPayload
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			out[p.Seq] = p
		}
	}
	return out
}

func TestLatestDataVersionIsAnsweredAndRecorded(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))

	// Nothing committed yet: the answer is null, and the run replays with it.
	id := h.startRun("ingest")
	h.replay(seqs(latest("raw.orders", "")), nil)
	t2 := h.poll(TaskRun)
	got := answers(t, t2.EventLog)
	require.Contains(t, got, 1)
	assert.Nil(t, got[1].DataVersion)

	// The run builds a data version with a cursor in its metadata.
	cmds := seqs(latest("raw.orders", ""), output("fetch", "raw.orders", ""))
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t2.ID, Commands: cmds}))
	a := h.poll(TaskStep)
	out := writeOutput(t, b, a, map[string]string{"a.csv": "x"})
	out.Meta = map[string]any{"max_ts": "2026-10-24T23:59:00Z"}
	require.NoError(t, h.core.CompleteStep(h.ctx, StepResult{AttemptID: a.ID, Status: state.AttemptSucceeded, Output: out}))
	h.waitStep(id, state.StepCompleted)
	h.replay(cmds, &RunOutcome{Status: state.RunCompleted})

	// The next run reads the cursor from the catalog.
	next := h.startRun("ingest")
	h.replay(seqs(latest("raw.orders", "")), nil)
	t3 := h.poll(TaskRun)
	p := answers(t, t3.EventLog)[1].DataVersion
	require.NotNil(t, p)
	assert.Equal(t, h.dataVersions(id)[0].ID, p.ID)
	assert.Equal(t, "2026-10-24T23:59:00Z", p.Meta["max_ts"])
	assert.NotEmpty(t, p.Location)
	assert.NotNil(t, p.CommittedAt)
	_ = next

	// A replay that asks for another dataset contradicts the event log.
	require.NoError(t, h.core.CompleteRunTask(h.ctx, RunTaskResult{TaskID: t3.ID, Commands: seqs(latest("raw.other", ""))}))
	gotRun := h.run(next)
	assert.Equal(t, state.RunStuck, gotRun.Status)
	assert.Equal(t, `command 1: replay issued latest_version "raw.other", the event log has latest_version "raw.orders"`, gotRun.Message)
}

func TestLatestDataVersionNeedsADataset(t *testing.T) {
	assert.ErrorContains(t, validateRunTaskResult(RunTaskResult{Commands: seqs(latest("", ""))}), "latest_version needs a dataset")
}
