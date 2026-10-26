package core

import (
	"encoding/json"
	"testing"

	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func latest(dataset, partition string) Command {
	return Command{Kind: CommandLatestPallet, Dataset: dataset, Partition: partition}
}

func answers(t *testing.T, logbook []state.Event) map[int]queryAnsweredPayload {
	t.Helper()
	out := map[int]queryAnsweredPayload{}
	for _, e := range logbook {
		if e.Type == state.EventQueryAnswered {
			var p queryAnsweredPayload
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			out[p.Seq] = p
		}
	}
	return out
}

func TestLatestPalletIsAnsweredAndRecorded(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil))

	// Nothing committed yet: the answer is null, and the haul replays with it.
	id := h.startHaul("ingest")
	h.replay(seqs(latest("raw.orders", "")), nil)
	t2 := h.poll(TaskHaul)
	got := answers(t, t2.Logbook)
	require.Contains(t, got, 1)
	assert.Nil(t, got[1].Pallet)

	// The haul builds a pallet with a cursor in its metadata.
	cmds := seqs(latest("raw.orders", ""), output("fetch", "raw.orders", ""))
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t2.ID, Commands: cmds}))
	a := h.poll(TaskLift)
	out := writeOutput(t, b, a, map[string]string{"a.csv": "x"})
	out.Meta = map[string]any{"max_ts": "2026-10-24T23:59:00Z"}
	require.NoError(t, h.core.CompleteLift(h.ctx, LiftResult{AttemptID: a.ID, Status: state.AttemptSucceeded, Output: out}))
	h.waitLift(id, state.LiftCompleted)
	h.replay(cmds, &HaulOutcome{Status: state.HaulCompleted})

	// The next run reads the cursor from the catalog.
	next := h.startHaul("ingest")
	h.replay(seqs(latest("raw.orders", "")), nil)
	t3 := h.poll(TaskHaul)
	p := answers(t, t3.Logbook)[1].Pallet
	require.NotNil(t, p)
	assert.Equal(t, h.pallets(id)[0].ID, p.ID)
	assert.Equal(t, "2026-10-24T23:59:00Z", p.Meta["max_ts"])
	assert.NotEmpty(t, p.Location)
	assert.NotNil(t, p.CommittedAt)
	_ = next

	// A replay that asks for another dataset contradicts the logbook.
	require.NoError(t, h.core.CompleteHaulTask(h.ctx, HaulTaskResult{TaskID: t3.ID, Commands: seqs(latest("raw.other", ""))}))
	gotHaul := h.haul(next)
	assert.Equal(t, state.HaulStuck, gotHaul.Status)
	assert.Equal(t, `command 1: replay issued latest_pallet "raw.other", the logbook has latest_pallet "raw.orders"`, gotHaul.Message)
}

func TestLatestPalletNeedsADataset(t *testing.T) {
	assert.ErrorContains(t, validateHaulTaskResult(HaulTaskResult{Commands: seqs(latest("", ""))}), "latest_pallet needs a dataset")
}
