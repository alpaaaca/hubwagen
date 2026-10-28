package core

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// threeVersions builds three versions of dataset "raw" (unpartitioned), an
// hour apart, with the build declaring keep.
func threeVersions(t *testing.T, h *harness, b blob.Bucket, keep DatasetSpec) []state.Pallet {
	t.Helper()
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b1", Hauls: []string{"pipeline"},
		Lifts: map[string]string{"extract": "v1"}, Datasets: []DatasetSpec{keep}}))
	var out []state.Pallet
	for i := range 3 {
		id, _ := h.runHaul(b, "b1", []step{{cmd: output("extract", "raw", "")}}, map[string]string{"extract": fmt.Sprint("v", i)})
		out = append(out, h.pallets(id)[0])
		h.advance(time.Minute)
	}
	return out
}

func TestRetentionExpiresOldVersions(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil), func(c *Config) { c.RetentionEvery = -1 })
	ps := threeVersions(t, h, b, DatasetSpec{Name: "raw", Versions: 2})

	dry, err := h.core.Expire(h.ctx, true)
	require.NoError(t, err)
	require.Len(t, dry, 1)
	assert.Equal(t, ps[0].ID, dry[0].Pallet.ID, "the oldest of three, keeping two")
	assert.Equal(t, "retention: raw keeps each partition's latest and the newest 2", dry[0].Reason)

	m, err := h.core.cfg.Pallets.Read(h.ctx, ps[0].Location)
	require.NoError(t, err)
	got, err := h.core.Expire(h.ctx, false)
	require.NoError(t, err)
	require.Len(t, got, 1)
	p, err := h.store.GetPallet(h.ctx, ps[0].ID)
	require.NoError(t, err)
	assert.Equal(t, state.PalletExpired, p.State)
	assert.Equal(t, dry[0].Reason, p.Reason)
	require.Eventually(t, func() bool {
		left, err := b.List(h.ctx, m.Data)
		return err == nil && len(left) == 0
	}, 5*time.Second, 10*time.Millisecond, "its data is deleted")
	_, err = h.core.cfg.Pallets.Read(h.ctx, ps[0].Location)
	assert.NoError(t, err, "its manifest stays, as a record")
	for _, keep := range ps[1:] {
		p, err := h.store.GetPallet(h.ctx, keep.ID)
		require.NoError(t, err)
		assert.Equal(t, state.PalletCommitted, p.State)
	}

	evs, err := h.store.EventsSince(h.ctx, 0, "", 1<<20)
	require.NoError(t, err)
	var expired []palletExpiredPayload
	for _, e := range evs {
		if e.Type == state.EventPalletExpired {
			var p palletExpiredPayload
			require.NoError(t, json.Unmarshal(e.Payload, &p))
			expired = append(expired, p)
		}
	}
	require.Len(t, expired, 1)
	assert.Equal(t, ps[0].ID, expired[0].Pallet)

	again, err := h.core.Expire(h.ctx, false)
	require.NoError(t, err)
	assert.Empty(t, again)
}

func TestRetentionRunsOnATimer(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil), func(c *Config) { c.RetentionEvery = time.Hour })
	ps := threeVersions(t, h, b, DatasetSpec{Name: "raw", Versions: 1})
	h.advance(time.Hour)
	for _, p := range ps[:2] {
		got, err := h.store.GetPallet(h.ctx, p.ID)
		require.NoError(t, err)
		assert.Equal(t, state.PalletExpired, got.State)
	}

	// A newer build declaring raw without keep removes its retention.
	require.NoError(t, h.core.RegisterBuild(h.ctx, BuildRequest{ID: "b2", Hauls: []string{"pipeline"},
		Lifts: map[string]string{"extract": "v1"}, Datasets: []DatasetSpec{{Name: "raw"}}}))
	h.runHaul(b, "b2", []step{{cmd: output("extract", "raw", "")}}, map[string]string{"extract": "v9"})
	h.advance(time.Hour)
	got, err := h.store.GetPallet(h.ctx, ps[2].ID)
	require.NoError(t, err)
	assert.Equal(t, state.PalletCommitted, got.State, "no retention: everything stays")
	keep, err := h.store.ListRetention(h.ctx)
	require.NoError(t, err)
	assert.Empty(t, keep)
}

func TestRetentionSurvivesRestart(t *testing.T) {
	b := newBucket(t)
	h := newHarness(t, withBucket(b, nil), func(c *Config) { c.RetentionEvery = -1 })
	threeVersions(t, h, b, DatasetSpec{Name: "raw", Age: 150 * time.Second}) // versions now 3, 2 and 1 minute old
	h.restart(withBucket(b, nil), func(c *Config) { c.RetentionEvery = -1 })
	got, err := h.core.Expire(h.ctx, true)
	require.NoError(t, err)
	assert.Len(t, got, 1, "only the first is older than 150s")
}

func TestDatasetValidation(t *testing.T) {
	h := newHarness(t)
	err := h.core.RegisterBuild(h.ctx, BuildRequest{ID: "bad", Datasets: []DatasetSpec{{Name: ""}, {Name: "a", Versions: -1}, {Name: "a"}}})
	require.ErrorIs(t, err, ErrInvalid)
	for _, want := range []string{"datasets[0]: needs a name", "datasets[1] (a): keep must not be negative", "datasets[2]: a declared twice"} {
		assert.ErrorContains(t, err, want)
	}
}
