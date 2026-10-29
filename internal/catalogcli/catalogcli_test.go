package catalogcli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/api"
	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/state"
)

var ctx = context.Background()

var t0 = time.Date(2026, 10, 24, 3, 0, 0, 0, time.UTC)

// A small catalog: raw.orders (two days) feeds mart.revenue, which with
// raw.orders again (a diamond) feeds reports.revenue.
// fixture is a catalog served over HTTP, and what serves it.
type fixture struct {
	c    *Client
	st   *state.Store
	core *core.Core
}

func serve(t *testing.T) fixture {
	t.Helper()
	dir := t.TempDir()
	st, err := state.Open(filepath.Join(dir, "state.db"))
	require.NoError(t, err)
	t.Cleanup(func() { st.Close() })
	_, err = st.StartTerm(ctx, 0)
	require.NoError(t, err)
	b, err := blob.Open(blob.FileURI(filepath.Join(dir, "bucket")))
	require.NoError(t, err)
	lay := layout.New(b.Base())
	ps := pallet.NewStore(b, lay)

	mk := func(id, ds, part string, at time.Duration) state.Pallet {
		staging := lay.LiftStaging("default", ds, part, "a-"+id)
		content := []byte(id + " data\n")
		require.NoError(t, b.Put(ctx, staging+"part-0.csv", content))
		sum := sha256.Sum256(content)
		m, err := pallet.New(id, "default", ds, part, staging,
			[]pallet.File{{Path: "part-0.csv", Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])}}, nil)
		require.NoError(t, err)
		uri, err := ps.Promote(ctx, m)
		require.NoError(t, err)
		return state.Pallet{ID: id, Tenant: "default", Dataset: ds, Partition: part, Location: uri, ContentHash: m.ContentHash,
			HaulID: "h-" + id, LiftSeq: 1, State: state.PalletCommitted, CreatedAt: t0, CommittedAt: ptr(t0.Add(at))}
	}
	pallets := []state.Pallet{
		mk("o1", "raw.orders", "2026-10-23", 0), mk("o2", "raw.orders", "2026-10-24", time.Hour),
		mk("o3", "raw.orders", "2026-10-24", 2*time.Hour),
		mk("m1", "mart.revenue", "2026-10-24", 3*time.Hour), mk("r1", "reports.revenue", "2026-10-24", 4*time.Hour),
	}
	_, err = st.Append(ctx, st.Term(), nil, func(tx *state.Tx) error {
		for _, d := range []state.Dataset{{Tenant: "default", Name: "raw.orders", PartitionKey: "day", CreatedAt: t0},
			{Tenant: "default", Name: "mart.revenue", PartitionKey: "day", CreatedAt: t0},
			{Tenant: "default", Name: "reports.revenue", PartitionKey: "day", CreatedAt: t0}} {
			if err := tx.PutDataset(d); err != nil {
				return err
			}
		}
		for _, id := range []string{"o1", "o2", "o3", "m1", "r1"} {
			params := map[string]any{"day": "2026-10-24", "label": "Q4 revenue"}
			if err := tx.PutHaul(state.Haul{ID: "h-" + id, Tenant: "default", Name: "daily", Dock: "default",
				Trigger: "manual", Params: params, Status: state.HaulCompleted, CreatedAt: t0}); err != nil {
				return err
			}
		}
		for _, p := range pallets {
			if err := tx.PutPallet(p); err != nil {
				return err
			}
		}
		for _, e := range []state.PalletInput{{PalletID: "m1", InputID: "o3", Name: "orders"},
			{PalletID: "r1", InputID: "m1", Name: "revenue"}, {PalletID: "r1", InputID: "o3", Name: "orders"}} {
			if err := tx.PutPalletInput(e); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	c := core.New(core.Config{Store: st, Term: st.Term(), Layout: lay, Pallets: ps})
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()
	require.NoError(t, c.Barrier(ctx))
	t.Cleanup(func() { cancel(); require.NoError(t, <-done) })

	mux := http.NewServeMux()
	path, h := api.NewCatalogHandler(c, st, ps)
	mux.Handle(path, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return fixture{NewClient(srv.URL), st, c}
}

func ptr[T any](v T) *T { return &v }

func lines(s string) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimRight(s, "\n"), "\n") {
		out = append(out, strings.Join(strings.Fields(l), " "))
	}
	return out
}

func TestDatasets(t *testing.T) {
	c := serve(t).c
	var out bytes.Buffer
	require.NoError(t, c.Datasets(ctx, &out, "default"))
	got := lines(out.String())
	assert.Equal(t, "DATASET PARTITIONED BY PARTITIONS LATEST KEEP", got[0])
	assert.Contains(t, got, "raw.orders day 2 2026-10-24 "+when(t0.Add(2*time.Hour))+" all")
}

func TestPallets(t *testing.T) {
	c := serve(t).c
	var out bytes.Buffer
	require.NoError(t, c.Pallets(ctx, &out, PalletsQuery{Tenant: "default", Dataset: "raw.orders"}))
	assert.Equal(t, []string{"PARTITION LATEST VERSION SAVED VERSIONS",
		"2026-10-23 o1 " + when(t0) + " 1", "2026-10-24 o3 " + when(t0.Add(2*time.Hour)) + " 2"}, lines(out.String()))

	out.Reset()
	day := "2026-10-24"
	require.NoError(t, c.Pallets(ctx, &out, PalletsQuery{Tenant: "default", Dataset: "raw.orders", Partition: &day}))
	got := lines(out.String())
	require.Len(t, got, 3)
	assert.True(t, strings.HasPrefix(got[1], "o3 2026-10-24 committed"), got[1])
	assert.True(t, strings.HasPrefix(got[2], "o2 "), "newest first")
}

func TestPalletShowsFilesAndLineage(t *testing.T) {
	c := serve(t).c
	var out bytes.Buffer
	require.NoError(t, c.Pallet(ctx, &out, "o3"))
	s := out.String()
	assert.Contains(t, s, "data version o3, committed")
	assert.Contains(t, s, "files      part-0.csv (8 B)")
	assert.Contains(t, s, "built from:\n  nothing")
	assert.Contains(t, s, "built into:\n  m1  mart.revenue 2026-10-24  (as orders)\n    r1  reports.revenue 2026-10-24  (as revenue)\n  r1  reports.revenue 2026-10-24  (as orders)  (shown above)\n")

	out.Reset()
	require.NoError(t, c.Pallet(ctx, &out, "r1"))
	assert.Contains(t, out.String(), "built from:\n  m1  mart.revenue 2026-10-24  (as revenue)\n    o3  raw.orders 2026-10-24  (as orders)\n  o3  raw.orders 2026-10-24  (as orders)  (shown above)\n")
	assert.Contains(t, out.String(), "built into:\n  nothing")

	assert.Error(t, c.Pallet(ctx, &out, "nope"))
}

func TestInvalidate(t *testing.T) {
	c := serve(t).c
	var out bytes.Buffer
	asked := 0
	no := func(n int) bool { asked = n; return false }
	require.NoError(t, c.Invalidate(ctx, &out, InvalidateQuery{ID: "m1", Reason: "bad", Confirm: no}))
	assert.Equal(t, 2, asked)
	assert.Equal(t, []string{"invalidating m1 marks 2 versions invalid:", "m1 mart.revenue 2026-10-24 committed",
		"r1 reports.revenue 2026-10-24 committed", "nothing changed"}, lines(out.String()))

	out.Reset()
	require.NoError(t, c.Invalidate(ctx, &out, InvalidateQuery{ID: "m1", Reason: "bad", DryRun: true}))
	assert.Contains(t, out.String(), "m1 would mark 2 versions")
	assert.Contains(t, out.String(), "hubwagen start daily --param day=2026-10-24 --param 'label=Q4 revenue'")

	out.Reset()
	require.NoError(t, c.Invalidate(ctx, &out, InvalidateQuery{ID: "m1", Reason: "bad", By: "ana", Confirm: func(int) bool { return true }}))
	assert.Contains(t, out.String(), "marked 2 versions invalid")
	assert.Contains(t, out.String(), "these recompute them (memoized steps whose code and inputs didn't change are reused):")

	out.Reset()
	require.NoError(t, c.Pallet(ctx, &out, "r1"))
	assert.Contains(t, out.String(), "data version r1, invalid\n  reason     built from invalidated version m1: bad")

	out.Reset()
	require.NoError(t, c.Invalidate(ctx, &out, InvalidateQuery{ID: "m1", Reason: "again"}))
	assert.Contains(t, out.String(), "nothing to mark")
}

func TestStale(t *testing.T) {
	f := serve(t)
	c, st := f.c, f.st
	var out bytes.Buffer
	require.NoError(t, c.Stale(ctx, &out, "default", ""))
	assert.Equal(t, "nothing is stale\n", out.String())

	// New orders for 2026-10-24 arrive: what was built from the old ones is stale.
	p := state.Pallet{ID: "o4", Tenant: "default", Dataset: "raw.orders", Partition: "2026-10-24", Location: "x",
		ContentHash: "sha256:new", State: state.PalletCommitted, CreatedAt: t0, CommittedAt: ptr(t0.Add(5 * time.Hour))}
	_, err := st.Append(ctx, st.Term(), nil, func(tx *state.Tx) error { return tx.PutPallet(p) })
	require.NoError(t, err)

	out.Reset()
	require.NoError(t, c.Stale(ctx, &out, "default", ""))
	got := lines(out.String())
	assert.Equal(t, "DATASET PARTITION LATEST VERSION WHY", got[0])
	assert.Contains(t, got, "mart.revenue 2026-10-24 m1 input raw.orders 2026-10-24 has a newer version o4")
	assert.Contains(t, got, "reports.revenue 2026-10-24 r1 input raw.orders 2026-10-24 has a newer version o4")
	assert.Contains(t, out.String(), "these refresh them (memoized steps whose code and inputs didn't change are reused):\n  hubwagen start daily --param day=2026-10-24 --param 'label=Q4 revenue'\n")
	assert.Equal(t, 1, strings.Count(out.String(), "hubwagen start"), "one start for hauls with the same params")

	out.Reset()
	require.NoError(t, c.Stale(ctx, &out, "default", "reports.revenue"))
	assert.NotContains(t, out.String(), "mart.revenue")
}

func TestGC(t *testing.T) {
	f := serve(t)
	var out bytes.Buffer
	require.NoError(t, f.c.GC(ctx, &out, true))
	assert.Equal(t, "nothing to expire\n", out.String(), "no retention: everything stays")

	require.NoError(t, f.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1",
		Datasets: []core.DatasetSpec{{Name: "raw.orders", Versions: 1}}}))
	out.Reset()
	require.NoError(t, f.c.Datasets(ctx, &out, "default"))
	assert.Contains(t, lines(out.String()), "raw.orders day 2 2026-10-24 "+when(t0.Add(2*time.Hour))+" 1 version")

	out.Reset()
	require.NoError(t, f.c.GC(ctx, &out, true))
	assert.Equal(t, []string{"would expire 1 version:", "o2 raw.orders 2026-10-24 saved " + when(t0.Add(time.Hour))}, lines(out.String()),
		"o3 is the latest of 10-24; o1 the only one of 10-23")
	out.Reset()
	require.NoError(t, f.c.GC(ctx, &out, false))
	assert.True(t, strings.HasPrefix(out.String(), "expired 1 version:"))
	out.Reset()
	require.NoError(t, f.c.Pallet(ctx, &out, "o2"))
	assert.Contains(t, out.String(), "data version o2, expired\n  reason     retention: raw.orders keeps each partition's latest and the newest 1")
	assert.Contains(t, out.String(), "files      can't be read: expired: its data was removed")
}
