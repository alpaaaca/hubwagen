package state

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dataset(name, key string) Dataset {
	return Dataset{Tenant: "t", Name: name, PartitionKey: key, CreatedAt: t0}
}

// pallet is a committed pallet of dataset ds, committed at t0 plus offset.
func pallet(id, ds, partition string, offset time.Duration) Pallet {
	return Pallet{
		ID: id, Tenant: "t", Dataset: ds, Partition: partition, Location: "file:///b/pallets/" + id + ".json",
		State: PalletCommitted, CreatedAt: t0, CommittedAt: ptr(t0.Add(offset)),
	}
}

func putCatalog(t *testing.T, s *Store, datasets []Dataset, pallets []Pallet, inputs ...PalletInput) {
	t.Helper()
	put(t, s, nil, func(tx *Tx) error {
		for _, d := range datasets {
			if err := tx.PutDataset(d); err != nil {
				return err
			}
		}
		for _, p := range pallets {
			if err := tx.PutPallet(p); err != nil {
				return err
			}
		}
		for _, in := range inputs {
			if err := tx.PutPalletInput(in); err != nil {
				return err
			}
		}
		return nil
	})
}

func ids(ps []Pallet) []string {
	var out []string
	for _, p := range ps {
		out = append(out, p.ID)
	}
	return out
}

func TestCatalogRoundTrip(t *testing.T) {
	s, _ := openFile(t)
	ds := dataset("marketing.daily_revenue", "day")
	p := Pallet{
		ID: "p1", Tenant: "t", Dataset: ds.Name, Partition: "2026-10-24", Location: "file:///b/m.json",
		ContentHash: "sha256:ab", Fingerprint: "fp1", Stats: map[string]any{"rows": 3},
		Meta: map[string]any{"max_ts": "2026-10-24T23:59:00Z"}, HaulID: "h1", LiftSeq: 3,
		State: PalletPending, CreatedAt: t0,
	}
	put(t, s, []Event{{Type: EventPalletRecorded, Tenant: "t", HaulID: "h1"}},
		func(tx *Tx) error { return tx.PutDataset(ds) },
		func(tx *Tx) error { return tx.PutPallet(p) },
	)

	// The promote finishes: committed. Identity fields are fixed at creation.
	p.State, p.CommittedAt = PalletCommitted, ptr(t0.Add(time.Second))
	changed := p
	changed.Location, changed.Fingerprint = "file:///elsewhere", "other"
	put(t, s, []Event{{Type: EventPalletCommitted, Tenant: "t", HaulID: "h1"}},
		func(tx *Tx) error {
			return tx.PutDataset(Dataset{Tenant: "t", Name: ds.Name, PartitionKey: "hour", CreatedAt: t0})
		},
		func(tx *Tx) error { return tx.PutPallet(changed) },
	)

	got, err := s.GetPallet(ctx, "p1")
	require.NoError(t, err)
	assert.Equal(t, json.Number("3"), got.Stats["rows"])
	got.Stats = p.Stats
	assert.Equal(t, p, got)

	gotDS, err := s.GetDataset(ctx, "t", ds.Name)
	require.NoError(t, err)
	assert.Equal(t, ds, gotDS, "partition key is fixed at creation")

	_, err = s.GetPallet(ctx, "nope")
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = s.GetDataset(ctx, "t", "nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestLatestPallet(t *testing.T) {
	s, _ := openFile(t)
	const ds = "raw.orders"
	pending := pallet("p5", ds, "2026-10-24", 0)
	pending.State, pending.CommittedAt = PalletPending, nil
	invalid := pallet("p6", ds, "2026-10-24", 5*time.Hour)
	invalid.State, invalid.Reason, invalid.RetiredAt = PalletInvalid, "bad source data", ptr(t0.Add(6*time.Hour))
	expired := pallet("p7", ds, "2026-10-24", 6*time.Hour)
	expired.State = PalletExpired
	putCatalog(t, s, []Dataset{dataset(ds, "day"), dataset("cfg.fx_rates", "")}, []Pallet{
		pallet("p1", ds, "2026-10-23", 3*time.Hour), // other partition, newer
		pallet("p2", ds, "2026-10-24", time.Hour),
		pallet("p3", ds, "2026-10-24", 2*time.Hour), // newest readable
		pallet("p4", ds, "2026-10-24", 2*time.Hour), // same commit time: the higher ID wins
		pending, invalid, expired,
		pallet("r1", "cfg.fx_rates", "", time.Hour),
	})

	got, err := s.LatestPallet(ctx, "t", ds, "2026-10-24")
	require.NoError(t, err)
	assert.Equal(t, "p4", got.ID)

	got, err = s.LatestPallet(ctx, "t", "cfg.fx_rates", "")
	require.NoError(t, err)
	assert.Equal(t, "r1", got.ID)

	_, err = s.LatestPallet(ctx, "t", ds, "2026-10-25")
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = s.LatestPallet(ctx, "other", ds, "2026-10-24")
	assert.ErrorIs(t, err, ErrNotFound)
}

// Diamond: raw1 → a, raw1 → b, raw2 → b, a → c, b → c.
func lineageFixture(t *testing.T, s *Store) {
	t.Helper()
	putCatalog(t, s, []Dataset{dataset("raw", ""), dataset("mid", ""), dataset("out", "")},
		[]Pallet{
			pallet("raw1", "raw", "", 0), pallet("raw2", "raw", "", 0),
			pallet("a", "mid", "", time.Hour), pallet("b", "mid", "", time.Hour),
			pallet("c", "out", "", 2*time.Hour), pallet("x", "out", "", 2*time.Hour), // unrelated
		},
		PalletInput{PalletID: "a", InputID: "raw1", Name: "orders"},
		PalletInput{PalletID: "b", InputID: "raw1", Name: "orders"},
		PalletInput{PalletID: "b", InputID: "raw2", Name: "rates"},
		PalletInput{PalletID: "c", InputID: "a", Name: "left"},
		PalletInput{PalletID: "c", InputID: "b", Name: "right"},
		PalletInput{PalletID: "c", InputID: "b", Name: "right"}, // written twice: a no-op
	)
}

func TestUpstreamLineage(t *testing.T) {
	s, _ := openFile(t)
	lineageFixture(t, s)

	l, err := s.Upstream(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "raw1", "raw2"}, ids(l.Pallets))
	assert.Equal(t, []PalletInput{
		{PalletID: "a", InputID: "raw1", Name: "orders"},
		{PalletID: "b", InputID: "raw1", Name: "orders"},
		{PalletID: "b", InputID: "raw2", Name: "rates"},
		{PalletID: "c", InputID: "a", Name: "left"},
		{PalletID: "c", InputID: "b", Name: "right"},
	}, l.Edges)

	l, err = s.Upstream(ctx, "raw2")
	require.NoError(t, err)
	assert.Equal(t, []string{"raw2"}, ids(l.Pallets))
	assert.Empty(t, l.Edges)

	_, err = s.Upstream(ctx, "nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestDownstreamLineage(t *testing.T) {
	s, _ := openFile(t)
	lineageFixture(t, s)

	l, err := s.Downstream(ctx, "raw1")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "raw1"}, ids(l.Pallets), "raw2 isn't built from raw1")
	assert.Equal(t, []PalletInput{
		{PalletID: "a", InputID: "raw1", Name: "orders"},
		{PalletID: "b", InputID: "raw1", Name: "orders"},
		{PalletID: "c", InputID: "a", Name: "left"},
		{PalletID: "c", InputID: "b", Name: "right"},
	}, l.Edges)

	l, err = s.Downstream(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(l.Pallets))
}

func TestListPalletsAndDatasets(t *testing.T) {
	s, _ := openFile(t)
	lineageFixture(t, s)
	invalid := pallet("y", "out", "", 3*time.Hour)
	invalid.State = PalletInvalid
	putCatalog(t, s, nil, []Pallet{invalid})

	ps, err := s.ListPallets(ctx, PalletFilter{Tenant: "t", Dataset: "out"})
	require.NoError(t, err)
	assert.Equal(t, []string{"y", "x", "c"}, ids(ps))

	ps, err = s.ListPallets(ctx, PalletFilter{Dataset: "out", States: []string{PalletCommitted}, Limit: 1})
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, ids(ps))
	ps, err = s.ListPallets(ctx, PalletFilter{Dataset: "out", States: []string{PalletCommitted}, BeforeID: "x"})
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(ps))

	none := "2026-10-24"
	ps, err = s.ListPallets(ctx, PalletFilter{Partition: &none})
	require.NoError(t, err)
	assert.Empty(t, ps)

	dss, err := s.ListDatasets(ctx, "t")
	require.NoError(t, err)
	var names []string
	for _, d := range dss {
		names = append(names, d.Name)
	}
	assert.Equal(t, []string{"mid", "out", "raw"}, names)
}

func TestCatalogForeignKeys(t *testing.T) {
	s, _ := openFile(t)
	putCatalog(t, s, []Dataset{dataset("raw", "")}, []Pallet{pallet("p1", "raw", "", 0)})
	for name, w := range map[string]func(tx *Tx) error{
		"pallet of unknown dataset":  func(tx *Tx) error { return tx.PutPallet(pallet("p2", "nope", "", 0)) },
		"input from unknown pallet":  func(tx *Tx) error { return tx.PutPalletInput(PalletInput{PalletID: "p1", InputID: "nope"}) },
		"input to an unknown pallet": func(tx *Tx) error { return tx.PutPalletInput(PalletInput{PalletID: "nope", InputID: "p1"}) },
	} {
		_, err := s.Append(ctx, s.Term(), nil, w)
		assert.Error(t, err, name)
	}
}

func TestMemoPallet(t *testing.T) {
	s, _ := openFile(t)
	old := pallet("p1", "raw", "", time.Hour)
	old.Fingerprint = "fp"
	newer := pallet("p2", "raw", "", 2*time.Hour)
	newer.Fingerprint = "fp"
	invalid := pallet("p3", "raw", "", 3*time.Hour)
	invalid.Fingerprint, invalid.State = "fp", PalletInvalid
	other := pallet("p4", "raw", "", 4*time.Hour)
	other.Fingerprint = "fp2"
	putCatalog(t, s, []Dataset{dataset("raw", "")}, []Pallet{old, newer, invalid, other})

	got, err := s.MemoPallet(ctx, "t", "fp", time.Time{})
	require.NoError(t, err)
	assert.Equal(t, "p2", got.ID, "the newest readable pallet with that fingerprint")

	_, err = s.MemoPallet(ctx, "t", "fp", t0.Add(150*time.Minute))
	assert.ErrorIs(t, err, ErrNotFound, "none young enough")
	_, err = s.MemoPallet(ctx, "other", "fp", time.Time{})
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBuilds(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, nil, func(tx *Tx) error {
		if err := tx.PutBuild(Build{ID: "b1", Hauls: []string{"daily", "backfill"}, Lifts: map[string]string{"a": "v1", "b": "v2"}, RegisteredAt: t0}); err != nil {
			return err
		}
		return tx.PutBuild(Build{ID: "empty", RegisteredAt: t0})
	})
	builds, err := s.LoadBuilds(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]Build{
		"b1":    {ID: "b1", Hauls: []string{"backfill", "daily"}, Lifts: map[string]string{"a": "v1", "b": "v2"}, RegisteredAt: t0},
		"empty": {ID: "empty", Lifts: map[string]string{}, RegisteredAt: t0},
	}, builds)
}

func TestGetLift(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(haul("h1", HaulRunning)) },
		func(tx *Tx) error { return tx.PutLift(lift("h1", 2, LiftCompleted)) })
	l, err := s.GetLift(ctx, "h1", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, l.Seq)
	_, err = s.GetLift(ctx, "h1", 1)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestListPartitions(t *testing.T) {
	s, _ := openFile(t)
	const ds = "raw.orders"
	invalid := pallet("p9", ds, "2026-10-24", 9*time.Hour)
	invalid.State = PalletInvalid
	putCatalog(t, s, []Dataset{dataset(ds, "day"), dataset("cfg", "")}, []Pallet{
		pallet("p1", ds, "2026-10-23", time.Hour),
		pallet("p2", ds, "2026-10-24", time.Hour),
		pallet("p3", ds, "2026-10-24", 2*time.Hour),
		pallet("p4", ds, "2026-10-25", time.Hour),
		invalid, // newer, but not committed
		pallet("c1", "cfg", "", time.Hour),
	})
	parts, err := s.ListPartitions(ctx, "t", ds, "", 0)
	require.NoError(t, err)
	got := map[string][2]any{}
	var order []string
	for _, p := range parts {
		order = append(order, p.Partition)
		got[p.Partition] = [2]any{p.Latest.ID, p.Pallets}
	}
	assert.Equal(t, []string{"2026-10-23", "2026-10-24", "2026-10-25"}, order)
	assert.Equal(t, [2]any{"p3", 2}, got["2026-10-24"], "the newest committed, of two")

	page, err := s.ListPartitions(ctx, "t", ds, "2026-10-23", 1)
	require.NoError(t, err)
	require.Len(t, page, 1)
	assert.Equal(t, "2026-10-24", page[0].Partition, "after a partition, one at a time")

	unpartitioned, err := s.ListPartitions(ctx, "t", "cfg", "", 0)
	require.NoError(t, err)
	require.Len(t, unpartitioned, 1)
	assert.Equal(t, "c1", unpartitioned[0].Latest.ID)
}

func TestStalePartitions(t *testing.T) {
	s, _ := openFile(t)
	const orders, rev = "raw.orders", "mart.revenue"
	same := pallet("o3", orders, "2026-10-02", 3*time.Hour)
	same.ContentHash = "sha256:b"
	o2 := pallet("o2", orders, "2026-10-02", time.Hour)
	o2.ContentHash = "sha256:b"
	// r1 and r2 were built by lift "build" (command 1 of haul h1) at code v1;
	// the orders have no lift.
	r1, r2 := pallet("r1", rev, "2026-10-01", time.Hour), pallet("r2", rev, "2026-10-02", 2*time.Hour)
	r1.HaulID, r1.LiftSeq, r2.HaulID, r2.LiftSeq = "h1", 1, "h1", 1
	putCatalog(t, s, []Dataset{dataset(orders, "day"), dataset(rev, "day")}, []Pallet{
		pallet("o1", orders, "2026-10-01", 0),
		pallet("o1b", orders, "2026-10-01", 2*time.Hour), // newer orders for 10-01
		o2, same, // newer orders for 10-02, but the same content
		r1, r2,
	}, PalletInput{PalletID: "r1", InputID: "o1", Name: "orders"}, PalletInput{PalletID: "r2", InputID: "o2", Name: "orders"})
	l := lift("h1", 1, LiftCompleted)
	l.Name, l.FPInputs = "build", json.RawMessage(`{"code_version":"v1"}`)
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(haul("h1", HaulCompleted)) }, func(tx *Tx) error { return tx.PutLift(l) },
		func(tx *Tx) error {
			return tx.PutBuild(Build{ID: "b1", Lifts: map[string]string{"build": "v1"}, RegisteredAt: t0})
		})

	stale, err := s.StalePartitions(ctx, "t", rev)
	require.NoError(t, err)
	require.Len(t, stale, 1)
	assert.Equal(t, "r1", stale[0].Pallet.ID)
	assert.Equal(t, "build", stale[0].Lift)
	assert.Equal(t, []string{"input raw.orders 2026-10-01 has a newer pallet o1b"}, stale[0].Reasons)

	// A newer build changes the lift's code: both are stale now.
	put(t, s, nil, func(tx *Tx) error {
		return tx.PutBuild(Build{ID: "b2", Lifts: map[string]string{"build": "v2"}, RegisteredAt: t0.Add(time.Hour)})
	})
	stale, err = s.StalePartitions(ctx, "t", "")
	require.NoError(t, err)
	require.Len(t, stale, 2, "only mart.revenue's pallets have a lift and inputs")
	assert.Equal(t, []string{"input raw.orders 2026-10-01 has a newer pallet o1b", "lift build: code changed (v1 → v2)"}, stale[0].Reasons)
	assert.Equal(t, []string{"lift build: code changed (v1 → v2)"}, stale[1].Reasons)
}

func TestExpiryCandidates(t *testing.T) {
	s, _ := openFile(t)
	const ds = "raw.orders"
	// Partition 10-01: p1 (oldest) … p5 (latest), an hour apart; 10-02: q1, q2.
	var ps []Pallet
	for i := 1; i <= 5; i++ {
		ps = append(ps, pallet(fmt.Sprintf("p%d", i), ds, "2026-10-01", time.Duration(i)*time.Hour))
	}
	ps = append(ps, pallet("q1", ds, "2026-10-02", time.Hour), pallet("q2", ds, "2026-10-02", 2*time.Hour))
	invalid := pallet("p0", ds, "2026-10-01", 0)
	invalid.State = PalletInvalid
	ps = append(ps, invalid)
	ps[1].HaulID = "h-open" // p2: built by a haul that will be running
	putCatalog(t, s, []Dataset{dataset(ds, "day")}, ps)
	now := t0.Add(6 * time.Hour)

	got := func(r Retention) []string {
		t.Helper()
		r.Dataset = ds
		c, err := s.ExpiryCandidates(ctx, r, now, 100)
		require.NoError(t, err)
		return ids(c)
	}
	assert.Equal(t, []string{"p1", "p2", "p3"}, got(Retention{Versions: 2}), "the newest two per partition stay")
	assert.Equal(t, []string{"p1", "q1", "p2", "p3", "p4"}, got(Retention{Versions: 1}), "only the latest, oldest first")
	assert.Equal(t, []string{"p1", "q1", "p2", "p3"}, got(Retention{Age: 150 * time.Minute}), "younger than 2.5 hours stays; q2 is the latest")
	assert.Equal(t, []string{"p1"}, got(Retention{Versions: 2, Age: 270 * time.Minute}), "kept by either rule")

	// p2 was built by a running haul, p1 is an input of an unfinished lift
	// and q1 a lift result in a running haul: all in use.
	l := lift("h-open", 1, LiftRunning)
	l.Inputs = json.RawMessage(`[{"name":"orders","pallet":"p1"}]`)
	put(t, s, []Event{{Type: "LiftFinished", HaulID: "h-open", Tenant: "t", Payload: json.RawMessage(`{"seq":2,"pallet":"q1"}`)}},
		func(tx *Tx) error { return tx.PutHaul(haul("h-open", HaulRunning)) }, func(tx *Tx) error { return tx.PutLift(l) })
	assert.Equal(t, []string{"p3", "p4"}, got(Retention{Versions: 1}), "p1, p2 and q1 are in use")
}
