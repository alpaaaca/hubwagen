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

// data version is a committed data version of dataset ds, committed at t0 plus offset.
func dataVersion(id, ds, partition string, offset time.Duration) DataVersion {
	return DataVersion{
		ID: id, Tenant: "t", Dataset: ds, Partition: partition, Location: "file:///b/data_versions/" + id + ".json",
		State: DataVersionCommitted, CreatedAt: t0, CommittedAt: ptr(t0.Add(offset)),
	}
}

func putCatalog(t *testing.T, s *Store, datasets []Dataset, dataVersions []DataVersion, inputs ...DataVersionInput) {
	t.Helper()
	put(t, s, nil, func(tx *Tx) error {
		for _, d := range datasets {
			if err := tx.PutDataset(d); err != nil {
				return err
			}
		}
		for _, p := range dataVersions {
			if err := tx.PutDataVersion(p); err != nil {
				return err
			}
		}
		for _, in := range inputs {
			if err := tx.PutDataVersionInput(in); err != nil {
				return err
			}
		}
		return nil
	})
}

func ids(ps []DataVersion) []string {
	var out []string
	for _, p := range ps {
		out = append(out, p.ID)
	}
	return out
}

func TestCatalogRoundTrip(t *testing.T) {
	s, _ := openFile(t)
	ds := dataset("marketing.daily_revenue", "day")
	p := DataVersion{
		ID: "p1", Tenant: "t", Dataset: ds.Name, Partition: "2026-10-24", Location: "file:///b/m.json",
		ContentHash: "sha256:ab", Fingerprint: "fp1", Stats: map[string]any{"rows": 3},
		Meta: map[string]any{"max_ts": "2026-10-24T23:59:00Z"}, RunID: "h1", StepSeq: 3,
		State: DataVersionPending, CreatedAt: t0,
	}
	put(t, s, []Event{{Type: EventDataVersionRecorded, Tenant: "t", RunID: "h1"}},
		func(tx *Tx) error { return tx.PutDataset(ds) },
		func(tx *Tx) error { return tx.PutDataVersion(p) },
	)

	// The promote finishes: committed. Identity fields are fixed at creation.
	p.State, p.CommittedAt = DataVersionCommitted, ptr(t0.Add(time.Second))
	changed := p
	changed.Location, changed.Fingerprint = "file:///elsewhere", "other"
	put(t, s, []Event{{Type: EventDataVersionCommitted, Tenant: "t", RunID: "h1"}},
		func(tx *Tx) error {
			return tx.PutDataset(Dataset{Tenant: "t", Name: ds.Name, PartitionKey: "hour", CreatedAt: t0})
		},
		func(tx *Tx) error { return tx.PutDataVersion(changed) },
	)

	got, err := s.GetDataVersion(ctx, "p1")
	require.NoError(t, err)
	assert.Equal(t, json.Number("3"), got.Stats["rows"])
	got.Stats = p.Stats
	assert.Equal(t, p, got)

	gotDS, err := s.GetDataset(ctx, "t", ds.Name)
	require.NoError(t, err)
	assert.Equal(t, ds, gotDS, "partition key is fixed at creation")

	_, err = s.GetDataVersion(ctx, "nope")
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = s.GetDataset(ctx, "t", "nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestLatestDataVersion(t *testing.T) {
	s, _ := openFile(t)
	const ds = "raw.orders"
	pending := dataVersion("p5", ds, "2026-10-24", 0)
	pending.State, pending.CommittedAt = DataVersionPending, nil
	invalid := dataVersion("p6", ds, "2026-10-24", 5*time.Hour)
	invalid.State, invalid.Reason, invalid.RetiredAt = DataVersionInvalid, "bad source data", ptr(t0.Add(6*time.Hour))
	expired := dataVersion("p7", ds, "2026-10-24", 6*time.Hour)
	expired.State = DataVersionExpired
	putCatalog(t, s, []Dataset{dataset(ds, "day"), dataset("cfg.fx_rates", "")}, []DataVersion{
		dataVersion("p1", ds, "2026-10-23", 3*time.Hour), // other partition, newer
		dataVersion("p2", ds, "2026-10-24", time.Hour),
		dataVersion("p3", ds, "2026-10-24", 2*time.Hour), // newest readable
		dataVersion("p4", ds, "2026-10-24", 2*time.Hour), // same commit time: the higher ID wins
		pending, invalid, expired,
		dataVersion("r1", "cfg.fx_rates", "", time.Hour),
	})

	got, err := s.LatestDataVersion(ctx, "t", ds, "2026-10-24")
	require.NoError(t, err)
	assert.Equal(t, "p4", got.ID)

	got, err = s.LatestDataVersion(ctx, "t", "cfg.fx_rates", "")
	require.NoError(t, err)
	assert.Equal(t, "r1", got.ID)

	_, err = s.LatestDataVersion(ctx, "t", ds, "2026-10-25")
	assert.ErrorIs(t, err, ErrNotFound)
	_, err = s.LatestDataVersion(ctx, "other", ds, "2026-10-24")
	assert.ErrorIs(t, err, ErrNotFound)
}

// Diamond: raw1 → a, raw1 → b, raw2 → b, a → c, b → c.
func lineageFixture(t *testing.T, s *Store) {
	t.Helper()
	putCatalog(t, s, []Dataset{dataset("raw", ""), dataset("mid", ""), dataset("out", "")},
		[]DataVersion{
			dataVersion("raw1", "raw", "", 0), dataVersion("raw2", "raw", "", 0),
			dataVersion("a", "mid", "", time.Hour), dataVersion("b", "mid", "", time.Hour),
			dataVersion("c", "out", "", 2*time.Hour), dataVersion("x", "out", "", 2*time.Hour), // unrelated
		},
		DataVersionInput{DataVersionID: "a", InputID: "raw1", Name: "orders"},
		DataVersionInput{DataVersionID: "b", InputID: "raw1", Name: "orders"},
		DataVersionInput{DataVersionID: "b", InputID: "raw2", Name: "rates"},
		DataVersionInput{DataVersionID: "c", InputID: "a", Name: "left"},
		DataVersionInput{DataVersionID: "c", InputID: "b", Name: "right"},
		DataVersionInput{DataVersionID: "c", InputID: "b", Name: "right"}, // written twice: a no-op
	)
}

func TestUpstreamLineage(t *testing.T) {
	s, _ := openFile(t)
	lineageFixture(t, s)

	l, err := s.Upstream(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "raw1", "raw2"}, ids(l.DataVersions))
	assert.Equal(t, []DataVersionInput{
		{DataVersionID: "a", InputID: "raw1", Name: "orders"},
		{DataVersionID: "b", InputID: "raw1", Name: "orders"},
		{DataVersionID: "b", InputID: "raw2", Name: "rates"},
		{DataVersionID: "c", InputID: "a", Name: "left"},
		{DataVersionID: "c", InputID: "b", Name: "right"},
	}, l.Edges)

	l, err = s.Upstream(ctx, "raw2")
	require.NoError(t, err)
	assert.Equal(t, []string{"raw2"}, ids(l.DataVersions))
	assert.Empty(t, l.Edges)

	_, err = s.Upstream(ctx, "nope")
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestDownstreamLineage(t *testing.T) {
	s, _ := openFile(t)
	lineageFixture(t, s)

	l, err := s.Downstream(ctx, "raw1")
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b", "c", "raw1"}, ids(l.DataVersions), "raw2 isn't built from raw1")
	assert.Equal(t, []DataVersionInput{
		{DataVersionID: "a", InputID: "raw1", Name: "orders"},
		{DataVersionID: "b", InputID: "raw1", Name: "orders"},
		{DataVersionID: "c", InputID: "a", Name: "left"},
		{DataVersionID: "c", InputID: "b", Name: "right"},
	}, l.Edges)

	l, err = s.Downstream(ctx, "c")
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(l.DataVersions))
}

func TestListDataVersionsAndDatasets(t *testing.T) {
	s, _ := openFile(t)
	lineageFixture(t, s)
	invalid := dataVersion("y", "out", "", 3*time.Hour)
	invalid.State = DataVersionInvalid
	putCatalog(t, s, nil, []DataVersion{invalid})

	ps, err := s.ListDataVersions(ctx, DataVersionFilter{Tenant: "t", Dataset: "out"})
	require.NoError(t, err)
	assert.Equal(t, []string{"y", "x", "c"}, ids(ps))

	ps, err = s.ListDataVersions(ctx, DataVersionFilter{Dataset: "out", States: []string{DataVersionCommitted}, Limit: 1})
	require.NoError(t, err)
	assert.Equal(t, []string{"x"}, ids(ps))
	ps, err = s.ListDataVersions(ctx, DataVersionFilter{Dataset: "out", States: []string{DataVersionCommitted}, BeforeID: "x"})
	require.NoError(t, err)
	assert.Equal(t, []string{"c"}, ids(ps))

	none := "2026-10-24"
	ps, err = s.ListDataVersions(ctx, DataVersionFilter{Partition: &none})
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
	putCatalog(t, s, []Dataset{dataset("raw", "")}, []DataVersion{dataVersion("p1", "raw", "", 0)})
	for name, w := range map[string]func(tx *Tx) error{
		"data version of unknown dataset": func(tx *Tx) error { return tx.PutDataVersion(dataVersion("p2", "nope", "", 0)) },
		"input from unknown data version": func(tx *Tx) error {
			return tx.PutDataVersionInput(DataVersionInput{DataVersionID: "p1", InputID: "nope"})
		},
		"input to an unknown data version": func(tx *Tx) error {
			return tx.PutDataVersionInput(DataVersionInput{DataVersionID: "nope", InputID: "p1"})
		},
	} {
		_, err := s.Append(ctx, s.Term(), nil, w)
		assert.Error(t, err, name)
	}
}

func TestMemoDataVersion(t *testing.T) {
	s, _ := openFile(t)
	old := dataVersion("p1", "raw", "", time.Hour)
	old.Fingerprint = "fp"
	newer := dataVersion("p2", "raw", "", 2*time.Hour)
	newer.Fingerprint = "fp"
	invalid := dataVersion("p3", "raw", "", 3*time.Hour)
	invalid.Fingerprint, invalid.State = "fp", DataVersionInvalid
	other := dataVersion("p4", "raw", "", 4*time.Hour)
	other.Fingerprint = "fp2"
	putCatalog(t, s, []Dataset{dataset("raw", "")}, []DataVersion{old, newer, invalid, other})

	got, err := s.MemoDataVersion(ctx, "t", "fp", time.Time{})
	require.NoError(t, err)
	assert.Equal(t, "p2", got.ID, "the newest readable data version with that fingerprint")

	_, err = s.MemoDataVersion(ctx, "t", "fp", t0.Add(150*time.Minute))
	assert.ErrorIs(t, err, ErrNotFound, "none young enough")
	_, err = s.MemoDataVersion(ctx, "other", "fp", time.Time{})
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestBuilds(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, nil, func(tx *Tx) error {
		if err := tx.PutBuild(Build{ID: "b1", Pipelines: []string{"daily", "backfill"}, Steps: map[string]string{"a": "v1", "b": "v2"}, RegisteredAt: t0}); err != nil {
			return err
		}
		return tx.PutBuild(Build{ID: "empty", RegisteredAt: t0})
	})
	builds, err := s.LoadBuilds(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]Build{
		"b1":    {ID: "b1", Pipelines: []string{"backfill", "daily"}, Steps: map[string]string{"a": "v1", "b": "v2"}, RegisteredAt: t0},
		"empty": {ID: "empty", Steps: map[string]string{}, RegisteredAt: t0},
	}, builds)
}

func TestCurrentCode(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, nil, func(tx *Tx) error {
		if err := tx.PutBuild(Build{ID: "b1", Pipelines: []string{"daily", "old"}, Steps: map[string]string{"a": "v1", "b": "v1"}, RegisteredAt: t0}); err != nil {
			return err
		}
		// Newer: changes a, drops b and old, adds c.
		return tx.PutBuild(Build{ID: "b2", Pipelines: []string{"daily"}, Steps: map[string]string{"a": "v2", "c": "v1"}, RegisteredAt: t0.Add(time.Hour)})
	})
	steps, pipelines, err := s.CurrentCode(ctx)
	require.NoError(t, err)
	assert.Equal(t, []Code{
		{Name: "a", Version: "v2", BuildID: "b2", RegisteredAt: t0.Add(time.Hour)},
		{Name: "b", Version: "v1", BuildID: "b1", RegisteredAt: t0},
		{Name: "c", Version: "v1", BuildID: "b2", RegisteredAt: t0.Add(time.Hour)},
	}, steps, "each from the newest build that defines it")
	assert.Equal(t, []Code{
		{Name: "daily", BuildID: "b2", RegisteredAt: t0.Add(time.Hour)},
		{Name: "old", BuildID: "b1", RegisteredAt: t0},
	}, pipelines)
}

func TestGetStep(t *testing.T) {
	s, _ := openFile(t)
	put(t, s, nil, func(tx *Tx) error { return tx.PutRun(run("h1", RunRunning)) },
		func(tx *Tx) error { return tx.PutStep(step("h1", 2, StepCompleted)) })
	l, err := s.GetStep(ctx, "h1", 2)
	require.NoError(t, err)
	assert.Equal(t, 2, l.Seq)
	_, err = s.GetStep(ctx, "h1", 1)
	assert.ErrorIs(t, err, ErrNotFound)
}

func TestListPartitions(t *testing.T) {
	s, _ := openFile(t)
	const ds = "raw.orders"
	invalid := dataVersion("p9", ds, "2026-10-24", 9*time.Hour)
	invalid.State = DataVersionInvalid
	putCatalog(t, s, []Dataset{dataset(ds, "day"), dataset("cfg", "")}, []DataVersion{
		dataVersion("p1", ds, "2026-10-23", time.Hour),
		dataVersion("p2", ds, "2026-10-24", time.Hour),
		dataVersion("p3", ds, "2026-10-24", 2*time.Hour),
		dataVersion("p4", ds, "2026-10-25", time.Hour),
		invalid, // newer, but not committed
		dataVersion("c1", "cfg", "", time.Hour),
	})
	parts, err := s.ListPartitions(ctx, "t", ds, "", 0)
	require.NoError(t, err)
	got := map[string][2]any{}
	var order []string
	for _, p := range parts {
		order = append(order, p.Partition)
		got[p.Partition] = [2]any{p.Latest.ID, p.DataVersions}
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
	same := dataVersion("o3", orders, "2026-10-02", 3*time.Hour)
	same.ContentHash = "sha256:b"
	o2 := dataVersion("o2", orders, "2026-10-02", time.Hour)
	o2.ContentHash = "sha256:b"
	// r1 and r2 were built by step "build" (command 1 of run h1) at code v1;
	// the orders have no step.
	r1, r2 := dataVersion("r1", rev, "2026-10-01", time.Hour), dataVersion("r2", rev, "2026-10-02", 2*time.Hour)
	r1.RunID, r1.StepSeq, r2.RunID, r2.StepSeq = "h1", 1, "h1", 1
	putCatalog(t, s, []Dataset{dataset(orders, "day"), dataset(rev, "day")}, []DataVersion{
		dataVersion("o1", orders, "2026-10-01", 0),
		dataVersion("o1b", orders, "2026-10-01", 2*time.Hour), // newer orders for 10-01
		o2, same, // newer orders for 10-02, but the same content
		r1, r2,
	}, DataVersionInput{DataVersionID: "r1", InputID: "o1", Name: "orders"}, DataVersionInput{DataVersionID: "r2", InputID: "o2", Name: "orders"})
	l := step("h1", 1, StepCompleted)
	l.Name, l.FPInputs = "build", json.RawMessage(`{"code_version":"v1"}`)
	put(t, s, nil, func(tx *Tx) error { return tx.PutRun(run("h1", RunCompleted)) }, func(tx *Tx) error { return tx.PutStep(l) },
		func(tx *Tx) error {
			return tx.PutBuild(Build{ID: "b1", Steps: map[string]string{"build": "v1"}, RegisteredAt: t0})
		})

	stale, err := s.StalePartitions(ctx, "t", rev)
	require.NoError(t, err)
	require.Len(t, stale, 1)
	assert.Equal(t, "r1", stale[0].DataVersion.ID)
	assert.Equal(t, "build", stale[0].Step)
	assert.Equal(t, []string{"input raw.orders 2026-10-01 has a newer version o1b"}, stale[0].Reasons)

	// A newer build changes the step's code: both are stale now.
	put(t, s, nil, func(tx *Tx) error {
		return tx.PutBuild(Build{ID: "b2", Steps: map[string]string{"build": "v2"}, RegisteredAt: t0.Add(time.Hour)})
	})
	stale, err = s.StalePartitions(ctx, "t", "")
	require.NoError(t, err)
	require.Len(t, stale, 2, "only mart.revenue's data versions have a step and inputs")
	assert.Equal(t, []string{"input raw.orders 2026-10-01 has a newer version o1b", "step build: code changed (v1 → v2)"}, stale[0].Reasons)
	assert.Equal(t, []string{"step build: code changed (v1 → v2)"}, stale[1].Reasons)
}

func TestExpiryCandidates(t *testing.T) {
	s, _ := openFile(t)
	const ds = "raw.orders"
	// Partition 10-01: p1 (oldest) … p5 (latest), an hour apart; 10-02: q1, q2.
	var ps []DataVersion
	for i := 1; i <= 5; i++ {
		ps = append(ps, dataVersion(fmt.Sprintf("p%d", i), ds, "2026-10-01", time.Duration(i)*time.Hour))
	}
	ps = append(ps, dataVersion("q1", ds, "2026-10-02", time.Hour), dataVersion("q2", ds, "2026-10-02", 2*time.Hour))
	invalid := dataVersion("p0", ds, "2026-10-01", 0)
	invalid.State = DataVersionInvalid
	ps = append(ps, invalid)
	ps[1].RunID = "h-open" // p2: built by a run that will be running
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

	// p2 was built by a run in progress, p1 is an input of an unfinished step
	// and q1 a step result in a run in progress: all in use.
	l := step("h-open", 1, StepRunning)
	l.Inputs = json.RawMessage(`[{"name":"orders","data_version":"p1"}]`)
	put(t, s, []Event{{Type: "StepFinished", RunID: "h-open", Tenant: "t", Payload: json.RawMessage(`{"seq":2,"data_version":"q1"}`)}},
		func(tx *Tx) error { return tx.PutRun(run("h-open", RunRunning)) }, func(tx *Tx) error { return tx.PutStep(l) })
	assert.Equal(t, []string{"p3", "p4"}, got(Retention{Versions: 1}), "p1, p2 and q1 are in use")
}
