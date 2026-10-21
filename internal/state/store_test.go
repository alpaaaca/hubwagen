package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	ctx = context.Background()
	t0  = time.Date(2026, 10, 21, 2, 0, 0, 123456789, time.UTC)
)

// openFile opens a store on a temp file with a started term. File-backed
// tests exercise the real writer/reader split; :memory: is covered separately.
func openFile(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	_, err = s.StartTerm(ctx, 0)
	require.NoError(t, err)
	return s, path
}

func ptr[T any](v T) *T { return &v }

// seed publishes one pipeline version for tenant "t", pipeline "p".
func seed(t *testing.T, s *Store) {
	t.Helper()
	_, err := s.Append(ctx, s.Term(), []Event{{Type: EventPipelineVersionPublished, Tenant: "t"}}, func(tx *Tx) error {
		if err := tx.PutTenant(Tenant{Name: "t", Weight: 1, CreatedAt: t0}); err != nil {
			return err
		}
		if err := tx.PutVersion(PipelineVersion{Hash: "h1", Tenant: "t", Pipeline: "p", SpecJSON: []byte(`{"name":"p"}`), PublishedAt: t0}); err != nil {
			return err
		}
		return tx.PutPipeline(Pipeline{Tenant: "t", Name: "p", CurrentVersion: "h1"})
	})
	require.NoError(t, err)
}

func run(id, key string, gen int, status string) Run {
	return Run{
		ID: id, Tenant: "t", Pipeline: "p", Version: "h1", RunKey: key, Generation: gen,
		Trigger: "cron", Priority: "scheduled", Status: status, CreatedAt: t0,
		IntervalStart: ptr(t0.Add(-24 * time.Hour)), IntervalEnd: ptr(t0),
		Params: map[string]any{"full_refresh": true, "n": 3},
	}
}

func TestAppendAssignsSeqAndTerm(t *testing.T) {
	s, _ := openFile(t)
	term := s.Term()

	got, err := s.Append(ctx, term, []Event{
		{Type: EventCronTicked, Tenant: "t", Time: t0},
		{Type: EventRunCreated, Tenant: "t", RunID: "r1", Time: t0, Payload: json.RawMessage(`{"run_key":"k"}`)},
	}, nil)
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, int64(1), got[0].Seq)
	assert.Equal(t, int64(2), got[1].Seq)
	assert.Equal(t, term, got[1].Term)

	evs, err := s.EventsSince(ctx, 0, "", 0)
	require.NoError(t, err)
	require.Len(t, evs, 2)
	assert.Equal(t, got[1], evs[1])
	assert.JSONEq(t, `{}`, string(evs[0].Payload), "nil payload is stored as {}")
}

func TestAppendIsAtomic(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)

	boom := errors.New("boom")
	_, err := s.Append(ctx, s.Term(), []Event{{Type: EventRunCreated, RunID: "r1"}}, func(tx *Tx) error {
		require.NoError(t, tx.PutRun(run("r1", "k", 1, RunQueued)))
		return boom
	})
	assert.ErrorIs(t, err, boom)

	_, err = s.GetRun(ctx, "r1")
	assert.ErrorIs(t, err, ErrNotFound, "projection write rolled back")
	evs, err := s.RunEvents(ctx, "r1")
	require.NoError(t, err)
	assert.Empty(t, evs, "event rolled back with it")
}

func TestDuplicateRunKeyFailsTheBatch(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)

	put := func(id string, gen int) error {
		_, err := s.Append(ctx, s.Term(), []Event{{Type: EventRunCreated, RunID: id}}, func(tx *Tx) error {
			return tx.PutRun(run(id, "t/p/2026-10-20", gen, RunQueued))
		})
		return err
	}
	require.NoError(t, put("r1", 1))
	assert.Error(t, put("r2", 1), "same key and generation")
	require.NoError(t, put("r3", 2), "next generation is fine")

	latest, err := s.LatestRunByKey(ctx, "t/p/2026-10-20")
	require.NoError(t, err)
	assert.Equal(t, "r3", latest.ID)
	evs, _ := s.RunEvents(ctx, "r2")
	assert.Empty(t, evs)
}

func TestTerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	require.NoError(t, err)

	assert.Equal(t, int64(0), s.Term())
	_, err = s.Append(ctx, 0, []Event{{Type: EventCronTicked}}, nil)
	assert.ErrorIs(t, err, ErrStaleTerm, "no term started yet")

	term, err := s.StartTerm(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), term)

	_, err = s.StartTerm(ctx, 1)
	assert.ErrorIs(t, err, ErrStaleTerm, "explicit terms must increase")
	term, err = s.StartTerm(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, int64(7), term)

	_, err = s.Append(ctx, 1, []Event{{Type: EventCronTicked}}, nil)
	assert.ErrorIs(t, err, ErrStaleTerm)
	require.NoError(t, s.Close())

	// The term survives a restart; the next single-leader term follows it.
	s, err = Open(path)
	require.NoError(t, err)
	defer s.Close()
	assert.Equal(t, int64(7), s.Term())
	term, err = s.StartTerm(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(8), term)
}

func TestAppendFencedByNewerTermFromAnotherProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	old, err := Open(path)
	require.NoError(t, err)
	defer old.Close()
	_, err = old.StartTerm(ctx, 0)
	require.NoError(t, err)

	newer, err := Open(path)
	require.NoError(t, err)
	defer newer.Close()
	_, err = newer.StartTerm(ctx, 0)
	require.NoError(t, err)

	_, err = old.Append(ctx, old.Term(), []Event{{Type: EventCronTicked}}, nil)
	assert.ErrorIs(t, err, ErrStaleTerm, "the old writer's in-memory term looks valid, the database's doesn't")
}

func TestProjectionsRoundTrip(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)

	r := run("r1", "k", 1, RunRunning)
	r.StartedAt = ptr(t0.Add(time.Second))
	tr := TaskRun{RunID: "r1", TaskID: "extract", Status: TaskRunning, Attempts: 1, ReadyAt: ptr(t0), StartedAt: ptr(t0)}
	att := Attempt{
		ID: "a1", RunID: "r1", TaskID: "extract", Number: 1, Term: s.Term(), Runtime: "agent", Pool: "local",
		Status: AttemptSucceeded, Reason: "exit_code", ExitCode: ptr(0), StagingURI: "file:///b/x/attempt-1/",
		LogsURI: "file:///b/logs/", Result: json.RawMessage(`{"rows":3}`), DispatchedAt: t0, FinishedAt: ptr(t0.Add(time.Minute)),
	}
	out := Output{RunID: "r1", TaskID: "extract", Name: "orders", URI: "file:///b/x/attempt-1/", AttemptID: "a1", Metadata: map[string]any{"rows": 3}}

	_, err := s.Append(ctx, s.Term(), []Event{{Type: EventAttemptFinished, RunID: "r1"}}, func(tx *Tx) error {
		for _, f := range []func() error{
			func() error { return tx.PutRun(r) },
			func() error { return tx.PutTaskRun(tr) },
			func() error { return tx.PutAttempt(att) },
			func() error { return tx.PutOutput(out) },
			func() error { return tx.PutScheduleCursor(ScheduleCursor{Tenant: "t", Pipeline: "p", LastTick: t0}) },
		} {
			if err := f(); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	gotRun, err := s.GetRun(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"full_refresh": true, "n": json.Number("3")}, gotRun.Params)
	gotRun.Params = r.Params
	assert.Equal(t, r, gotRun)

	trs, err := s.ListTaskRuns(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, []TaskRun{tr}, trs)

	atts, err := s.ListAttempts(ctx, "r1", "extract")
	require.NoError(t, err)
	assert.Equal(t, []Attempt{att}, atts)

	outs, err := s.ListOutputs(ctx, "r1")
	require.NoError(t, err)
	require.Len(t, outs, 1)
	assert.Equal(t, json.Number("3"), outs[0].Metadata["rows"])
}

func TestUpsertUpdatesMutableFieldsOnly(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)
	write := func(r Run) {
		_, err := s.Append(ctx, s.Term(), nil, func(tx *Tx) error { return tx.PutRun(r) })
		require.NoError(t, err)
	}

	r := run("r1", "k", 1, RunQueued)
	write(r)
	r.Status, r.StartedAt, r.FinishedAt = RunSucceeded, ptr(t0), ptr(t0.Add(time.Hour))
	r.RunKey = "changed" // identity fields are fixed at creation
	write(r)

	got, err := s.GetRun(ctx, "r1")
	require.NoError(t, err)
	assert.Equal(t, RunSucceeded, got.Status)
	assert.Equal(t, "k", got.RunKey)
	assert.Equal(t, t0.Add(time.Hour), *got.FinishedAt)
}

func TestLoadCatalogAndActive(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)

	_, err := s.Append(ctx, s.Term(), nil, func(tx *Tx) error {
		// h1 stays in use by an active run after the pipeline moves to h2;
		// h0 is neither current nor in use and must not be loaded.
		for _, v := range []PipelineVersion{
			{Hash: "h0", Tenant: "t", Pipeline: "p", SpecJSON: []byte(`{}`), PublishedAt: t0.Add(-time.Hour)},
			{Hash: "h2", Tenant: "t", Pipeline: "p", SpecJSON: []byte(`{}`), PublishedAt: t0.Add(time.Hour)},
		} {
			if err := tx.PutVersion(v); err != nil {
				return err
			}
		}
		if err := tx.PutPipeline(Pipeline{Tenant: "t", Name: "p", CurrentVersion: "h2"}); err != nil {
			return err
		}
		for _, r := range []Run{run("r1", "k1", 1, RunRunning), run("r2", "k2", 1, RunSucceeded), run("r3", "k3", 1, RunQueued)} {
			if err := tx.PutRun(r); err != nil {
				return err
			}
			if err := tx.PutTaskRun(TaskRun{RunID: r.ID, TaskID: "a", Status: TaskPending}); err != nil {
				return err
			}
		}
		return tx.PutAttempt(Attempt{ID: "x1", RunID: "r1", TaskID: "a", Number: 1, Term: 1, Runtime: "agent", Status: AttemptRunning, DispatchedAt: t0})
	})
	require.NoError(t, err)

	cat, err := s.LoadCatalog(ctx)
	require.NoError(t, err)
	assert.Len(t, cat.Tenants, 1)
	assert.Equal(t, []Pipeline{{Tenant: "t", Name: "p", CurrentVersion: "h2"}}, cat.Pipelines)
	assert.ElementsMatch(t, []string{"h1", "h2"}, keys(cat.Versions))

	act, err := s.LoadActive(ctx)
	require.NoError(t, err)
	var ids []string
	for _, r := range act.Runs {
		ids = append(ids, r.ID)
	}
	assert.Equal(t, []string{"r1", "r3"}, ids)
	assert.Len(t, act.TaskRuns, 2)
	assert.Len(t, act.Attempts, 1)
	assert.Equal(t, int64(1), act.LastSeq, "only seed wrote an event")
}

func keys[V any](m map[string]V) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestListRunsFiltersAndPages(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)
	_, err := s.Append(ctx, s.Term(), nil, func(tx *Tx) error {
		for i := 1; i <= 5; i++ {
			st := RunSucceeded
			if i%2 == 0 {
				st = RunFailed
			}
			if err := tx.PutRun(run(fmt.Sprintf("r%d", i), fmt.Sprintf("k%d", i), 1, st)); err != nil {
				return err
			}
		}
		return nil
	})
	require.NoError(t, err)

	ids := func(f RunFilter) []string {
		runs, err := s.ListRuns(ctx, f)
		require.NoError(t, err)
		var out []string
		for _, r := range runs {
			out = append(out, r.ID)
		}
		return out
	}
	assert.Equal(t, []string{"r5", "r4", "r3", "r2", "r1"}, ids(RunFilter{}))
	assert.Equal(t, []string{"r4", "r2"}, ids(RunFilter{Tenant: "t", Pipeline: "p", Statuses: []string{RunFailed}}))
	assert.Equal(t, []string{"r5", "r4"}, ids(RunFilter{Limit: 2}))
	assert.Equal(t, []string{"r3", "r2"}, ids(RunFilter{BeforeID: "r4", Limit: 2}))
	assert.Empty(t, ids(RunFilter{Tenant: "other"}))
}

func TestEventsSinceFiltersByTenant(t *testing.T) {
	s, _ := openFile(t)
	_, err := s.Append(ctx, s.Term(), []Event{
		{Type: EventCronTicked, Tenant: "a"}, {Type: EventCronTicked, Tenant: "b"}, {Type: EventCronTicked, Tenant: "a"},
	}, nil)
	require.NoError(t, err)

	evs, err := s.EventsSince(ctx, 1, "a", 10)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, int64(3), evs[0].Seq)
}

func TestPipelineQueries(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)

	p, err := s.GetPipeline(ctx, "t", "p")
	require.NoError(t, err)
	assert.Equal(t, "h1", p.CurrentVersion)
	_, err = s.GetPipeline(ctx, "t", "nope")
	assert.ErrorIs(t, err, ErrNotFound)

	v, err := s.GetVersion(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, `{"name":"p"}`, string(v.SpecJSON))
	assert.Equal(t, t0, v.PublishedAt)

	// Republishing the same hash is a no-op.
	_, err = s.Append(ctx, s.Term(), nil, func(tx *Tx) error {
		return tx.PutVersion(PipelineVersion{Hash: "h1", Tenant: "t", Pipeline: "p", SpecJSON: []byte(`{"other":1}`), PublishedAt: t0.Add(time.Hour)})
	})
	require.NoError(t, err)
	vs, err := s.ListVersions(ctx, "t", "p")
	require.NoError(t, err)
	require.Len(t, vs, 1)
	assert.Equal(t, `{"name":"p"}`, string(vs[0].SpecJSON))

	ps, err := s.ListPipelines(ctx, "")
	require.NoError(t, err)
	assert.Len(t, ps, 1)
}

func TestForeignKeysCatchBadReferences(t *testing.T) {
	s, _ := openFile(t)
	_, err := s.Append(ctx, s.Term(), nil, func(tx *Tx) error {
		return tx.PutTaskRun(TaskRun{RunID: "missing", TaskID: "a", Status: TaskPending})
	})
	assert.Error(t, err)
}

func TestMemoryStore(t *testing.T) {
	s, err := Open(":memory:")
	require.NoError(t, err)
	defer s.Close()
	_, err = s.StartTerm(ctx, 0)
	require.NoError(t, err)
	seed(t, s)
	p, err := s.GetPipeline(ctx, "t", "p")
	require.NoError(t, err)
	assert.Equal(t, "h1", p.CurrentVersion)
}

func TestReadsRunConcurrentlyWithWrites(t *testing.T) {
	s, _ := openFile(t)
	seed(t, s)

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				_, err := s.ListRuns(ctx, RunFilter{Limit: 10})
				assert.NoError(t, err)
			}
		}()
	}
	for i := 0; i < 50; i++ {
		_, err := s.Append(ctx, s.Term(), []Event{{Type: EventRunCreated}}, func(tx *Tx) error {
			return tx.PutRun(run(fmt.Sprintf("r%03d", i), fmt.Sprintf("k%d", i), 1, RunQueued))
		})
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()

	runs, err := s.ListRuns(ctx, RunFilter{Limit: 100})
	require.NoError(t, err)
	assert.Len(t, runs, 50)
}

func TestPathWithSpaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir with spaces", "state.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	s, err := Open(path)
	require.NoError(t, err)
	defer s.Close()
	_, err = s.StartTerm(ctx, 0)
	require.NoError(t, err)
	seed(t, s)
	_, err = s.GetPipeline(ctx, "t", "p")
	require.NoError(t, err)
}

// BenchmarkAppend measures group commit: one fsync per batch, so larger
// batches amortise it. Input for spike R3 (single-leader throughput).
func BenchmarkAppend(b *testing.B) {
	for _, size := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			s, err := Open(filepath.Join(b.TempDir(), "bench.db"))
			require.NoError(b, err)
			defer s.Close()
			_, err = s.StartTerm(ctx, 0)
			require.NoError(b, err)

			seedTx := func(tx *Tx) error {
				if err := tx.PutTenant(Tenant{Name: "t", CreatedAt: t0}); err != nil {
					return err
				}
				if err := tx.PutVersion(PipelineVersion{Hash: "h1", Tenant: "t", Pipeline: "p", SpecJSON: []byte(`{}`), PublishedAt: t0}); err != nil {
					return err
				}
				if err := tx.PutPipeline(Pipeline{Tenant: "t", Name: "p", CurrentVersion: "h1"}); err != nil {
					return err
				}
				return tx.PutRun(run("r", "k", 1, RunRunning))
			}
			_, err = s.Append(ctx, s.Term(), nil, seedTx)
			require.NoError(b, err)

			events := make([]Event, size)
			for i := range events {
				events[i] = Event{Type: EventTaskReady, Tenant: "t", RunID: "r", Time: t0, Payload: json.RawMessage(`{"task":"x"}`)}
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := s.Append(ctx, s.Term(), events, func(tx *Tx) error {
					for j := 0; j < size; j++ {
						if err := tx.PutTaskRun(TaskRun{RunID: "r", TaskID: fmt.Sprintf("t%d", j), Status: TaskReady, ReadyAt: &t0}); err != nil {
							return err
						}
					}
					return nil
				})
				if err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.N*size)/b.Elapsed().Seconds(), "events/s")
		})
	}
}
