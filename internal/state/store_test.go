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

func TestAppendAssignsSeqAndTerm(t *testing.T) {
	s, _ := openFile(t)
	term := s.Term()

	got, err := s.Append(ctx, term, []Event{
		{Type: EventLeaderTermStarted, Time: t0},
		{Type: EventHaulStarted, Tenant: "t", HaulID: "h1", Time: t0, Payload: json.RawMessage(`{"name":"x"}`)},
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
	boom := errors.New("boom")
	_, err := s.Append(ctx, s.Term(), []Event{{Type: EventHaulStarted, HaulID: "h1"}}, func(tx *Tx) error {
		require.NoError(t, tx.PutHaul(haul("h1", HaulRunning)))
		return boom
	})
	assert.ErrorIs(t, err, boom)

	_, err = s.GetHaul(ctx, "h1")
	assert.ErrorIs(t, err, ErrNotFound, "projection write rolled back")
	evs, err := s.HaulEvents(ctx, "h1")
	require.NoError(t, err)
	assert.Empty(t, evs, "event rolled back with it")
}

func TestTerms(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path)
	require.NoError(t, err)

	assert.Equal(t, int64(0), s.Term())
	_, err = s.Append(ctx, 0, []Event{{Type: EventLeaderTermStarted}}, nil)
	assert.ErrorIs(t, err, ErrStaleTerm, "no term started yet")

	term, err := s.StartTerm(ctx, 0)
	require.NoError(t, err)
	assert.Equal(t, int64(1), term)

	_, err = s.StartTerm(ctx, 1)
	assert.ErrorIs(t, err, ErrStaleTerm, "explicit terms must increase")
	term, err = s.StartTerm(ctx, 7)
	require.NoError(t, err)
	assert.Equal(t, int64(7), term)

	_, err = s.Append(ctx, 1, []Event{{Type: EventLeaderTermStarted}}, nil)
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

	_, err = old.Append(ctx, old.Term(), []Event{{Type: EventLeaderTermStarted}}, nil)
	assert.ErrorIs(t, err, ErrStaleTerm, "the old writer's in-memory term looks valid, the database's doesn't")
}

func TestEventsSinceFiltersByTenant(t *testing.T) {
	s, _ := openFile(t)
	_, err := s.Append(ctx, s.Term(), []Event{
		{Type: EventHaulStarted, Tenant: "a"}, {Type: EventHaulStarted, Tenant: "b"}, {Type: EventHaulStarted, Tenant: "a"},
	}, nil)
	require.NoError(t, err)

	evs, err := s.EventsSince(ctx, 1, "a", 10)
	require.NoError(t, err)
	require.Len(t, evs, 1)
	assert.Equal(t, int64(3), evs[0].Seq)
}

func TestMemoryStore(t *testing.T) {
	s, err := Open(":memory:")
	require.NoError(t, err)
	defer s.Close()
	_, err = s.StartTerm(ctx, 0)
	require.NoError(t, err)
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(haul("h1", HaulRunning)) })
	h, err := s.GetHaul(ctx, "h1")
	require.NoError(t, err)
	assert.Equal(t, HaulRunning, h.Status)
}

func TestReadsRunConcurrentlyWithWrites(t *testing.T) {
	s, _ := openFile(t)

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
				_, err := s.EventsSince(ctx, 0, "", 10)
				assert.NoError(t, err)
			}
		}()
	}
	for i := 0; i < 50; i++ {
		id := fmt.Sprintf("h%03d", i)
		_, err := s.Append(ctx, s.Term(), []Event{{Type: EventHaulStarted, HaulID: id}}, func(tx *Tx) error {
			return tx.PutHaul(haul(id, HaulRunning))
		})
		require.NoError(t, err)
	}
	close(stop)
	wg.Wait()

	evs, err := s.EventsSince(ctx, 0, "", 100)
	require.NoError(t, err)
	assert.Len(t, evs, 50)
}

func TestPathWithSpaces(t *testing.T) {
	path := filepath.Join(t.TempDir(), "dir with spaces", "state.db")
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o755))
	s, err := Open(path)
	require.NoError(t, err)
	defer s.Close()
	_, err = s.StartTerm(ctx, 0)
	require.NoError(t, err)
	put(t, s, nil, func(tx *Tx) error { return tx.PutHaul(haul("h1", HaulRunning)) })
	_, err = s.GetHaul(ctx, "h1")
	require.NoError(t, err)
}

// BenchmarkAppend measures group commit: one fsync per batch, so larger
// batches amortise it. Input for spike S3 (single-leader throughput).
func BenchmarkAppend(b *testing.B) {
	for _, size := range []int{1, 16, 128} {
		b.Run(fmt.Sprintf("batch=%d", size), func(b *testing.B) {
			s, err := Open(filepath.Join(b.TempDir(), "bench.db"))
			require.NoError(b, err)
			defer s.Close()
			_, err = s.StartTerm(ctx, 0)
			require.NoError(b, err)
			_, err = s.Append(ctx, s.Term(), nil, func(tx *Tx) error { return tx.PutHaul(haul("h", HaulRunning)) })
			require.NoError(b, err)

			events := make([]Event, size)
			for i := range events {
				events[i] = Event{Type: EventLiftScheduled, Tenant: "t", HaulID: "h", Time: t0, Payload: json.RawMessage(`{"seq":1}`)}
			}
			seq := 0
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_, err := s.Append(ctx, s.Term(), events, func(tx *Tx) error {
					for j := 0; j < size; j++ {
						seq++
						if err := tx.PutLift(lift("h", seq, LiftScheduled)); err != nil {
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
