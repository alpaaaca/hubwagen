package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

var (
	// ErrStaleTerm means a write carried a leadership term older than the
	// store's current one: the writer has been deposed and must stop.
	ErrStaleTerm = errors.New("state: stale leadership term")
	ErrNotFound  = errors.New("state: not found")
)

// Store is the SQLite-backed event log and projections.
//
// Writes go through one connection (Append, StartTerm). Reads use a separate
// read-only pool, so API queries never wait behind the writer — except for
// in-memory databases, which are a single connection shared by both.
type Store struct {
	w *sql.DB // single writer connection
	r *sql.DB // read pool (== w for :memory:)

	mu   sync.Mutex // serialises writes and guards term
	term int64
}

// Open opens (creating if needed) the database at path and applies
// migrations. path ":memory:" gives a private in-memory database for tests.
func Open(path string) (*Store, error) {
	if path == ":memory:" {
		db, err := sql.Open("sqlite", dsn(":memory:", false))
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(1)
		return finishOpen(db, db)
	}

	w, err := sql.Open("sqlite", dsn(path, false))
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1)
	// sql.Open is lazy: the read pool connects only on first use, after
	// finishOpen has migrated (and so created the file in WAL mode).
	r, err := sql.Open("sqlite", dsn(path, true))
	if err != nil {
		w.Close()
		return nil, err
	}
	r.SetMaxOpenConns(8)
	return finishOpen(w, r)
}

func finishOpen(w, r *sql.DB) (*Store, error) {
	s := &Store{w: w, r: r}
	if err := migrate(w); err != nil {
		s.Close()
		return nil, fmt.Errorf("migrations: %w", err)
	}
	term, err := readTerm(context.Background(), w)
	if err != nil {
		s.Close()
		return nil, err
	}
	s.term = term
	return s, nil
}

// dsn builds a modernc.org/sqlite DSN. Pragmas go in the DSN so every pooled
// connection gets them. The writer uses BEGIN IMMEDIATE so a transaction
// takes the write lock up front instead of failing on upgrade, and
// synchronous=FULL so a committed batch survives power loss.
func dsn(path string, readOnly bool) string {
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	if readOnly {
		q.Set("mode", "ro")
	} else {
		q.Add("_pragma", "journal_mode(WAL)")
		q.Add("_pragma", "synchronous(FULL)")
		q.Set("_txlock", "immediate")
	}
	if path == ":memory:" {
		return ":memory:?" + q.Encode()
	}
	return "file:" + (&url.URL{Path: path}).EscapedPath() + "?" + q.Encode()
}

func (s *Store) Close() error {
	err := s.w.Close()
	if s.r != s.w {
		err = errors.Join(err, s.r.Close())
	}
	return err
}

// Term is the leadership term this store accepts writes for.
func (s *Store) Term() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term
}

// StartTerm begins a new leadership term. term 0 means "the next one" (single
// leader profiles); otherwise term must be greater than the stored term (it
// comes from a Kubernetes Lease). After StartTerm, writes for older terms
// fail with ErrStaleTerm.
func (s *Store) StartTerm(ctx context.Context, term int64) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	tx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()

	cur, err := readTerm(ctx, tx)
	if err != nil {
		return 0, err
	}
	switch {
	case term == 0:
		term = cur + 1
	case term <= cur:
		return 0, fmt.Errorf("%w: term %d is not after current term %d", ErrStaleTerm, term, cur)
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO meta (key, value) VALUES ('term', ?) ON CONFLICT (key) DO UPDATE SET value = excluded.value`,
		strconv.FormatInt(term, 10)); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	s.term = term
	return term, nil
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func readTerm(ctx context.Context, q queryer) (int64, error) {
	var v string
	err := q.QueryRowContext(ctx, `SELECT value FROM meta WHERE key = 'term'`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}

// Append commits events and the projection updates made by apply in one
// transaction, and returns the events with Seq and Term set. If apply returns
// an error, or any write fails (for example a duplicate run key), nothing is
// committed. It returns only once the transaction is durable.
//
// apply must only write through tx; reading through the Store inside apply
// can deadlock an in-memory database.
func (s *Store) Append(ctx context.Context, term int64, events []Event, apply func(tx *Tx) error) ([]Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.term == 0 {
		return nil, fmt.Errorf("%w: no leadership term started (call StartTerm first)", ErrStaleTerm)
	}
	if term != s.term {
		return nil, fmt.Errorf("%w: append for term %d, store is at term %d", ErrStaleTerm, term, s.term)
	}

	sqlTx, err := s.w.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer sqlTx.Rollback()

	// Another process may have started a newer term on the same file.
	if cur, err := readTerm(ctx, sqlTx); err != nil {
		return nil, err
	} else if cur != term {
		return nil, fmt.Errorf("%w: database is at term %d, append for term %d", ErrStaleTerm, cur, term)
	}

	out := make([]Event, len(events))
	for i, e := range events {
		if e.Time.IsZero() {
			e.Time = time.Now()
		}
		payload := string(e.Payload)
		if payload == "" {
			payload = "{}"
		}
		res, err := sqlTx.ExecContext(ctx,
			`INSERT INTO events (term, ts, tenant, type, run_id, task_id, attempt_id, haul_id, payload)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			term, formatTime(e.Time), e.Tenant, string(e.Type), e.RunID, e.TaskID, e.AttemptID, e.HaulID, payload)
		if err != nil {
			return nil, fmt.Errorf("append %s event: %w", e.Type, err)
		}
		if e.Seq, err = res.LastInsertId(); err != nil {
			return nil, err
		}
		e.Term = term
		out[i] = e
	}

	if apply != nil {
		if err := apply(&Tx{ctx: ctx, tx: sqlTx}); err != nil {
			return nil, err
		}
	}
	if err := sqlTx.Commit(); err != nil {
		return nil, err
	}
	return out, nil
}

// tsLayout is fixed-width UTC with nanoseconds, so stored timestamps sort
// lexically in time order.
const tsLayout = "2006-01-02T15:04:05.000000000Z"

func formatTime(t time.Time) string { return t.UTC().Format(tsLayout) }

func formatTimePtr(t *time.Time) any {
	if t == nil {
		return nil
	}
	return formatTime(*t)
}

func parseTime(s string) (time.Time, error) { return time.Parse(tsLayout, s) }

func parseTimePtr(ns sql.NullString) (*time.Time, error) {
	if !ns.Valid {
		return nil, nil
	}
	t, err := parseTime(ns.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
