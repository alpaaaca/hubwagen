package state

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
)

// Read side. Safe to call from any goroutine; on a file database these use
// the read-only pool and see the last committed state.

// decodeObject decodes a JSON object, keeping numbers as json.Number so
// integer params round-trip exactly. "{}" decodes to nil.
func decodeObject(s string) (map[string]any, error) {
	if s == "" || s == "{}" {
		return nil, nil
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(s)))
	dec.UseNumber()
	var m map[string]any
	err := dec.Decode(&m)
	return m, err
}

const eventColumns = `seq, term, ts, tenant, type, attempt_id, haul_id, payload`

func scanEvent(sc interface{ Scan(...any) error }) (Event, error) {
	var (
		v       Event
		ts, typ string
		payload string
	)
	if err := sc.Scan(&v.Seq, &v.Term, &ts, &v.Tenant, &typ, &v.AttemptID, &v.HaulID, &payload); err != nil {
		return v, err
	}
	v.Type = EventType(typ)
	v.Payload = json.RawMessage(payload)
	var err error
	v.Time, err = parseTime(ts)
	return v, err
}

// EventsSince returns up to limit events with seq > afterSeq, oldest first,
// for one tenant or all if tenant is empty. Live subscribers use it to catch
// up after a reconnect.
func (s *Store) EventsSince(ctx context.Context, afterSeq int64, tenant string, limit int) ([]Event, error) {
	if limit <= 0 {
		limit = 500
	}
	if tenant == "" {
		return queryAll(ctx, s.r, scanEvent,
			`SELECT `+eventColumns+` FROM events WHERE seq > ? ORDER BY seq LIMIT ?`, afterSeq, limit)
	}
	return queryAll(ctx, s.r, scanEvent,
		`SELECT `+eventColumns+` FROM events WHERE tenant = ? AND seq > ? ORDER BY seq LIMIT ?`, tenant, afterSeq, limit)
}

type rowQueryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func queryAll[T any](ctx context.Context, db rowQueryer, scan func(interface{ Scan(...any) error }) (T, error), q string, args ...any) ([]T, error) {
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []T
	for rows.Next() {
		v, err := scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// LastSeq is the newest event's sequence number (0 if there is none).
func (s *Store) LastSeq(ctx context.Context) (int64, error) {
	var n int64
	err := s.r.QueryRowContext(ctx, `SELECT COALESCE(MAX(seq), 0) FROM events`).Scan(&n)
	return n, err
}
