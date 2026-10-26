package state

import (
	"context"
	"time"
)

// EventBuildRegistered: a worker build announced its lifts' code versions.
const EventBuildRegistered EventType = "BuildRegistered"

// Build is a worker build: the code version of each lift it can run.
type Build struct {
	ID           string
	Lifts        map[string]string // lift name → code version
	RegisteredAt time.Time
}

// PutBuild records a build. Builds are immutable: writing one again is a
// no-op (the core rejects a build ID reused with other versions).
func (t *Tx) PutBuild(v Build) error {
	if err := t.exec(`INSERT INTO builds (id, registered_at) VALUES (?, ?) ON CONFLICT (id) DO NOTHING`,
		v.ID, formatTime(v.RegisteredAt)); err != nil {
		return err
	}
	for lift, version := range v.Lifts {
		if err := t.exec(`
			INSERT INTO build_lifts (build_id, lift, code_version) VALUES (?, ?, ?)
			ON CONFLICT (build_id, lift) DO NOTHING`, v.ID, lift, version); err != nil {
			return err
		}
	}
	return nil
}

// LoadBuilds reads every build, for a new leader.
func (s *Store) LoadBuilds(ctx context.Context) (map[string]Build, error) {
	rows, err := s.r.QueryContext(ctx, `
		SELECT b.id, b.registered_at, l.lift, l.code_version
		FROM builds b LEFT JOIN build_lifts l ON l.build_id = b.id ORDER BY b.id, l.lift`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Build{}
	for rows.Next() {
		var (
			id, registered string
			lift, version  *string
		)
		if err := rows.Scan(&id, &registered, &lift, &version); err != nil {
			return nil, err
		}
		b, ok := out[id]
		if !ok {
			at, err := parseTime(registered)
			if err != nil {
				return nil, err
			}
			b = Build{ID: id, Lifts: map[string]string{}, RegisteredAt: at}
		}
		if lift != nil {
			b.Lifts[*lift] = *version
		}
		out[id] = b
	}
	return out, rows.Err()
}
