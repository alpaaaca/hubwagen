package state

import (
	"context"
	"time"
)

// EventBuildRegistered: a worker build announced its hauls and its lifts'
// code versions.
const EventBuildRegistered EventType = "BuildRegistered"

// Build is a worker build: the hauls it can replay and the code version of
// each lift it can run.
type Build struct {
	ID           string
	Hauls        []string          // empty for builds registered before hauls were recorded
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
	for _, haul := range v.Hauls {
		if err := t.exec(`INSERT INTO build_hauls (build_id, haul) VALUES (?, ?) ON CONFLICT (build_id, haul) DO NOTHING`,
			v.ID, haul); err != nil {
			return err
		}
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	hauls, err := s.r.QueryContext(ctx, `SELECT build_id, haul FROM build_hauls ORDER BY build_id, haul`)
	if err != nil {
		return nil, err
	}
	defer hauls.Close()
	for hauls.Next() {
		var id, haul string
		if err := hauls.Scan(&id, &haul); err != nil {
			return nil, err
		}
		if b, ok := out[id]; ok {
			b.Hauls = append(b.Hauls, haul)
			out[id] = b
		}
	}
	return out, hauls.Err()
}
