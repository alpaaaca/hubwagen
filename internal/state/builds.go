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
	Hauls        []string            // empty for builds registered before hauls were recorded
	Info         map[string]HaulInfo // by haul name; only hauls that have any
	Lifts        map[string]string   // lift name → code version
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
		info := v.Info[haul]
		if err := t.exec(`INSERT INTO build_hauls (build_id, haul, title, description) VALUES (?, ?, ?, ?)
			ON CONFLICT (build_id, haul) DO NOTHING`, v.ID, haul, info.Title, info.Description); err != nil {
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

// HaulInfo describes a haul for people.
type HaulInfo struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// Pipeline is a haul some build defines, as the newest such build describes it.
type Pipeline struct {
	Name string
	HaulInfo
}

// ListPipelines returns every haul a build defines, by name.
func (s *Store) ListPipelines(ctx context.Context) ([]Pipeline, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (Pipeline, error) {
		var p Pipeline
		err := sc.Scan(&p.Name, &p.Title, &p.Description)
		return p, err
	}, `SELECT bh.haul, bh.title, bh.description FROM build_hauls bh JOIN builds b ON b.id = bh.build_id
		WHERE (b.registered_at, b.id) = (
			SELECT b2.registered_at, b2.id FROM build_hauls bh2 JOIN builds b2 ON b2.id = bh2.build_id
			WHERE bh2.haul = bh.haul ORDER BY b2.registered_at DESC, b2.id DESC LIMIT 1)
		ORDER BY bh.haul`)
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

// Code is a pipeline or step as the newest build that defines it has it:
// what the server runs now. Version is the step's code version (empty for
// a pipeline: builds record which pipelines they define, not their code).
type Code struct {
	Name         string
	Version      string
	BuildID      string
	RegisteredAt time.Time
}

// CurrentCode returns every step and pipeline some build defines, by name,
// from the newest build that defines it.
func (s *Store) CurrentCode(ctx context.Context) (steps, pipelines []Code, err error) {
	scan := func(sc interface{ Scan(...any) error }) (Code, error) {
		var c Code
		var at string
		if err := sc.Scan(&c.Name, &c.Version, &c.BuildID, &at); err != nil {
			return c, err
		}
		c.RegisteredAt, err = parseTime(at)
		return c, err
	}
	steps, err = queryAll(ctx, s.r, scan, `
		SELECT bl.lift, bl.code_version, b.id, b.registered_at FROM build_lifts bl JOIN builds b ON b.id = bl.build_id
		WHERE (b.registered_at, b.id) = (
			SELECT b2.registered_at, b2.id FROM build_lifts bl2 JOIN builds b2 ON b2.id = bl2.build_id
			WHERE bl2.lift = bl.lift ORDER BY b2.registered_at DESC, b2.id DESC LIMIT 1)
		ORDER BY bl.lift`)
	if err != nil {
		return nil, nil, err
	}
	pipelines, err = queryAll(ctx, s.r, scan, `
		SELECT bh.haul, '', b.id, b.registered_at FROM build_hauls bh JOIN builds b ON b.id = bh.build_id
		WHERE (b.registered_at, b.id) = (
			SELECT b2.registered_at, b2.id FROM build_hauls bh2 JOIN builds b2 ON b2.id = bh2.build_id
			WHERE bh2.haul = bh.haul ORDER BY b2.registered_at DESC, b2.id DESC LIMIT 1)
		ORDER BY bh.haul`)
	return steps, pipelines, err
}
