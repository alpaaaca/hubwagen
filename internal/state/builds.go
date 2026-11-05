package state

import (
	"context"
	"time"
)

// EventBuildRegistered: a worker build announced its runs and its steps'
// code versions.
const EventBuildRegistered EventType = "BuildRegistered"

// Build is a worker build: the runs it can replay and the code version of
// each step it can run.
type Build struct {
	ID           string
	Pipelines    []string           // empty for builds registered before runs were recorded
	Info         map[string]RunInfo // by run name; only runs that have any
	Steps        map[string]string  // step name → code version
	RegisteredAt time.Time
}

// PutBuild records a build. Builds are immutable: writing one again is a
// no-op (the core rejects a build ID reused with other versions).
func (t *Tx) PutBuild(v Build) error {
	if err := t.exec(`INSERT INTO builds (id, registered_at) VALUES (?, ?) ON CONFLICT (id) DO NOTHING`,
		v.ID, formatTime(v.RegisteredAt)); err != nil {
		return err
	}
	for _, run := range v.Pipelines {
		info := v.Info[run]
		if err := t.exec(`INSERT INTO build_pipelines (build_id, run, title, description) VALUES (?, ?, ?, ?)
			ON CONFLICT (build_id, run) DO NOTHING`, v.ID, run, info.Title, info.Description); err != nil {
			return err
		}
	}
	for step, version := range v.Steps {
		if err := t.exec(`
			INSERT INTO build_steps (build_id, step, code_version) VALUES (?, ?, ?)
			ON CONFLICT (build_id, step) DO NOTHING`, v.ID, step, version); err != nil {
			return err
		}
	}
	return nil
}

// RunInfo describes a run for people.
type RunInfo struct {
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
}

// Pipeline is a pipeline as the newest build that defines it describes it.
type Pipeline struct {
	Name string
	RunInfo
}

// ListPipelines returns every pipeline a build defines, by name.
func (s *Store) ListPipelines(ctx context.Context) ([]Pipeline, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (Pipeline, error) {
		var p Pipeline
		err := sc.Scan(&p.Name, &p.Title, &p.Description)
		return p, err
	}, `SELECT bh.run, bh.title, bh.description FROM build_pipelines bh JOIN builds b ON b.id = bh.build_id
		WHERE (b.registered_at, b.id) = (
			SELECT b2.registered_at, b2.id FROM build_pipelines bh2 JOIN builds b2 ON b2.id = bh2.build_id
			WHERE bh2.run = bh.run ORDER BY b2.registered_at DESC, b2.id DESC LIMIT 1)
		ORDER BY bh.run`)
}

// LoadBuilds reads every build, for a new leader.
func (s *Store) LoadBuilds(ctx context.Context) (map[string]Build, error) {
	rows, err := s.r.QueryContext(ctx, `
		SELECT b.id, b.registered_at, l.step, l.code_version
		FROM builds b LEFT JOIN build_steps l ON l.build_id = b.id ORDER BY b.id, l.step`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Build{}
	for rows.Next() {
		var (
			id, registered string
			step, version  *string
		)
		if err := rows.Scan(&id, &registered, &step, &version); err != nil {
			return nil, err
		}
		b, ok := out[id]
		if !ok {
			at, err := parseTime(registered)
			if err != nil {
				return nil, err
			}
			b = Build{ID: id, Steps: map[string]string{}, RegisteredAt: at}
		}
		if step != nil {
			b.Steps[*step] = *version
		}
		out[id] = b
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	runs, err := s.r.QueryContext(ctx, `SELECT build_id, run FROM build_pipelines ORDER BY build_id, run`)
	if err != nil {
		return nil, err
	}
	defer runs.Close()
	for runs.Next() {
		var id, run string
		if err := runs.Scan(&id, &run); err != nil {
			return nil, err
		}
		if b, ok := out[id]; ok {
			b.Pipelines = append(b.Pipelines, run)
			out[id] = b
		}
	}
	return out, runs.Err()
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
		SELECT bl.step, bl.code_version, b.id, b.registered_at FROM build_steps bl JOIN builds b ON b.id = bl.build_id
		WHERE (b.registered_at, b.id) = (
			SELECT b2.registered_at, b2.id FROM build_steps bl2 JOIN builds b2 ON b2.id = bl2.build_id
			WHERE bl2.step = bl.step ORDER BY b2.registered_at DESC, b2.id DESC LIMIT 1)
		ORDER BY bl.step`)
	if err != nil {
		return nil, nil, err
	}
	pipelines, err = queryAll(ctx, s.r, scan, `
		SELECT bh.run, '', b.id, b.registered_at FROM build_pipelines bh JOIN builds b ON b.id = bh.build_id
		WHERE (b.registered_at, b.id) = (
			SELECT b2.registered_at, b2.id FROM build_pipelines bh2 JOIN builds b2 ON b2.id = bh2.build_id
			WHERE bh2.run = bh.run ORDER BY b2.registered_at DESC, b2.id DESC LIMIT 1)
		ORDER BY bh.run`)
	return steps, pipelines, err
}
