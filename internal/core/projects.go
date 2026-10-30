package core

import (
	"context"
	"fmt"
	"maps"
	"regexp"
	"slices"

	"github.com/alper/hubwagen/internal/state"
)

// Projects (docs/projects.md). A deployment is state: Deploy registers the
// project's build (so its pipelines are accepted and their schedules apply
// before any worker runs) and records the next version, in one batch,
// before anything acts on it. Who runs the workers (internal/pools, from
// the server's process) reads the projects back and follows them.

// ErrConflict: the request clashes with what the server has (a pipeline
// another project defines, a project with open runs).
var ErrConflict = fmt.Errorf("conflict")

// DeployRequest is a project's new version.
type DeployRequest struct {
	Project string
	Build   BuildRequest
	// Image: its workers are containers of it. Dir (and Python): they run
	// in place, on the server's machine. Exactly one.
	Image  string
	Dir    string
	Python string
	Env    map[string]string
	Pools  []state.DeployedPool
	By     string
}

var projectName = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]*$`)

// Deploy records a project's next version and returns it.
func (c *Core) Deploy(ctx context.Context, req DeployRequest) (state.Deployment, error) {
	return submit(ctx, c, func(c *Core) (state.Deployment, error) {
		if err := c.checkDeploy(req); err != nil {
			return state.Deployment{}, err
		}
		if err := c.registerBuild(req.Build); err != nil {
			return state.Deployment{}, err
		}
		version := 1
		if p := c.projects[req.Project]; p != nil {
			version = p.Version + 1
		}
		d := state.Deployment{Project: req.Project, Version: version, BuildID: req.Build.ID, Image: req.Image,
			Dir: req.Dir, Python: req.Python, Env: req.Env, Pools: req.Pools,
			Hauls: slices.Compact(slices.Sorted(slices.Values(req.Build.Hauls))), By: req.By, DeployedAt: c.now()}
		p := &state.Project{Deployment: d}
		c.projects[req.Project] = p
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutProject(*p) })
		c.emit(state.EventProjectDeployed, "", d)
		return d, nil
	})
}

func (c *Core) checkDeploy(req DeployRequest) error {
	if !projectName.MatchString(req.Project) {
		return fmt.Errorf("%w: project name %q: lower-case letters, digits, - and _", ErrInvalid, req.Project)
	}
	if (req.Image == "") == (req.Dir == "") {
		return fmt.Errorf("%w: project %s: give an image or a directory", ErrInvalid, req.Project)
	}
	if len(req.Pools) == 0 {
		return fmt.Errorf("%w: project %s: at least one pool", ErrInvalid, req.Project)
	}
	seen := map[string]bool{}
	for _, p := range req.Pools {
		switch {
		case !projectName.MatchString(p.Name) || seen[p.Name]:
			return fmt.Errorf("%w: project %s: pool name %q: unique, lower-case letters, digits, - and _", ErrInvalid, req.Project, p.Name)
		case p.Min < 0 || p.Max < p.Min || p.Max < 1:
			return fmt.Errorf("%w: project %s: pool %s: want 0 ≤ min ≤ max, max ≥ 1", ErrInvalid, req.Project, p.Name)
		case p.Lifts < 1 || len(p.Docks) == 0:
			return fmt.Errorf("%w: project %s: pool %s: needs docks and lifts ≥ 1", ErrInvalid, req.Project, p.Name)
		}
		seen[p.Name] = true
	}
	// One project per pipeline name (several projects on a server come
	// later: docs/projects.md).
	for _, name := range slices.Sorted(maps.Keys(c.projects)) {
		other := c.projects[name]
		if name == req.Project || other.DeletedAt != nil {
			continue
		}
		for _, h := range req.Build.Hauls {
			if slices.Contains(other.Hauls, h) {
				return fmt.Errorf("%w: pipeline %s belongs to project %s", ErrConflict, h, name)
			}
		}
	}
	return nil
}

// DeleteProject stops a project: its workers go and its pipelines take no
// new runs; its history and data stay. With runs still open it refuses,
// unless cancelRuns, which cancels them. It returns how many it cancelled.
func (c *Core) DeleteProject(ctx context.Context, name, by string, cancelRuns bool) (int, error) {
	return submit(ctx, c, func(c *Core) (int, error) {
		p := c.projects[name]
		if p == nil || p.DeletedAt != nil {
			return 0, fmt.Errorf("%w: project %s", ErrNotFound, name)
		}
		var open []*haulState
		for _, id := range slices.Sorted(maps.Keys(c.hauls)) {
			if h := c.hauls[id]; slices.Contains(p.Hauls, h.rec.Name) {
				open = append(open, h)
			}
		}
		if len(open) > 0 && !cancelRuns {
			return 0, fmt.Errorf("%w: project %s has %d open runs: wait for them, or cancel them with the delete", ErrConflict, name, len(open))
		}
		for _, h := range open {
			c.finishHaul(h, state.HaulCancelled, "cancelled", fmt.Sprintf("project %s deleted by %s", name, by), nil)
		}
		at := c.now()
		p.DeletedAt = &at
		deleted := *p
		c.b.catalog = append(c.b.catalog, func(tx *state.Tx) error { return tx.PutProject(deleted) })
		c.emit(state.EventProjectDeleted, "", map[string]any{"project": name, "version": p.Version, "by": by, "cancelled_runs": len(open)})
		return len(open), nil
	})
}

// Projects returns every project, deleted ones too, by name.
func (c *Core) Projects(ctx context.Context) ([]state.Project, error) {
	return submit(ctx, c, func(c *Core) ([]state.Project, error) {
		var out []state.Project
		for _, name := range slices.Sorted(maps.Keys(c.projects)) {
			out = append(out, *c.projects[name])
		}
		return out, nil
	})
}

// deletedProjectOf is the deleted project that defined haul, if no project
// still running defines it.
func (c *Core) deletedProjectOf(haul string) string {
	deleted := ""
	for _, name := range slices.Sorted(maps.Keys(c.projects)) {
		p := c.projects[name]
		if !slices.Contains(p.Hauls, haul) {
			continue
		}
		if p.DeletedAt == nil {
			return ""
		}
		deleted = name
	}
	return deleted
}

func (c *Core) restoreProjects(ctx context.Context) error {
	ps, err := c.cfg.Store.ListProjects(ctx)
	if err != nil {
		return err
	}
	for _, p := range ps {
		c.projects[p.Project] = &p
	}
	return nil
}
