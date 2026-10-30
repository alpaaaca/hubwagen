package state

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

// Projects (docs/projects.md): a deployment is a project's build, how its
// workers run (an image, or a directory on the server's machine) and its
// pools. The server runs the current deployment of each project that isn't
// deleted.
const (
	EventProjectDeployed EventType = "ProjectDeployed"
	EventProjectDeleted  EventType = "ProjectDeleted"
)

// Deployment is one version of a project, as deployed.
type Deployment struct {
	Project string `json:"project"`
	Version int    `json:"version"` // 1, 2, … per project
	BuildID string `json:"build"`
	// Image: its workers are containers of it; Dir: they run in place on
	// the server's machine, with Python.
	Image  string            `json:"image,omitempty"`
	Dir    string            `json:"dir,omitempty"`
	Python string            `json:"python,omitempty"`
	Env    map[string]string `json:"env,omitempty"`
	// Secrets: variables whose values the server reads from its secrets
	// when it starts a worker (env name → secret name). Never the values.
	Secrets map[string]string `json:"secrets,omitempty"`
	Pools   []DeployedPool    `json:"pools"`
	// Hauls are the pipelines its build defines.
	Hauls      []string  `json:"hauls"`
	By         string    `json:"by,omitempty"`
	DeployedAt time.Time `json:"deployed_at"`
}

// DeployedPool is a pool of a deployment's workers.
type DeployedPool struct {
	Name      string        `json:"name"`
	Docks     []string      `json:"docks"`
	Min       int           `json:"min"`
	Max       int           `json:"max"`
	Lifts     int           `json:"lifts"`
	IdleAfter time.Duration `json:"idle_after"`
}

// Project is a project's current deployment.
type Project struct {
	Deployment
	DeletedAt *time.Time `json:"deleted_at,omitempty"`
}

// PutProject records a project's current deployment, or its deletion.
func (t *Tx) PutProject(p Project) error {
	d, err := json.Marshal(p.Deployment)
	if err != nil {
		return err
	}
	return t.exec(`INSERT INTO projects (name, version, deployment, deployed_at, deleted_at) VALUES (?, ?, ?, ?, ?)
		ON CONFLICT (name) DO UPDATE SET version = excluded.version, deployment = excluded.deployment,
			deployed_at = excluded.deployed_at, deleted_at = excluded.deleted_at`,
		p.Project, p.Version, string(d), formatTime(p.DeployedAt), formatTimePtr(p.DeletedAt))
}

// ListProjects returns every project, deleted ones too, by name.
func (s *Store) ListProjects(ctx context.Context) ([]Project, error) {
	return queryAll(ctx, s.r, func(sc interface{ Scan(...any) error }) (Project, error) {
		var p Project
		var d string
		var deleted sql.NullString
		if err := sc.Scan(&d, &deleted); err != nil {
			return p, err
		}
		if err := json.Unmarshal([]byte(d), &p.Deployment); err != nil {
			return p, err
		}
		var err error
		p.DeletedAt, err = parseTimePtr(deleted)
		return p, err
	}, `SELECT deployment, deleted_at FROM projects ORDER BY name`)
}
