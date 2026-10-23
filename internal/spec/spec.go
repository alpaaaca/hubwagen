package spec

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Spec is a compiled, normalised pipeline version: every default applied,
// templates checked, dependencies sorted, tasks in topological order. It is
// the only form of a pipeline the control plane ever reads.
//
// Its identity is the SHA-256 of its canonical JSON encoding (see Canonical),
// so the JSON field names below are part of the version contract: renaming
// one changes every hash.
type Spec struct {
	APIVersion  string            `json:"api_version"`
	Tenant      string            `json:"tenant"`
	Name        string            `json:"name"`
	Description string            `json:"description,omitempty"`
	Labels      map[string]string `json:"labels,omitempty"`
	Schedule    *Schedule         `json:"schedule,omitempty"`
	Triggers    []Trigger         `json:"triggers,omitempty"`
	Params      []Param           `json:"params,omitempty"`
	Concurrency Concurrency       `json:"concurrency"`
	Tasks       []Task            `json:"tasks"`
}

type Schedule struct {
	Cron     string `json:"cron"`
	Timezone string `json:"timezone"`
	Interval string `json:"interval"`
	Catchup  bool   `json:"catchup"`
	// Start is the earliest interval start to schedule, RFC 3339 in UTC.
	Start string `json:"start,omitempty"`
}

type Trigger struct {
	Asset     string `json:"asset"`
	Partition string `json:"partition"`
}

type Param struct {
	Name string `json:"name"`
	Type string `json:"type"`
	// Default is nil for required params; otherwise a string, int64 or bool
	// matching Type.
	Default any `json:"default,omitempty"`
}

type Concurrency struct {
	MaxActiveRuns int    `json:"max_active_runs"`
	OnOverlap     string `json:"on_overlap"`
}

type Task struct {
	ID             string            `json:"id"`
	DependsOn      []string          `json:"depends_on,omitempty"`
	TriggerRule    string            `json:"trigger_rule"`
	OnFailure      string            `json:"on_failure"`
	Runtime        Runtime           `json:"runtime"`
	Command        []string          `json:"command"`
	Args           []string          `json:"args,omitempty"`
	Env            map[string]string `json:"env,omitempty"`
	Secrets        []Secret          `json:"secrets,omitempty"`
	Inputs         []Input           `json:"inputs,omitempty"`
	Outputs        []Output          `json:"outputs,omitempty"`
	Retry          Retry             `json:"retry"`
	TimeoutSeconds int64             `json:"timeout_seconds"`
}

type Runtime struct {
	Kind      string     `json:"kind"`
	Pool      string     `json:"pool,omitempty"`
	Image     string     `json:"image,omitempty"`
	Resources *Resources `json:"resources,omitempty"`
}

type Resources struct {
	CPU    string `json:"cpu,omitempty"`
	Memory string `json:"memory,omitempty"`
}

type Retry struct {
	Limit          int    `json:"limit"`
	Backoff        string `json:"backoff"`
	InitialSeconds int64  `json:"initial_seconds"`
	MaxSeconds     int64  `json:"max_seconds"`
}

type Secret struct {
	Name string `json:"name"`
	Key  string `json:"key"`
	Env  string `json:"env"`
}

type Input struct {
	Name       string `json:"name"`
	FromTask   string `json:"from_task"`
	FromOutput string `json:"from_output"`
}

type Output struct {
	Name      string `json:"name"`
	Asset     string `json:"asset,omitempty"`
	Partition string `json:"partition,omitempty"`
}

// Canonical returns the RFC 8785 canonical JSON encoding of s. These are the
// exact bytes stored in the bucket and hashed for the version ID.
func (s *Spec) Canonical() ([]byte, error) {
	return canonicalJSON(s)
}

// Hash returns the version ID: hex SHA-256 of the canonical encoding.
func (s *Spec) Hash() (string, error) {
	b, err := s.Canonical()
	if err != nil {
		return "", err
	}
	return HashBytes(b), nil
}

// HashBytes hashes an already-canonical encoding.
func HashBytes(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// Task returns the task with the given ID, or nil.
func (s *Spec) Task(id string) *Task {
	for i := range s.Tasks {
		if s.Tasks[i].ID == id {
			return &s.Tasks[i]
		}
	}
	return nil
}

// Decode parses a compiled spec (as written by Canonical), rejecting unknown
// fields. It does not re-run compile-time validation: the bytes were produced
// by Compile and are identified by their hash.
func Decode(data []byte) (*Spec, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var s Spec
	if err := dec.Decode(&s); err != nil {
		return nil, fmt.Errorf("decode spec: %w", err)
	}
	if s.APIVersion != APIVersion {
		return nil, fmt.Errorf("decode spec: unsupported api_version %q", s.APIVersion)
	}
	if len(s.Tasks) == 0 {
		return nil, fmt.Errorf("decode spec: no tasks")
	}
	return &s, nil
}
