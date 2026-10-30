package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// KindProject is a project file: a repo's application, as `hubwagen deploy`
// hands it to a server (docs/projects.md). Its pipelines are code, found
// from pyproject.toml; the file says how to build and run that code.
const KindProject = "Project"

// ProjectFiles are the names `hubwagen deploy` looks for in a repo, in
// order: hubwagen.yaml if it is a project file, else hubwagen.project.yaml
// (while a repo's hubwagen.yaml is still a server config for hubwagen dev).
var ProjectFiles = []string{FileName, "hubwagen.project.yaml"}

type Project struct {
	APIVersion string                `yaml:"apiVersion"`
	Kind       string                `yaml:"kind"`
	Name       string                `yaml:"name"`
	Build      ProjectBuild          `yaml:"build"`
	Pools      []ProjectPool         `yaml:"pools"`
	Env        map[string]ProjectEnv `yaml:"env"`

	// Path is the file it came from; Dir its directory, the project's
	// root. Build paths are resolved against it.
	Path string `yaml:"-"`
	Dir  string `yaml:"-"`
}

// ProjectBuild is how the project's image is built: Dockerfile in
// Context, tagged Image:<version>. Without a Dockerfile there is no image:
// the code runs in place (a local server's process launcher).
type ProjectBuild struct {
	Dockerfile string            `yaml:"dockerfile"`
	Context    string            `yaml:"context"` // default: the project's root
	Image      string            `yaml:"image"`   // repository, without a tag
	Args       map[string]string `yaml:"args"`    // --build-arg NAME=value
}

// ProjectPool is a pool of the project's workers: min always run, up to
// max when work waits on its docks.
type ProjectPool struct {
	Name      string            `yaml:"name"`
	Docks     []string          `yaml:"docks"` // default [default]
	Min       *int              `yaml:"min"`   // default 0
	Max       *int              `yaml:"max"`   // default max(min, 1)
	Lifts     *int              `yaml:"lifts"` // default 4
	IdleAfter Duration          `yaml:"idle_after"`
	Resources map[string]string `yaml:"resources"` // with pod steps (worker-pools step 5)
}

// Bounds is the pool's min and max.
func (p ProjectPool) Bounds() (lo, hi int) {
	if p.Min != nil {
		lo = *p.Min
	}
	hi = max(lo, 1)
	if p.Max != nil {
		hi = *p.Max
	}
	return lo, hi
}

// DockList is the docks the pool's workers poll.
func (p ProjectPool) DockList() []string {
	if len(p.Docks) == 0 {
		return []string{"default"}
	}
	return p.Docks
}

// LiftCount is how many steps one worker runs at once.
func (p ProjectPool) LiftCount() int {
	if p.Lifts == nil {
		return DefaultLifts
	}
	return *p.Lifts
}

// ProjectEnv is a variable for the project's workers: a plain value, or
// {secret: name}, a secret held on the server's side, never in the file.
type ProjectEnv struct {
	Value  string
	Secret string
}

func (e *ProjectEnv) UnmarshalYAML(n *yaml.Node) error {
	switch n.Kind {
	case yaml.ScalarNode:
		e.Value = n.Value
		return nil
	case yaml.MappingNode:
		var v struct {
			Secret string `yaml:"secret"`
		}
		if err := n.Decode(&v); err != nil {
			return err
		}
		if v.Secret == "" || len(n.Content) != 2 {
			return fmt.Errorf("line %d: want a value or {secret: name}", n.Line)
		}
		e.Secret = v.Secret
		return nil
	}
	return fmt.Errorf("line %d: want a value or {secret: name}", n.Line)
}

// FindProject picks the project file: explicit if set, else the first of
// ProjectFiles in dir that is one (kind: Project).
func FindProject(explicit, dir string) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	for _, name := range ProjectFiles {
		p := filepath.Join(dir, name)
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var head struct {
			Kind string `yaml:"kind"`
		}
		if yaml.Unmarshal(data, &head) == nil && head.Kind == KindProject {
			return p, nil
		}
	}
	return "", fmt.Errorf("no project file in %s: %s with kind: %s (docs/projects.md)", dir, strings.Join(ProjectFiles, " or "), KindProject)
}

// LoadProject reads and checks a project file. Like a config, it may use
// ${NAME} and ${NAME:-default}, and every problem is reported at once.
func LoadProject(path string) (*Project, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("project: %w", err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	p, err := ParseProject(data, filepath.Dir(abs))
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	p.Path = abs
	return p, nil
}

// ParseProject decodes and checks a project file whose root is dir.
func ParseProject(data []byte, dir string) (*Project, error) {
	data, err := expand(data, os.LookupEnv)
	if err != nil {
		return nil, err
	}
	var p Project
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&p); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	var errs []string
	add := func(format string, args ...any) { errs = append(errs, fmt.Sprintf(format, args...)) }
	if p.APIVersion != APIVersion {
		add("apiVersion: must be %s, got %q", APIVersion, p.APIVersion)
	}
	if p.Kind != KindProject {
		add("kind: must be %s, got %q (kind: Config is a server's config)", KindProject, p.Kind)
	}
	switch {
	case p.Name == "":
		add("name: required, e.g. revenue")
	case !workerName.MatchString(p.Name):
		add("name: %q must be lower-case letters, digits, - and _", p.Name)
	}
	if p.Build.Dockerfile != "" && p.Build.Image == "" {
		add("build.image: required with build.dockerfile (the repository it is pushed to, e.g. ghcr.io/acme/%s)", p.Name)
	}
	if p.Build.Image != "" && strings.ContainsAny(p.Build.Image[strings.LastIndexByte(p.Build.Image, '/')+1:], ":@") {
		add("build.image: %q: the repository only; deploy tags it with the version", p.Build.Image)
	}
	if len(p.Pools) == 0 {
		add("pools: at least one, e.g. [{name: default, min: 0, max: 3}]")
	}
	seen := map[string]bool{}
	for i, pool := range p.Pools {
		at := fmt.Sprintf("pools[%d]", i)
		switch {
		case pool.Name == "":
			add("%s.name: required", at)
		case !workerName.MatchString(pool.Name):
			add("%s.name: %q must be lower-case letters, digits, - and _", at, pool.Name)
		case seen[pool.Name]:
			add("%s.name: %q is used twice", at, pool.Name)
		}
		seen[pool.Name] = true
		lo, hi := pool.Bounds()
		switch {
		case lo < 0 || hi < 0:
			add("%s: min and max must be 0 or more", at)
		case lo > hi:
			add("%s: min %d is more than max %d", at, lo, hi)
		}
		if pool.LiftCount() < 1 {
			add("%s.lifts: must be 1 or more", at)
		}
		for j, d := range pool.Docks {
			if d == "" {
				add("%s.docks[%d]: empty", at, j)
			}
		}
	}
	for k := range p.Env {
		if k == "" || strings.HasPrefix(k, "HUBWAGEN_") {
			add("env.%s: names starting with HUBWAGEN_ are set by hubwagen", k)
		}
	}
	if len(errs) > 0 {
		return nil, errors.New(strings.Join(errs, "\n"))
	}
	p.Dir = dir
	if p.Build.Context == "" {
		p.Build.Context = "."
	}
	p.Build.Context = abs(dir, p.Build.Context)
	if p.Build.Dockerfile != "" {
		p.Build.Dockerfile = abs(dir, p.Build.Dockerfile)
	}
	return &p, nil
}
