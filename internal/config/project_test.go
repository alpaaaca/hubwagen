package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseProject(t *testing.T) {
	dir := t.TempDir()
	p, err := ParseProject([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Project
name: revenue
build: {dockerfile: Dockerfile, image: ghcr.io/acme/revenue}
pools:
  - {name: default, min: 1, max: 6, lifts: 2}
  - {name: gpu, docks: [gpu], max: 2, resources: {gpu: "1"}}
env:
  DB_URL: {secret: revenue-db}
  LOG_LEVEL: info
`), dir)
	require.NoError(t, err)
	assert.Equal(t, "revenue", p.Name)
	assert.Equal(t, filepath.Join(dir, "Dockerfile"), p.Build.Dockerfile)
	assert.Equal(t, dir, p.Build.Context)
	lo, hi := p.Pools[0].Bounds()
	assert.Equal(t, []int{1, 6, 2}, []int{lo, hi, p.Pools[0].LiftCount()})
	lo, hi = p.Pools[1].Bounds()
	assert.Equal(t, []int{0, 2, DefaultLifts}, []int{lo, hi, p.Pools[1].LiftCount()})
	assert.Equal(t, ProjectEnv{Secret: "revenue-db"}, p.Env["DB_URL"])
	assert.Equal(t, ProjectEnv{Value: "info"}, p.Env["LOG_LEVEL"])
}

func TestParseProjectReportsAllProblems(t *testing.T) {
	_, err := ParseProject([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
name: Revenue
build: {dockerfile: Dockerfile, image: "ghcr.io/acme/revenue:1"}
pools:
  - {name: a, min: 3, max: 2}
  - {name: a, lifts: 0}
env:
  HUBWAGEN_SERVER: x
`), t.TempDir())
	require.Error(t, err)
	for _, want := range []string{
		"kind: must be Project, got \"Config\"",
		`name: "Revenue" must be lower-case`,
		"build.image: \"ghcr.io/acme/revenue:1\": the repository only",
		"pools[0]: min 3 is more than max 2",
		`pools[1].name: "a" is used twice`,
		"pools[1].lifts: must be 1 or more",
		"env.HUBWAGEN_SERVER: names starting with HUBWAGEN_ are set by hubwagen",
	} {
		assert.Contains(t, err.Error(), want)
	}

	_, err = ParseProject([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Project\nname: x\npools: [{name: a}]\nenv: {A: {secret: s, other: 1}}\n"), t.TempDir())
	assert.ErrorContains(t, err, "want a value or {secret: name}")
}

func TestFindProject(t *testing.T) {
	dir := t.TempDir()
	_, err := FindProject("", dir)
	assert.ErrorContains(t, err, "no project file in "+dir)

	// A server config named hubwagen.yaml isn't one; hubwagen.project.yaml is.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hubwagen.yaml"), []byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "hubwagen.project.yaml"), []byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Project\n"), 0o644))
	p, err := FindProject("", dir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "hubwagen.project.yaml"), p)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "hubwagen.yaml"), []byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Project\n"), 0o644))
	p, err = FindProject("", dir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "hubwagen.yaml"), p, "hubwagen.yaml first")
}
