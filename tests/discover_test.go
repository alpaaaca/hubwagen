package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscoverDir_RecursiveAndValid(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(`
name: dag_a
steps:
  - id: s1
    executor: shell
    command: "echo a"
`), 0o644))

	nested := filepath.Join(dir, "examples")
	require.NoError(t, os.MkdirAll(nested, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(nested, "b.yml"), []byte(`
name: dag_b
steps:
  - id: s1
    executor: shell
    command: "echo b"
`), 0o644))

	// Non-DAG file must be ignored.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "README.md"), []byte("hi"), 0o644))

	found, err := dag.DiscoverDir(dir)
	require.NoError(t, err)
	require.Len(t, found, 2, "expected both the top-level and nested DAG files, and nothing else")

	names := map[string]bool{}
	for _, d := range found {
		require.NoError(t, d.ParseErr)
		require.NotNil(t, d.DAG)
		assert.NotEmpty(t, d.YAMLHash)
		names[d.DAG.Name] = true
	}
	assert.True(t, names["dag_a"])
	assert.True(t, names["dag_b"])
}

func TestDiscoverDir_InvalidFileIsolated(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "good.yaml"), []byte(`
name: good_dag
steps:
  - id: s1
    executor: shell
    command: "echo good"
`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("not: [valid: yaml"), 0o644))

	found, err := dag.DiscoverDir(dir)
	require.NoError(t, err, "a per-file parse error must not fail the whole walk")
	require.Len(t, found, 2)

	var sawGood, sawBad bool
	for _, d := range found {
		switch {
		case d.ParseErr == nil:
			sawGood = true
			assert.Equal(t, "good_dag", d.DAG.Name)
		default:
			sawBad = true
			assert.Nil(t, d.DAG)
		}
	}
	assert.True(t, sawGood, "the valid file must still be discovered")
	assert.True(t, sawBad, "the invalid file must still be listed, flagged rather than omitted")
}

func TestDiscoverDir_InvalidDAGSemantics(t *testing.T) {
	dir := t.TempDir()
	// Parses as YAML fine, but fails dag.Validate (cycle).
	require.NoError(t, os.WriteFile(filepath.Join(dir, "cycle.yaml"), []byte(`
name: cycle_dag
steps:
  - id: a
    executor: shell
    command: "echo a"
    depends_on: [b]
  - id: b
    executor: shell
    command: "echo b"
    depends_on: [a]
`), 0o644))

	found, err := dag.DiscoverDir(dir)
	require.NoError(t, err)
	require.Len(t, found, 1)
	assert.Error(t, found[0].ParseErr)
	assert.Nil(t, found[0].DAG)
}

func TestDiscoverDir_MissingDirectory(t *testing.T) {
	_, err := dag.DiscoverDir(filepath.Join(t.TempDir(), "does-not-exist"))
	assert.Error(t, err)
}

func TestDiscoverDir_EmptyDirectory(t *testing.T) {
	found, err := dag.DiscoverDir(t.TempDir())
	require.NoError(t, err)
	assert.Empty(t, found)
}
