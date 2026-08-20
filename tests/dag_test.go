package tests

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}

// --- Parser ---

func TestParseFile_ValidYAML(t *testing.T) {
	path := writeYAML(t, `
name: my_dag
steps:
  - id: a
    executor: shell
    command: "echo hello"
`)
	d, err := dag.ParseFile(path)
	require.NoError(t, err)
	assert.Equal(t, "my_dag", d.Name)
	require.Len(t, d.Steps, 1)
	assert.Equal(t, "a", d.Steps[0].ID)
	assert.Equal(t, "shell", d.Steps[0].Executor)
}

func TestParseFile_MissingFile(t *testing.T) {
	_, err := dag.ParseFile("/nonexistent/path/dag.yaml")
	assert.Error(t, err)
}

func TestParseFile_MalformedYAML(t *testing.T) {
	path := writeYAML(t, "name: [\nbad yaml")
	_, err := dag.ParseFile(path)
	assert.Error(t, err)
}

func TestParseFile_HelloWorld(t *testing.T) {
	path := filepath.Join("..", "dags", "examples", "hello_world.yaml")
	d, err := dag.ParseFile(path)
	require.NoError(t, err)
	assert.Equal(t, "hello_world", d.Name)
	assert.Len(t, d.Steps, 3)
	assert.Equal(t, []string{"step_a"}, d.Steps[1].DependsOn)
	assert.Equal(t, []string{"step_b"}, d.Steps[2].DependsOn)
}

// --- Validator ---

func TestValidate_ValidDAG(t *testing.T) {
	d := &dag.DAG{
		Name: "valid",
		Steps: []dag.Step{
			{ID: "a", Executor: "shell", Command: "echo a"},
			{ID: "b", Executor: "shell", Command: "echo b", DependsOn: []string{"a"}},
		},
	}
	assert.Empty(t, dag.Validate(d))
}

func TestValidate_EmptyName(t *testing.T) {
	d := &dag.DAG{
		Steps: []dag.Step{{ID: "a", Executor: "shell", Command: "echo a"}},
	}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "name")
}

func TestValidate_InvalidNameChars(t *testing.T) {
	d := &dag.DAG{
		Name:  "my dag!",
		Steps: []dag.Step{{ID: "a", Executor: "shell", Command: "echo a"}},
	}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "name")
}

func TestValidate_NoSteps(t *testing.T) {
	d := &dag.DAG{Name: "empty"}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "step")
}

func TestValidate_DuplicateStepID(t *testing.T) {
	d := &dag.DAG{
		Name: "dup",
		Steps: []dag.Step{
			{ID: "a", Executor: "shell", Command: "echo a"},
			{ID: "a", Executor: "shell", Command: "echo a2"},
		},
	}
	errs := dag.Validate(d)
	assert.True(t, len(errs) >= 1)
	found := false
	for _, e := range errs {
		if e.Error() == `duplicate step id "a"` {
			found = true
		}
	}
	assert.True(t, found, "expected duplicate step id error")
}

func TestValidate_UnknownDependency(t *testing.T) {
	d := &dag.DAG{
		Name: "baddep",
		Steps: []dag.Step{
			{ID: "a", Executor: "shell", Command: "echo a", DependsOn: []string{"ghost"}},
		},
	}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "ghost")
}

func TestValidate_UnknownExecutor(t *testing.T) {
	d := &dag.DAG{
		Name:  "badexec",
		Steps: []dag.Step{{ID: "a", Executor: "magic", Command: "echo a"}},
	}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "executor")
}

func TestValidate_ShellMissingCommand(t *testing.T) {
	d := &dag.DAG{
		Name:  "nocmd",
		Steps: []dag.Step{{ID: "a", Executor: "shell"}},
	}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "command")
}

func TestValidate_InvalidOnFailure(t *testing.T) {
	d := &dag.DAG{
		Name:  "badfail",
		Steps: []dag.Step{{ID: "a", Executor: "shell", Command: "echo a", OnFailure: "explode"}},
	}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "on_failure")
}

func TestValidate_Cycle(t *testing.T) {
	d := &dag.DAG{
		Name: "cyclic",
		Steps: []dag.Step{
			{ID: "a", Executor: "shell", Command: "echo a", DependsOn: []string{"b"}},
			{ID: "b", Executor: "shell", Command: "echo b", DependsOn: []string{"a"}},
		},
	}
	errs := dag.Validate(d)
	require.Len(t, errs, 1)
	assert.Contains(t, errs[0].Error(), "cycle")
}

// --- TopologicalWaves ---

func TestTopologicalWaves_Sequential(t *testing.T) {
	steps := []dag.Step{
		{ID: "a", Executor: "shell", Command: "echo a"},
		{ID: "b", Executor: "shell", Command: "echo b", DependsOn: []string{"a"}},
		{ID: "c", Executor: "shell", Command: "echo c", DependsOn: []string{"b"}},
	}
	waves, err := dag.TopologicalWaves(steps)
	require.NoError(t, err)
	require.Len(t, waves, 3)
	assert.Equal(t, []string{"a"}, waves[0])
	assert.Equal(t, []string{"b"}, waves[1])
	assert.Equal(t, []string{"c"}, waves[2])
}

func TestTopologicalWaves_Parallel(t *testing.T) {
	steps := []dag.Step{
		{ID: "a", Executor: "shell", Command: "echo a"},
		{ID: "b", Executor: "shell", Command: "echo b"},
		{ID: "join", Executor: "shell", Command: "echo join", DependsOn: []string{"a", "b"}},
	}
	waves, err := dag.TopologicalWaves(steps)
	require.NoError(t, err)
	require.Len(t, waves, 2)
	assert.ElementsMatch(t, []string{"a", "b"}, waves[0])
	assert.Equal(t, []string{"join"}, waves[1])
}

func TestTopologicalWaves_Cycle(t *testing.T) {
	steps := []dag.Step{
		{ID: "a", Executor: "shell", Command: "echo a", DependsOn: []string{"b"}},
		{ID: "b", Executor: "shell", Command: "echo b", DependsOn: []string{"a"}},
	}
	_, err := dag.TopologicalWaves(steps)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cycle")
}

func TestTopologicalWaves_SingleStep(t *testing.T) {
	steps := []dag.Step{
		{ID: "only", Executor: "shell", Command: "echo only"},
	}
	waves, err := dag.TopologicalWaves(steps)
	require.NoError(t, err)
	require.Len(t, waves, 1)
	assert.Equal(t, []string{"only"}, waves[0])
}
