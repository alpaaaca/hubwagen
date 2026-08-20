package tests

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func runShell(ctx context.Context, cmd string, env map[string]string) (executor.StepResult, string, error) {
	ex := &executor.ShellExecutor{}
	var buf bytes.Buffer
	step := dag.Step{ID: "t", Executor: "shell", Command: cmd, Env: env}
	result, err := ex.Run(ctx, step, &buf)
	return result, buf.String(), err
}

func TestShellExecutor_Stdout(t *testing.T) {
	result, out, err := runShell(context.Background(), "echo hello", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode)
	assert.Contains(t, out, "hello")
	t.Log("captured stdout:\n", out)
}

func TestShellExecutor_Stderr(t *testing.T) {
	result, out, err := runShell(context.Background(), "echo error-line >&2", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode)
	assert.Contains(t, out, "error-line")
	t.Log("captured stderr:\n", out)
}

func TestShellExecutor_MultiLineOutput(t *testing.T) {
	result, out, err := runShell(context.Background(), "echo line1 && echo line2 && echo line3", nil)
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	assert.Len(t, lines, 3)
	assert.Equal(t, "line1", strings.TrimSpace(lines[0]))
	assert.Equal(t, "line2", strings.TrimSpace(lines[1]))
	assert.Equal(t, "line3", strings.TrimSpace(lines[2]))
	t.Log("captured lines:\n", out)
}

func TestShellExecutor_NonZeroExitCode(t *testing.T) {
	result, _, err := runShell(context.Background(), "exit 42", nil)
	require.NoError(t, err)
	assert.Equal(t, 42, result.ExitCode)
}

func TestShellExecutor_EnvVar(t *testing.T) {
	result, out, err := runShell(context.Background(), "echo $HUBWAGEN_TEST_VAR", map[string]string{
		"HUBWAGEN_TEST_VAR": "injected-value",
	})
	require.NoError(t, err)
	assert.Equal(t, 0, result.ExitCode)
	assert.Contains(t, out, "injected-value")
	t.Log("captured env output:\n", out)
}

func TestShellExecutor_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	start := time.Now()
	result, _, _ := runShell(ctx, "sleep 60", nil)
	elapsed := time.Since(start)

	assert.NotEqual(t, 0, result.ExitCode)
	assert.Less(t, elapsed, 10*time.Second, "should have been killed well before 60s")
	t.Logf("killed after %v, exit code %d", elapsed.Round(time.Millisecond), result.ExitCode)
}

func TestShellExecutor_OutputBeforeFailure(t *testing.T) {
	result, out, err := runShell(context.Background(), "echo before-fail && exit 1", nil)
	require.NoError(t, err)
	assert.Equal(t, 1, result.ExitCode)
	assert.Contains(t, out, "before-fail")
	t.Log("output before failure:\n", out)
}
