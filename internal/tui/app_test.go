package tui

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	teatest "github.com/charmbracelet/x/exp/teatest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	"github.com/alper/hubwagen/internal/scheduler"
	"github.com/alper/hubwagen/internal/store"
)

// These tests exercise Update()/View() directly with no real I/O beyond a
// real :memory: SQLite Store and NoopExecutor — per constitution Principle
// IV, no mocking of Store or Executor. This file lives in package tui
// (white-box) rather than the repo's usual external `tests` package
// specifically because the screen models (dagListModel etc.) are
// intentionally unexported; meaningfully testing their Update()/View()
// requires that access.

func openMemStore(t *testing.T) store.Store {
	t.Helper()
	st, err := store.Open(":memory:")
	require.NoError(t, err)
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func writeDAG(t *testing.T, dir, name string) {
	t.Helper()
	content := "name: " + name + "\nsteps:\n  - id: s1\n    executor: noop\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, name+".yaml"), []byte(content), 0o644))
}

// --- Pure unit tests: dagListModel ---

func TestDagListModel_LoadRendersDiscoveredDAGs(t *testing.T) {
	dir := t.TempDir()
	writeDAG(t, dir, "alpha")
	writeDAG(t, dir, "beta")
	st := openMemStore(t)

	m := newDAGListModel(context.Background(), st, dir, nil)
	msg := m.loadCmd()() // execute the tea.Cmd synchronously to get its Msg
	loaded, ok := msg.(dagListLoadedMsg)
	require.True(t, ok, "loadCmd must produce a dagListLoadedMsg")
	require.NoError(t, loaded.err)

	m, _ = m.Update(loaded)
	require.Len(t, m.allRows, 2)
	assert.Contains(t, m.View(), "alpha")
	assert.Contains(t, m.View(), "beta")
	assert.Contains(t, m.View(), "no runs yet", "a DAG with no run history must say so, not error")
}

func TestDagListModel_FilterNarrowsRows(t *testing.T) {
	dir := t.TempDir()
	writeDAG(t, dir, "hello_world")
	writeDAG(t, dir, "totally_different")
	st := openMemStore(t)

	m := newDAGListModel(context.Background(), st, dir, nil)
	loaded := m.loadCmd()().(dagListLoadedMsg)
	m, _ = m.Update(loaded)
	require.Len(t, m.allRows, 2)

	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	assert.True(t, m.filtering)
	for _, r := range "hello" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	assert.Len(t, m.visibleRows, 1, "only hello_world should match the 'hello' filter")
	assert.Equal(t, "hello_world", m.visibleRows[0].Name)

	// esc clears the filter and restores the full list.
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	assert.False(t, m.filtering)
	assert.Len(t, m.visibleRows, 2)
}

func TestDagListModel_InvalidDAGShownNotOmitted(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "broken.yaml"), []byte("not: [valid"), 0o644))
	st := openMemStore(t)

	m := newDAGListModel(context.Background(), st, dir, nil)
	loaded := m.loadCmd()().(dagListLoadedMsg)
	m, _ = m.Update(loaded)

	require.Len(t, m.allRows, 1)
	assert.True(t, m.allRows[0].Discovered.ParseErr != nil)
	assert.Contains(t, m.View(), "invalid")
}

func TestDagListModel_EnterOnValidRowNavigates(t *testing.T) {
	dir := t.TempDir()
	writeDAG(t, dir, "nav_target")
	st := openMemStore(t)

	m := newDAGListModel(context.Background(), st, dir, nil)
	loaded := m.loadCmd()().(dagListLoadedMsg)
	m, _ = m.Update(loaded)

	m, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	require.NotNil(t, cmd)
	nav, ok := cmd().(navigateToRunHistoryMsg)
	require.True(t, ok, "enter on a valid row must emit navigateToRunHistoryMsg")
	assert.Equal(t, "nav_target", nav.dagName)
}

// --- Pure unit test: rundetail / stepview render real trigger data ---

func TestRunHistoryModel_ShowsTriggeredRun(t *testing.T) {
	dir := t.TempDir()
	writeDAG(t, dir, "seeded")
	st := openMemStore(t)
	ctx := context.Background()

	found, err := dag.DiscoverDir(dir)
	require.NoError(t, err)
	require.Len(t, found, 1)
	executors := map[string]executor.Executor{"noop": &executor.NoopExecutor{}}
	result, err := scheduler.TriggerManual(ctx, st, found[0], executors, io.Discard, "manual")
	require.NoError(t, err)

	m := newRunHistoryModel(ctx, st, "seeded", found[0].DAG.Steps, 80, 24)
	loaded := m.loadCmd()().(runHistoryLoadedMsg)
	require.NoError(t, loaded.err)
	m, _ = m.Update(loaded)

	require.Len(t, m.runs, 1)
	assert.Equal(t, result.RunID, m.runs[0].ID)
	assert.Contains(t, m.View(), "succeeded")
}

func TestStepViewModel_ShowsStepStatus(t *testing.T) {
	dir := t.TempDir()
	writeDAG(t, dir, "seeded_steps")
	st := openMemStore(t)
	ctx := context.Background()

	found, err := dag.DiscoverDir(dir)
	require.NoError(t, err)
	executors := map[string]executor.Executor{"noop": &executor.NoopExecutor{}}
	result, err := scheduler.TriggerManual(ctx, st, found[0], executors, io.Discard, "manual")
	require.NoError(t, err)

	m := newStepViewModel(ctx, st, "seeded_steps", result.RunID, found[0].DAG.Steps, 80, 24)
	loaded := m.loadCmd()().(stepListLoadedMsg)
	require.NoError(t, loaded.err)
	require.Len(t, loaded.steps, 1)
	assert.Equal(t, "s1", loaded.steps[0].StepID)
	assert.Equal(t, "succeeded", loaded.steps[0].Status)
}

// --- teatest: the couple of key-sequence flows that matter ---
// (reserve teatest for navigation flows, not every screen's every detail.)

func TestApp_NavigationFlow(t *testing.T) {
	dir := t.TempDir()
	writeDAG(t, dir, "flow_dag")
	st := openMemStore(t)
	ctx := context.Background()

	found, err := dag.DiscoverDir(dir)
	require.NoError(t, err)
	executors := map[string]executor.Executor{"noop": &executor.NoopExecutor{}}
	_, err = scheduler.TriggerManual(ctx, st, found[0], executors, io.Discard, "manual")
	require.NoError(t, err)

	app := NewApp(ctx, st, dir, executors)
	tm := teatest.NewTestModel(t, app, teatest.WithInitialTermSize(100, 30))

	waitForOutput(t, tm, "flow_dag")

	tm.Send(tea.KeyMsg{Type: tea.KeyEnter}) // list -> run history
	waitForOutput(t, tm, "RUN ID")

	tm.Send(tea.KeyMsg{Type: tea.KeyEnter}) // run history -> step view
	waitForOutput(t, tm, "STEP")

	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b")}) // back to run history
	waitForOutput(t, tm, "RUN ID")

	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b")}) // back to DAG list
	waitForOutput(t, tm, "DAG List")

	// 'b' on the root screen is a documented no-op (contracts/keybindings.md) —
	// verified here by the program simply continuing to run and quitting
	// cleanly afterward, rather than by re-scanning for unchanged output:
	// bubbletea skips writing a new frame when the view is byte-identical to
	// the last one, so a fresh WaitFor here would never see new bytes.
	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b")})

	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("q")})
	tm.WaitFinished(t, teatest.WithFinalTimeout(3*time.Second))
}

func waitForOutput(t *testing.T, tm *teatest.TestModel, want string) {
	t.Helper()
	teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
		return strings.Contains(string(b), want)
	}, teatest.WithDuration(3*time.Second), teatest.WithCheckInterval(50*time.Millisecond))
}
