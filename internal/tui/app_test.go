package tui

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	teatest "github.com/charmbracelet/x/exp/teatest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/devmode"
	"github.com/alper/hubwagen/internal/state"
)

// These tests drive Update()/View() directly against a real dev engine
// (state store, core, local runtime, file:// bucket in a temp dir) — no
// fakes. They live in package tui because the screen models are unexported.

var ctx = context.Background()

type env struct {
	t      *testing.T
	dir    string // pipeline files
	engine *devmode.Engine
	loader *devmode.Loader
	src    EngineSource
}

func newEnv(t *testing.T) *env {
	t.Helper()
	e, err := devmode.Start(ctx, devmode.Config{DataDir: t.TempDir(), DisableSchedules: true})
	require.NoError(t, err)
	t.Cleanup(func() { e.Close() })
	dir := t.TempDir()
	l := devmode.NewLoader(e.Core, []string{dir})
	return &env{t: t, dir: dir, engine: e, loader: l, src: EngineSource{Engine: e, Loader: l}}
}

// pipeline writes a pipeline file whose tasks run script (a shell snippet),
// a then b, and loads it.
func (e *env) pipeline(name, schedule, script string) {
	e.t.Helper()
	content := "apiVersion: hubwagen.dev/v1alpha1\nkind: Pipeline\nmetadata: {name: " + name + ", tenant: t}\nspec:\n" +
		schedule +
		"  defaults: {runtime: {kind: agent, pool: local}}\n" +
		"  tasks:\n" +
		"    - {id: a, command: [/bin/sh, -c], args: ['" + script + "']}\n" +
		"    - {id: b, depends_on: [a], command: [/bin/sh, -c], args: ['echo second']}\n"
	e.write(name+".yaml", content)
}

func (e *env) write(file, content string) {
	e.t.Helper()
	require.NoError(e.t, os.WriteFile(filepath.Join(e.dir, file), []byte(content), 0o644))
	_, err := e.loader.Load(ctx)
	require.NoError(e.t, err)
}

func (e *env) runToCompletion(name string) string {
	e.t.Helper()
	res, err := e.src.Trigger(ctx, "t", name)
	require.NoError(e.t, err)
	run, err := e.engine.WaitRun(ctx, res.RunID)
	require.NoError(e.t, err)
	require.Equal(e.t, state.RunSucceeded, run.Status)
	return res.RunID
}

func loadList(t *testing.T, m dagListModel) dagListModel {
	t.Helper()
	msg, ok := m.loadCmd()().(dagListLoadedMsg)
	require.True(t, ok)
	require.NoError(t, msg.err)
	m, _ = m.Update(msg)
	return m
}

func TestPipelineListShowsPipelinesSchedulesAndInvalidFiles(t *testing.T) {
	e := newEnv(t)
	e.pipeline("alpha", "", "echo hi")
	e.pipeline("beta", "  schedule: {cron: \"0 2 * * *\", timezone: Europe/Berlin, interval: daily}\n", "echo hi")
	e.write("broken.yaml", "apiVersion: hubwagen.dev/v1alpha1\nkind: Pipeline\nmetadata: {name: Bad, tenant: t}\n")

	m := loadList(t, newDAGListModel(ctx, e.src))
	require.Len(t, m.allRows, 3)
	view := m.View()
	assert.Contains(t, view, "t/alpha")
	assert.Contains(t, view, "manual")
	assert.Contains(t, view, "0 2 * * * Europe/Berlin")
	assert.Contains(t, view, "no runs yet")
	assert.Contains(t, view, "broken.yaml (invalid)")
	assert.Equal(t, "t/beta", m.allRows[2].Name)
	assert.NotEqual(t, "—", m.allRows[2].NextRunDisp, "scheduled pipelines show their next run")
}

func TestPipelineListFilter(t *testing.T) {
	e := newEnv(t)
	e.pipeline("hello_world", "", "echo hi")
	e.pipeline("totally_different", "", "echo hi")
	m := loadList(t, newDAGListModel(ctx, e.src))

	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("/")})
	for _, r := range "hello" {
		m, _ = m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{r}})
	}
	require.Len(t, m.visibleRows, 1)
	assert.Equal(t, "t/hello_world", m.visibleRows[0].Name)
	m, _ = m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	assert.Len(t, m.visibleRows, 2)
}

func TestInvalidRowShowsItsErrorInsteadOfNavigating(t *testing.T) {
	e := newEnv(t)
	e.write("broken.yaml", "apiVersion: hubwagen.dev/v1alpha1\nkind: Pipeline\nmetadata: {name: Bad, tenant: t}\n")
	m := loadList(t, newDAGListModel(ctx, e.src))
	m, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	assert.Nil(t, cmd)
	assert.Contains(t, m.View(), "metadata.name")
}

func TestTriggerFromTheList(t *testing.T) {
	e := newEnv(t)
	e.pipeline("p", "", "echo hi")
	m := loadList(t, newDAGListModel(ctx, e.src))

	m, cmd := m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("t")})
	require.NotNil(t, cmd)
	res := cmd().(triggerResultMsg)
	require.NoError(t, res.err)
	m, _ = m.Update(res)
	assert.Contains(t, m.View(), "triggered t/p (run "+res.result.RunID+")")
	_, err := e.engine.WaitRun(ctx, res.result.RunID)
	require.NoError(t, err)
}

func TestRunHistoryAndTaskView(t *testing.T) {
	e := newEnv(t)
	e.pipeline("p", "", "echo first line; echo oops >&2")
	runID := e.runToCompletion("p")

	h := newRunHistoryModel(ctx, e.src, "t", "p", nil, 120, 30)
	loaded := h.loadCmd()().(runHistoryLoadedMsg)
	require.NoError(t, loaded.err)
	h, _ = h.Update(loaded)
	require.Len(t, h.runs, 1)
	assert.Contains(t, h.View(), runID)
	assert.Contains(t, h.View(), "✓ succeeded")

	shape := []TaskShape{{ID: "a"}, {ID: "b", DependsOn: []string{"a"}}}
	v := newStepViewModel(ctx, e.src, "t/p", runID, shape, 120, 30)
	tasks := v.loadCmd()().(stepListLoadedMsg)
	require.NoError(t, tasks.err)
	require.Len(t, tasks.tasks, 2)
	v, cmd := v.Update(tasks)
	require.NotNil(t, cmd, "selecting the first task loads its log")
	v, _ = v.Update(cmd())
	view := v.View()
	assert.Contains(t, view, "first line")
	assert.Contains(t, view, "[stderr] oops")

	v, _ = v.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("v")})
	assert.Contains(t, v.View(), "┌", "graph view draws boxes")
}

func TestNavigationFlow(t *testing.T) {
	e := newEnv(t)
	e.pipeline("flow", "", "echo hi")
	e.runToCompletion("flow")

	tm := teatest.NewTestModel(t, NewApp(ctx, e.src), teatest.WithInitialTermSize(120, 30))
	waitFor(t, tm, "t/flow")
	tm.Send(tea.KeyMsg{Type: tea.KeyEnter})
	waitFor(t, tm, "RUN ID")
	tm.Send(tea.KeyMsg{Type: tea.KeyEnter})
	waitFor(t, tm, "TASK")
	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b")})
	waitFor(t, tm, "RUN ID")
	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("b")})
	waitFor(t, tm, "Pipelines")
	tm.Send(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("q")})
	tm.WaitFinished(t, teatest.WithFinalTimeout(3*time.Second))
}

func waitFor(t *testing.T, tm *teatest.TestModel, want string) {
	t.Helper()
	teatest.WaitFor(t, tm.Output(), func(b []byte) bool {
		return strings.Contains(string(b), want)
	}, teatest.WithDuration(5*time.Second), teatest.WithCheckInterval(50*time.Millisecond))
}
