package tui

import (
	"context"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	"github.com/alper/hubwagen/internal/store"
)

// refreshInterval is the poll cadence every screen uses to notice status
// changes made outside the dashboard (a scheduled run, a separate `hubwagen
// run` invocation) — comfortably inside SC-003's 3-second budget.
const refreshInterval = 2 * time.Second

type screen int

const (
	screenDAGList screen = iota
	screenRunHistory
	screenStepView
)

// Navigation messages — screens signal a desired transition by returning a
// tea.Cmd that produces one of these; the root App intercepts them and
// switches screens. Screens never reach into each other directly.
//
// steps carries the DAG's structural definition (IDs + depends_on) forward
// from the DAG list — where it's already known via the discovered DAG —
// through to Step View, which needs it to draw the dependency graph. The
// store only has step *status* (store.StepRun), not the *shape* of the DAG,
// so this has to be threaded through rather than re-fetched from the store.
type navigateToRunHistoryMsg struct {
	dagName string
	steps   []dag.Step
}
type navigateToStepViewMsg struct {
	dagName, runID string
	steps          []dag.Step
}
type navigateBackMsg struct{}

// App is the root bubbletea model: the screen router and holder of state
// shared across screens (the Store handle, dags directory, executors used
// for triggering). It reads/writes pipeline state exclusively through
// store.Store, per constitution Principle III.
type App struct {
	ctx       context.Context
	store     store.Store
	dagsDir   string
	executors map[string]executor.Executor

	screen screen
	width  int
	height int

	showHelp bool

	dagList    dagListModel
	runHistory runHistoryModel
	stepView   stepViewModel
}

// NewApp constructs the root model. The DAG list is loaded lazily via Init.
func NewApp(ctx context.Context, st store.Store, dagsDir string, executors map[string]executor.Executor) App {
	return App{
		ctx:       ctx,
		store:     st,
		dagsDir:   dagsDir,
		executors: executors,
		screen:    screenDAGList,
		dagList:   newDAGListModel(ctx, st, dagsDir, executors),
	}
}

func (a App) Init() tea.Cmd {
	return a.dagList.Init()
}

func (a App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		a.width, a.height = msg.Width, msg.Height
		a.dagList = a.dagList.SetSize(msg.Width, msg.Height)
		a.runHistory = a.runHistory.SetSize(msg.Width, msg.Height)
		a.stepView = a.stepView.SetSize(msg.Width, msg.Height)
		return a, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, Keys.Quit):
			return a, tea.Quit
		case key.Matches(msg, Keys.Help):
			a.showHelp = !a.showHelp
			return a, nil
		}

	case navigateToRunHistoryMsg:
		a.runHistory = newRunHistoryModel(a.ctx, a.store, msg.dagName, msg.steps, a.width, a.height)
		a.screen = screenRunHistory
		return a, a.runHistory.Init()

	case navigateToStepViewMsg:
		a.stepView = newStepViewModel(a.ctx, a.store, msg.dagName, msg.runID, msg.steps, a.width, a.height)
		a.screen = screenStepView
		return a, a.stepView.Init()

	case navigateBackMsg:
		switch a.screen {
		case screenStepView:
			a.screen = screenRunHistory
		case screenRunHistory:
			a.screen = screenDAGList
		case screenDAGList:
			// No-op — the DAG list is the root screen (contracts/keybindings.md).
		}
		return a, nil
	}

	// Not a global/navigation message — forward to the active screen.
	var cmd tea.Cmd
	switch a.screen {
	case screenDAGList:
		a.dagList, cmd = a.dagList.Update(msg)
	case screenRunHistory:
		a.runHistory, cmd = a.runHistory.Update(msg)
	case screenStepView:
		a.stepView, cmd = a.stepView.Update(msg)
	}
	return a, cmd
}

func (a App) View() string {
	var body string
	switch a.screen {
	case screenDAGList:
		body = a.dagList.View()
	case screenRunHistory:
		body = a.runHistory.View()
	case screenStepView:
		body = a.stepView.View()
	}
	if a.showHelp {
		body += "\n\n" + a.renderHelp()
	}
	return body
}

// renderHelp lists every binding the active screen responds to, built from
// the same keys.go source the screens actually use (FR-013,
// contracts/keybindings.md) — never a separately maintained copy.
func (a App) renderHelp() string {
	bindings := Keys.GlobalHelp()
	switch a.screen {
	case screenDAGList:
		bindings = append(bindings, Keys.DagListExtra()...)
	case screenStepView:
		bindings = append(bindings, Keys.StepViewExtra()...)
	}
	line := "help: "
	for i, b := range bindings {
		if i > 0 {
			line += "  ·  "
		}
		h := b.Help()
		line += h.Key + " " + h.Desc
	}
	return styleHelp.Render(line)
}
