package tui

import (
	"context"
	"time"

	"github.com/charmbracelet/bubbles/key"
	tea "github.com/charmbracelet/bubbletea"
)

// refreshInterval is the poll cadence every screen uses to notice status
// changes made outside the dashboard (scheduled runs, task progress).
const refreshInterval = 2 * time.Second

type screen int

const (
	screenPipelineList screen = iota
	screenRunHistory
	screenTaskView
)

// Navigation messages — screens signal a desired transition by returning a
// tea.Cmd that produces one of these; the root App intercepts them and
// switches screens. Screens never reach into each other directly.
//
// tasks carries the pipeline's graph (task IDs + depends_on) from the list
// through to the run view, which draws it in graph mode.
type navigateToRunHistoryMsg struct {
	tenant, pipeline string
	tasks            []TaskShape
}
type navigateToTaskViewMsg struct {
	tenant, pipeline, runID string
	tasks                   []TaskShape
}
type navigateBackMsg struct{}

// App is the root bubbletea model: the screen router. Every screen reads and
// acts through the same Source.
type App struct {
	ctx context.Context
	src Source

	screen screen
	width  int
	height int

	showHelp bool

	pipelineList pipelineListModel
	runHistory   runHistoryModel
	taskView     taskViewModel
}

// NewApp constructs the root model. The pipeline list is loaded lazily via Init.
func NewApp(ctx context.Context, src Source) App {
	return App{
		ctx:          ctx,
		src:          src,
		screen:       screenPipelineList,
		pipelineList: newPipelineListModel(ctx, src),
	}
}

func (a App) Init() tea.Cmd {
	return a.pipelineList.Init()
}

func (a App) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		a.width, a.height = msg.Width, msg.Height
		a.pipelineList = a.pipelineList.SetSize(msg.Width, msg.Height)
		a.runHistory = a.runHistory.SetSize(msg.Width, msg.Height)
		a.taskView = a.taskView.SetSize(msg.Width, msg.Height)
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
		a.runHistory = newRunHistoryModel(a.ctx, a.src, msg.tenant, msg.pipeline, msg.tasks, a.width, a.height)
		a.screen = screenRunHistory
		return a, a.runHistory.Init()

	case navigateToTaskViewMsg:
		a.taskView = newTaskViewModel(a.ctx, a.src, msg.tenant+"/"+msg.pipeline, msg.runID, msg.tasks, a.width, a.height)
		a.screen = screenTaskView
		return a, a.taskView.Init()

	case navigateBackMsg:
		switch a.screen {
		case screenTaskView:
			a.screen = screenRunHistory
		case screenRunHistory:
			a.screen = screenPipelineList
		case screenPipelineList:
			// No-op — the pipeline list is the root screen.
		}
		return a, nil
	}

	// Not a global/navigation message — forward to the active screen.
	var cmd tea.Cmd
	switch a.screen {
	case screenPipelineList:
		a.pipelineList, cmd = a.pipelineList.Update(msg)
	case screenRunHistory:
		a.runHistory, cmd = a.runHistory.Update(msg)
	case screenTaskView:
		a.taskView, cmd = a.taskView.Update(msg)
	}
	return a, cmd
}

func (a App) View() string {
	var body string
	switch a.screen {
	case screenPipelineList:
		body = a.pipelineList.View()
	case screenRunHistory:
		body = a.runHistory.View()
	case screenTaskView:
		body = a.taskView.View()
	}
	if a.showHelp {
		body += "\n\n" + a.renderHelp()
	}
	return body
}

// renderHelp lists every binding the active screen responds to, built from
// the same keys.go source the screens actually use — never a separately maintained copy.
func (a App) renderHelp() string {
	bindings := Keys.GlobalHelp()
	switch a.screen {
	case screenPipelineList:
		bindings = append(bindings, Keys.PipelineListExtra()...)
	case screenTaskView:
		bindings = append(bindings, Keys.TaskViewExtra()...)
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
