package tui

import (
	"context"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/store"
)

// stepListLoadedMsg carries the result of a step-status poll.
type stepListLoadedMsg struct {
	steps []*store.StepRun
	err   error
}

// stepViewTickMsg fires every refreshInterval.
type stepViewTickMsg struct{}

// logLineMsg carries one line read from the active step's log channel.
type logLineMsg struct{ line string }

// logStreamClosedMsg signals the active step's log channel has been closed
// (store.StreamLogs closes it once the step reaches a terminal status).
type logStreamClosedMsg struct{}

// stepViewModel is Screen 3 (US3): every step's status, plus a live-tailing
// log pane for whichever step is selected.
type stepViewModel struct {
	ctx      context.Context
	store    store.Store
	dagName  string
	runID    string
	dagSteps []dag.Step // the DAG's structure (IDs + depends_on), for graph mode

	table     table.Model
	viewport  viewport.Model
	steps     []*store.StepRun
	graphView bool // toggled by 'v' — table (default) vs. dependency graph

	selectedStepID string
	logCh          <-chan string
	logCancel      context.CancelFunc
	logContent     string // plain string, not strings.Builder: this model is
	// copied by value on every Update() call (see the value-receiver methods
	// throughout this file), and a copied-then-reused strings.Builder panics
	// at runtime ("illegal use of non-zero Builder copied by value")

	width, height int
	err           string
}

func newStepViewModel(ctx context.Context, st store.Store, dagName, runID string, steps []dag.Step, w, h int) stepViewModel {
	cols := []table.Column{
		{Title: "STEP", Width: 20},
		{Title: "STATUS", Width: 16},
		{Title: "DURATION", Width: 10},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true), table.WithHeight(10))
	vp := viewport.New(w, 10)
	m := stepViewModel{ctx: ctx, store: st, dagName: dagName, runID: runID, dagSteps: steps, table: t, viewport: vp}
	return m.SetSize(w, h)
}

func (m stepViewModel) Init() tea.Cmd {
	return tea.Batch(m.loadCmd(), m.tickCmd())
}

func (m stepViewModel) SetSize(w, h int) stepViewModel {
	m.width, m.height = w, h
	if h > 14 {
		m.table.SetHeight(6)
		m.viewport.Height = h - 14
	}
	m.viewport.Width = w
	m.table.SetWidth(w)
	return m
}

func (m stepViewModel) tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg { return stepViewTickMsg{} })
}

func (m stepViewModel) loadCmd() tea.Cmd {
	st, ctx, runID := m.store, m.ctx, m.runID
	return func() tea.Msg {
		steps, err := st.ListStepRuns(ctx, runID)
		return stepListLoadedMsg{steps: steps, err: err}
	}
}

// selectStepCmd cancels any previous step's log subscription and starts a
// fresh one for stepID via store.StreamLogs. StreamLogs already delivers
// every existing line before switching to live tail and closes the channel
// once the step is terminal (internal/store/sqlite.go), so no separate
// GetLogs backfill call is needed — one subscription covers both.
func (m *stepViewModel) selectStepCmd(stepID string) tea.Cmd {
	if m.logCancel != nil {
		m.logCancel()
	}
	logCtx, cancel := context.WithCancel(m.ctx)
	m.logCancel = cancel
	m.selectedStepID = stepID
	m.logContent = ""
	m.viewport.SetContent("")

	st, runID := m.store, m.runID
	return func() tea.Msg {
		ch, err := st.StreamLogs(logCtx, runID, stepID)
		if err != nil {
			return logStreamClosedMsg{}
		}
		return startedLogStreamMsg{ch: ch}
	}
}

// startedLogStreamMsg hands the fresh channel back to Update, which then
// arms the first waitForLogLine read on it.
type startedLogStreamMsg struct{ ch <-chan string }

func waitForLogLine(ch <-chan string) tea.Cmd {
	return func() tea.Msg {
		line, ok := <-ch
		if !ok {
			return logStreamClosedMsg{}
		}
		return logLineMsg{line: line}
	}
}

func (m stepViewModel) Update(msg tea.Msg) (stepViewModel, tea.Cmd) {
	switch msg := msg.(type) {
	case stepListLoadedMsg:
		if msg.err != nil {
			m.err = msg.err.Error()
			return m, nil
		}
		m.err = ""
		m.steps = msg.steps
		rows := make([]table.Row, 0, len(msg.steps))
		for _, s := range msg.steps {
			duration := "—"
			if s.StartedAt != nil {
				end := time.Now()
				if s.FinishedAt != nil {
					end = *s.FinishedAt
				}
				duration = end.Sub(*s.StartedAt).Round(time.Second).String()
			}
			rows = append(rows, table.Row{s.StepID, RenderStatus(s.Status), duration})
		}
		m.table.SetRows(rows)

		var cmd tea.Cmd
		if m.selectedStepID == "" && len(m.steps) > 0 {
			cmd = m.selectStepCmd(m.steps[0].StepID)
		}
		return m, cmd

	case stepViewTickMsg:
		if m.allStepsTerminal() {
			return m, nil
		}
		return m, tea.Batch(m.loadCmd(), m.tickCmd())

	case startedLogStreamMsg:
		m.logCh = msg.ch
		return m, waitForLogLine(m.logCh)

	case logLineMsg:
		// Check scroll position BEFORE appending — if the user was already
		// at the bottom (the common "tailing" case), keep following new
		// output; if they've scrolled up to read earlier lines, leave the
		// viewport where they left it.
		atBottom := m.viewport.AtBottom()
		if m.logContent == "" {
			m.logContent = msg.line
		} else {
			m.logContent += "\n" + msg.line
		}
		m.viewport.SetContent(m.logContent)
		if atBottom {
			m.viewport.GotoBottom()
		}
		return m, waitForLogLine(m.logCh)

	case logStreamClosedMsg:
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, Keys.Up), key.Matches(msg, Keys.Down):
			var cmd tea.Cmd
			m.table, cmd = m.table.Update(msg)
			if step, ok := m.selectedStep(); ok && step.StepID != m.selectedStepID {
				return m, tea.Batch(cmd, m.selectStepCmd(step.StepID))
			}
			return m, cmd
		case key.Matches(msg, Keys.Top):
			m.viewport.GotoTop()
			return m, nil
		case key.Matches(msg, Keys.Bottom):
			m.viewport.GotoBottom()
			return m, nil
		case key.Matches(msg, Keys.Graph):
			m.graphView = !m.graphView
			return m, nil
		case key.Matches(msg, Keys.Back):
			if m.logCancel != nil {
				m.logCancel()
			}
			return m, func() tea.Msg { return navigateBackMsg{} }
		case key.Matches(msg, Keys.Refresh):
			return m, m.loadCmd()
		}
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m stepViewModel) allStepsTerminal() bool {
	if len(m.steps) == 0 {
		return false
	}
	for _, s := range m.steps {
		if !isTerminalStatus(s.Status) {
			return false
		}
	}
	return true
}

func (m stepViewModel) selectedStep() (*store.StepRun, bool) {
	cursor := m.table.Cursor()
	if cursor < 0 || cursor >= len(m.steps) {
		return nil, false
	}
	return m.steps[cursor], true
}

func (m stepViewModel) View() string {
	var b strings.Builder
	b.WriteString(styleTitle.Render("hubwagen — run " + m.runID + " (" + m.dagName + ")"))
	b.WriteString("\n\n")
	if m.graphView {
		b.WriteString(m.renderGraphView())
	} else {
		b.WriteString(colorizeStatuses(m.table.View()))
	}
	b.WriteString("\n\n")
	if m.selectedStepID != "" {
		b.WriteString(styleMuted.Render("log — " + m.selectedStepID + "\n"))
	}
	b.WriteString(m.viewport.View())
	if m.err != "" {
		b.WriteString("\n" + styleError.Render("error: "+m.err))
	}
	mode := "table"
	if m.graphView {
		mode = "graph"
	}
	b.WriteString("\n\n" + styleHelp.Render(
		"↑/↓ select step · g/G log top/bottom · v "+mode+" view · b back · ? help · q quit"))
	return b.String()
}

// renderGraphView builds the graph.go input (step structure + current
// status per step) from this model's DAG definition and latest step-status
// poll, then renders it. Falls back to a plain message if the DAG structure
// wasn't threaded through navigation (shouldn't happen via the normal list
// -> history -> step-view path, but keeps this screen robust regardless).
func (m stepViewModel) renderGraphView() string {
	if len(m.dagSteps) == 0 {
		return styleMuted.Render("(DAG structure unavailable for this run)")
	}

	statusByID := make(map[string]string, len(m.steps))
	for _, s := range m.steps {
		statusByID[s.StepID] = s.Status
	}

	byID := make(map[string]stepNode, len(m.dagSteps))
	for _, s := range m.dagSteps {
		byID[s.ID] = stepNode{ID: s.ID, DependsOn: s.DependsOn, Status: statusByID[s.ID]}
	}

	waves, err := dag.TopologicalWaves(m.dagSteps)
	if err != nil {
		return styleError.Render("error building graph: " + err.Error())
	}

	return renderGraph(waves, byID)
}
