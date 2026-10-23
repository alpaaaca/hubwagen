package tui

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/alper/hubwagen/internal/state"
)

// taskListLoadedMsg carries the result of a task-status poll.
type taskListLoadedMsg struct {
	tasks []TaskInfo
	err   error
}

// taskViewTickMsg fires every refreshInterval.
type taskViewTickMsg struct{}

// logsLoadedMsg carries a selected task's log lines. logsURI identifies which
// attempt they belong to, so a late result for a previous selection is
// dropped.
type logsLoadedMsg struct {
	logsURI string
	lines   []string
	err     error
}

// taskViewModel is the run screen: every task's status, plus a log pane for
// the selected task that follows its newest attempt while it runs.
type taskViewModel struct {
	ctx      context.Context
	src      Source
	pipeline string // tenant/name, for the title
	runID    string
	shape    []TaskShape // the pipeline's graph, for graph mode

	table     table.Model
	viewport  viewport.Model
	tasks     []TaskInfo
	graphView bool // toggled by 'v' — table (default) vs. dependency graph

	selectedTaskID string
	logsURI        string // the selected task's newest attempt
	logLines       int    // lines shown, to detect growth

	width, height int
	err           string
}

func newTaskViewModel(ctx context.Context, src Source, pipeline, runID string, shape []TaskShape, w, h int) taskViewModel {
	cols := []table.Column{
		{Title: "TASK", Width: 22},
		{Title: "STATUS", Width: 18},
		{Title: "TRIES", Width: 5},
		{Title: "DURATION", Width: 10},
		{Title: "REASON", Width: 14},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true), table.WithHeight(10))
	vp := viewport.New(w, 10)
	m := taskViewModel{ctx: ctx, src: src, pipeline: pipeline, runID: runID, shape: shape, table: t, viewport: vp}
	return m.SetSize(w, h)
}

func (m taskViewModel) Init() tea.Cmd {
	return tea.Batch(m.loadCmd(), m.tickCmd())
}

func (m taskViewModel) SetSize(w, h int) taskViewModel {
	m.width, m.height = w, h
	if h > 14 {
		m.table.SetHeight(6)
		m.viewport.Height = h - 14
	}
	m.viewport.Width = w
	m.table.SetWidth(w)
	return m
}

func (m taskViewModel) tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg { return taskViewTickMsg{} })
}

func (m taskViewModel) loadCmd() tea.Cmd {
	src, ctx, runID := m.src, m.ctx, m.runID
	return func() tea.Msg {
		tasks, err := src.Tasks(ctx, runID)
		return taskListLoadedMsg{tasks: tasks, err: err}
	}
}

func (m taskViewModel) logsCmd(uri string) tea.Cmd {
	if uri == "" {
		return nil
	}
	src, ctx := m.src, m.ctx
	return func() tea.Msg {
		lines, err := src.Logs(ctx, uri)
		return logsLoadedMsg{logsURI: uri, lines: lines, err: err}
	}
}

// selectTask switches the log pane to taskID's newest attempt.
func (m *taskViewModel) selectTask(t TaskInfo) tea.Cmd {
	m.selectedTaskID = t.TaskID
	m.logsURI = t.LogsURI
	m.logLines = 0
	m.viewport.SetContent(styleMuted.Render("(no output yet)"))
	return m.logsCmd(t.LogsURI)
}

func (m taskViewModel) Update(msg tea.Msg) (taskViewModel, tea.Cmd) {
	switch msg := msg.(type) {
	case taskListLoadedMsg:
		if msg.err != nil {
			m.err = msg.err.Error()
			return m, nil
		}
		m.err = ""
		m.tasks = msg.tasks
		rows := make([]table.Row, 0, len(msg.tasks))
		for _, t := range msg.tasks {
			duration := "—"
			if t.StartedAt != nil {
				end := time.Now()
				if t.FinishedAt != nil {
					end = *t.FinishedAt
				}
				duration = end.Sub(*t.StartedAt).Round(time.Second).String()
			}
			reason := ""
			if t.Status != state.TaskSucceeded {
				reason = t.Reason
			}
			rows = append(rows, table.Row{t.TaskID, RenderStatus(t.Status), itoa(t.Attempts), duration, reason})
		}
		m.table.SetRows(rows)

		// Follow the selected task to its newest attempt (a retry starts a
		// new log), and keep polling its log while it may still grow.
		if m.selectedTaskID == "" && len(m.tasks) > 0 {
			return m, m.selectTask(m.tasks[0])
		}
		for _, t := range m.tasks {
			if t.TaskID != m.selectedTaskID {
				continue
			}
			if t.LogsURI != m.logsURI {
				return m, m.selectTask(t)
			}
			return m, m.logsCmd(m.logsURI)
		}
		return m, nil

	case taskViewTickMsg:
		if m.allTerminal() {
			return m, nil
		}
		return m, tea.Batch(m.loadCmd(), m.tickCmd())

	case logsLoadedMsg:
		if msg.logsURI != m.logsURI || len(msg.lines) == m.logLines && msg.err == nil {
			return m, nil
		}
		// Keep following the end if the user was there; leave them where
		// they are if they scrolled up to read.
		atBottom := m.viewport.AtBottom()
		m.logLines = len(msg.lines)
		content := strings.Join(msg.lines, "\n")
		if msg.err != nil {
			content += "\n" + styleError.Render("reading logs: "+msg.err.Error())
		}
		m.viewport.SetContent(content)
		if atBottom {
			m.viewport.GotoBottom()
		}
		return m, nil

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, Keys.Up), key.Matches(msg, Keys.Down):
			var cmd tea.Cmd
			m.table, cmd = m.table.Update(msg)
			if t, ok := m.selectedTask(); ok && t.TaskID != m.selectedTaskID {
				return m, tea.Batch(cmd, m.selectTask(t))
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
			return m, func() tea.Msg { return navigateBackMsg{} }
		case key.Matches(msg, Keys.Refresh):
			return m, m.loadCmd()
		}
	}

	var cmd tea.Cmd
	m.viewport, cmd = m.viewport.Update(msg)
	return m, cmd
}

func (m taskViewModel) allTerminal() bool {
	if len(m.tasks) == 0 {
		return false
	}
	for _, t := range m.tasks {
		if !state.TaskTerminal(t.Status) {
			return false
		}
	}
	return true
}

func (m taskViewModel) selectedTask() (TaskInfo, bool) {
	cursor := m.table.Cursor()
	if cursor < 0 || cursor >= len(m.tasks) {
		return TaskInfo{}, false
	}
	return m.tasks[cursor], true
}

func (m taskViewModel) View() string {
	var b strings.Builder
	b.WriteString(styleTitle.Render("hubwagen — run " + m.runID + " (" + m.pipeline + ")"))
	b.WriteString("\n\n")
	if m.graphView {
		b.WriteString(m.renderGraphView())
	} else {
		b.WriteString(colorizeStatuses(m.table.View()))
	}
	b.WriteString("\n\n")
	if m.selectedTaskID != "" {
		b.WriteString(styleMuted.Render("log — " + m.selectedTaskID + "\n"))
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
		"↑/↓ select task · g/G log top/bottom · v "+mode+" view · b back · ? help · q quit"))
	return b.String()
}

// renderGraphView draws the pipeline's graph with each task's current status.
func (m taskViewModel) renderGraphView() string {
	if len(m.shape) == 0 {
		return styleMuted.Render("(pipeline graph unavailable for this run)")
	}
	status := make(map[string]string, len(m.tasks))
	for _, t := range m.tasks {
		status[t.TaskID] = t.Status
	}
	byID := make(map[string]taskNode, len(m.shape))
	for _, s := range m.shape {
		byID[s.ID] = taskNode{ID: s.ID, DependsOn: s.DependsOn, Status: status[s.ID]}
	}
	return renderGraph(layers(m.shape), byID)
}

func itoa(n int) string {
	if n == 0 {
		return "—"
	}
	return strconv.Itoa(n)
}
