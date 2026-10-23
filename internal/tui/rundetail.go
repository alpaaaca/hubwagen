package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/alper/hubwagen/internal/state"
)

// runHistoryLoadedMsg carries the result of a run-history poll.
type runHistoryLoadedMsg struct {
	runs []state.Run
	err  error
}

// runHistoryTickMsg fires every refreshInterval. Distinct type per screen
// so a stray tick from elsewhere is silently ignored.
type runHistoryTickMsg struct{}

// historyDepth is how many recent runs the screen shows.
const historyDepth = 20

// runHistoryModel is the second screen: the last historyDepth runs of a
// selected pipeline, most-recent-first.
type runHistoryModel struct {
	ctx      context.Context
	src      Source
	tenant   string
	pipeline string
	tasks    []TaskShape // the pipeline's graph, carried forward for the run view's graph mode

	table table.Model
	runs  []state.Run

	width, height int
	err           string
}

func newRunHistoryModel(ctx context.Context, src Source, tenant, pipeline string, tasks []TaskShape, w, h int) runHistoryModel {
	cols := []table.Column{
		{Title: "RUN ID", Width: 28},
		{Title: "TRIGGER", Width: 8},
		{Title: "INTERVAL", Width: 18},
		{Title: "STATUS", Width: 18},
		{Title: "STARTED", Width: 20},
		{Title: "DURATION", Width: 10},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true), table.WithHeight(15))
	m := runHistoryModel{ctx: ctx, src: src, tenant: tenant, pipeline: pipeline, tasks: tasks, table: t}
	return m.SetSize(w, h)
}

func (m runHistoryModel) Init() tea.Cmd {
	return tea.Batch(m.loadCmd(), m.tickCmd())
}

func (m runHistoryModel) SetSize(w, h int) runHistoryModel {
	m.width, m.height = w, h
	if h > 8 {
		m.table.SetHeight(h - 8)
	}
	m.table.SetWidth(w)
	return m
}

func (m runHistoryModel) tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg { return runHistoryTickMsg{} })
}

func (m runHistoryModel) loadCmd() tea.Cmd {
	src, ctx, tenant, pipeline := m.src, m.ctx, m.tenant, m.pipeline
	return func() tea.Msg {
		runs, err := src.Runs(ctx, tenant, pipeline, historyDepth)
		return runHistoryLoadedMsg{runs: runs, err: err}
	}
}

func (m runHistoryModel) anyNonTerminal() bool {
	if len(m.runs) == 0 {
		return true // nothing loaded yet — keep polling until the first load lands
	}
	for _, r := range m.runs {
		if !state.RunTerminal(r.Status) {
			return true
		}
	}
	return false
}

func (m runHistoryModel) Update(msg tea.Msg) (runHistoryModel, tea.Cmd) {
	switch msg := msg.(type) {
	case runHistoryLoadedMsg:
		if msg.err != nil {
			m.err = msg.err.Error()
			return m, nil
		}
		m.err = ""
		m.runs = msg.runs
		rows := make([]table.Row, 0, len(msg.runs))
		for _, r := range msg.runs {
			started := "—"
			if r.StartedAt != nil {
				started = r.StartedAt.Format("2006-01-02 15:04:05")
			}
			duration := "—"
			if r.StartedAt != nil && r.FinishedAt != nil {
				duration = r.FinishedAt.Sub(*r.StartedAt).Round(time.Second).String()
			}
			interval := "—"
			if r.IntervalStart != nil {
				interval = r.IntervalStart.Local().Format("2006-01-02 15:04")
			}
			if r.Generation > 1 {
				interval += fmt.Sprintf(" g%d", r.Generation)
			}
			rows = append(rows, table.Row{r.ID, r.Trigger, interval, RenderStatus(r.Status), started, duration})
		}
		m.table.SetRows(rows)
		return m, nil

	case runHistoryTickMsg:
		if !m.anyNonTerminal() {
			return m, nil
		}
		return m, tea.Batch(m.loadCmd(), m.tickCmd())

	case tea.KeyMsg:
		switch {
		case key.Matches(msg, Keys.Enter):
			run, ok := m.selectedRun()
			if !ok {
				return m, nil
			}
			msg := navigateToTaskViewMsg{tenant: m.tenant, pipeline: m.pipeline, runID: run.ID, tasks: m.tasks}
			return m, func() tea.Msg { return msg }
		case key.Matches(msg, Keys.Back):
			return m, func() tea.Msg { return navigateBackMsg{} }
		case key.Matches(msg, Keys.Refresh):
			return m, m.loadCmd()
		}
	}

	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	return m, cmd
}

func (m runHistoryModel) selectedRun() (state.Run, bool) {
	cursor := m.table.Cursor()
	if cursor < 0 || cursor >= len(m.runs) {
		return state.Run{}, false
	}
	return m.runs[cursor], true
}

func (m runHistoryModel) View() string {
	var b strings.Builder
	b.WriteString(styleTitle.Render("hubwagen — " + m.tenant + "/" + m.pipeline))
	b.WriteString("\n\n")
	if len(m.runs) == 0 && m.err == "" {
		b.WriteString(styleMuted.Render("No runs yet for " + m.tenant + "/" + m.pipeline + ". Press b, then t to trigger one.\n"))
	} else {
		b.WriteString(colorizeStatuses(m.table.View()))
		b.WriteString("\n")
	}
	if m.err != "" {
		b.WriteString("\n" + styleError.Render("error: "+m.err))
	}
	b.WriteString("\n\n" + styleHelp.Render("↑/↓ select · enter task detail · b back · ? help · q quit"))
	return b.String()
}
