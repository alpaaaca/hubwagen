package tui

import (
	"context"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/store"
)

// runHistoryLoadedMsg carries the result of a run-history poll.
type runHistoryLoadedMsg struct {
	runs []*store.Run
	err  error
}

// runHistoryTickMsg fires every refreshInterval. Distinct type per screen
// so a stray tick from elsewhere is silently ignored.
type runHistoryTickMsg struct{}

// historyDepth is the clarified run-history depth (spec.md clarification).
const historyDepth = 20

// runHistoryModel is Screen 2 (US2): the last historyDepth runs of a
// selected DAG, most-recent-first.
type runHistoryModel struct {
	ctx     context.Context
	store   store.Store
	dagName string
	steps   []dag.Step // the DAG's structure, carried forward for Step View's graph mode

	table table.Model
	runs  []*store.Run

	width, height int
	err           string
}

func newRunHistoryModel(ctx context.Context, st store.Store, dagName string, steps []dag.Step, w, h int) runHistoryModel {
	cols := []table.Column{
		{Title: "RUN ID", Width: 28},
		{Title: "TRIGGER", Width: 10},
		{Title: "STATUS", Width: 16},
		{Title: "STARTED", Width: 20},
		{Title: "DURATION", Width: 10},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true), table.WithHeight(15))
	m := runHistoryModel{ctx: ctx, store: st, dagName: dagName, steps: steps, table: t}
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
	st, ctx, dagName := m.store, m.ctx, m.dagName
	return func() tea.Msg {
		runs, err := st.ListRuns(ctx, dagName, historyDepth)
		return runHistoryLoadedMsg{runs: runs, err: err}
	}
}

func (m runHistoryModel) anyNonTerminal() bool {
	if len(m.runs) == 0 {
		return true // nothing loaded yet — keep polling until the first load lands
	}
	for _, r := range m.runs {
		if !isTerminalStatus(r.Status) {
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
			rows = append(rows, table.Row{r.ID, r.Trigger, RenderStatus(r.Status), started, duration})
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
			dagName, runID, steps := m.dagName, run.ID, m.steps
			return m, func() tea.Msg {
				return navigateToStepViewMsg{dagName: dagName, runID: runID, steps: steps}
			}
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

func (m runHistoryModel) selectedRun() (*store.Run, bool) {
	cursor := m.table.Cursor()
	if cursor < 0 || cursor >= len(m.runs) {
		return nil, false
	}
	return m.runs[cursor], true
}

func (m runHistoryModel) View() string {
	var b strings.Builder
	b.WriteString(styleTitle.Render("hubwagen — " + m.dagName))
	b.WriteString("\n\n")
	if len(m.runs) == 0 && m.err == "" {
		b.WriteString(styleMuted.Render("No runs yet for " + m.dagName + ".\n"))
	} else {
		b.WriteString(colorizeStatuses(m.table.View()))
		b.WriteString("\n")
	}
	if m.err != "" {
		b.WriteString("\n" + styleError.Render("error: "+m.err))
	}
	b.WriteString("\n\n" + styleHelp.Render("↑/↓ select · enter step detail · b back · ? help · q quit"))
	return b.String()
}

func isTerminalStatus(s string) bool {
	switch s {
	case "succeeded", "failed", "cancelled", "skipped":
		return true
	default:
		return false
	}
}
