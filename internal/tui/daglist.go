package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"

	"github.com/alper/hubwagen/internal/core"
)

// dagListLoadedMsg carries the result of a pipeline + last-run poll.
type dagListLoadedMsg struct {
	rows []dagListRow
	err  error
}

// dagListTickMsg fires every refreshInterval to trigger a re-poll (FR-008,
// FR-009, SC-003). Distinct type per screen so a stray tick from a screen
// the user has since navigated away from is silently ignored elsewhere.
type dagListTickMsg struct{}

// triggerResultMsg carries the outcome of a manual trigger (US4).
type triggerResultMsg struct {
	name   string
	result core.TriggerResult
	err    error
}

// dagListRow is one rendered row — either a pipeline or a file that failed
// to load, flagged rather than omitted.
type dagListRow struct {
	Pipeline     PipelineInfo
	Name         string
	ScheduleDisp string
	LastStatus   string
	LastRunDisp  string
	NextRunDisp  string
}

func (r dagListRow) invalid() bool { return r.Pipeline.Err != "" }

// dagListModel is the home screen: every pipeline, its schedule, its
// last-run status, and its next scheduled run, kept live via a poll tick.
type dagListModel struct {
	ctx context.Context
	src Source

	table table.Model

	allRows     []dagListRow // unfiltered, sorted by name
	visibleRows []dagListRow // post-filter subset, index-aligned with m.table's rows
	filter      textinput.Model
	filtering   bool

	width, height int
	err           string
	statusMsg     string
}

func newDAGListModel(ctx context.Context, src Source) dagListModel {
	cols := []table.Column{
		{Title: "NAME", Width: 28},
		{Title: "SCHEDULE", Width: 28},
		{Title: "LAST STATUS", Width: 16},
		{Title: "LAST RUN", Width: 20},
		{Title: "NEXT RUN", Width: 20},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true), table.WithHeight(15))

	fi := textinput.New()
	fi.Placeholder = "filter by name…"

	return dagListModel{ctx: ctx, src: src, table: t, filter: fi}
}

func (m dagListModel) Init() tea.Cmd {
	return tea.Batch(m.loadCmd(), m.tickCmd())
}

func (m dagListModel) SetSize(w, h int) dagListModel {
	m.width, m.height = w, h
	if h > 8 {
		m.table.SetHeight(h - 8)
	}
	m.table.SetWidth(w)
	return m
}

func (m dagListModel) tickCmd() tea.Cmd {
	return tea.Tick(refreshInterval, func(time.Time) tea.Msg { return dagListTickMsg{} })
}

func (m dagListModel) loadCmd() tea.Cmd {
	src, ctx := m.src, m.ctx
	return func() tea.Msg {
		pipes, err := src.Pipelines(ctx)
		if err != nil {
			return dagListLoadedMsg{err: err}
		}
		rows := make([]dagListRow, 0, len(pipes))
		for _, p := range pipes {
			if p.Err != "" {
				rows = append(rows, dagListRow{Pipeline: p, Name: p.Key() + " (invalid)",
					ScheduleDisp: "—", LastStatus: "invalid", LastRunDisp: "—", NextRunDisp: "—"})
				continue
			}
			row := dagListRow{Pipeline: p, Name: p.Key(), ScheduleDisp: "manual", NextRunDisp: "—"}
			if p.Schedule != "" {
				row.ScheduleDisp = p.Schedule
			}
			switch {
			case p.Paused:
				row.NextRunDisp = "paused"
			case p.NextRun != nil:
				row.NextRunDisp = p.NextRun.Local().Format("2006-01-02 15:04")
			}
			runs, err := src.Runs(ctx, p.Tenant, p.Name, 1)
			if err == nil && len(runs) > 0 {
				row.LastStatus = runs[0].Status
				row.LastRunDisp = relativeTime(runs[0].CreatedAt)
			} else {
				row.LastStatus = "no runs yet"
				row.LastRunDisp = "—"
			}
			rows = append(rows, row)
		}
		return dagListLoadedMsg{rows: rows}
	}
}

func (m dagListModel) triggerCmd(p PipelineInfo) tea.Cmd {
	src, ctx := m.src, m.ctx
	return func() tea.Msg {
		res, err := src.Trigger(ctx, p.Tenant, p.Name)
		return triggerResultMsg{name: p.Key(), result: res, err: err}
	}
}

func (m dagListModel) Update(msg tea.Msg) (dagListModel, tea.Cmd) {
	switch msg := msg.(type) {
	case dagListLoadedMsg:
		if msg.err != nil {
			m.err = msg.err.Error()
			return m, nil
		}
		m.err = ""
		m.allRows = msg.rows
		m.applyFilter()
		return m, nil

	case dagListTickMsg:
		return m, tea.Batch(m.loadCmd(), m.tickCmd())

	case triggerResultMsg:
		if msg.err != nil {
			m.statusMsg = fmt.Sprintf("trigger failed for %s: %v", msg.name, msg.err)
		} else if msg.result.Skipped {
			m.statusMsg = msg.result.Reason
		} else {
			m.statusMsg = fmt.Sprintf("triggered %s (run %s)", msg.name, msg.result.RunID)
		}
		return m, m.loadCmd()

	case tea.KeyMsg:
		if m.filtering {
			switch {
			case key.Matches(msg, Keys.Back):
				m.filtering = false
				m.filter.SetValue("")
				m.filter.Blur()
				m.applyFilter()
				return m, nil
			case key.Matches(msg, Keys.Enter):
				m.filtering = false
				m.filter.Blur()
				return m, nil
			}
			var cmd tea.Cmd
			m.filter, cmd = m.filter.Update(msg)
			m.applyFilter()
			return m, cmd
		}

		switch {
		case key.Matches(msg, Keys.Filter):
			m.filtering = true
			m.statusMsg = ""
			return m, m.filter.Focus()
		case key.Matches(msg, Keys.Trigger):
			row, ok := m.selectedRow()
			if !ok || row.invalid() {
				m.statusMsg = "select a valid pipeline to trigger"
				return m, nil
			}
			return m, m.triggerCmd(row.Pipeline)
		case key.Matches(msg, Keys.Enter):
			row, ok := m.selectedRow()
			if !ok {
				return m, nil
			}
			if row.invalid() {
				m.statusMsg = row.Pipeline.Key() + ": " + row.Pipeline.Err
				return m, nil
			}
			p := row.Pipeline
			return m, func() tea.Msg {
				return navigateToRunHistoryMsg{tenant: p.Tenant, pipeline: p.Name, tasks: p.Tasks}
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

func (m *dagListModel) applyFilter() {
	needle := strings.ToLower(strings.TrimSpace(m.filter.Value()))
	rows := make([]table.Row, 0, len(m.allRows))
	visible := make([]dagListRow, 0, len(m.allRows))
	for _, r := range m.allRows {
		if needle != "" && !strings.Contains(strings.ToLower(r.Name), needle) {
			continue
		}
		visible = append(visible, r)
		rows = append(rows, table.Row{r.Name, r.ScheduleDisp, RenderStatus(r.LastStatus), r.LastRunDisp, r.NextRunDisp})
	}
	m.visibleRows = visible
	m.table.SetRows(rows)
}

func (m dagListModel) selectedRow() (dagListRow, bool) {
	cursor := m.table.Cursor()
	if cursor < 0 || cursor >= len(m.visibleRows) {
		return dagListRow{}, false
	}
	return m.visibleRows[cursor], true
}

func (m dagListModel) View() string {
	var b strings.Builder
	b.WriteString(styleTitle.Render("hubwagen — Pipelines"))
	b.WriteString("\n\n")
	if m.filtering {
		b.WriteString("filter: " + m.filter.View() + "\n\n")
	}
	if len(m.allRows) == 0 && m.err == "" {
		b.WriteString(styleMuted.Render("No pipelines yet. Add a .yaml file to the watched directory to get started.\n"))
	} else {
		b.WriteString(colorizeStatuses(m.table.View()))
		b.WriteString("\n")
	}
	if m.err != "" {
		b.WriteString("\n" + styleError.Render("error: "+m.err))
	}
	if m.statusMsg != "" {
		b.WriteString("\n" + styleMuted.Render(m.statusMsg))
	}
	b.WriteString("\n\n" + styleHelp.Render("↑/↓ select · enter run history (or error) · t trigger now · / filter · ? help · q quit"))
	return b.String()
}

func relativeTime(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return t.Format("2006-01-02 15:04")
	}
}
