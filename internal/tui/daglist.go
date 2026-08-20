package tui

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/table"
	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/robfig/cron/v3"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	"github.com/alper/hubwagen/internal/scheduler"
	"github.com/alper/hubwagen/internal/store"
)

// dagListLoadedMsg carries the result of a discovery + last-run poll.
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
	dagName string
	result  scheduler.TriggerResult
	err     error
}

// dagListRow is one rendered row — either a valid DAG or an invalid file,
// flagged rather than omitted.
type dagListRow struct {
	Discovered   dag.Discovered
	Name         string
	ScheduleDisp string
	LastStatus   string
	LastRunDisp  string
	NextRunDisp  string
}

// dagListModel is Screen 1 (US1): every discovered DAG, its schedule, its
// last-run status, and its next scheduled run, kept live via a poll tick.
type dagListModel struct {
	ctx       context.Context
	store     store.Store
	dagsDir   string
	executors map[string]executor.Executor

	table table.Model

	allRows     []dagListRow // unfiltered, sorted by name
	visibleRows []dagListRow // post-filter subset, index-aligned with m.table's rows
	filter      textinput.Model
	filtering   bool

	width, height int
	err           string
	statusMsg     string
}

func newDAGListModel(ctx context.Context, st store.Store, dagsDir string, executors map[string]executor.Executor) dagListModel {
	cols := []table.Column{
		{Title: "NAME", Width: 24},
		{Title: "SCHEDULE", Width: 16},
		{Title: "LAST STATUS", Width: 16},
		{Title: "LAST RUN", Width: 20},
		{Title: "NEXT RUN", Width: 20},
	}
	t := table.New(table.WithColumns(cols), table.WithFocused(true), table.WithHeight(15))

	fi := textinput.New()
	fi.Placeholder = "filter by name…"

	return dagListModel{
		ctx:       ctx,
		store:     st,
		dagsDir:   dagsDir,
		executors: executors,
		table:     t,
		filter:    fi,
	}
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
	dagsDir := m.dagsDir
	st := m.store
	ctx := m.ctx
	return func() tea.Msg {
		discovered, err := dag.DiscoverDir(dagsDir)
		if err != nil {
			return dagListLoadedMsg{err: err}
		}

		rows := make([]dagListRow, 0, len(discovered))
		for _, d := range discovered {
			if d.ParseErr != nil {
				rows = append(rows, dagListRow{
					Discovered:   d,
					Name:         filepath.Base(d.Path) + " (invalid)",
					ScheduleDisp: "—",
					LastStatus:   "invalid",
					LastRunDisp:  "—",
					NextRunDisp:  "—",
				})
				continue
			}

			row := dagListRow{Discovered: d, Name: d.DAG.Name}
			if d.DAG.Schedule == "" {
				row.ScheduleDisp = "manual"
				row.NextRunDisp = "—"
			} else {
				row.ScheduleDisp = d.DAG.Schedule
				if sched, err := cron.ParseStandard(d.DAG.Schedule); err == nil {
					row.NextRunDisp = sched.Next(time.Now()).Format("2006-01-02 15:04")
				} else {
					row.NextRunDisp = "invalid schedule"
				}
			}

			runs, err := st.ListRuns(ctx, d.DAG.Name, 1)
			if err == nil && len(runs) > 0 {
				row.LastStatus = runs[0].Status
				if runs[0].StartedAt != nil {
					row.LastRunDisp = relativeTime(*runs[0].StartedAt)
				}
			} else {
				row.LastStatus = "no runs yet"
				row.LastRunDisp = "—"
			}
			rows = append(rows, row)
		}

		sort.Slice(rows, func(i, j int) bool { return rows[i].Name < rows[j].Name })
		return dagListLoadedMsg{rows: rows}
	}
}

func (m dagListModel) triggerCmd(d dag.Discovered) tea.Cmd {
	executors := m.executors
	st := m.store
	ctx := m.ctx
	return func() tea.Msg {
		res, err := scheduler.TriggerManual(ctx, st, d, executors, discardWriter{}, "manual")
		return triggerResultMsg{dagName: d.DAG.Name, result: res, err: err}
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
			m.statusMsg = fmt.Sprintf("trigger failed for %s: %v", msg.dagName, msg.err)
		} else if msg.result.Skipped {
			m.statusMsg = msg.result.Reason
		} else {
			m.statusMsg = fmt.Sprintf("triggered %s (run %s)", msg.dagName, msg.result.RunID)
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
			if !ok || row.Discovered.ParseErr != nil {
				m.statusMsg = "select a valid DAG to trigger"
				return m, nil
			}
			return m, m.triggerCmd(row.Discovered)
		case key.Matches(msg, Keys.Enter):
			row, ok := m.selectedRow()
			if !ok || row.Discovered.ParseErr != nil {
				return m, nil
			}
			dagName, steps := row.Discovered.DAG.Name, row.Discovered.DAG.Steps
			return m, func() tea.Msg { return navigateToRunHistoryMsg{dagName: dagName, steps: steps} }
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
	b.WriteString(styleTitle.Render("hubwagen — DAG List"))
	b.WriteString("\n\n")
	if m.filtering {
		b.WriteString("filter: " + m.filter.View() + "\n\n")
	}
	if len(m.allRows) == 0 && m.err == "" {
		b.WriteString(styleMuted.Render("No DAGs found under " + m.dagsDir + ". Add a .yaml file there to get started.\n"))
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
	b.WriteString("\n\n" + styleHelp.Render("↑/↓ select · enter run history · t trigger now · / filter · ? help · q quit"))
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
