// Package tui implements hubwagen's terminal dashboard — a bubbletea
// application that reads and acts exclusively through a Source.
package tui

import (
	"strings"

	"github.com/charmbracelet/lipgloss"
)

var (
	colorFailed = lipgloss.Color("1") // red
	colorMuted  = lipgloss.Color("8") // gray
	colorAccent = lipgloss.Color("6") // cyan
)

// statusSymbols pairs every run/task status with a single glyph. Rendered as
// "<symbol> <label>" via RenderStatus — symbol + text together means
// status is never shown by color alone; color (via colorizeStatuses below)
// is additive on top of that, never the only signal.
var statusSymbols = map[string]string{
	"succeeded":       "✓",
	"failed":          "✗",
	"upstream_failed": "✗",
	"running":         "●",
	"dispatched":      "◐",
	"ready":           "◐",
	"retry_wait":      "↻",
	"queued":          "○",
	"pending":         "○",
	"skipped":         "⧗",
	"cancelled":       "⊘",
	"invalid":         "!",
}

// statusColors pairs every status with a color, used only by
// colorizeStatuses — never embedded directly into a bubbles/table cell (see
// that function's doc comment for why).
var statusColors = map[string]lipgloss.Color{
	"succeeded":       lipgloss.Color("2"), // green
	"failed":          lipgloss.Color("1"), // red
	"upstream_failed": lipgloss.Color("1"),
	"running":         lipgloss.Color("3"), // yellow
	"dispatched":      lipgloss.Color("3"),
	"ready":           lipgloss.Color("3"),
	"retry_wait":      lipgloss.Color("3"),
	"queued":          colorMuted,
	"pending":         colorMuted,
	"skipped":         colorMuted,
	"cancelled":       colorMuted,
	"invalid":         lipgloss.Color("5"), // magenta
}

// RenderStatus renders a status string as "<symbol> <label>", plain text,
// no ANSI styling. Unknown statuses render as-is with no symbol, so an
// unexpected value never panics or vanishes. This is what must go into a
// bubbles/table cell (table.Row{...} values) — never a colored string: that
// component's column-width truncation (runewidth.Truncate) is not
// ANSI-escape-aware, so a styled string placed directly in a cell gets its
// width miscounted (escape bytes counted as visible columns) and
// truncates/misaligns unpredictably.
func RenderStatus(status string) string {
	symbol, ok := statusSymbols[status]
	if !ok {
		return status
	}
	return symbol + " " + status
}

// colorizeStatuses re-colors every occurrence of a RenderStatus() phrase
// (e.g. "✓ succeeded") found in an already-rendered string — call this on
// the *output* of table.View(), never on a value going *into* a table row.
// By the time table.View() has run, bubbles/table has already measured,
// padded, and truncated everything as plain text; wrapping an
// already-positioned substring in color afterward is safe because ANSI SGR
// codes are zero-width to the terminal, so this cannot un-align columns the
// way pre-styling a cell value does.
func colorizeStatuses(rendered string) string {
	for status, color := range statusColors {
		plain := RenderStatus(status)
		if !strings.Contains(rendered, plain) {
			continue
		}
		colored := lipgloss.NewStyle().Foreground(color).Render(plain)
		rendered = strings.ReplaceAll(rendered, plain, colored)
	}
	return rendered
}

// colorStatus renders a single status as "<symbol> <label>", colored. Unlike
// RenderStatus, this embeds ANSI styling directly — safe to use in
// hand-rolled renderers (graph.go) that never re-measure or truncate their
// own output, but NOT safe to place into a bubbles/table cell (see
// RenderStatus's doc comment for why).
func colorStatus(status string) string {
	plain := RenderStatus(status)
	color, ok := statusColors[status]
	if !ok {
		return plain
	}
	return lipgloss.NewStyle().Foreground(color).Render(plain)
}

var (
	styleTitle = lipgloss.NewStyle().Bold(true).Foreground(colorAccent)
	styleMuted = lipgloss.NewStyle().Foreground(colorMuted)
	styleHelp  = lipgloss.NewStyle().Foreground(colorMuted).Italic(true)
	styleError = lipgloss.NewStyle().Foreground(colorFailed).Bold(true)
)
