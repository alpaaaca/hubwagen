package tui

import (
	"sort"
	"strings"
)

// taskNode is the minimal per-task info renderGraph needs: identity,
// dependency edges, and current status.
type taskNode struct {
	ID        string
	DependsOn []string
	Status    string // "" if no run/status known yet
}

// graphEdge is a column-position pair used while drawing connector lines
// between two adjacent wave rows.
type graphEdge struct{ pCol, cCol int }

const (
	graphStatusColWidth = 12 // fits "⊘ cancelled" (11 visible chars) with room to spare
	graphGap            = 3  // spaces between adjacent boxes in the same wave row
)

// renderGraph lays out a pipeline's tasks as rows of connected boxes, one
// row per layer (see layers). Layers are only a drawing aid: the engine
// starts each task as soon as its own dependencies finish. Each box is
// colored/symbolized by its current status via colorStatus.
//
// This is a hand-rolled renderer, not a bubbles/table — so unlike
// RenderStatus/colorizeStatuses elsewhere in this package, color can be
// embedded directly while building each line: nothing here re-measures or
// truncates already-styled text, which is what forced the
// render-plain-then-recolor approach for table cells.
//
// Layout is exact and clean for the common shapes (linear chains, a single
// task fanning out to several, several converging on one). For a wave
// transition with multiple distinct parents *and* multiple distinct
// children at once (genuine many-to-many crossing edges), it degrades to
// plain vertical stubs rather than guessing at which line should cross which
// — see connectSimpleStar's doc comment.
// layers groups tasks for drawing: a task's layer is one more than the
// highest layer among its dependencies; tasks within a layer are sorted by
// ID. Tasks are assumed acyclic (the compiler guarantees it).
func layers(tasks []TaskShape) [][]string {
	deps := make(map[string][]string, len(tasks))
	for _, t := range tasks {
		deps[t.ID] = t.DependsOn
	}
	level := map[string]int{}
	var depth func(id string) int
	depth = func(id string) int {
		if l, ok := level[id]; ok {
			return l
		}
		level[id] = 0 // guards against cycles in malformed input
		l := 0
		for _, d := range deps[id] {
			if _, known := deps[d]; known {
				l = max(l, depth(d)+1)
			}
		}
		level[id] = l
		return l
	}
	var out [][]string
	for _, t := range tasks {
		l := depth(t.ID)
		for len(out) <= l {
			out = append(out, nil)
		}
		out[l] = append(out[l], t.ID)
	}
	for _, row := range out {
		sort.Strings(row)
	}
	return out
}

func renderGraph(waves [][]string, byID map[string]taskNode) string {
	if len(waves) == 0 {
		return styleMuted.Render("(no tasks)")
	}

	maxIDLen := 0
	for _, s := range byID {
		if l := len([]rune(s.ID)); l > maxIDLen {
			maxIDLen = l
		}
	}
	boxWidth := maxIDLen + 4 // "┌" + "─"*(maxIDLen+2) + "┐"
	cellWidth := boxWidth + 1 + graphStatusColWidth

	var lines []string
	var prevCenters []int
	var prevIDs []string

	for wi, wave := range waves {
		top, mid, bot, centers := renderBoxRow(wave, byID, maxIDLen, boxWidth, cellWidth)
		if wi > 0 {
			lines = append(lines, connectWaves(prevIDs, wave, byID, prevCenters, centers)...)
		}
		lines = append(lines, top, mid, bot)
		prevCenters, prevIDs = centers, wave
	}

	return strings.Join(lines, "\n")
}

func renderBoxRow(ids []string, byID map[string]taskNode, innerWidth, boxWidth, cellWidth int) (top, mid, bot string, centers []int) {
	sep := strings.Repeat(" ", graphGap)
	var topCells, midCells, botCells []string
	col := 0
	for _, id := range ids {
		status := byID[id].Status
		t := "┌" + strings.Repeat("─", innerWidth+2) + "┐"
		b := "└" + strings.Repeat("─", innerWidth+2) + "┘"
		m := "│ " + padRight(id, innerWidth) + " │ " + padColoredRight(colorStatus(status), statusVisibleWidth(status), graphStatusColWidth)
		topCells = append(topCells, padRight(t, cellWidth))
		botCells = append(botCells, padRight(b, cellWidth))
		midCells = append(midCells, m)
		centers = append(centers, col+boxWidth/2)
		col += cellWidth + graphGap
	}
	return strings.Join(topCells, sep), strings.Join(midCells, sep), strings.Join(botCells, sep), centers
}

// connectWaves draws the connector rows between two adjacent waves.
func connectWaves(parentIDs, childIDs []string, byID map[string]taskNode, parentCenters, childCenters []int) []string {
	parentCol := make(map[string]int, len(parentIDs))
	for i, id := range parentIDs {
		parentCol[id] = parentCenters[i]
	}
	childCol := make(map[string]int, len(childIDs))
	for i, id := range childIDs {
		childCol[id] = childCenters[i]
	}

	var edges []graphEdge
	for _, cid := range childIDs {
		for _, dep := range byID[cid].DependsOn {
			if pc, ok := parentCol[dep]; ok {
				edges = append(edges, graphEdge{pCol: pc, cCol: childCol[cid]})
			}
		}
	}
	if len(edges) == 0 {
		// No adjacent-wave edges for this transition — e.g. every child's
		// dependencies are more than one wave back. Nothing to draw; the
		// tasks' own wave grouping still shows execution order.
		return []string{""}
	}

	width := 0
	for _, e := range edges {
		if e.pCol > width {
			width = e.pCol
		}
		if e.cCol > width {
			width = e.cCol
		}
	}
	width += 4

	return connectSimpleStar(edges, width)
}

// connectSimpleStar draws a 3-row bus connector (stub down from parent, a
// horizontal bar with junctions, stub up into child) for the shapes this
// covers cleanly: a single parent fanning out to N children, N parents
// converging on a single child, or a plain 1:1 line (straight or jogged).
//
// If a transition has multiple *distinct* parents each feeding multiple
// *distinct* children in the same task — genuine many-to-many crossing
// edges — precisely routing which line goes to which box needs real lane
// assignment (the technique tools like `git log --graph` use). Rather than
// risk drawing a plausible-looking but *wrong* diagram, this degrades to
// independent vertical stubs (still correct — every connection point is
// still marked — just not joined with routed lines). None of hubwagen's
// example pipelines hit this case; everything there is chains or single
// fan-out/fan-in.
func connectSimpleStar(edges []graphEdge, width int) []string {
	distinctParents := map[int]bool{}
	distinctChildren := map[int]bool{}
	for _, e := range edges {
		distinctParents[e.pCol] = true
		distinctChildren[e.cCol] = true
	}

	stubDown := blankRow(width)
	stubUp := blankRow(width)

	simple := len(distinctParents) == 1 || len(distinctChildren) == 1 || len(edges) == 1
	if !simple {
		// Many-to-many: independent stubs only, no routed bar.
		for c := range distinctParents {
			stubDown[c] = '│'
		}
		for c := range distinctChildren {
			stubUp[c] = '│'
		}
		return []string{string(stubDown), string(stubUp)}
	}

	bar := blankRow(width)
	for c := range distinctParents {
		stubDown[c] = '│'
	}
	for c := range distinctChildren {
		stubUp[c] = '│'
	}

	minCol, maxCol := edges[0].pCol, edges[0].pCol
	joinCols := map[int]bool{}
	for _, e := range edges {
		for _, c := range [2]int{e.pCol, e.cCol} {
			joinCols[c] = true
			if c < minCol {
				minCol = c
			}
			if c > maxCol {
				maxCol = c
			}
		}
	}
	for c := minCol; c <= maxCol; c++ {
		bar[c] = '─'
	}
	// A column can simultaneously be a parent's position (needs an "up"
	// connection to the stub above) AND a different edge's child position
	// (needs a "down" connection to the stub below) — this happens often
	// here since each wave's box columns are numbered from 0 independently,
	// so a parent alone in its wave and a child alone in its wave can land
	// on the exact same column by coincidence. Picking the junction glyph
	// from which of the 4 directions (up/down/left/right) actually need to
	// connect at each column — rather than a fixed "leftmost/rightmost
	// corner" rule — handles that overlap correctly instead of drawing a
	// corner that silently drops one of the connections.
	for c := range joinCols {
		bar[c] = boxJunction(distinctParents[c], distinctChildren[c], c > minCol, c < maxCol)
	}

	return []string{string(stubDown), string(bar), string(stubUp)}
}

// boxJunction picks the box-drawing character that connects exactly the
// directions requested. up = a stub descends into this column from the
// parent row above; down = a stub continues from this column into the
// child row below; left/right = the horizontal bar continues in that
// direction. Any combination not covering all 4 falls back sensibly (e.g.
// up-only still renders as a plain vertical bar).
func boxJunction(up, down, left, right bool) rune {
	switch {
	case up && down && left && right:
		return '┼'
	case up && down && left:
		return '┤'
	case up && down && right:
		return '├'
	case up && down:
		return '│'
	case up && left && right:
		return '┴'
	case down && left && right:
		return '┬'
	case up && left:
		return '┘'
	case up && right:
		return '└'
	case down && left:
		return '┐'
	case down && right:
		return '┌'
	case left && right:
		return '─'
	case left:
		return '─'
	case right:
		return '─'
	default:
		return '│'
	}
}

func blankRow(width int) []rune {
	r := make([]rune, width)
	for i := range r {
		r[i] = ' '
	}
	return r
}

func padRight(s string, width int) string {
	l := len([]rune(s))
	if l >= width {
		return s
	}
	return s + strings.Repeat(" ", width-l)
}

// padColoredRight pads a possibly-ANSI-styled string s, whose true visible
// width is visibleWidth (NOT len([]rune(s)) — escape-sequence bytes inflate
// the rune count past the actual on-screen width), out to targetWidth
// visible columns. Appending plain spaces after a styled string is safe:
// ANSI SGR codes are zero-width, so this can't affect alignment the way
// pre-styling a bubbles/table cell does (see RenderStatus's doc comment).
func padColoredRight(s string, visibleWidth, targetWidth int) string {
	if visibleWidth >= targetWidth {
		return s
	}
	return s + strings.Repeat(" ", targetWidth-visibleWidth)
}

// statusVisibleWidth returns the rendered (plain-text) width of
// RenderStatus(status), used to keep every box's status column the same
// visible width regardless of which status it holds.
func statusVisibleWidth(status string) int {
	return len([]rune(RenderStatus(status)))
}
