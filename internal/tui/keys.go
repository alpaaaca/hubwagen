package tui

import "github.com/charmbracelet/bubbles/key"

// KeyMap is the single source of truth for every keybinding hubwagen tui
// responds to, across all screens. The '?' help overlay renders directly
// from this — never a separately maintained copy of the same information.
type KeyMap struct {
	// Global — every screen.
	Up      key.Binding
	Down    key.Binding
	Enter   key.Binding
	Back    key.Binding
	Refresh key.Binding
	Help    key.Binding
	Quit    key.Binding

	// pipeline list screen only.
	Filter  key.Binding
	Trigger key.Binding

	// Task view screen only (log viewport).
	Top    key.Binding
	Bottom key.Binding
	Graph  key.Binding
}

// Keys is the app-wide keymap instance every screen shares.
var Keys = KeyMap{
	Up:      key.NewBinding(key.WithKeys("up", "k"), key.WithHelp("↑/k", "move up")),
	Down:    key.NewBinding(key.WithKeys("down", "j"), key.WithHelp("↓/j", "move down")),
	Enter:   key.NewBinding(key.WithKeys("enter"), key.WithHelp("enter", "select")),
	Back:    key.NewBinding(key.WithKeys("b", "esc"), key.WithHelp("b/esc", "back")),
	Refresh: key.NewBinding(key.WithKeys("r"), key.WithHelp("r", "refresh now")),
	Help:    key.NewBinding(key.WithKeys("?"), key.WithHelp("?", "help")),
	Quit:    key.NewBinding(key.WithKeys("q", "ctrl+c"), key.WithHelp("q", "quit")),

	Filter:  key.NewBinding(key.WithKeys("/"), key.WithHelp("/", "filter")),
	Trigger: key.NewBinding(key.WithKeys("t"), key.WithHelp("t", "trigger now")),

	Top:    key.NewBinding(key.WithKeys("g"), key.WithHelp("g", "log top")),
	Bottom: key.NewBinding(key.WithKeys("G"), key.WithHelp("G", "log bottom")),
	Graph:  key.NewBinding(key.WithKeys("v"), key.WithHelp("v", "toggle graph view")),
}

// GlobalHelp returns the bindings that apply on every screen.
func (k KeyMap) GlobalHelp() []key.Binding {
	return []key.Binding{k.Up, k.Down, k.Enter, k.Back, k.Refresh, k.Help, k.Quit}
}

// PipelineListExtra returns the pipeline list screen's additional bindings.
func (k KeyMap) PipelineListExtra() []key.Binding {
	return []key.Binding{k.Filter, k.Trigger}
}

// TaskViewExtra returns the task view screen's additional bindings.
func (k KeyMap) TaskViewExtra() []key.Binding {
	return []key.Binding{k.Top, k.Bottom, k.Graph}
}
