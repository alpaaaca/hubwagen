package tui

// discardWriter is an io.Writer that discards everything written to it. Used
// as the "console" output sink when triggering a run from the dashboard —
// step output is still durably captured to the store via the engine's own
// log.StoreWriter regardless of what this sink does (see internal/engine's
// per-step MultiWriter wiring); the dashboard just has no console to print
// to, so the human-readable stream is simply dropped here.
type discardWriter struct{}

func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }
