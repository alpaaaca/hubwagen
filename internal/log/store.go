package log

import (
	"context"
	"strings"

	"github.com/alper/hubwagen/internal/store"
)

// StoreWriter is an io.Writer that durably appends each complete line it
// receives to a Store's logs table, tagged with the owning run and step.
// It is meant to be registered as a sink on a MultiWriter, which guarantees
// Write is only ever called with a single, newline-terminated line — so no
// buffering is needed here.
type StoreWriter struct {
	Store  store.Store
	RunID  string
	StepID string
}

func (w *StoreWriter) Write(p []byte) (int, error) {
	line := strings.TrimRight(string(p), "\r\n")
	if line != "" {
		if err := w.Store.AppendLog(context.Background(), w.RunID, w.StepID, line); err != nil {
			return 0, err
		}
	}
	return len(p), nil
}
