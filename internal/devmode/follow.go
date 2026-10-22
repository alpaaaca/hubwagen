package devmode

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// WaitRun polls until the run reaches a terminal status.
func (e *Engine) WaitRun(ctx context.Context, runID string) (state.Run, error) {
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		r, err := e.Store.GetRun(ctx, runID)
		if err != nil {
			return r, err
		}
		if state.RunTerminal(r.Status) {
			return r, nil
		}
		select {
		case <-ctx.Done():
			return r, ctx.Err()
		case err := <-e.done:
			e.done <- err // keep it for Close
			return r, fmt.Errorf("engine stopped: %w", err)
		case <-t.C:
		}
	}
}

// Follow calls fn with a one-line description of every notable event after
// afterSeq (runs created, started and finished; tasks retried and finished),
// polling the event log until ctx ends. Headless `hubwagen dev` prints these.
func (e *Engine) Follow(ctx context.Context, afterSeq int64, fn func(string)) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		evs, err := e.Store.EventsSince(ctx, afterSeq, "", 500)
		if err == nil {
			for _, ev := range evs {
				afterSeq = ev.Seq
				if line := Describe(ev); line != "" {
					fn(line)
				}
			}
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// LastSeq is the newest event's sequence number.
func (e *Engine) LastSeq(ctx context.Context) int64 {
	a, err := e.Store.LoadActive(ctx)
	if err != nil {
		return 0
	}
	return a.LastSeq
}

// Describe renders an event for humans, or "" for events not worth a line.
func Describe(ev state.Event) string {
	var p map[string]any
	_ = json.Unmarshal(ev.Payload, &p)
	str := func(k string) string { s, _ := p[k].(string); return s }
	run := ev.RunID
	if len(run) > 10 {
		run = run[len(run)-10:]
	}
	switch ev.Type {
	case state.EventRunCreated:
		return fmt.Sprintf("run …%s created for %s/%s (%s)", run, ev.Tenant, str("pipeline"), str("trigger"))
	case state.EventRunFinished:
		return fmt.Sprintf("run …%s %s", run, str("status"))
	case state.EventTaskFinished:
		return fmt.Sprintf("run …%s: task %s %s", run, ev.TaskID, str("status"))
	case state.EventTaskRetryScheduled:
		return fmt.Sprintf("run …%s: task %s will retry (attempt %v)", run, ev.TaskID, p["next_attempt"])
	case state.EventAttemptFinished:
		if str("status") == state.AttemptFailed {
			return fmt.Sprintf("run …%s: task %s attempt failed: %s %s", run, ev.TaskID, str("reason"), str("message"))
		}
	case state.EventPipelineVersionPublished:
		h := str("hash")
		if len(h) > 12 {
			h = h[:12]
		}
		return fmt.Sprintf("published %s/%s version %s", ev.Tenant, str("pipeline"), h)
	}
	return ""
}
