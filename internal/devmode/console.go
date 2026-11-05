package devmode

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/alper/moksha/internal/api"
	"github.com/alper/moksha/internal/runlog"
	"github.com/alper/moksha/internal/state"
)

// Console is what `moksha dev` prints: a line when a run starts and when
// it ends, and in between what its steps printed and logged, each line
// marked with the run and the step. With events, it also prints the
// engine's own events (Describe), for debugging moksha itself.
func (e *Engine) Console(ctx context.Context, afterSeq int64, events bool, print func(string)) {
	c := &console{e: e, print: print, runs: map[string]run{}, announced: map[string]bool{}}
	e.Logs.OnAppend(c.logs)
	go c.watchWaiting(ctx, hintAfter)
	e.Follow(ctx, afterSeq, nil, func(ev state.Event) {
		if events {
			if line := Describe(ev); line != "" {
				print("· " + line)
			}
			return
		}
		if line := c.describeRun(ctx, ev); line != "" {
			print(line)
		}
	})
}

type run struct {
	label   string // "Daily revenue day=2026-10-07"
	inputs  string // "day=2026-10-07", or a short ID without params
	started time.Time
}

type console struct {
	e     *Engine
	print func(string)
	mu    sync.Mutex
	runs  map[string]run // by run ID
	// Runs whose start was printed. Step output arrives at once, events a
	// poll later: a run's first output line prints its start first.
	announced map[string]bool
}

// announce reports whether runID's start is still to be printed, and
// takes that on.
func (c *console) announce(runID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.announced[runID] {
		return false
	}
	c.announced[runID] = true
	return true
}

func (c *console) run(ctx context.Context, runID string) run {
	c.mu.Lock()
	r, ok := c.runs[runID]
	c.mu.Unlock()
	if ok {
		return r
	}
	h, err := c.e.Store.GetRun(ctx, runID)
	if err != nil {
		return run{label: short(runID), inputs: short(runID)}
	}
	title := h.Name
	if ps, err := c.e.Store.ListPipelines(ctx); err == nil {
		for _, p := range ps {
			if p.Name == h.Name && p.Title != "" {
				title = p.Title
			}
		}
	}
	r = run{inputs: inputs(h.Params, h.ID), started: h.CreatedAt}
	r.label = title + " " + r.inputs
	c.mu.Lock()
	c.runs[runID] = r
	c.mu.Unlock()
	return r
}

// hintAfter is how long a run waits before the console says why.
var hintAfter = 3 * time.Second

// watchWaiting prints why runs wait (no worker connected, ...) once they
// have waited for hintAfter: each reason once, until no run waits.
func (c *console) watchWaiting(ctx context.Context, hintAfter time.Duration) {
	t := time.NewTicker(hintAfter / 3)
	defer t.Stop()
	hinted := map[string]bool{}
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		runs, err := c.e.Store.ListRuns(ctx, state.RunFilter{Statuses: []string{state.RunRunning}, Limit: 1000})
		if err != nil || len(runs) == 0 {
			clear(hinted)
			continue
		}
		ids := make([]string, len(runs))
		for i, h := range runs {
			ids[i] = h.ID
		}
		ws, err := c.e.Core.Waiting(ctx, ids)
		if err != nil {
			continue
		}
		if len(ws) == 0 {
			clear(hinted)
		}
		for _, id := range slices.Sorted(maps.Keys(ws)) {
			w := ws[id]
			text := api.WaitingText(w)
			if time.Since(w.Since) < hintAfter || hinted[text] {
				continue
			}
			hinted[text] = true
			c.print(fmt.Sprintf("… %s is waiting: %s", c.run(ctx, id).label, text))
		}
	}
}

// inputs renders params as k=v pairs, for telling runs apart.
func inputs(params map[string]any, id string) string {
	if len(params) == 0 {
		return short(id)
	}
	var parts []string
	for _, k := range slices.Sorted(maps.Keys(params)) {
		v, ok := params[k].(string)
		if !ok {
			b, _ := json.Marshal(params[k])
			v = string(b)
		}
		if len(v) > 40 {
			v = v[:37] + "…"
		}
		parts = append(parts, k+"="+v)
	}
	return strings.Join(parts, " ")
}

func (c *console) describeRun(ctx context.Context, ev state.Event) string {
	switch ev.Type {
	case state.EventRunStarted, state.EventRunFinished, state.EventRunStuck, state.EventStepFinished:
	default:
		return ""
	}
	r := c.run(ctx, ev.RunID)
	var p struct {
		Status, Reason, Message string
		Seq                     int
		Reused                  bool
	}
	_ = json.Unmarshal(ev.Payload, &p)
	switch ev.Type {
	case state.EventStepFinished:
		// A reused step prints nothing: say why it is quiet.
		if !p.Reused {
			return ""
		}
		name := ""
		if l, err := c.e.Store.GetStep(ctx, ev.RunID, p.Seq); err == nil {
			name = l.Name
		}
		return fmt.Sprintf("  %s › %s ↺ reused: same inputs and code, nothing ran", r.inputs, name)
	case state.EventRunStarted:
		if !c.announce(ev.RunID) {
			return ""
		}
		return fmt.Sprintf("▶ %s started", r.label)
	case state.EventRunStuck:
		return fmt.Sprintf("■ %s stuck: %s", r.label, p.Message)
	}
	took := ""
	if !r.started.IsZero() {
		took = " in " + roundDuration(ev.Time.Sub(r.started))
	}
	switch p.Status {
	case state.RunCompleted:
		return fmt.Sprintf("✓ %s completed%s", r.label, took)
	case "continued":
		return fmt.Sprintf("↻ %s continued as a new run", r.label)
	}
	why := strings.TrimSpace(strings.TrimPrefix(p.Reason+": "+p.Message, ": "))
	if why != "" {
		why = ": " + why
	}
	return fmt.Sprintf("✗ %s %s%s%s", r.label, p.Status, took, why)
}

func roundDuration(d time.Duration) string {
	switch {
	case d < time.Second:
		return d.Round(time.Millisecond).String()
	case d < time.Minute:
		return d.Round(100 * time.Millisecond).String()
	}
	return d.Round(time.Second).String()
}

// logs prints step output as it arrives: "  day=2026-10-07 › extract │ text".
func (c *console) logs(es []runlog.Entry) {
	ctx := context.Background()
	names := map[string]map[int]string{}
	for _, e := range es {
		if names[e.RunID] == nil {
			names[e.RunID] = map[int]string{}
			if steps, err := c.e.Store.ListSteps(ctx, e.RunID); err == nil {
				for _, l := range steps {
					names[e.RunID][l.Seq] = l.Name
				}
			}
		}
		r := c.run(ctx, e.RunID)
		if c.announce(e.RunID) {
			c.print(fmt.Sprintf("▶ %s started", r.label))
		}
		prefix := fmt.Sprintf("  %s › %s │ ", r.inputs, names[e.RunID][e.Seq])
		text := e.Text
		if e.Stream == "log" && e.Level != "" && e.Level != "INFO" {
			text = e.Level + " " + text
		}
		for _, line := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
			c.print(prefix + line)
		}
	}
}
