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

	"github.com/alper/hubwagen/internal/runlog"
	"github.com/alper/hubwagen/internal/state"
)

// Console is what `hubwagen dev` prints: a line when a run starts and when
// it ends, and in between what its lifts printed and logged, each line
// marked with the run and the lift. With events, it also prints the
// engine's own events (Describe), for debugging hubwagen itself.
func (e *Engine) Console(ctx context.Context, afterSeq int64, events bool, print func(string)) {
	c := &console{e: e, print: print, runs: map[string]run{}, announced: map[string]bool{}}
	e.Logs.OnAppend(c.logs)
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
	runs  map[string]run // by haul ID
	// Runs whose start was printed. Lift output arrives at once, events a
	// poll later: a run's first output line prints its start first.
	announced map[string]bool
}

// announce reports whether haulID's start is still to be printed, and
// takes that on.
func (c *console) announce(haulID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.announced[haulID] {
		return false
	}
	c.announced[haulID] = true
	return true
}

func (c *console) run(ctx context.Context, haulID string) run {
	c.mu.Lock()
	r, ok := c.runs[haulID]
	c.mu.Unlock()
	if ok {
		return r
	}
	h, err := c.e.Store.GetHaul(ctx, haulID)
	if err != nil {
		return run{label: short(haulID), inputs: short(haulID)}
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
	c.runs[haulID] = r
	c.mu.Unlock()
	return r
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
	case state.EventHaulStarted, state.EventHaulFinished, state.EventHaulStuck:
	default:
		return ""
	}
	r := c.run(ctx, ev.HaulID)
	var p struct{ Status, Reason, Message string }
	_ = json.Unmarshal(ev.Payload, &p)
	switch ev.Type {
	case state.EventHaulStarted:
		if !c.announce(ev.HaulID) {
			return ""
		}
		return fmt.Sprintf("▶ %s started", r.label)
	case state.EventHaulStuck:
		return fmt.Sprintf("■ %s stuck: %s", r.label, p.Message)
	}
	took := ""
	if !r.started.IsZero() {
		took = " in " + roundDuration(ev.Time.Sub(r.started))
	}
	switch p.Status {
	case state.HaulCompleted:
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

// logs prints lift output as it arrives: "  day=2026-10-07 › extract │ text".
func (c *console) logs(es []runlog.Entry) {
	ctx := context.Background()
	names := map[string]map[int]string{}
	for _, e := range es {
		if names[e.HaulID] == nil {
			names[e.HaulID] = map[int]string{}
			if lifts, err := c.e.Store.ListLifts(ctx, e.HaulID); err == nil {
				for _, l := range lifts {
					names[e.HaulID][l.Seq] = l.Name
				}
			}
		}
		r := c.run(ctx, e.HaulID)
		if c.announce(e.HaulID) {
			c.print(fmt.Sprintf("▶ %s started", r.label))
		}
		prefix := fmt.Sprintf("  %s › %s │ ", r.inputs, names[e.HaulID][e.Seq])
		text := e.Text
		if e.Stream == "log" && e.Level != "" && e.Level != "INFO" {
			text = e.Level + " " + text
		}
		for _, line := range strings.Split(strings.TrimRight(text, "\n"), "\n") {
			c.print(prefix + line)
		}
	}
}
