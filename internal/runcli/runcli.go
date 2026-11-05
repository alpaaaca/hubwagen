// Package runcli is what `moksha start` and `moksha replay` do, apart
// from flag parsing: turn --param flags into parameter sets, start runs
// over RunService, wait for them, and report per step what ran and why.
package runcli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/history"
)

// MaxRuns caps how many runs one replay may start.
const MaxRuns = 10000

// ExpandParams turns name=value flags into one parameter set per run. A
// value is JSON if it parses as JSON (3, true, {"a":1}), else a string. A
// value a..b over dates (2026-03-01..2026-03-31, daily) or integers (1..5)
// is a range, inclusive; several ranges multiply.
func ExpandParams(flags []string) ([]map[string]any, error) {
	sets := []map[string]any{{}}
	for _, f := range flags {
		name, value, ok := strings.Cut(f, "=")
		if !ok || name == "" {
			return nil, fmt.Errorf("--param %q: want name=value", f)
		}
		values, err := expandValue(value)
		if err != nil {
			return nil, fmt.Errorf("--param %s: %w", name, err)
		}
		if len(sets)*len(values) > MaxRuns {
			return nil, fmt.Errorf("more than %d runs", MaxRuns)
		}
		var next []map[string]any
		for _, set := range sets {
			for _, v := range values {
				n := make(map[string]any, len(set)+1)
				for k, sv := range set {
					n[k] = sv
				}
				n[name] = v
				next = append(next, n)
			}
		}
		sets = next
	}
	return sets, nil
}

func expandValue(s string) ([]any, error) {
	from, to, isRange := strings.Cut(s, "..")
	if !isRange {
		return []any{parseValue(s)}, nil
	}
	if a, err := time.Parse(time.DateOnly, from); err == nil {
		b, err := time.Parse(time.DateOnly, to)
		if err != nil {
			return nil, fmt.Errorf("range %q: %q is not a date", s, to)
		}
		if b.Before(a) {
			return nil, fmt.Errorf("range %q ends before it starts", s)
		}
		var out []any
		for d := a; !d.After(b); d = d.AddDate(0, 0, 1) {
			if len(out) == MaxRuns {
				return nil, fmt.Errorf("range %q has more than %d values", s, MaxRuns)
			}
			out = append(out, d.Format(time.DateOnly))
		}
		return out, nil
	}
	a, errA := strconv.ParseInt(from, 10, 64)
	b, errB := strconv.ParseInt(to, 10, 64)
	if errA != nil || errB != nil {
		return nil, fmt.Errorf("range %q: want dates (YYYY-MM-DD..YYYY-MM-DD) or integers", s)
	}
	if b < a {
		return nil, fmt.Errorf("range %q ends before it starts", s)
	}
	if b-a >= MaxRuns {
		return nil, fmt.Errorf("range %q has more than %d values", s, MaxRuns)
	}
	var out []any
	for i := a; i <= b; i++ {
		out = append(out, json.Number(strconv.FormatInt(i, 10)))
	}
	return out, nil
}

func parseValue(s string) any {
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil || dec.More() {
		return s
	}
	return v
}

// Client talks to a server's RunService.
type Client struct {
	runs mokshav1connect.RunServiceClient
	// Hint, if set, is told why runs wait (no worker, ...) once they have
	// waited for HintAfter (default DefaultHintAfter); each reason once
	// until no run waits any more.
	Hint      func(string)
	HintAfter time.Duration
}

// DefaultHintAfter is how long a run waits before Wait says why.
const DefaultHintAfter = 3 * time.Second

func NewClient(server string) *Client {
	return &Client{runs: mokshav1connect.NewRunServiceClient(http.DefaultClient, strings.TrimSuffix(server, "/"))}
}

// RunRequest is what to start.
type RunRequest struct {
	Tenant, Name, Queue string
	Params              map[string]any
}

// Start starts one run and returns its ID.
func (c *Client) Start(ctx context.Context, r RunRequest) (string, error) {
	params, err := json.Marshal(r.Params)
	if err != nil {
		return "", err
	}
	res, err := c.runs.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{
		Tenant: r.Tenant, Name: r.Name, Queue: r.Queue, ParamsJson: string(params), Trigger: "the CLI"}))
	if err != nil {
		return "", err
	}
	return res.Msg.RunId, nil
}

// Signal sends a signal to a run; payload is JSON ("" for null). It
// returns the signal's ID.
func (c *Client) Signal(ctx context.Context, runID, name, payload string) (string, error) {
	res, err := c.runs.SignalRun(ctx, connect.NewRequest(&mokshav1.SignalRunRequest{
		RunId: runID, Name: name, PayloadJson: payload}))
	if err != nil {
		return "", err
	}
	return res.Msg.SignalId, nil
}

// Schedules writes the server's run schedules as a table, next run first.
func (c *Client) Schedules(ctx context.Context, w io.Writer) error {
	res, err := c.runs.ListSchedules(ctx, connect.NewRequest(&mokshav1.ListSchedulesRequest{}))
	if err != nil {
		return err
	}
	scheds, subs := res.Msg.Schedules, res.Msg.Subscriptions
	if len(scheds) == 0 && len(subs) == 0 {
		_, err := fmt.Fprintln(w, "no schedules or subscriptions (a pipeline gets one with @mk.pipeline(schedule=...) or subscribe=...)")
		return err
	}
	if len(subs) > 0 {
		defer func() {
			tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
			fmt.Fprintln(tw, "\nPIPELINE\tON NEW DATA IN\tDEBOUNCE\tMAX WAIT\tMAX BATCH\tWAITING")
			for _, s := range subs {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\n", s.Pipeline, strings.Join(s.Datasets, ", "),
					s.Debounce.AsDuration(), s.MaxWait.AsDuration(), s.MaxBatch, s.Waiting)
			}
			tw.Flush()
		}()
	}
	if len(scheds) == 0 {
		return nil
	}
	sort.SliceStable(scheds, func(i, j int) bool { return scheds[i].NextRun.AsTime().Before(scheds[j].NextRun.AsTime()) })
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "PIPELINE\tCRON\tCRON TIMEZONE\tINTERVAL\tCATCH-UP\tNEXT RUN")
	for _, s := range scheds {
		tz := s.Timezone
		if tz == "" {
			tz = "UTC"
		}
		next := "-"
		if s.NextRun != nil {
			next = s.NextRun.AsTime().UTC().Format("2006-01-02 15:04 UTC") // shown in UTC, like every time
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", s.Pipeline, s.Cron, tz, s.Interval, s.CatchUp, next)
	}
	return tw.Flush()
}

// Wait polls until no run is running any more (stuck counts as settled: it
// needs a fix and a new build) and returns them in the order given. A run
// that continued as new is followed to the end of its chain: what is
// returned is the last run.
func (c *Client) Wait(ctx context.Context, ids []string, every time.Duration) ([]*mokshav1.Run, error) {
	ids = slices.Clone(ids)
	got := make([]*mokshav1.Run, len(ids))
	after := c.HintAfter
	if after <= 0 {
		after = DefaultHintAfter
	}
	waitingSince := map[string]time.Time{} // by run ID, on our clock: the server's may differ
	hinted := map[string]bool{}
	for {
		pending, waiting := 0, 0
		for i, id := range ids {
			if got[i] != nil && got[i].Status != "running" {
				continue
			}
			res, err := c.runs.GetRun(ctx, connect.NewRequest(&mokshav1.GetRunRequest{RunId: id}))
			if err != nil {
				return got, err
			}
			got[i] = res.Msg.Run
			if got[i].Status == "continued" && got[i].ContinuedAs != "" {
				ids[i], got[i].Status = got[i].ContinuedAs, "running" // follow the chain
			}
			if got[i].Status == "running" {
				pending++
			}
			w := got[i].Waiting
			if w == nil || got[i].Status != "running" {
				delete(waitingSince, id)
				continue
			}
			waiting++
			if _, ok := waitingSince[id]; !ok {
				waitingSince[id] = time.Now()
			}
			if c.Hint != nil && time.Since(waitingSince[id]) >= after && !hinted[w.Message] {
				hinted[w.Message] = true
				c.Hint("waiting: " + w.Message)
			}
		}
		if waiting == 0 {
			clear(hinted)
		}
		if pending == 0 {
			return got, nil
		}
		select {
		case <-ctx.Done():
			return got, ctx.Err()
		case <-time.After(every):
		}
	}
}

// Report summarises runs (runs): per step (in the order the runs called
// them), how many were reused and how many ran, grouped by why, in plain
// words; then how the runs ended.
func Report(runs []*mokshav1.Run) string {
	type counts struct {
		reused int
		ran    map[string]int // reason → count
		other  map[string]int // status (reason) → count, for steps that didn't complete
	}
	var order []string
	byStep := map[string]*counts{}
	statuses := map[string]int{}
	var problems []string
	for _, h := range runs {
		if h == nil {
			continue
		}
		statuses[h.Status]++
		if h.Status != "completed" && h.Status != "running" {
			problems = append(problems, fmt.Sprintf("%s %s: %s", h.Id, h.Status, strings.TrimSpace(h.Reason+" "+h.Message)))
		}
		for _, l := range h.Steps {
			c := byStep[l.Name]
			if c == nil {
				c = &counts{ran: map[string]int{}, other: map[string]int{}}
				byStep[l.Name] = c
				order = append(order, l.Name)
			}
			switch {
			case l.Reused:
				c.reused++
			case l.Status == "completed":
				c.ran[history.WhyItRan(l.RunReason)]++
			default:
				key := l.Status
				if l.Reason != "" {
					key += " (" + l.Reason + ")"
				}
				c.other[key]++
			}
		}
	}

	var b bytes.Buffer
	fmt.Fprintf(&b, "%d run%s\n", len(runs), plural(len(runs)))
	tw := tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
	for _, name := range order {
		c := byStep[name]
		var parts []string
		if c.reused > 0 {
			parts = append(parts, fmt.Sprintf("%d reused", c.reused))
		}
		for _, reason := range sortedKeys(c.ran) {
			part := fmt.Sprintf("%d ran", c.ran[reason])
			if reason != "" {
				part += " (" + reason + ")"
			}
			parts = append(parts, part)
		}
		for _, key := range sortedKeys(c.other) {
			parts = append(parts, fmt.Sprintf("%d %s", c.other[key], key))
		}
		fmt.Fprintf(tw, "  %s\t%s\n", name, strings.Join(parts, ", "))
	}
	tw.Flush()
	var ends []string
	for _, st := range sortedKeys(statuses) {
		ends = append(ends, fmt.Sprintf("%d %s", statuses[st], st))
	}
	fmt.Fprintln(&b, strings.Join(ends, ", "))
	for _, p := range problems {
		fmt.Fprintln(&b, "  "+p)
	}
	return b.String()
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
