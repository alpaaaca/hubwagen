// Package haulcli is what `hubwagen start` and `hubwagen replay` do, apart
// from flag parsing: turn --param flags into parameter sets, start hauls
// over HaulService, wait for them, and report per lift what ran and why.
package haulcli

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

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
)

// MaxHauls caps how many hauls one replay may start.
const MaxHauls = 10000

// ExpandParams turns name=value flags into one parameter set per haul. A
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
		if len(sets)*len(values) > MaxHauls {
			return nil, fmt.Errorf("more than %d hauls", MaxHauls)
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
			if len(out) == MaxHauls {
				return nil, fmt.Errorf("range %q has more than %d values", s, MaxHauls)
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
	if b-a >= MaxHauls {
		return nil, fmt.Errorf("range %q has more than %d values", s, MaxHauls)
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

// Client talks to a server's HaulService.
type Client struct {
	hauls hubwagenv1connect.HaulServiceClient
}

func NewClient(server string) *Client {
	return &Client{hauls: hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, strings.TrimSuffix(server, "/"))}
}

// HaulRequest is what to start.
type HaulRequest struct {
	Tenant, Name, Dock string
	Params             map[string]any
}

// Start starts one haul and returns its ID.
func (c *Client) Start(ctx context.Context, r HaulRequest) (string, error) {
	params, err := json.Marshal(r.Params)
	if err != nil {
		return "", err
	}
	res, err := c.hauls.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{
		Tenant: r.Tenant, Name: r.Name, Dock: r.Dock, ParamsJson: string(params), Trigger: "the CLI"}))
	if err != nil {
		return "", err
	}
	return res.Msg.HaulId, nil
}

// Signal sends a signal to a haul; payload is JSON ("" for null). It
// returns the signal's ID.
func (c *Client) Signal(ctx context.Context, haulID, name, payload string) (string, error) {
	res, err := c.hauls.SignalHaul(ctx, connect.NewRequest(&hubwagenv1.SignalHaulRequest{
		HaulId: haulID, Name: name, PayloadJson: payload}))
	if err != nil {
		return "", err
	}
	return res.Msg.SignalId, nil
}

// Schedules writes the server's haul schedules as a table, next run first.
func (c *Client) Schedules(ctx context.Context, w io.Writer) error {
	res, err := c.hauls.ListSchedules(ctx, connect.NewRequest(&hubwagenv1.ListSchedulesRequest{}))
	if err != nil {
		return err
	}
	scheds, subs := res.Msg.Schedules, res.Msg.Subscriptions
	if len(scheds) == 0 && len(subs) == 0 {
		_, err := fmt.Fprintln(w, "no schedules or subscriptions (a haul gets one with @hw.haul(schedule=...) or subscribe=...)")
		return err
	}
	if len(subs) > 0 {
		defer func() {
			tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
			fmt.Fprintln(tw, "\nHAUL\tON DATASETS\tDEBOUNCE\tMAX WAIT\tMAX BATCH\tWAITING")
			for _, s := range subs {
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%d\n", s.Haul, strings.Join(s.Datasets, ", "),
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
	fmt.Fprintln(tw, "HAUL\tCRON\tTIMEZONE\tINTERVAL\tCATCH-UP\tNEXT RUN")
	for _, s := range scheds {
		tz := s.Timezone
		if tz == "" {
			tz = "UTC"
		}
		next := "-"
		if s.NextRun != nil {
			if loc, err := time.LoadLocation(tz); err == nil {
				next = s.NextRun.AsTime().In(loc).Format("2006-01-02 15:04 MST")
			}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\n", s.Haul, s.Cron, tz, s.Interval, s.CatchUp, next)
	}
	return tw.Flush()
}

// Wait polls until no haul is running any more (stuck counts as settled: it
// needs a fix and a new build) and returns them in the order given. A haul
// that continued as new is followed to the end of its chain: what is
// returned is the last haul.
func (c *Client) Wait(ctx context.Context, ids []string, every time.Duration) ([]*hubwagenv1.Haul, error) {
	ids = slices.Clone(ids)
	got := make([]*hubwagenv1.Haul, len(ids))
	for {
		pending := 0
		for i, id := range ids {
			if got[i] != nil && got[i].Status != "running" {
				continue
			}
			res, err := c.hauls.GetHaul(ctx, connect.NewRequest(&hubwagenv1.GetHaulRequest{HaulId: id}))
			if err != nil {
				return got, err
			}
			got[i] = res.Msg.Haul
			if got[i].Status == "continued" && got[i].ContinuedAs != "" {
				ids[i], got[i].Status = got[i].ContinuedAs, "running" // follow the chain
			}
			if got[i].Status == "running" {
				pending++
			}
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

// Report summarises hauls: per lift (in the order the hauls called them),
// how many were reused and how many ran, grouped by why; then how the hauls
// ended.
func Report(hauls []*hubwagenv1.Haul) string {
	type counts struct {
		reused int
		ran    map[string]int // reason → count
		other  map[string]int // status (reason) → count, for lifts that didn't complete
	}
	var order []string
	byLift := map[string]*counts{}
	statuses := map[string]int{}
	var problems []string
	for _, h := range hauls {
		if h == nil {
			continue
		}
		statuses[h.Status]++
		if h.Status != "completed" && h.Status != "running" {
			problems = append(problems, fmt.Sprintf("%s %s: %s", h.Id, h.Status, strings.TrimSpace(h.Reason+" "+h.Message)))
		}
		for _, l := range h.Lifts {
			c := byLift[l.Name]
			if c == nil {
				c = &counts{ran: map[string]int{}, other: map[string]int{}}
				byLift[l.Name] = c
				order = append(order, l.Name)
			}
			switch {
			case l.Reused:
				c.reused++
			case l.Status == "completed":
				c.ran[l.RunReason]++
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
	fmt.Fprintf(&b, "%d hauls\n", len(hauls))
	tw := tabwriter.NewWriter(&b, 0, 0, 3, ' ', 0)
	for _, name := range order {
		c := byLift[name]
		var parts []string
		if c.reused > 0 {
			parts = append(parts, fmt.Sprintf("%d reused", c.reused))
		}
		for _, reason := range sortedKeys(c.ran) {
			part := fmt.Sprintf("%d run", c.ran[reason])
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

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
