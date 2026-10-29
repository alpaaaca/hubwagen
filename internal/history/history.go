// Package history tells what happened in a run, in plain words: a run's
// events become sentences ("extract_orders done in 0.8 s, wrote a new
// version of raw.orders for 2026-10-07"). The web UI shows them as the
// run's history.
//
// The words are the ones people read, not the engine's: a haul is a run of
// a pipeline, a lift is a step, a pallet is a version of a dataset (a
// step's output), a memoized lift is reused. Events that are only the
// engine's bookkeeping (haul tasks, an attempt that succeeded) are kept as
// Engine entries, for debugging hubwagen itself.
package history

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// Kinds of entries, for the UI to mark them.
const (
	KindStarted  = "started"  // the run started
	KindFinished = "finished" // the run ended: completed, failed, cancelled, continued
	KindStuck    = "stuck"
	KindStep     = "step"    // a step was queued, started or done
	KindReused   = "reused"  // a step was reused
	KindProblem  = "problem" // an attempt failed, a step failed, a worker error
	KindRetry    = "retry"
	KindWait     = "wait" // a sleep, a signal, a step waiting for another
	KindRun      = "run"  // a sub-run started or ended
	KindData     = "data" // a lookup in the catalog
	KindEngine   = "engine"
)

// Entry is one line of a run's history.
type Entry struct {
	Time time.Time
	Kind string
	// The step it is about: its sequence number and name; 0 and "" for the
	// run itself.
	Seq  int
	Step string
	// The sentence, without the step's name, which the UI shows beside it.
	Text string
	// More, when there is: an error message, the step's arguments.
	Detail string
	// A run it refers to (the one a step was reused from, a sub-run).
	Run string
	// The event it tells.
	Event state.Event
}

// Lookup finds what the events only refer to by ID. Either may be nil.
type Lookup struct {
	Pallet func(id string) (state.Pallet, bool)
	Haul   func(id string) (state.Haul, bool)
}

// Tell turns a run's events (in order) into its history, one entry per
// event.
func Tell(evs []state.Event, look Lookup) []Entry {
	t := teller{look: look, names: map[int]string{}, started: map[int]time.Time{}, reused: map[int]bool{}}
	for _, ev := range evs {
		if ev.Type == state.EventLiftReused || ev.Type == state.EventLiftWaiting {
			var p struct {
				Seq int `json:"seq"`
			}
			_ = json.Unmarshal(ev.Payload, &p)
			t.reused[p.Seq] = true
		}
	}
	out := make([]Entry, 0, len(evs))
	for _, ev := range evs {
		e := t.tell(ev)
		e.Time, e.Event = ev.Time, ev
		if e.Kind == "" {
			e.Kind = KindEngine
		}
		if e.Text == "" {
			e.Text = string(ev.Type)
		}
		if e.Seq != 0 && e.Step == "" {
			e.Step = t.names[e.Seq]
		}
		out = append(out, e)
	}
	return out
}

type teller struct {
	look    Lookup
	begin   time.Time
	name    string // the run's pipeline and inputs
	inputs  string
	names   map[int]string    // step names by seq
	started map[int]time.Time // when a step's current attempt started
	reused  map[int]bool      // steps reused, or waiting to reuse another's result
}

// payload is what events carry; each event fills some of it.
type payload struct {
	Name        string          `json:"name"`
	Trigger     string          `json:"trigger"`
	Params      map[string]any  `json:"params"`
	Seq         int             `json:"seq"`
	Number      int             `json:"number"`
	NextAttempt int             `json:"next_attempt"`
	Worker      string          `json:"worker"`
	Status      string          `json:"status"`
	Reason      string          `json:"reason"`
	Message     string          `json:"message"`
	Pallet      json.RawMessage `json:"pallet"`
	Reused      bool            `json:"reused"`
	Haul        string          `json:"haul"`
	OnSeq       int             `json:"on_seq"`
	RunReason   string          `json:"run_reason"`
	Memo        string          `json:"memo"`
	Args        json.RawMessage `json:"args"`
	RetryAt     time.Time       `json:"retry_at"`
	FireAt      time.Time       `json:"fire_at"`
	TimedOut    bool            `json:"timed_out"`
}

func (t *teller) tell(ev state.Event) Entry {
	var p payload
	d := json.NewDecoder(bytes.NewReader(ev.Payload))
	d.UseNumber() // inputs keep their digits
	_ = d.Decode(&p)
	why := strings.TrimSpace(p.Reason + ": " + p.Message)
	why = strings.Trim(why, ": ")
	switch ev.Type {
	case state.EventHaulStarted:
		t.begin, t.name, t.inputs = ev.Time, p.Name, Inputs(p.Params)
		text := "Started " + Trigger(p.Trigger)
		if in := Inputs(p.Params); in != "" {
			text += ", for " + in
		}
		e := Entry{Kind: KindStarted, Text: text}
		if id, ok := strings.CutPrefix(p.Trigger, "haul "); ok {
			e.Run = id
		} else if id, ok := strings.CutPrefix(p.Trigger, "continue "); ok {
			e.Run = id
		}
		return e
	case state.EventHaulFinished:
		took := ""
		if !t.begin.IsZero() {
			took = " in " + Took(ev.Time.Sub(t.begin))
		}
		switch p.Status {
		case state.HaulCompleted:
			return Entry{Kind: KindFinished, Text: "Finished" + took}
		case state.HaulFailed:
			return Entry{Kind: KindFinished, Text: "Failed" + took, Detail: why}
		case state.HaulCancelled:
			return Entry{Kind: KindFinished, Text: "Cancelled", Detail: why}
		case state.HaulContinued:
			return Entry{Kind: KindFinished, Text: "Continued as a new run" + took}
		}
		return Entry{Kind: KindFinished, Text: "Ended: " + p.Status, Detail: why}
	case state.EventHaulStuck:
		text := "Stuck: the pipeline's code no longer matches what this run already did; it waits until code that matches is deployed, or it is cancelled"
		if p.Reason != "nondeterminism" {
			text = "Stuck"
		}
		return Entry{Kind: KindStuck, Text: text, Detail: p.Message, Seq: p.Seq}
	case state.EventHaulTaskFailed:
		return Entry{Kind: KindProblem, Text: "The pipeline's code couldn't run on a worker; it runs again", Detail: why}

	case state.EventLiftScheduled:
		t.names[p.Seq] = p.Name
		if t.reused[p.Seq] {
			return Entry{Seq: p.Seq, Text: "queued"} // the reuse says what happened
		}
		text := "queued"
		if r := WhyItRan(p.RunReason); r != "" {
			text += "; it runs because " + r
		}
		return Entry{Kind: KindStep, Seq: p.Seq, Text: text, Detail: args(p.Args)}
	case state.EventLiftReused:
		e := Entry{Kind: KindReused, Seq: p.Seq,
			Text: "reused: same inputs and same code as before, so it didn't run again"}
		run := p.Haul
		if id := str(p.Pallet); id != "" && t.look.Pallet != nil {
			if pl, ok := t.look.Pallet(id); ok {
				run = pl.HaulID
				e.Text = "reused the existing " + version(pl) + ": same inputs and same code, so it didn't run again"
			}
		}
		if run != "" && run != ev.HaulID {
			e.Run = run
			e.Text += " (from " + t.runName(run) + ")"
		}
		return e
	case state.EventLiftWaiting:
		e := Entry{Kind: KindWait, Seq: p.Seq,
			Text: "waits for the same step with the same inputs, already running elsewhere, to reuse its result"}
		if p.Haul != "" && p.Haul != ev.HaulID {
			e.Run = p.Haul
			e.Text = "waits for the same step with the same inputs, already running in " + t.runName(p.Haul) + ", to reuse its result"
		}
		return e
	case state.EventLiftAttemptDispatched:
		t.started[p.Seq] = ev.Time
		text := "started"
		if p.Number > 1 {
			text = fmt.Sprintf("attempt %d started", p.Number)
		}
		if p.Worker != "" {
			text += " on " + p.Worker
		}
		return Entry{Kind: KindStep, Seq: p.Seq, Text: text}
	case state.EventLiftAttemptFinished:
		switch p.Status {
		case state.AttemptSucceeded:
			return Entry{Seq: p.Seq, Text: fmt.Sprintf("attempt %d succeeded", p.Number)}
		case state.AttemptLost:
			return Entry{Kind: KindProblem, Seq: p.Seq, Text: fmt.Sprintf("attempt %d lost: its worker stopped answering", p.Number), Detail: why}
		case state.AttemptCancelled:
			return Entry{Seq: p.Seq, Text: fmt.Sprintf("attempt %d cancelled", p.Number), Detail: why}
		}
		return Entry{Kind: KindProblem, Seq: p.Seq, Text: fmt.Sprintf("attempt %d failed", p.Number), Detail: why}
	case state.EventLiftRetryScheduled:
		text := fmt.Sprintf("tries again (attempt %d)", p.NextAttempt)
		if wait := p.RetryAt.Sub(ev.Time); wait > 0 {
			text = fmt.Sprintf("tries again in %s (attempt %d)", Took(wait), p.NextAttempt)
		}
		return Entry{Kind: KindRetry, Seq: p.Seq, Text: text}
	case state.EventLiftFinished:
		if p.Reused {
			return Entry{Seq: p.Seq, Text: "done (reused)"}
		}
		took := ""
		if at, ok := t.started[p.Seq]; ok {
			took = " in " + Took(ev.Time.Sub(at))
		}
		switch p.Status {
		case state.LiftCompleted:
			text := "done" + took
			if id := str(p.Pallet); id != "" && t.look.Pallet != nil {
				if pl, ok := t.look.Pallet(id); ok {
					text += ", wrote a new " + version(pl)
				}
			}
			return Entry{Kind: KindStep, Seq: p.Seq, Text: text}
		case state.LiftCancelled:
			return Entry{Kind: KindStep, Seq: p.Seq, Text: "cancelled", Detail: why}
		}
		return Entry{Kind: KindProblem, Seq: p.Seq, Text: "failed" + took, Detail: why}

	case state.EventTimerStarted:
		text := "Sleeps"
		if d := p.FireAt.Sub(ev.Time); d > 0 {
			text = "Sleeps for " + Took(d)
		}
		return Entry{Kind: KindWait, Text: text}
	case state.EventTimerFired:
		return Entry{Kind: KindWait, Text: "Woke up"}
	case state.EventTimerCancelled:
		return Entry{Text: "Sleep cancelled"}
	case state.EventSignalReceived:
		return Entry{Kind: KindWait, Text: fmt.Sprintf("Got the signal “%s”", p.Name)}
	case state.EventSignalDelivered:
		if p.TimedOut {
			return Entry{Kind: KindWait, Text: fmt.Sprintf("Stopped waiting for the signal “%s”: time ran out", p.Name)}
		}
		return Entry{Kind: KindWait, Text: fmt.Sprintf("Went on with the signal “%s”", p.Name)}
	case state.EventChildStarted:
		return Entry{Kind: KindRun, Text: "Started a run of " + p.Name, Run: p.Haul}
	case state.EventChildFinished:
		if p.Haul == "" {
			return Entry{Kind: KindProblem, Text: "Couldn't start a run of " + p.Name, Detail: p.Message}
		}
		return Entry{Kind: KindRun, Text: fmt.Sprintf("The run of %s it started %s", p.Name, p.Status), Run: p.Haul, Detail: why}
	case state.EventQueryAnswered:
		var a struct {
			Pallet *struct {
				Dataset   string `json:"dataset"`
				Partition string `json:"partition"`
			} `json:"pallet"`
		}
		_ = json.Unmarshal(ev.Payload, &a)
		if a.Pallet == nil {
			return Entry{Kind: KindData, Text: "Looked up the latest version of a dataset: there is none yet"}
		}
		return Entry{Kind: KindData, Text: "Looked up the latest " + version(state.Pallet{Dataset: a.Pallet.Dataset, Partition: a.Pallet.Partition})}
	case state.EventPalletInvalidated:
		return Entry{Kind: KindProblem, Text: "A step's output couldn't be saved", Detail: why}

	case state.EventHaulTaskQueued:
		return Entry{Text: "Waits for a worker to run the pipeline's code"}
	case state.EventHaulTaskDispatched:
		return Entry{Text: "The pipeline's code runs on " + orElse(p.Worker, "a worker")}
	case state.EventHaulTaskCompleted:
		return Entry{Text: "The pipeline's code ran up to where it waits"}
	}
	return Entry{}
}

// runName names another run by its pipeline and inputs.
func (t *teller) runName(id string) string {
	if t.look.Haul != nil {
		if h, ok := t.look.Haul(id); ok {
			if h.Name == t.name && Inputs(h.Params) == t.inputs {
				return "an earlier run with the same inputs"
			}
			if in := Inputs(h.Params); in != "" {
				return "the run of " + h.Name + " for " + in
			}
			return "a run of " + h.Name + " (" + h.CreatedAt.UTC().Format("2006-01-02 15:04") + " UTC)"
		}
	}
	return "an earlier run"
}

// version names a pallet as people do: a version of its dataset.
func version(p state.Pallet) string {
	s := "version of " + p.Dataset
	if p.Partition != "" {
		s += " for " + p.Partition
	}
	return s
}

// Trigger says what started a run, to follow "Started".
func Trigger(t string) string {
	switch {
	case t == "", t == "manual":
		return "by hand"
	case t == "schedule":
		return "by its schedule"
	case t == "subscription":
		return "by new data"
	case strings.HasPrefix(t, "haul "):
		return "by another run"
	case strings.HasPrefix(t, "continue "):
		return "as the continuation of an earlier run"
	}
	return "from " + t
}

// WhyItRan says why a step ran, from what memoization decided (its run
// reason), to follow "it runs because"; "" when there is nothing to say
// (the step isn't reused at all).
func WhyItRan(r string) string {
	switch {
	case r == "", r == "not memoized":
		return ""
	case strings.HasPrefix(r, "no previous run"):
		return "it hasn't run with these inputs before"
	case r == "unknown code version":
		return "its code version is unknown, so it can't be compared"
	case r == "arguments are not JSON":
		return "its arguments can't be compared"
	case r == "earlier pallet invalidated":
		return "its earlier output was marked invalid"
	case strings.HasPrefix(r, "expired"):
		return "its earlier output is too old " + strings.TrimPrefix(r, "expired ")
	case strings.HasPrefix(r, "the earlier run's output can't be read"):
		return r
	}
	r = strings.NewReplacer("code changed", "its code changed", "input changed", "an input changed",
		"params changed", "its arguments changed").Replace(r)
	return r
}

// Inputs formats a run's inputs as name=value pairs, sorted by name; an
// interval as its dates, and on its own (a scheduled run's) without a name.
func Inputs(params map[string]any) string {
	if iv, ok := params["interval"]; ok && len(params) == 1 {
		return value(iv)
	}
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+"="+value(params[k]))
	}
	return strings.Join(parts, ", ")
}

func value(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case map[string]any:
		if iv, ok := x["$interval"].(map[string]any); ok && len(x) == 1 {
			return interval(iv)
		}
		for _, k := range []string{"$date", "$datetime"} {
			if s, ok := x[k].(string); ok && len(x) == 1 {
				return s
			}
		}
	}
	b, _ := json.Marshal(v)
	return string(b)
}

// interval shows a data interval by its dates: one day as that day,
// midnight to midnight as "start – end" (end exclusive), else times.
func interval(iv map[string]any) string {
	s, _ := iv["start"].(string)
	e, _ := iv["end"].(string)
	start, err1 := time.Parse(time.RFC3339Nano, s)
	end, err2 := time.Parse(time.RFC3339Nano, e)
	if err1 != nil || err2 != nil {
		return s + " – " + e
	}
	midnight := func(t time.Time) bool { return t.Hour() == 0 && t.Minute() == 0 && t.Second() == 0 }
	if midnight(start) && midnight(end) {
		if start.AddDate(0, 0, 1).Equal(end) {
			return start.Format("2006-01-02")
		}
		return start.Format("2006-01-02") + " – " + end.AddDate(0, 0, -1).Format("2006-01-02")
	}
	return start.Format("2006-01-02 15:04") + " – " + end.Format("15:04")
}

// Took formats a duration as people say it: "45 ms", "3.2 s", "4 min 5 s".
func Took(d time.Duration) string {
	switch {
	case d < time.Second:
		return fmt.Sprintf("%d ms", d.Milliseconds())
	case d < 10*time.Second:
		return fmt.Sprintf("%.1f s", d.Seconds())
	case d < time.Minute:
		return fmt.Sprintf("%d s", int(d.Round(time.Second).Seconds()))
	case d < time.Hour:
		d = d.Round(time.Second)
		return fmt.Sprintf("%d min %d s", int(d.Minutes()), int(d.Seconds())%60)
	}
	d = d.Round(time.Minute)
	return fmt.Sprintf("%d h %d min", int(d.Hours()), int(d.Minutes())%60)
}

// args shows a step's arguments, or "" for none.
func args(raw json.RawMessage) string {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" || s == "[]" || s == "{}" {
		return ""
	}
	return "arguments: " + s
}

func str(raw json.RawMessage) string {
	var s string
	_ = json.Unmarshal(raw, &s)
	return s
}

func orElse(s, d string) string {
	if s == "" {
		return d
	}
	return s
}
