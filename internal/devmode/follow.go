package devmode

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alper/moksha/internal/state"
)

// Follow calls fn with a one-line description of every notable event after
// afterSeq (runs started, stuck and finished; step attempts dispatched;
// steps finished, reused and retried; builds registered), and each with
// every event, polling the event log until ctx ends. Either may be nil.
// `moksha dev --events` prints these.
func (e *Engine) Follow(ctx context.Context, afterSeq int64, fn func(string), each func(state.Event)) {
	t := time.NewTicker(250 * time.Millisecond)
	defer t.Stop()
	for {
		evs, err := e.Store.EventsSince(ctx, afterSeq, "", 500)
		if err == nil {
			for _, ev := range evs {
				afterSeq = ev.Seq
				if each != nil {
					each(ev)
				}
				if fn == nil {
					continue
				}
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
	n, err := e.Store.LastSeq(ctx)
	if err != nil {
		return 0
	}
	return n
}

// short abbreviates a run ID to its last characters.
func short(id string) string {
	if len(id) > 10 {
		return "…" + id[len(id)-10:]
	}
	return id
}

// Describe renders an event for humans, or "" for events not worth a line.
func Describe(ev state.Event) string {
	var p map[string]any
	_ = json.Unmarshal(ev.Payload, &p)
	str := func(k string) string { s, _ := p[k].(string); return s }
	num := func(k string) int { f, _ := p[k].(float64); return int(f) }
	run := short(ev.RunID)
	why := strings.TrimSpace(str("reason") + " " + str("message"))
	switch ev.Type {
	case state.EventRunStarted:
		params, _ := json.Marshal(p["params"])
		return fmt.Sprintf("run %s %s started %s", run, str("name"), params)
	case state.EventRunFinished:
		if why != "" && str("status") != state.RunCompleted {
			return fmt.Sprintf("run %s %s: %s", run, str("status"), why)
		}
		return fmt.Sprintf("run %s %s", run, str("status"))
	case state.EventRunStuck:
		return fmt.Sprintf("run %s stuck: %s", run, str("message"))
	case state.EventStepScheduled:
		return fmt.Sprintf("run %s: step %d %s scheduled (%s)", run, num("seq"), str("name"), str("run_reason"))
	case state.EventDataVersionInvalidated:
		if ev.RunID != "" { // a promote that failed
			return fmt.Sprintf("run %s: data version %s invalid: %s", run, short(str("data_version")), why)
		}
		return fmt.Sprintf("data version %s (%s %s) invalidated: %s", short(str("data_version")), str("dataset"), str("partition"), str("reason"))
	case state.EventSubscriptionSet:
		ds, _ := json.Marshal(p["datasets"])
		return fmt.Sprintf("subscription %s: on %s", str("pipeline"), ds)
	case state.EventSubscriptionRemoved:
		return fmt.Sprintf("subscription %s removed", str("pipeline"))
	case state.EventSubscriptionStarted:
		n := 0
		if ps, ok := p["versions"].([]any); ok {
			n = len(ps)
		}
		if str("error") != "" {
			return fmt.Sprintf("subscription %s: couldn't start: %s", str("pipeline"), str("error"))
		}
		return fmt.Sprintf("subscription %s: started run %s with %d data versions", str("pipeline"), short(str("run_id")), n)
	case state.EventScheduleSet:
		return fmt.Sprintf("schedule %s: %s (%s, catch-up %s)", str("pipeline"), str("cron"), str("interval"), str("catch_up"))
	case state.EventScheduleRemoved:
		return fmt.Sprintf("schedule %s removed", str("pipeline"))
	case state.EventScheduleFired:
		switch {
		case str("skipped") != "":
			return fmt.Sprintf("schedule %s: skipped %s..%s (%s)", str("pipeline"), str("start"), str("end"), str("skipped"))
		case str("error") != "":
			return fmt.Sprintf("schedule %s: couldn't start: %s", str("pipeline"), str("error"))
		}
		return fmt.Sprintf("schedule %s: started run %s for %s..%s", str("pipeline"), short(str("run_id")), str("start"), str("end"))
	case state.EventChildStarted:
		return fmt.Sprintf("run %s: started child run %s %s", run, short(str("run")), str("name"))
	case state.EventChildFinished:
		if str("run") == "" {
			return fmt.Sprintf("run %s: child run %s couldn't start: %s", run, str("name"), str("message"))
		}
		return fmt.Sprintf("run %s: child run %s %s", run, short(str("run")), str("status"))
	case state.EventSignalReceived:
		return fmt.Sprintf("run %s: signal %s received", run, str("name"))
	case state.EventSignalDelivered:
		if p["timed_out"] == true {
			return fmt.Sprintf("run %s: wait %d for signal %s timed out", run, num("seq"), str("name"))
		}
		return fmt.Sprintf("run %s: wait %d got signal %s", run, num("seq"), str("name"))
	case state.EventStepReused:
		if str("data_version") == "" {
			return fmt.Sprintf("run %s: step %d reused the result of run %s step %d", run, num("seq"), str("run"), num("on_seq"))
		}
		return fmt.Sprintf("run %s: step %d reused data version %s", run, num("seq"), str("data_version"))
	case state.EventStepAttemptDispatched:
		return fmt.Sprintf("run %s: step %d attempt %d → %s", run, num("seq"), num("number"), str("worker"))
	case state.EventStepRetryScheduled:
		return fmt.Sprintf("run %s: step %d will retry (attempt %d)", run, num("seq"), num("next_attempt"))
	case state.EventStepFinished:
		if p["reused"] == true {
			return ""
		}
		if str("status") != state.StepCompleted {
			return fmt.Sprintf("run %s: step %d %s: %s", run, num("seq"), str("status"), why)
		}
		return fmt.Sprintf("run %s: step %d completed", run, num("seq"))
	case state.EventBuildRegistered:
		steps, _ := p["steps"].(map[string]any)
		pipelines, _ := p["pipelines"].([]any)
		return fmt.Sprintf("build %s registered (%d pipelines, %d steps)", str("id"), len(pipelines), len(steps))
	}
	return ""
}
