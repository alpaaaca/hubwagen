package devmode

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/alper/hubwagen/internal/state"
)

// Follow calls fn with a one-line description of every notable event after
// afterSeq (hauls started, stuck and finished; lift attempts dispatched;
// lifts finished, reused and retried; builds registered), polling the event
// log until ctx ends.
// `hubwagen dev` prints these.
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
	n, err := e.Store.LastSeq(ctx)
	if err != nil {
		return 0
	}
	return n
}

// short abbreviates a haul ID to its last characters.
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
	haul := short(ev.HaulID)
	why := strings.TrimSpace(str("reason") + " " + str("message"))
	switch ev.Type {
	case state.EventHaulStarted:
		params, _ := json.Marshal(p["params"])
		return fmt.Sprintf("haul %s %s started %s", haul, str("name"), params)
	case state.EventHaulFinished:
		if why != "" && str("status") != state.HaulCompleted {
			return fmt.Sprintf("haul %s %s: %s", haul, str("status"), why)
		}
		return fmt.Sprintf("haul %s %s", haul, str("status"))
	case state.EventHaulStuck:
		return fmt.Sprintf("haul %s stuck: %s", haul, str("message"))
	case state.EventLiftScheduled:
		return fmt.Sprintf("haul %s: lift %d %s scheduled (%s)", haul, num("seq"), str("name"), str("run_reason"))
	case state.EventScheduleSet:
		return fmt.Sprintf("schedule %s: %s (%s, catch-up %s)", str("haul"), str("cron"), str("interval"), str("catch_up"))
	case state.EventScheduleRemoved:
		return fmt.Sprintf("schedule %s removed", str("haul"))
	case state.EventScheduleFired:
		switch {
		case str("skipped") != "":
			return fmt.Sprintf("schedule %s: skipped %s..%s (%s)", str("haul"), str("start"), str("end"), str("skipped"))
		case str("error") != "":
			return fmt.Sprintf("schedule %s: couldn't start: %s", str("haul"), str("error"))
		}
		return fmt.Sprintf("schedule %s: started haul %s for %s..%s", str("haul"), short(str("haul_id")), str("start"), str("end"))
	case state.EventChildStarted:
		return fmt.Sprintf("haul %s: started child haul %s %s", haul, short(str("haul")), str("name"))
	case state.EventChildFinished:
		if str("haul") == "" {
			return fmt.Sprintf("haul %s: child haul %s couldn't start: %s", haul, str("name"), str("message"))
		}
		return fmt.Sprintf("haul %s: child haul %s %s", haul, short(str("haul")), str("status"))
	case state.EventSignalReceived:
		return fmt.Sprintf("haul %s: signal %s received", haul, str("name"))
	case state.EventSignalDelivered:
		if p["timed_out"] == true {
			return fmt.Sprintf("haul %s: wait %d for signal %s timed out", haul, num("seq"), str("name"))
		}
		return fmt.Sprintf("haul %s: wait %d got signal %s", haul, num("seq"), str("name"))
	case state.EventLiftReused:
		if str("pallet") == "" {
			return fmt.Sprintf("haul %s: lift %d reused the result of haul %s lift %d", haul, num("seq"), str("haul"), num("on_seq"))
		}
		return fmt.Sprintf("haul %s: lift %d reused pallet %s", haul, num("seq"), str("pallet"))
	case state.EventLiftAttemptDispatched:
		return fmt.Sprintf("haul %s: lift %d attempt %d → %s", haul, num("seq"), num("number"), str("worker"))
	case state.EventLiftRetryScheduled:
		return fmt.Sprintf("haul %s: lift %d will retry (attempt %d)", haul, num("seq"), num("next_attempt"))
	case state.EventLiftFinished:
		if p["reused"] == true {
			return ""
		}
		if str("status") != state.LiftCompleted {
			return fmt.Sprintf("haul %s: lift %d %s: %s", haul, num("seq"), str("status"), why)
		}
		return fmt.Sprintf("haul %s: lift %d completed", haul, num("seq"))
	case state.EventBuildRegistered:
		lifts, _ := p["lifts"].(map[string]any)
		hauls, _ := p["hauls"].([]any)
		return fmt.Sprintf("build %s registered (%d hauls, %d lifts)", str("id"), len(hauls), len(lifts))
	}
	return ""
}
