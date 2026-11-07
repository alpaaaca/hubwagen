package core

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/alper/moksha/internal/state"
)

// Planning (moksha replay --dry-run). The server runs no pipeline code, so
// it can't know which steps a new run will call: that is known only when a
// worker replays it. What it can say is what the latest earlier run with
// the same inputs did, and which of its steps would run again: those
// without reuse=, those whose code changed since, those that didn't
// complete. A step after one that runs again may run too, if that one
// returns something new; nobody can know that beforehand.

// StepPlan is what one step of the earlier run would do in a new one.
type StepPlan struct {
	Seq   int
	Name  string
	Reuse bool   // it would be reused (Why: the condition, if any)
	Why   string // in words, to follow "runs because" or "reused"
}

// RunPlan is a new run's plan: its latest earlier run with the same inputs
// (nil: none, so every step runs) and that run's steps.
type RunPlan struct {
	Earlier *state.Run
	Steps   []StepPlan
}

// PlanRun plans a run of name with params, reusing as reuse says (""/auto
// or none). It reads the store, not the loop's state, so it doesn't hold
// the loop up.
func (c *Core) PlanRun(ctx context.Context, tenant, name string, params map[string]any, reuse string) (RunPlan, error) {
	switch reuse {
	case "", "auto", state.ReuseNone:
	default:
		return RunPlan{}, fmt.Errorf("%w: reuse %q: want auto or none", ErrInvalid, reuse)
	}
	st := c.cfg.Store
	run, err := st.LatestRunWithParams(ctx, tenant, name, params)
	if errors.Is(err, state.ErrNotFound) {
		return RunPlan{}, nil
	}
	if err != nil {
		return RunPlan{}, err
	}
	plan := RunPlan{Earlier: &run}
	steps, err := st.ListSteps(ctx, run.ID)
	if err != nil {
		return plan, err
	}
	policies, err := stepPolicies(ctx, st, run.ID)
	if err != nil {
		return plan, err
	}
	before := false // a step before this one runs again
	for _, l := range steps {
		p := StepPlan{Seq: l.Seq, Name: l.Name}
		memo, _ := parseMemo(policies[l.Seq])
		then, now, err := st.StepCodeVersions(ctx, l, run.BuildID)
		if err != nil {
			return plan, err
		}
		switch {
		case !memo.on:
			p.Why = "it has no reuse="
		case reuse == state.ReuseNone:
			p.Why = "the run would be started with --reuse=none"
		case l.Status != state.StepCompleted:
			p.Why = fmt.Sprintf("it didn't complete in that run (%s)", l.Status)
		case now == "":
			p.Why = "no build has its code any more"
		case then != now:
			p.Why = fmt.Sprintf("its code changed (%s → %s)", then, now)
		default:
			p.Reuse = true
			switch {
			case before:
				p.Why = "unless a step before it returns something new"
			case memo.ttl > 0:
				p.Why = fmt.Sprintf("if its result is younger than %s", memo.ttl)
			}
		}
		before = before || !p.Reuse
		plan.Steps = append(plan.Steps, p)
	}
	return plan, nil
}

// stepPolicies returns each step's memo policy in a run, by seq, from its
// StepScheduled events (the steps table doesn't keep it).
func stepPolicies(ctx context.Context, st *state.Store, runID string) (map[int]string, error) {
	evs, err := st.RunEvents(ctx, runID)
	if err != nil {
		return nil, err
	}
	out := map[int]string{}
	for _, e := range evs {
		if e.Type != state.EventStepScheduled {
			continue
		}
		var p stepScheduledPayload
		if json.Unmarshal(e.Payload, &p) == nil {
			out[p.Seq] = p.Memo
		}
	}
	return out, nil
}
