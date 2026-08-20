// Package scheduler holds the shared "start a run" entry point used by
// cmd/run.go, the TUI's trigger action, and (eventually) a cron loop — one
// code path for starting a run, not one copy per caller.
package scheduler

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/engine"
	"github.com/alper/hubwagen/internal/executor"
	"github.com/alper/hubwagen/internal/store"
	"github.com/oklog/ulid/v2"
)

// TriggerResult reports what TriggerManual did: it either started a run
// (RunID set) or declined to because one was already in progress (Skipped).
type TriggerResult struct {
	RunID   string
	Skipped bool
	Reason  string
}

// nonTerminalRunStatuses mirrors dag.StatusPending/StatusRunning as plain
// strings, since store.Run.Status is an untyped string field.
var nonTerminalRunStatuses = map[string]bool{
	string(dag.StatusPending): true,
	string(dag.StatusRunning): true,
}

// TriggerManual starts one run of d.DAG: it upserts the DAG record (using
// d.YAMLHash so the file isn't re-read/re-hashed), declines to start a
// second overlapping run if one is already in progress (the on_overlap:
// "skip" behavior — the DAG schema's default; "queue" and "cancel_running"
// are not implemented and behave the same as "skip", see
// contracts/trigger.md), creates a Run record, executes it via engine.Run,
// and updates the Run record on completion.
//
// d.DAG must be non-nil (i.e. d.ParseErr == nil) — callers must not pass a
// Discovered value that failed to parse.
func TriggerManual(
	ctx context.Context,
	st store.Store,
	d dag.Discovered,
	executors map[string]executor.Executor,
	out io.Writer,
	source string,
) (TriggerResult, error) {
	if d.DAG == nil {
		return TriggerResult{}, fmt.Errorf("trigger: %s has no parsed DAG (ParseErr: %v)", d.Path, d.ParseErr)
	}
	dagDef := d.DAG

	if err := st.UpsertDAG(ctx, dagDef.Name, dagDef.Description, dagDef.Schedule, d.YAMLHash); err != nil {
		return TriggerResult{}, fmt.Errorf("upserting DAG record: %w", err)
	}

	prior, err := st.ListRuns(ctx, dagDef.Name, 1)
	if err != nil {
		return TriggerResult{}, fmt.Errorf("checking prior runs: %w", err)
	}
	if len(prior) > 0 && nonTerminalRunStatuses[prior[0].Status] {
		return TriggerResult{
			Skipped: true,
			Reason:  fmt.Sprintf("%s is already running (run %s)", dagDef.Name, prior[0].ID),
		}, nil
	}

	now := time.Now()
	run := &store.Run{
		ID:        ulid.Make().String(),
		DAGName:   dagDef.Name,
		Status:    string(dag.StatusRunning),
		Trigger:   source,
		StartedAt: &now,
		CreatedAt: now,
	}
	if err := st.CreateRun(ctx, run); err != nil {
		return TriggerResult{}, fmt.Errorf("creating run record: %w", err)
	}

	if out == nil {
		out = io.Discard
	}
	eng := &engine.Engine{}
	runErr := eng.Run(ctx, dagDef, run, executors, st, out)

	finishedAt := time.Now()
	run.FinishedAt = &finishedAt
	if runErr != nil {
		run.Status = string(dag.StatusFailed)
	} else {
		run.Status = string(dag.StatusSucceeded)
	}
	_ = st.UpdateRun(ctx, run)

	return TriggerResult{RunID: run.ID}, runErr
}
