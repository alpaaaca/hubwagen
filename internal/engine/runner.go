package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"text/template"
	"time"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	"github.com/alper/hubwagen/internal/store"
	"github.com/oklog/ulid/v2"
)

func newULID() string {
	return ulid.Make().String()
}

func nameOrID(s dag.Step) string {
	if s.Name != "" {
		return s.Name
	}
	return s.ID
}

func effectiveRetry(step dag.Step) dag.RetryPolicy {
	if step.Retry != nil {
		return *step.Retry
	}
	return dag.RetryPolicy{}
}

// templateCtx holds the values available as {{ .Field }} in step YAML values.
type templateCtx struct {
	RunDate string
	RunID   string
	DAGName string
	Env     map[string]string
}

func renderTemplate(text string, tc templateCtx) (string, error) {
	tmpl, err := template.New("").Parse(text)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, tc); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func buildTemplateCtx(run *store.Run, env map[string]string) templateCtx {
	runDate := time.Now().UTC().Format("2006-01-02")
	if run.StartedAt != nil {
		runDate = run.StartedAt.UTC().Format("2006-01-02")
	}
	return templateCtx{
		RunDate: runDate,
		RunID:   run.ID,
		DAGName: run.DAGName,
		Env:     env,
	}
}

// RunStep executes a single step against exec, recording all state transitions
// to st before and after. cancelSiblings is called when the step fails and
// on_failure is "stop", so peer goroutines in the same wave are cancelled.
// Returns the final status string.
func RunStep(ctx context.Context, step dag.Step, run *store.Run, exec executor.Executor, st store.Store, logWriter io.Writer, cancelSiblings func()) (string, error) {
	// State-transition writes must persist even after ctx is cancelled by a
	// sibling failure — otherwise a cancelled step's final status silently
	// fails to write and the step is stuck at "running" forever. Execution
	// (exec.Run) still honours ctx/execCtx cancellation; only bookkeeping
	// writes use storeCtx.
	storeCtx := context.WithoutCancel(ctx)
	stepStart := time.Now()
	slog.Info("step started", "dag_name", run.DAGName, "run_id", run.ID, "step_id", step.ID, "executor", step.Executor)

	stepRun := &store.StepRun{
		ID:       newULID(),
		RunID:    run.ID,
		StepID:   step.ID,
		StepName: nameOrID(step),
		Status:   string(dag.StatusPending),
		Executor: step.Executor,
	}
	if err := st.CreateStep(storeCtx, stepRun); err != nil {
		return string(dag.StatusFailed), fmt.Errorf("create step record: %w", err)
	}

	// Render template tokens in command and env values at run time.
	tc := buildTemplateCtx(run, step.Env)
	rendered, err := renderTemplate(step.Command, tc)
	if err != nil {
		stepRun.Status = string(dag.StatusFailed)
		_ = st.UpdateStep(storeCtx, stepRun)
		slog.Error("step finished", "dag_name", run.DAGName, "run_id", run.ID, "step_id", step.ID,
			"status", stepRun.Status, "err", err, "duration_ms", time.Since(stepStart).Milliseconds())
		return string(dag.StatusFailed), fmt.Errorf("rendering command template for step %q: %w", step.ID, err)
	}
	step.Command = rendered

	for k, v := range step.Env {
		rv, err := renderTemplate(v, tc)
		if err != nil {
			stepRun.Status = string(dag.StatusFailed)
			_ = st.UpdateStep(storeCtx, stepRun)
			slog.Error("step finished", "dag_name", run.DAGName, "run_id", run.ID, "step_id", step.ID,
				"status", stepRun.Status, "err", err, "duration_ms", time.Since(stepStart).Milliseconds())
			return string(dag.StatusFailed), fmt.Errorf("rendering env %q template for step %q: %w", k, step.ID, err)
		}
		step.Env[k] = rv
	}

	policy := effectiveRetry(step)

	var lastResult executor.StepResult
	var lastErr error

	for attempt := 0; attempt <= policy.Limit; attempt++ {
		if attempt > 0 {
			interval := time.Duration(policy.IntervalSec) * time.Second
			if policy.Backoff {
				interval <<= attempt - 1
			}
			select {
			case <-ctx.Done():
				stepRun.Status = string(dag.StatusCancelled)
				_ = st.UpdateStep(storeCtx, stepRun)
				return string(dag.StatusCancelled), ctx.Err()
			case <-time.After(interval):
			}
		}

		// Write RUNNING before executing — Constitution invariant I.
		startTime := time.Now()
		stepRun.Status = string(dag.StatusRunning)
		stepRun.StartedAt = &startTime
		if err := st.UpdateStep(storeCtx, stepRun); err != nil {
			return string(dag.StatusFailed), err
		}

		execCtx := ctx
		var cancelFn context.CancelFunc
		if step.TimeoutSec > 0 {
			execCtx, cancelFn = context.WithTimeout(ctx, time.Duration(step.TimeoutSec)*time.Second)
		}

		lastResult, lastErr = exec.Run(execCtx, step, logWriter)

		if cancelFn != nil {
			cancelFn()
		}

		if lastResult.ExitCode == 0 && lastErr == nil {
			break
		}

		slog.Debug("step attempt failed", "dag_name", run.DAGName, "run_id", run.ID, "step_id", step.ID,
			"attempt", attempt, "retry_limit", policy.Limit, "exit_code", lastResult.ExitCode, "err", lastErr)
	}

	finishedAt := time.Now()
	stepRun.FinishedAt = &finishedAt
	stepRun.ExitCode = &lastResult.ExitCode

	switch {
	case lastResult.ExitCode == 0 && lastErr == nil:
		stepRun.Status = string(dag.StatusSucceeded)
	case ctx.Err() != nil:
		// ctx was cancelled out from under us — by a sibling's on_failure:stop
		// or by the run being cancelled — rather than this step failing on its
		// own. Record it as cancelled and don't trigger another round of
		// sibling cancellation.
		stepRun.Status = string(dag.StatusCancelled)
	default:
		stepRun.Status = string(dag.StatusFailed)
		// Cancel peer goroutines in the same wave when on_failure is "stop".
		onFailure := step.OnFailure
		if onFailure == "" {
			onFailure = "stop"
		}
		if onFailure == "stop" && cancelSiblings != nil {
			cancelSiblings()
		}
	}
	_ = st.UpdateStep(storeCtx, stepRun)

	slog.Info("step finished", "dag_name", run.DAGName, "run_id", run.ID, "step_id", step.ID,
		"status", stepRun.Status, "exit_code", lastResult.ExitCode, "duration_ms", time.Since(stepStart).Milliseconds())

	return stepRun.Status, lastErr
}
