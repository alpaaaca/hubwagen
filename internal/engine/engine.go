package engine

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	internallog "github.com/alper/hubwagen/internal/log"
	"github.com/alper/hubwagen/internal/store"
)

type Engine struct{}

type stepResult struct {
	stepID string
	status string
	err    error
}

// Run executes d with goroutine-per-step parallelism within each topological wave.
func (e *Engine) Run(
	ctx context.Context,
	d *dag.DAG,
	run *store.Run,
	executors map[string]executor.Executor,
	st store.Store,
	out io.Writer,
) error {
	runStart := time.Now()
	slog.Info("dag run started", "dag_name", d.Name, "run_id", run.ID, "trigger", run.Trigger, "steps", len(d.Steps))

	waves, err := dag.TopologicalWaves(d.Steps)
	if err != nil {
		return fmt.Errorf("topological sort: %w", err)
	}

	stepByID := make(map[string]dag.Step, len(d.Steps))
	for _, s := range d.Steps {
		stepByID[s.ID] = s
	}

	dependents := buildDependents(d.Steps)
	skip := make(map[string]bool)
	dagFailed := false

	for _, wave := range waves {
		// Partition wave into active and already-skipped steps.
		var activeIDs []string
		for _, stepID := range wave {
			if skip[stepID] {
				_ = createSkipped(ctx, stepByID[stepID], run, st)
			} else {
				activeIDs = append(activeIDs, stepID)
			}
		}

		if len(activeIDs) == 0 {
			continue
		}

		// Pre-validate executors before launching any goroutines.
		for _, stepID := range activeIDs {
			s := stepByID[stepID]
			if _, ok := executors[s.Executor]; !ok {
				return fmt.Errorf("no executor registered for type %q (step %q)", s.Executor, s.ID)
			}
		}

		resultCh := make(chan stepResult, len(activeIDs))

		var wmu sync.Mutex
		waveCancelFuncs := make(map[string]context.CancelFunc)

		for _, stepID := range activeIDs {
			s := stepByID[stepID]

			// Merge DAG-level env into step env; step-level values take precedence.
			if len(d.Env) > 0 {
				merged := make(map[string]string, len(d.Env)+len(s.Env))
				for k, v := range d.Env {
					merged[k] = v
				}
				for k, v := range s.Env {
					merged[k] = v
				}
				s.Env = merged
			}

			stepCtx, cancel := context.WithCancel(ctx)
			wmu.Lock()
			waveCancelFuncs[stepID] = cancel
			wmu.Unlock()

			go func(id string, step dag.Step, sCtx context.Context, cancelFn context.CancelFunc) {
				defer func() {
					wmu.Lock()
					delete(waveCancelFuncs, id)
					wmu.Unlock()
					cancelFn()
				}()

				cancelSiblings := func() {
					wmu.Lock()
					defer wmu.Unlock()
					for sibID, fn := range waveCancelFuncs {
						if sibID != id {
							fn()
						}
					}
				}

				exec := executors[step.Executor]
				// Fan step output to both the console (prefixed, human-readable)
				// and the store (durable, per-line, queryable via GetLogs/StreamLogs).
				stepOut := &internallog.MultiWriter{}
				stepOut.Add(&prefixWriter{prefix: fmt.Sprintf("[%s] ", id), sink: out})
				stepOut.Add(&internallog.StoreWriter{Store: st, RunID: run.ID, StepID: id})
				status, err := RunStep(sCtx, step, run, exec, st, stepOut, cancelSiblings)
				resultCh <- stepResult{stepID: id, status: status, err: err}
			}(stepID, s, stepCtx, cancel)
		}

		// Collect all results for this wave before moving to the next.
		for range activeIDs {
			res := <-resultCh
			if res.status != string(dag.StatusFailed) {
				continue
			}
			dagFailed = true
			s := stepByID[res.stepID]
			onFailure := s.OnFailure
			if onFailure == "" {
				onFailure = "stop"
			}
			switch onFailure {
			case "stop":
				for _, w := range waves {
					for _, id := range w {
						if id != res.stepID {
							skip[id] = true
						}
					}
				}
			case "skip_dependents":
				markAllDependents(res.stepID, dependents, skip)
			}
		}
	}

	status := string(dag.StatusSucceeded)
	if dagFailed {
		status = string(dag.StatusFailed)
	}
	slog.Info("dag run finished", "dag_name", d.Name, "run_id", run.ID, "status", status,
		"duration_ms", time.Since(runStart).Milliseconds())

	if dagFailed {
		return fmt.Errorf("one or more steps failed")
	}
	return nil
}

type prefixWriter struct {
	prefix string
	sink   io.Writer
}

func (p *prefixWriter) Write(b []byte) (int, error) {
	_, err := fmt.Fprintf(p.sink, "%s%s", p.prefix, b)
	return len(b), err
}

func buildDependents(steps []dag.Step) map[string][]string {
	m := make(map[string][]string)
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			m[dep] = append(m[dep], s.ID)
		}
	}
	return m
}

func markAllDependents(stepID string, dependents map[string][]string, skip map[string]bool) {
	for _, dep := range dependents[stepID] {
		if !skip[dep] {
			skip[dep] = true
			markAllDependents(dep, dependents, skip)
		}
	}
}

func createSkipped(ctx context.Context, s dag.Step, run *store.Run, st store.Store) error {
	now := time.Now()
	sr := &store.StepRun{
		ID:         newULID(),
		RunID:      run.ID,
		StepID:     s.ID,
		StepName:   nameOrID(s),
		Status:     string(dag.StatusSkipped),
		Executor:   s.Executor,
		StartedAt:  &now,
		FinishedAt: &now,
	}
	if err := st.CreateStep(ctx, sr); err != nil {
		return err
	}
	return st.UpdateStep(ctx, sr)
}
