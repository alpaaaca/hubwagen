package executor

import (
	"context"
	"io"
	"time"

	"github.com/alper/hubwagen/internal/dag"
)

type NoopExecutor struct {
	ExitCode int
	Sleep    time.Duration
}

func (n *NoopExecutor) Type() string { return "noop" }

func (n *NoopExecutor) Run(ctx context.Context, step dag.Step, logWriter io.Writer) (StepResult, error) {
	if n.Sleep > 0 {
		select {
		case <-time.After(n.Sleep):
		case <-ctx.Done():
			return StepResult{ExitCode: 1}, ctx.Err()
		}
	}
	return StepResult{ExitCode: n.ExitCode}, nil
}
