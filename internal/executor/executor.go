package executor

import (
	"context"
	"io"

	"github.com/alper/hubwagen/internal/dag"
)

type StepResult struct {
	ExitCode int
	Stdout   string
	Stderr   string
	Output   map[string]string
	Meta     map[string]any
}

type Executor interface {
	Run(ctx context.Context, step dag.Step, logWriter io.Writer) (StepResult, error)
	Type() string
}
