package store

import (
	"context"
	"time"
)

type Run struct {
	ID         string
	DAGName    string
	Status     string
	Trigger    string
	StartedAt  *time.Time
	FinishedAt *time.Time
	CreatedAt  time.Time
}

type StepRun struct {
	ID         string
	RunID      string
	StepID     string
	StepName   string
	Status     string
	Executor   string
	StartedAt  *time.Time
	FinishedAt *time.Time
	ExitCode   *int
	Meta       map[string]any
}

type Store interface {
	CreateRun(ctx context.Context, run *Run) error
	UpdateRun(ctx context.Context, run *Run) error
	GetRun(ctx context.Context, id string) (*Run, error)
	ListRuns(ctx context.Context, dagName string, limit int) ([]*Run, error)

	CreateStep(ctx context.Context, step *StepRun) error
	UpdateStep(ctx context.Context, step *StepRun) error
	ListStepRuns(ctx context.Context, runID string) ([]*StepRun, error)

	AppendLog(ctx context.Context, runID, stepID, line string) error
	GetLogs(ctx context.Context, runID, stepID string) ([]string, error)
	StreamLogs(ctx context.Context, runID, stepID string) (<-chan string, error)

	UpsertDAG(ctx context.Context, name, description, schedule, yamlHash string) error

	MarkInterruptedRunsFailed(ctx context.Context) error
	Close() error
}
