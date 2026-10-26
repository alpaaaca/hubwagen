// Package state is the control plane's durable state: an append-only event
// log plus projection tables that the API queries, both in one SQLite
// database written only by the leader's core goroutine.
//
// Every write goes through Store.Append, which commits a batch of events and
// the projection updates they imply in a single transaction. Nothing else in
// the system writes scheduler state.
package state

import (
	"encoding/json"
	"time"
)

// EventType names a state change. Payload shapes are owned by the core.
type EventType string

const (
	EventLeaderTermStarted        EventType = "LeaderTermStarted"
	EventTenantUpserted           EventType = "TenantUpserted"
	EventPipelineVersionPublished EventType = "PipelineVersionPublished"
	EventPipelinePaused           EventType = "PipelinePaused"
	EventPipelineResumed          EventType = "PipelineResumed"
	EventCronTicked               EventType = "CronTicked"
	EventRunCreated               EventType = "RunCreated"
	EventRunStarted               EventType = "RunStarted"
	EventTaskReady                EventType = "TaskReady"
	EventTaskSkipped              EventType = "TaskSkipped"
	EventTaskUpstreamFailed       EventType = "TaskUpstreamFailed"
	EventAttemptDispatched        EventType = "AttemptDispatched"
	EventAttemptStarted           EventType = "AttemptStarted"
	EventAttemptFinished          EventType = "AttemptFinished"
	EventAttemptLost              EventType = "AttemptLost"
	EventOutputCommitted          EventType = "OutputCommitted"
	EventTaskRetryScheduled       EventType = "TaskRetryScheduled"
	EventTaskFinished             EventType = "TaskFinished"
	EventRunFinished              EventType = "RunFinished"
	EventCancelRequested          EventType = "CancelRequested"

	// v2: hauls and lifts. Every one carries HaulID.
	EventHaulStarted           EventType = "HaulStarted"
	EventHaulTaskQueued        EventType = "HaulTaskQueued"
	EventHaulTaskDispatched    EventType = "HaulTaskDispatched"
	EventHaulTaskCompleted     EventType = "HaulTaskCompleted" // carries the commands
	EventHaulTaskFailed        EventType = "HaulTaskFailed"    // worker error or lost
	EventHaulStuck             EventType = "HaulStuck"
	EventHaulFinished          EventType = "HaulFinished"
	EventLiftScheduled         EventType = "LiftScheduled"
	EventLiftAttemptDispatched EventType = "LiftAttemptDispatched"
	EventLiftAttemptStarted    EventType = "LiftAttemptStarted"
	EventLiftAttemptFinished   EventType = "LiftAttemptFinished"
	EventLiftRetryScheduled    EventType = "LiftRetryScheduled"
	EventLiftFinished          EventType = "LiftFinished"  // carries the result
	EventLiftReused            EventType = "LiftReused"    // memoized: an earlier pallet stands in
	EventLiftWaiting           EventType = "LiftWaiting"   // single-flight: waits for another lift
	EventQueryAnswered         EventType = "QueryAnswered" // a haul's catalog query and its answer
	EventTimerStarted          EventType = "TimerStarted"
	EventTimerFired            EventType = "TimerFired"
	EventTimerCancelled        EventType = "TimerCancelled"
)

// Event is one immutable record in the log. Seq and Term are assigned by
// Store.Append; callers set the rest.
type Event struct {
	Seq       int64
	Term      int64
	Time      time.Time
	Tenant    string
	Type      EventType
	RunID     string
	TaskID    string
	AttemptID string
	HaulID    string          // v2: a haul's logbook is its events in seq order
	Payload   json.RawMessage // JSON object; nil is stored as {}
}

// Run statuses.
const (
	RunQueued    = "queued"
	RunRunning   = "running"
	RunSucceeded = "succeeded"
	RunFailed    = "failed"
	RunCancelled = "cancelled"
)

// Task run statuses.
const (
	TaskPending        = "pending"
	TaskReady          = "ready"
	TaskDispatched     = "dispatched"
	TaskRunning        = "running"
	TaskRetryWait      = "retry_wait"
	TaskSucceeded      = "succeeded"
	TaskFailed         = "failed"
	TaskCancelled      = "cancelled"
	TaskSkipped        = "skipped"
	TaskUpstreamFailed = "upstream_failed"
)

// Attempt statuses.
const (
	AttemptDispatched = "dispatched"
	AttemptRunning    = "running"
	AttemptSucceeded  = "succeeded"
	AttemptFailed     = "failed"
	AttemptLost       = "lost"
	AttemptCancelled  = "cancelled"
	AttemptSuperseded = "superseded"
)

// RunTerminal reports whether a run status is final.
func RunTerminal(status string) bool {
	return status == RunSucceeded || status == RunFailed || status == RunCancelled
}

// TaskTerminal reports whether a task run status is final.
func TaskTerminal(status string) bool {
	switch status {
	case TaskSucceeded, TaskFailed, TaskCancelled, TaskSkipped, TaskUpstreamFailed:
		return true
	}
	return false
}

// AttemptTerminal reports whether an attempt status is final.
func AttemptTerminal(status string) bool {
	switch status {
	case AttemptSucceeded, AttemptFailed, AttemptLost, AttemptCancelled, AttemptSuperseded:
		return true
	}
	return false
}

type Tenant struct {
	Name            string
	Weight          int
	MaxRunningTasks int // 0 = unlimited
	CreatedAt       time.Time
}

// PipelineVersion is one published, immutable compiled spec.
type PipelineVersion struct {
	Hash        string
	Tenant      string
	Pipeline    string
	SpecJSON    []byte // canonical bytes, see internal/spec
	PublishedAt time.Time
	PublishedBy string
}

type Pipeline struct {
	Tenant         string
	Name           string
	CurrentVersion string
	Paused         bool
}

type ScheduleCursor struct {
	Tenant   string
	Pipeline string
	LastTick time.Time
}

type Run struct {
	ID            string
	Tenant        string
	Pipeline      string
	Version       string
	RunKey        string
	Generation    int
	Trigger       string
	Priority      string
	IntervalStart *time.Time
	IntervalEnd   *time.Time
	Params        map[string]any
	Status        string
	CreatedAt     time.Time
	StartedAt     *time.Time
	FinishedAt    *time.Time
}

type TaskRun struct {
	RunID          string
	TaskID         string
	Status         string
	Attempts       int
	ReadyAt        *time.Time
	RetryAt        *time.Time
	WinningAttempt string
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

type Attempt struct {
	ID             string
	RunID          string
	TaskID         string
	Number         int
	Term           int64
	Runtime        string
	Pool           string
	AgentID        string
	Status         string
	Reason         string
	ExitCode       *int
	StagingURI     string
	LogsURI        string
	Result         json.RawMessage // the task's result file, if any
	LeaseExpiresAt *time.Time
	DispatchedAt   time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
}

type Output struct {
	RunID     string
	TaskID    string
	Name      string
	URI       string
	AttemptID string
	Metadata  map[string]any
}
