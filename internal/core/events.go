package core

import (
	"encoding/json"
	"time"

	"github.com/alper/hubwagen/internal/pallet"
)

// Event payloads. They are stored as JSON in the event log and read by the
// API and tests; add fields freely, but don't rename existing ones.

type leaderTermStartedPayload struct {
	Term          int64 `json:"term"`
	ActiveRuns    int   `json:"active_runs"`
	LostAttempts  int   `json:"lost_attempts"`
	ActiveHauls   int   `json:"active_hauls,omitempty"`
	LostHaulTasks int   `json:"lost_haul_tasks,omitempty"`
}

type tenantUpsertedPayload struct {
	Weight          int `json:"weight"`
	MaxRunningTasks int `json:"max_running_tasks"`
}

type versionPublishedPayload struct {
	Pipeline    string `json:"pipeline"`
	Hash        string `json:"hash"`
	PublishedBy string `json:"published_by,omitempty"`
}

type runCreatedPayload struct {
	Pipeline      string         `json:"pipeline"`
	Version       string         `json:"version"`
	RunKey        string         `json:"run_key"`
	Generation    int            `json:"generation"`
	Trigger       string         `json:"trigger"`
	Priority      string         `json:"priority"`
	IntervalStart *time.Time     `json:"interval_start,omitempty"`
	IntervalEnd   *time.Time     `json:"interval_end,omitempty"`
	Params        map[string]any `json:"params,omitempty"`
}

type runFinishedPayload struct {
	Status string `json:"status"`
}

type cancelRequestedPayload struct {
	Scope       string `json:"scope"`
	Reason      string `json:"reason,omitempty"`
	RequestedBy string `json:"requested_by,omitempty"`
}

type taskFinishedPayload struct {
	Status         string `json:"status"`
	WinningAttempt string `json:"winning_attempt,omitempty"`
}

type taskRetryPayload struct {
	NextAttempt int       `json:"next_attempt"`
	RetryAt     time.Time `json:"retry_at"`
}

type attemptDispatchedPayload struct {
	Number  int                     `json:"number"`
	Runtime string                  `json:"runtime"`
	Pool    string                  `json:"pool,omitempty"`
	Outputs map[string]OutputTarget `json:"outputs,omitempty"`
}

type attemptStartedPayload struct {
	AgentID string `json:"agent_id,omitempty"`
}

type attemptFinishedPayload struct {
	Status   string `json:"status"`
	Reason   string `json:"reason,omitempty"`
	Message  string `json:"message,omitempty"`
	ExitCode *int   `json:"exit_code,omitempty"`
	Winner   string `json:"winner,omitempty"`
}

type outputCommittedPayload struct {
	Output string `json:"output"`
	URI    string `json:"uri"`
}

// v2: haul events. Seq is the sequence number of the command that created a
// lift or timer.

type haulStartedPayload struct {
	Name    string         `json:"name"`
	Dock    string         `json:"dock"`
	Trigger string         `json:"trigger"`
	Params  map[string]any `json:"params,omitempty"`
}

type haulTaskPayload struct {
	Number int    `json:"number"`
	Worker string `json:"worker,omitempty"`
}

type haulTaskCompletedPayload struct {
	Number   int          `json:"number"`
	BuildID  string       `json:"build_id,omitempty"`
	Commands []Command    `json:"commands,omitempty"` // only the new ones
	Outcome  *HaulOutcome `json:"outcome,omitempty"`
}

type haulTaskFailedPayload struct {
	Number  int    `json:"number"`
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

type haulStuckPayload struct {
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
	Seq     int    `json:"seq,omitempty"` // the contradicting command
}

type haulFinishedPayload struct {
	Status  string          `json:"status"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type liftScheduledPayload struct {
	Seq     int             `json:"seq"`
	Name    string          `json:"name"`
	Dock    string          `json:"dock"`
	Args    json.RawMessage `json:"args,omitempty"`
	Timeout time.Duration   `json:"timeout,omitempty"`
	Retry   *RetryPolicy    `json:"retry,omitempty"`
	Output  *OutputSpec     `json:"output,omitempty"`
	Inputs  []InputRef      `json:"inputs,omitempty"`
	Memo    string          `json:"memo,omitempty"`
	// Fingerprint and RunReason: what memoization decided.
	Fingerprint string `json:"fingerprint,omitempty"`
	RunReason   string `json:"run_reason,omitempty"`
}

type liftRetryScheduledPayload struct {
	Seq         int       `json:"seq"`
	NextAttempt int       `json:"next_attempt"`
	RetryAt     time.Time `json:"retry_at"`
}

type liftAttemptDispatchedPayload struct {
	Seq    int    `json:"seq"`
	Number int    `json:"number"`
	Worker string `json:"worker"`
}

type liftAttemptFinishedPayload struct {
	Seq     int    `json:"seq"`
	Number  int    `json:"number"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// liftFinishedPayload is what a replay reads as the lift's result.
type liftFinishedPayload struct {
	Seq     int             `json:"seq"`
	Status  string          `json:"status"`
	Result  json.RawMessage `json:"result,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
	Pallet  string          `json:"pallet,omitempty"` // the pallet it produced (or reused)
	Reused  bool            `json:"reused,omitempty"`
}

type liftReusedPayload struct {
	Seq         int    `json:"seq"`
	Pallet      string `json:"pallet"`
	Fingerprint string `json:"fingerprint"`
}

type buildRegisteredPayload struct {
	ID    string            `json:"id"`
	Lifts map[string]string `json:"lifts"`
}

type timerStartedPayload struct {
	Seq    int       `json:"seq"`
	FireAt time.Time `json:"fire_at"`
}

type timerPayload struct {
	Seq int `json:"seq"`
}

// Catalog events.

type datasetCreatedPayload struct {
	Name         string `json:"name"`
	PartitionKey string `json:"partition_key,omitempty"`
}

// palletRecordedPayload carries the whole manifest, so a new leader can redo
// the promote.
type palletRecordedPayload struct {
	Seq      int             `json:"seq"`
	Manifest pallet.Manifest `json:"manifest"`
	Location string          `json:"location"`
}

type palletPayload struct {
	Pallet  string `json:"pallet"`
	Seq     int    `json:"seq"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}
