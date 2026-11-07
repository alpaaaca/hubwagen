package core

import (
	"encoding/json"
	"time"

	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/state"
)

// Event payloads. They are stored as JSON in the event log and read by the
// API and tests; add fields freely, but don't rename existing ones.

type leaderTermStartedPayload struct {
	Term         int64 `json:"term"`
	ActiveRuns   int   `json:"active_runs"`
	LostRunTasks int   `json:"lost_run_tasks"`
}

// Run events. Seq is the sequence number of the command that created a
// step or timer.

type runStartedPayload struct {
	Name    string         `json:"name"`
	Queue   string         `json:"queue"`
	Trigger string         `json:"trigger"`
	Params  map[string]any `json:"params,omitempty"`
	Parent  string         `json:"parent,omitempty"` // a child run: the run that started it
	Reuse   string         `json:"reuse,omitempty"`  // "none": the run reuses no step
}

type subscriptionSetPayload struct {
	Pipeline string        `json:"pipeline"`
	Datasets []string      `json:"datasets"`
	Debounce time.Duration `json:"debounce"`
	MaxWait  time.Duration `json:"max_wait"`
	MaxBatch int           `json:"max_batch"`
	Build    string        `json:"build"`
}

type subscriptionRemovedPayload struct {
	Pipeline string `json:"pipeline"`
	Build    string `json:"build"`
}

type subscriptionStartedPayload struct {
	Pipeline     string   `json:"pipeline"`
	DataVersions []string `json:"data_versions"`
	RunID        string   `json:"run_id,omitempty"`
	Error        string   `json:"error,omitempty"`
}

type scheduleSetPayload struct {
	Pipeline string    `json:"pipeline"`
	Cron     string    `json:"cron"`
	Timezone string    `json:"timezone,omitempty"`
	Interval string    `json:"interval"`
	CatchUp  string    `json:"catch_up"`
	Build    string    `json:"build"`
	Cursor   time.Time `json:"cursor"`
}

type scheduleRemovedPayload struct {
	Pipeline string `json:"pipeline"`
	Build    string `json:"build"` // the build that defines the run without a schedule
}

// scheduleFiredPayload is one due interval: the run it started, or why
// none was (Skipped, Error).
type scheduleFiredPayload struct {
	Pipeline string    `json:"pipeline"`
	Tick     time.Time `json:"tick"`
	Start    time.Time `json:"start"`
	End      time.Time `json:"end"`
	RunID    string    `json:"run_id,omitempty"`
	Skipped  string    `json:"skipped,omitempty"`
	Error    string    `json:"error,omitempty"`
}

type childStartedPayload struct {
	Seq  int    `json:"seq"`
	Name string `json:"name"`
	Run  string `json:"run"`
}

// childFinishedPayload answers a start_run command Seq: the waited-for
// child ended with Status, or (Run empty) it couldn't start.
type childFinishedPayload struct {
	Seq     int             `json:"seq"`
	Name    string          `json:"name"`
	Run     string          `json:"run,omitempty"`
	Status  string          `json:"status"`
	Result  json.RawMessage `json:"result,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
}

type runTaskPayload struct {
	Number int    `json:"number"`
	Worker string `json:"worker,omitempty"`
}

type runTaskCompletedPayload struct {
	Number   int         `json:"number"`
	BuildID  string      `json:"build_id,omitempty"`
	Commands []Command   `json:"commands,omitempty"` // only the new ones
	Markers  []Marker    `json:"markers,omitempty"`  // only the new ones
	Outcome  *RunOutcome `json:"outcome,omitempty"`
}

type runTaskFailedPayload struct {
	Number  int    `json:"number"`
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
}

type runStuckPayload struct {
	Reason  string `json:"reason"`
	Message string `json:"message,omitempty"`
	Seq     int    `json:"seq,omitempty"` // the contradicting command
}

type runFinishedPayload struct {
	Status  string          `json:"status"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
	Result  json.RawMessage `json:"result,omitempty"`
}

type stepScheduledPayload struct {
	Seq     int             `json:"seq"`
	Name    string          `json:"name"`
	Queue   string          `json:"queue"`
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

type stepRetryScheduledPayload struct {
	Seq         int       `json:"seq"`
	NextAttempt int       `json:"next_attempt"`
	RetryAt     time.Time `json:"retry_at"`
}

type stepAttemptDispatchedPayload struct {
	Seq    int    `json:"seq"`
	Number int    `json:"number"`
	Worker string `json:"worker"`
}

type stepAttemptFinishedPayload struct {
	Seq     int    `json:"seq"`
	Number  int    `json:"number"`
	Status  string `json:"status"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// stepFinishedPayload is what a replay reads as the step's result.
type stepFinishedPayload struct {
	Seq         int             `json:"seq"`
	Status      string          `json:"status"`
	Result      json.RawMessage `json:"result,omitempty"`
	Reason      string          `json:"reason,omitempty"`
	Message     string          `json:"message,omitempty"`
	DataVersion string          `json:"data_version,omitempty"` // the data version it produced (or reused)
	Reused      bool            `json:"reused,omitempty"`
}

// stepReusedPayload names what stands in for the step: a data version, or for a
// step without an output, the result of the step (Run, OnSeq) that ran.
type stepReusedPayload struct {
	Seq         int    `json:"seq"`
	DataVersion string `json:"data_version,omitempty"`
	Run         string `json:"run,omitempty"`
	OnSeq       int    `json:"on_seq,omitempty"`
	Fingerprint string `json:"fingerprint"`
}

// queryAnsweredPayload is a latest_version answer; DataVersion is null when the
// partition has no committed data version.
type queryAnsweredPayload struct {
	Seq         int                `json:"seq"`
	DataVersion *dataVersionAnswer `json:"data_version"`
}

type dataVersionAnswer struct {
	ID          string         `json:"id"`
	Dataset     string         `json:"dataset"`
	Partition   string         `json:"partition,omitempty"`
	Location    string         `json:"location"`
	ContentHash string         `json:"content_hash,omitempty"`
	Meta        map[string]any `json:"meta,omitempty"`
	Stats       map[string]any `json:"stats,omitempty"`
	CommittedAt *time.Time     `json:"committed_at,omitempty"`
}

// stepWaitingPayload names the run a step waits for.
type stepWaitingPayload struct {
	Seq         int    `json:"seq"`
	Fingerprint string `json:"fingerprint"`
	Run         string `json:"run"`    // the running step's run
	OnSeq       int    `json:"on_seq"` // and its seq
}

type buildRegisteredPayload struct {
	ID        string                   `json:"id"`
	Pipelines []string                 `json:"pipelines,omitempty"`
	Info      map[string]state.RunInfo `json:"info,omitempty"`
	Steps     map[string]string        `json:"steps"`
}

type timerStartedPayload struct {
	Seq    int       `json:"seq"`
	FireAt time.Time `json:"fire_at"`
}

type timerPayload struct {
	Seq int `json:"seq"`
}

type signalReceivedPayload struct {
	Signal  string          `json:"signal"`
	Name    string          `json:"name"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// signalDeliveredPayload ends the wait_signal command Seq: with a signal, or
// TimedOut.
type signalDeliveredPayload struct {
	Seq      int             `json:"seq"`
	Name     string          `json:"name"`
	Signal   string          `json:"signal,omitempty"`
	Payload  json.RawMessage `json:"payload,omitempty"`
	TimedOut bool            `json:"timed_out,omitempty"`
}

// Catalog events.

type datasetCreatedPayload struct {
	Name         string `json:"name"`
	PartitionKey string `json:"partition_key,omitempty"`
}

// dataVersionRecordedPayload carries the whole manifest, so a new leader can redo
// the promote.
type dataVersionRecordedPayload struct {
	Seq      int                  `json:"seq"`
	Manifest dataversion.Manifest `json:"manifest"`
	Location string               `json:"location"`
}

type retentionSetPayload struct {
	Dataset  string        `json:"dataset"`
	Versions int           `json:"versions,omitempty"`
	Age      time.Duration `json:"age,omitempty"`
	Build    string        `json:"build"`
}

type dataVersionExpiredPayload struct {
	DataVersion string `json:"data_version"`
	Dataset     string `json:"dataset"`
	Partition   string `json:"partition,omitempty"`
	Reason      string `json:"reason"`
}

// dataVersionInvalidatedPayload: InvalidateDataVersion marked DataVersion invalid, because
// of Cause (itself, or a data version it was built from).
type dataVersionInvalidatedPayload struct {
	DataVersion string `json:"data_version"`
	Dataset     string `json:"dataset"`
	Partition   string `json:"partition,omitempty"`
	Reason      string `json:"reason"`
	Cause       string `json:"cause"`
	By          string `json:"by,omitempty"`
}

type dataVersionPayload struct {
	DataVersion string `json:"data_version"`
	Seq         int    `json:"seq"`
	Reason      string `json:"reason,omitempty"`
	Message     string `json:"message,omitempty"`
}
