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
	ActiveHauls   int   `json:"active_hauls"`
	LostHaulTasks int   `json:"lost_haul_tasks"`
}

// Haul events. Seq is the sequence number of the command that created a
// lift or timer.

type haulStartedPayload struct {
	Name    string         `json:"name"`
	Dock    string         `json:"dock"`
	Trigger string         `json:"trigger"`
	Params  map[string]any `json:"params,omitempty"`
	Parent  string         `json:"parent,omitempty"` // a child haul: the haul that started it
}

type subscriptionSetPayload struct {
	Haul     string        `json:"haul"`
	Datasets []string      `json:"datasets"`
	Debounce time.Duration `json:"debounce"`
	MaxWait  time.Duration `json:"max_wait"`
	MaxBatch int           `json:"max_batch"`
	Build    string        `json:"build"`
}

type subscriptionRemovedPayload struct {
	Haul  string `json:"haul"`
	Build string `json:"build"`
}

type subscriptionStartedPayload struct {
	Haul    string   `json:"haul"`
	Pallets []string `json:"pallets"`
	HaulID  string   `json:"haul_id,omitempty"`
	Error   string   `json:"error,omitempty"`
}

type scheduleSetPayload struct {
	Haul     string    `json:"haul"`
	Cron     string    `json:"cron"`
	Timezone string    `json:"timezone,omitempty"`
	Interval string    `json:"interval"`
	CatchUp  string    `json:"catch_up"`
	Build    string    `json:"build"`
	Cursor   time.Time `json:"cursor"`
}

type scheduleRemovedPayload struct {
	Haul  string `json:"haul"`
	Build string `json:"build"` // the build that defines the haul without a schedule
}

// scheduleFiredPayload is one due interval: the haul it started, or why
// none was (Skipped, Error).
type scheduleFiredPayload struct {
	Haul    string    `json:"haul"`
	Tick    time.Time `json:"tick"`
	Start   time.Time `json:"start"`
	End     time.Time `json:"end"`
	HaulID  string    `json:"haul_id,omitempty"`
	Skipped string    `json:"skipped,omitempty"`
	Error   string    `json:"error,omitempty"`
}

type childStartedPayload struct {
	Seq  int    `json:"seq"`
	Name string `json:"name"`
	Haul string `json:"haul"`
}

// childFinishedPayload answers a start_haul command Seq: the waited-for
// child ended with Status, or (Haul empty) it couldn't start.
type childFinishedPayload struct {
	Seq     int             `json:"seq"`
	Name    string          `json:"name"`
	Haul    string          `json:"haul,omitempty"`
	Status  string          `json:"status"`
	Result  json.RawMessage `json:"result,omitempty"`
	Reason  string          `json:"reason,omitempty"`
	Message string          `json:"message,omitempty"`
}

type haulTaskPayload struct {
	Number int    `json:"number"`
	Worker string `json:"worker,omitempty"`
}

type haulTaskCompletedPayload struct {
	Number   int          `json:"number"`
	BuildID  string       `json:"build_id,omitempty"`
	Commands []Command    `json:"commands,omitempty"` // only the new ones
	Markers  []Marker     `json:"markers,omitempty"`  // only the new ones
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

// liftReusedPayload names what stands in for the lift: a pallet, or for a
// lift without an output, the result of the lift (Haul, OnSeq) that ran.
type liftReusedPayload struct {
	Seq         int    `json:"seq"`
	Pallet      string `json:"pallet,omitempty"`
	Haul        string `json:"haul,omitempty"`
	OnSeq       int    `json:"on_seq,omitempty"`
	Fingerprint string `json:"fingerprint"`
}

// queryAnsweredPayload is a latest_pallet answer; Pallet is null when the
// partition has no committed pallet.
type queryAnsweredPayload struct {
	Seq    int           `json:"seq"`
	Pallet *palletAnswer `json:"pallet"`
}

type palletAnswer struct {
	ID          string         `json:"id"`
	Dataset     string         `json:"dataset"`
	Partition   string         `json:"partition,omitempty"`
	Location    string         `json:"location"`
	ContentHash string         `json:"content_hash,omitempty"`
	Meta        map[string]any `json:"meta,omitempty"`
	Stats       map[string]any `json:"stats,omitempty"`
	CommittedAt *time.Time     `json:"committed_at,omitempty"`
}

// liftWaitingPayload names the run a lift waits for.
type liftWaitingPayload struct {
	Seq         int    `json:"seq"`
	Fingerprint string `json:"fingerprint"`
	Haul        string `json:"haul"`   // the running lift's haul
	OnSeq       int    `json:"on_seq"` // and its seq
}

type buildRegisteredPayload struct {
	ID    string            `json:"id"`
	Hauls []string          `json:"hauls,omitempty"`
	Lifts map[string]string `json:"lifts"`
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

// palletRecordedPayload carries the whole manifest, so a new leader can redo
// the promote.
type palletRecordedPayload struct {
	Seq      int             `json:"seq"`
	Manifest pallet.Manifest `json:"manifest"`
	Location string          `json:"location"`
}

// palletInvalidatedPayload: InvalidatePallet marked Pallet invalid, because
// of Cause (itself, or a pallet it was built from).
type palletInvalidatedPayload struct {
	Pallet    string `json:"pallet"`
	Dataset   string `json:"dataset"`
	Partition string `json:"partition,omitempty"`
	Reason    string `json:"reason"`
	Cause     string `json:"cause"`
	By        string `json:"by,omitempty"`
}

type palletPayload struct {
	Pallet  string `json:"pallet"`
	Seq     int    `json:"seq"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}
