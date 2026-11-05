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
	EventLeaderTermStarted EventType = "LeaderTermStarted"

	// Pipelines and steps. Every one carries RunID.
	EventRunStarted            EventType = "RunStarted"
	EventRunTaskQueued         EventType = "RunTaskQueued"
	EventRunTaskDispatched     EventType = "RunTaskDispatched"
	EventRunTaskCompleted      EventType = "RunTaskCompleted" // carries the commands
	EventRunTaskFailed         EventType = "RunTaskFailed"    // worker error or lost
	EventRunStuck              EventType = "RunStuck"
	EventRunFinished           EventType = "RunFinished"
	EventStepScheduled         EventType = "StepScheduled"
	EventStepAttemptDispatched EventType = "StepAttemptDispatched"
	EventStepAttemptStarted    EventType = "StepAttemptStarted"
	EventStepAttemptFinished   EventType = "StepAttemptFinished"
	EventStepRetryScheduled    EventType = "StepRetryScheduled"
	EventStepFinished          EventType = "StepFinished"  // carries the result
	EventStepReused            EventType = "StepReused"    // memoized: an earlier data version stands in
	EventStepWaiting           EventType = "StepWaiting"   // single-flight: waits for another step
	EventQueryAnswered         EventType = "QueryAnswered" // a run's catalog query and its answer
	EventTimerStarted          EventType = "TimerStarted"
	EventTimerFired            EventType = "TimerFired"
	EventTimerCancelled        EventType = "TimerCancelled"
	EventSignalReceived        EventType = "SignalReceived"  // a signal sent to the run
	EventSignalDelivered       EventType = "SignalDelivered" // a wait_signal got a signal or timed out
	EventChildStarted          EventType = "ChildStarted"    // a start_run started its child
	EventChildFinished         EventType = "ChildFinished"   // a waited-for child ended, or couldn't start
)

// Event is one immutable record in the log. Seq and Term are assigned by
// Store.Append; callers set the rest.
type Event struct {
	Seq       int64
	Term      int64
	Time      time.Time
	Tenant    string
	Type      EventType
	AttemptID string          // a run task or step attempt, if the event is about one
	RunID     string          // a run's event log is its events in seq order
	Payload   json.RawMessage // JSON object; nil is stored as {}
}

// Attempt statuses, for step attempts.
const (
	AttemptDispatched = "dispatched"
	AttemptRunning    = "running"
	AttemptSucceeded  = "succeeded"
	AttemptFailed     = "failed"
	AttemptLost       = "lost"
	AttemptCancelled  = "cancelled"
)

// AttemptTerminal reports whether an attempt status is final.
func AttemptTerminal(status string) bool {
	switch status {
	case AttemptSucceeded, AttemptFailed, AttemptLost, AttemptCancelled:
		return true
	}
	return false
}
