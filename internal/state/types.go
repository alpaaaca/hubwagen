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

	// Hauls and lifts. Every one carries HaulID.
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
	EventSignalReceived        EventType = "SignalReceived"  // a signal sent to the haul
	EventSignalDelivered       EventType = "SignalDelivered" // a wait_signal got a signal or timed out
)

// Event is one immutable record in the log. Seq and Term are assigned by
// Store.Append; callers set the rest.
type Event struct {
	Seq       int64
	Term      int64
	Time      time.Time
	Tenant    string
	Type      EventType
	AttemptID string          // a haul task or lift attempt, if the event is about one
	HaulID    string          // a haul's logbook is its events in seq order
	Payload   json.RawMessage // JSON object; nil is stored as {}
}

// Attempt statuses, for lift attempts.
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
