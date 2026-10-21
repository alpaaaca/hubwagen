package core

import "time"

// Event payloads. They are stored as JSON in the event log and read by the
// API and tests; add fields freely, but don't rename existing ones.

type leaderTermStartedPayload struct {
	Term         int64 `json:"term"`
	ActiveRuns   int   `json:"active_runs"`
	LostAttempts int   `json:"lost_attempts"`
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
