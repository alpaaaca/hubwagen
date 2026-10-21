package taskexec

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/state"
)

// Marker is the content of a task commit marker (core.Assignment.CommitURI).
type Marker struct {
	V           int                     `json:"v"`
	AttemptID   string                  `json:"attempt_id"`
	Attempt     int                     `json:"attempt"`
	CommittedAt time.Time               `json:"committed_at"`
	Outputs     map[string]MarkerOutput `json:"outputs"`
}

type MarkerOutput struct {
	URI      string         `json:"uri"`
	Metadata map[string]any `json:"metadata,omitempty"`
}

// result is the optional file a task writes to $HUBWAGEN_RESULT_FILE.
type result struct {
	Outputs  map[string]map[string]any `json:"outputs"`
	Metadata map[string]any            `json:"metadata"`
}

// commit runs after a zero exit: it reads the result file, then tries to
// create the task's commit marker. Losing that race means another attempt
// already committed, and this one is superseded by it.
func commit(ctx context.Context, asg core.Assignment, o Options, resultFile string, rep core.Report) core.Report {
	var res result
	raw, err := os.ReadFile(resultFile)
	switch {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return failed(rep, "result_error", "reading result file: %v", err)
	case len(raw) > maxResultBytes:
		return failed(rep, "result_error", "result file is %d bytes, the limit is %d", len(raw), maxResultBytes)
	default:
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&res); err != nil {
			return failed(rep, "result_error", "result file is not valid: %v", err)
		}
		for name := range res.Outputs {
			if _, ok := asg.Outputs[name]; !ok {
				return failed(rep, "result_error", "result file describes undeclared output %q", name)
			}
		}
		if err := o.Bucket.Put(ctx, asg.ResultURI, raw); err != nil {
			return failed(rep, "commit_error", "storing result file: %v", err)
		}
		rep.Result = raw
	}

	m := Marker{V: 1, AttemptID: asg.AttemptID, Attempt: asg.Attempt, CommittedAt: time.Now().UTC(),
		Outputs: map[string]MarkerOutput{}}
	for name, target := range asg.Outputs {
		m.Outputs[name] = MarkerOutput{URI: target.Staging, Metadata: res.Outputs[name]}
	}
	data, _ := json.Marshal(m)

	err = o.Bucket.PutIfAbsent(ctx, asg.CommitURI, data)
	if errors.Is(err, blob.ErrExists) {
		winner, rerr := ReadMarker(ctx, o.Bucket, asg.CommitURI)
		if rerr != nil {
			return failed(rep, "commit_error", "commit marker exists but can't be read: %v", rerr)
		}
		rep.Status = state.AttemptSuperseded
		rep.Reason = "superseded"
		rep.Message = fmt.Sprintf("attempt %d (%s) committed first", winner.Attempt, winner.AttemptID)
		rep.Winner = winner.AttemptID
		rep.Outputs = winner.refs()
		return rep
	}
	if err != nil {
		// Retryable: the marker wasn't created, so no attempt has won yet.
		return failed(rep, "commit_error", "creating commit marker: %v", err)
	}

	// Per-output markers for anyone browsing the data; the task marker above
	// is the one that counts.
	for name, target := range asg.Outputs {
		if err := o.Bucket.PutIfAbsent(ctx, layout.CommitMarker(target.Generation), data); err != nil && !errors.Is(err, blob.ErrExists) {
			rep.Message = fmt.Sprintf("output %s: writing _COMMITTED: %v", name, err)
		}
	}
	rep.Status = state.AttemptSucceeded
	rep.Reason = "exit_code"
	rep.Outputs = m.refs()
	return rep
}

func failed(rep core.Report, reason, format string, args ...any) core.Report {
	rep.Status = state.AttemptFailed
	rep.Reason = reason
	rep.Message = fmt.Sprintf(format, args...)
	return rep
}

func (m Marker) refs() []core.OutputRef {
	var out []core.OutputRef
	for name, o := range m.Outputs {
		out = append(out, core.OutputRef{Name: name, URI: o.URI, Metadata: o.Metadata})
	}
	return out
}

// ReadMarker reads a task commit marker.
func ReadMarker(ctx context.Context, b blob.Bucket, uri string) (Marker, error) {
	var m Marker
	data, err := b.Get(ctx, uri)
	if err != nil {
		return m, err
	}
	err = json.Unmarshal(data, &m)
	return m, err
}

// Recovery implements core.Recovery by checking commit markers: an in-flight
// attempt whose task has a marker either won (succeeded) or lost to another
// attempt (superseded). Attempts without a marker are left to the core,
// which records them as lost.
type Recovery struct {
	Bucket blob.Bucket
}

func (r Recovery) Recover(ctx context.Context, inFlight []core.InFlight) []core.Report {
	var out []core.Report
	for _, f := range inFlight {
		m, err := ReadMarker(ctx, r.Bucket, f.CommitURI)
		if err != nil {
			continue
		}
		rep := core.Report{AttemptID: f.Attempt.ID, Outputs: m.refs()}
		if m.AttemptID == f.Attempt.ID {
			rep.Status, rep.Reason, rep.Message = state.AttemptSucceeded, "recovered", "commit marker found after leader restart"
		} else {
			rep.Status, rep.Reason, rep.Winner = state.AttemptSuperseded, "superseded", m.AttemptID
		}
		out = append(out, rep)
	}
	return out
}
