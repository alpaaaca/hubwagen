package devmode

import (
	"context"
	"testing"
	"time"

	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestFollowDescribesEvents(t *testing.T) {
	e, err := Start(ctx, Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer e.Close()
	require.NoError(t, e.Core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily"}}))
	_, err = e.Core.StartRun(ctx, core.StartRunRequest{Tenant: "t", Name: "daily", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)

	var lines []string
	fctx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancel()
	e.Follow(fctx, 0, func(s string) { lines = append(lines, s) }, nil)
	require.Len(t, lines, 2)
	assert.Equal(t, "build b1 registered (1 pipelines, 0 steps)", lines[0])
	assert.Contains(t, lines[1], `daily started {"day":"2026-10-24"}`)
	assert.Positive(t, e.LastSeq(ctx))
}

func TestDescribe(t *testing.T) {
	ev := func(typ state.EventType, payload string) state.Event {
		return state.Event{Type: typ, RunID: "01HXXXXXXXXXXABCDEFGHIJ", Payload: []byte(payload)}
	}
	for want, e := range map[string]state.Event{
		"run …ABCDEFGHIJ failed: step_failed extract: boom":                  ev(state.EventRunFinished, `{"status":"failed","reason":"step_failed","message":"extract: boom"}`),
		"run …ABCDEFGHIJ completed":                                          ev(state.EventRunFinished, `{"status":"completed"}`),
		"run …ABCDEFGHIJ: step 2 extract scheduled (code changed (v1 → v2))": ev(state.EventStepScheduled, `{"seq":2,"name":"extract","run_reason":"code changed (v1 → v2)"}`),
		"run …ABCDEFGHIJ: step 1 attempt 2 → py-3":                           ev(state.EventStepAttemptDispatched, `{"seq":1,"number":2,"worker":"py-3"}`),
		"run …ABCDEFGHIJ: step 1 reused data version p1":                     ev(state.EventStepReused, `{"seq":1,"data_version":"p1"}`),
		"build b1 registered (1 pipelines, 2 steps)":                              {Type: state.EventBuildRegistered, Payload: []byte(`{"id":"b1","pipelines":["daily"],"steps":{"a":"v1","b":"v1"}}`)},
		"":                                                                   ev(state.EventStepFinished, `{"seq":1,"status":"completed","reused":true}`),
	} {
		assert.Equal(t, want, Describe(e))
	}
}
