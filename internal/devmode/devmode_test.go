package devmode

import (
	"context"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestFollowDescribesEvents(t *testing.T) {
	e, err := Start(ctx, Config{DataDir: t.TempDir()})
	require.NoError(t, err)
	defer e.Close()
	require.NoError(t, e.Core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily"}}))
	_, err = e.Core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)

	var lines []string
	fctx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancel()
	e.Follow(fctx, 0, func(s string) { lines = append(lines, s) }, nil)
	require.Len(t, lines, 2)
	assert.Equal(t, "build b1 registered (1 hauls, 0 lifts)", lines[0])
	assert.Contains(t, lines[1], `daily started {"day":"2026-10-24"}`)
	assert.Positive(t, e.LastSeq(ctx))
}

func TestDescribe(t *testing.T) {
	ev := func(typ state.EventType, payload string) state.Event {
		return state.Event{Type: typ, HaulID: "01HXXXXXXXXXXABCDEFGHIJ", Payload: []byte(payload)}
	}
	for want, e := range map[string]state.Event{
		"haul …ABCDEFGHIJ failed: lift_failed extract: boom":                  ev(state.EventHaulFinished, `{"status":"failed","reason":"lift_failed","message":"extract: boom"}`),
		"haul …ABCDEFGHIJ completed":                                          ev(state.EventHaulFinished, `{"status":"completed"}`),
		"haul …ABCDEFGHIJ: lift 2 extract scheduled (code changed (v1 → v2))": ev(state.EventLiftScheduled, `{"seq":2,"name":"extract","run_reason":"code changed (v1 → v2)"}`),
		"haul …ABCDEFGHIJ: lift 1 attempt 2 → py-3":                           ev(state.EventLiftAttemptDispatched, `{"seq":1,"number":2,"worker":"py-3"}`),
		"haul …ABCDEFGHIJ: lift 1 reused pallet p1":                           ev(state.EventLiftReused, `{"seq":1,"pallet":"p1"}`),
		"build b1 registered (1 hauls, 2 lifts)":                              {Type: state.EventBuildRegistered, Payload: []byte(`{"id":"b1","hauls":["daily"],"lifts":{"a":"v1","b":"v1"}}`)},
		"":                                                                    ev(state.EventLiftFinished, `{"seq":1,"status":"completed","reused":true}`),
	} {
		assert.Equal(t, want, Describe(e))
	}
}
