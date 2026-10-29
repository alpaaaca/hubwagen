package devmode

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func TestDataDirIsLocked(t *testing.T) {
	dir := t.TempDir()
	e, err := Start(ctx, Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()
	_, err = Start(ctx, Config{DataDir: dir})
	assert.ErrorIs(t, err, ErrLocked)
}

func TestStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	e, err := Start(ctx, Config{DataDir: dir})
	require.NoError(t, err)
	require.NoError(t, e.Core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily"}}))
	res, err := e.Core.StartHaul(ctx, core.StartHaulRequest{Tenant: "t", Name: "daily", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)
	require.NoError(t, e.Close())

	e, err = Start(ctx, Config{DataDir: dir})
	require.NoError(t, err)
	defer e.Close()
	h, err := e.Store.GetHaul(ctx, res.HaulID)
	require.NoError(t, err)
	assert.Equal(t, state.HaulRunning, h.Status)
	task, ok, err := e.Core.Poll(ctx, core.PollRequest{WorkerID: "w", Docks: []string{core.DefaultDock}})
	require.NoError(t, err)
	require.True(t, ok, "the queued haul task was restored")
	assert.Equal(t, res.HaulID, task.HaulID)

	var lines []string
	fctx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
	defer cancel()
	e.Follow(fctx, 0, func(s string) { lines = append(lines, s) }, nil)
	require.Len(t, lines, 2)
	assert.Equal(t, "build b1 registered (1 hauls, 0 lifts)", lines[0])
	assert.Contains(t, lines[1], `daily started {"day":"2026-10-24"}`)
	assert.Positive(t, e.LastSeq(ctx))
}

func TestServesTheAPI(t *testing.T) {
	e, err := Start(ctx, Config{DataDir: t.TempDir(), Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	defer e.Close()
	require.NotEmpty(t, e.APIAddr)

	for _, path := range []string{"WorkerService/Heartbeat", "HaulService/GetHaul"} {
		resp, err := http.Post("http://"+e.APIAddr+"/hubwagen.v1."+path, "application/json", strings.NewReader(`{"workerId":"w1","haulId":"x"}`))
		require.NoError(t, err)
		resp.Body.Close()
		assert.Contains(t, []int{http.StatusOK, http.StatusNotFound}, resp.StatusCode, path)
	}

	// A long poll in flight doesn't hold up Close.
	go http.Post("http://"+e.APIAddr+"/hubwagen.v1.WorkerService/PollLiftTask", "application/json",
		strings.NewReader(`{"workerId":"w1","docks":["default"],"wait":"30s"}`))
	time.Sleep(100 * time.Millisecond)
	began := time.Now()
	require.NoError(t, e.Close())
	assert.Less(t, time.Since(began), 5*time.Second)
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
