package server

import (
	"context"
	"io"
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

func TestHealth(t *testing.T) {
	e, err := Start(ctx, Config{DataDir: t.TempDir(), Listen: "127.0.0.1:0"})
	require.NoError(t, err)
	defer e.Close()

	get := func() (int, string) {
		resp, err := http.Get("http://" + e.APIAddr + "/healthz")
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, strings.TrimSpace(string(body))
	}
	code, body := get()
	assert.Equal(t, http.StatusOK, code)
	assert.Equal(t, "ok", body)

	// The core stops on its own (here: cancelled) while the API still answers.
	e.cancel()
	<-e.done
	e.done <- nil // for Close
	code, body = get()
	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "stopped", body)
}
