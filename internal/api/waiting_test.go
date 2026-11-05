package api

import (
	"io"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/core"
)

func TestWaitingOverTheAPI(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	c := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily"}}))
	res, err := c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{Tenant: "t", Name: "daily"}))
	require.NoError(t, err)
	id := res.Msg.RunId

	got, err := c.GetRun(ctx, connect.NewRequest(&mokshav1.GetRunRequest{RunId: id}))
	require.NoError(t, err)
	w := got.Msg.Run.Waiting
	require.NotNil(t, w)
	assert.Equal(t, []string{"no_worker", "run", "daily", "default"}, []string{w.Reason, w.Kind, w.Name, w.Queue})
	assert.Contains(t, w.Message, "no worker is connected")
	assert.NotNil(t, w.Since)
	list, err := c.ListRuns(ctx, connect.NewRequest(&mokshav1.ListRunsRequest{}))
	require.NoError(t, err)
	require.Len(t, list.Msg.Runs, 1)
	assert.Equal(t, "no_worker", list.Msg.Runs[0].Waiting.GetReason())

	// A worker takes it: the wait is over, and the worker is listed.
	_, ok, err := s.core.Poll(ctx, core.PollRequest{WorkerID: "w1", BuildID: "b1", Queues: []string{"default"}})
	require.NoError(t, err)
	require.True(t, ok)
	got, err = c.GetRun(ctx, connect.NewRequest(&mokshav1.GetRunRequest{RunId: id}))
	require.NoError(t, err)
	assert.Nil(t, got.Msg.Run.Waiting)
	workers, err := c.ListWorkers(ctx, connect.NewRequest(&mokshav1.ListWorkersRequest{}))
	require.NoError(t, err)
	require.Len(t, workers.Msg.Workers, 1)
	wk := workers.Msg.Workers[0]
	assert.Equal(t, []string{"w1", "b1"}, []string{wk.Id, wk.BuildId})
	assert.Equal(t, []string{"default"}, wk.Queues)
	assert.Equal(t, []string{"run", "step"}, wk.Kinds)
	assert.False(t, wk.Polling)
}

func TestWaitingText(t *testing.T) {
	for _, tc := range []struct {
		w    core.Waiting
		want string
	}{
		{core.Waiting{Reason: core.WaitNoWorker, Kind: core.TaskRun, Name: "daily", Queue: "default"},
			"no worker is connected to run the pipeline's code. Start one with `python -m moksha.worker <modules>` (or `moksha workers --config moksha.yaml`)"},
		{core.Waiting{Reason: core.WaitNoWorker, Kind: core.TaskStep, Name: "train", Queue: "gpu"},
			"no worker is connected on queue \"gpu\" to run step train. Start one with `python -m moksha.worker --queue gpu <modules>` (or `moksha workers --config moksha.yaml`)"},
		{core.Waiting{Reason: core.WaitNoBuild, Kind: core.TaskRun, Name: "daily", Queue: "default", Workers: 1, Builds: []string{"0123456789abcdef"}},
			"1 worker is connected, but its code doesn't define pipeline daily (build 0123456789ab). Restart a worker with code that has it"},
		{core.Waiting{Reason: core.WaitNoBuild, Kind: core.TaskStep, Name: "load", Queue: "default", Workers: 2, Builds: []string{"b1", "b2"}},
			"2 workers are connected, but their code doesn't define step load (build b1, b2). Restart a worker with code that has it"},
		{core.Waiting{Reason: core.WaitBusy, Kind: core.TaskStep, Name: "load", Queue: "default", Workers: 1},
			"step load waits for a free worker: the workers that can run it are busy"},
		{core.Waiting{Reason: core.WaitStarting, Kind: core.TaskRun, Name: "daily", Queue: "default", Pool: "py"},
			"a worker is starting in pool py to run the pipeline's code"},
		{core.Waiting{Reason: core.WaitQueueLimit, Kind: core.TaskStep, Name: "train", Queue: "gpu", Workers: 1},
			"step train waits for a free slot: queue \"gpu\" runs as many steps at once as it may"},
	} {
		assert.Equal(t, tc.want, WaitingText(tc.w))
	}
}

func TestListPools(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	c := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))
	at := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.core.SetPools(ctx, []core.PoolStatus{{Name: "py", Queues: []string{"default"}, Min: 0, Max: 3,
		Workers: []string{"py-1"}, Starting: 1, Decision: "started py-1: 1 run waits on default", DecidedAt: at}}))
	res, err := c.ListPools(ctx, connect.NewRequest(&mokshav1.ListPoolsRequest{}))
	require.NoError(t, err)
	require.Len(t, res.Msg.Pools, 1)
	p := res.Msg.Pools[0]
	assert.Equal(t, []any{"py", int32(3), []string{"py-1"}, int32(1), "started py-1: 1 run waits on default"},
		[]any{p.Name, p.Max, p.Workers, p.Starting, p.Decision})
	assert.Equal(t, at, p.DecidedAt.AsTime())
}

func TestPoolsAsJSON(t *testing.T) {
	s := start(t)
	srv := httptestServer(t, PoolsHandler(s.core))
	at := time.Date(2026, 10, 30, 12, 0, 0, 0, time.UTC)
	require.NoError(t, s.core.SetPools(ctx, []core.PoolStatus{
		{Name: "py", Queues: []string{"default"}, Max: 3, Workers: []string{"py-1"}, Desired: 1},
		{Name: "gpu", Queues: []string{"gpu"}, Max: 4, Desired: 2, External: true, Decision: "wants 2 workers (was 0): 3 steps wait on gpu", DecidedAt: at},
	}))
	get := func(path string) (int, string) {
		res, err := http.Get(srv + path)
		require.NoError(t, err)
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		return res.StatusCode, string(b)
	}
	code, body := get("/v1/pools/gpu")
	assert.Equal(t, http.StatusOK, code)
	assert.JSONEq(t, `{"name":"gpu","launcher":"external","queues":["gpu"],"min":0,"max":4,"desired":2,"workers":0,
		"decision":"wants 2 workers (was 0): 3 steps wait on gpu","decided_at":"2026-10-30T12:00:00Z"}`, body)
	code, body = get("/v1/pools")
	assert.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `"name":"py","launcher":"process"`)
	code, _ = get("/v1/pools/nope")
	assert.Equal(t, http.StatusNotFound, code)
}
