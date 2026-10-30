package api

import (
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/core"
)

func TestWaitingOverTheAPI(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewHaulHandler(s.core, s.store)
	mux.Handle(path, h)
	c := hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, httptestServer(t, mux))
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily"}}))
	res, err := c.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{Tenant: "t", Name: "daily"}))
	require.NoError(t, err)
	id := res.Msg.HaulId

	got, err := c.GetHaul(ctx, connect.NewRequest(&hubwagenv1.GetHaulRequest{HaulId: id}))
	require.NoError(t, err)
	w := got.Msg.Haul.Waiting
	require.NotNil(t, w)
	assert.Equal(t, []string{"no_worker", "haul", "daily", "default"}, []string{w.Reason, w.Kind, w.Name, w.Dock})
	assert.Contains(t, w.Message, "no worker is connected")
	assert.NotNil(t, w.Since)
	list, err := c.ListHauls(ctx, connect.NewRequest(&hubwagenv1.ListHaulsRequest{}))
	require.NoError(t, err)
	require.Len(t, list.Msg.Hauls, 1)
	assert.Equal(t, "no_worker", list.Msg.Hauls[0].Waiting.GetReason())

	// A worker takes it: the wait is over, and the worker is listed.
	_, ok, err := s.core.Poll(ctx, core.PollRequest{WorkerID: "w1", BuildID: "b1", Docks: []string{"default"}})
	require.NoError(t, err)
	require.True(t, ok)
	got, err = c.GetHaul(ctx, connect.NewRequest(&hubwagenv1.GetHaulRequest{HaulId: id}))
	require.NoError(t, err)
	assert.Nil(t, got.Msg.Haul.Waiting)
	workers, err := c.ListWorkers(ctx, connect.NewRequest(&hubwagenv1.ListWorkersRequest{}))
	require.NoError(t, err)
	require.Len(t, workers.Msg.Workers, 1)
	wk := workers.Msg.Workers[0]
	assert.Equal(t, []string{"w1", "b1"}, []string{wk.Id, wk.BuildId})
	assert.Equal(t, []string{"default"}, wk.Docks)
	assert.Equal(t, []string{"haul", "lift"}, wk.Kinds)
	assert.False(t, wk.Polling)
}

func TestWaitingText(t *testing.T) {
	for _, tc := range []struct {
		w    core.Waiting
		want string
	}{
		{core.Waiting{Reason: core.WaitNoWorker, Kind: core.TaskHaul, Name: "daily", Dock: "default"},
			"no worker is connected to run the pipeline's code. Start one with `python -m hubwagen.worker <modules>` (or `hubwagen workers --config hubwagen.yaml`)"},
		{core.Waiting{Reason: core.WaitNoWorker, Kind: core.TaskLift, Name: "train", Dock: "gpu"},
			"no worker is connected on dock \"gpu\" to run step train. Start one with `python -m hubwagen.worker --dock gpu <modules>` (or `hubwagen workers --config hubwagen.yaml`)"},
		{core.Waiting{Reason: core.WaitNoBuild, Kind: core.TaskHaul, Name: "daily", Dock: "default", Workers: 1, Builds: []string{"0123456789abcdef"}},
			"1 worker is connected, but its code doesn't define pipeline daily (build 0123456789ab). Restart a worker with code that has it"},
		{core.Waiting{Reason: core.WaitNoBuild, Kind: core.TaskLift, Name: "load", Dock: "default", Workers: 2, Builds: []string{"b1", "b2"}},
			"2 workers are connected, but their code doesn't define step load (build b1, b2). Restart a worker with code that has it"},
		{core.Waiting{Reason: core.WaitBusy, Kind: core.TaskLift, Name: "load", Dock: "default", Workers: 1},
			"step load waits for a free worker: the workers that can run it are busy"},
		{core.Waiting{Reason: core.WaitDockLimit, Kind: core.TaskLift, Name: "train", Dock: "gpu", Workers: 1},
			"step train waits for a free slot: dock \"gpu\" runs as many steps at once as it may"},
	} {
		assert.Equal(t, tc.want, WaitingText(tc.w))
	}
}
