package api

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/state"
)

func TestRunService(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	srv := httptestServer(t, mux)
	c := mokshav1connect.NewRunServiceClient(http.DefaultClient, srv)

	_, err := c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{Tenant: "t", Name: "daily_revenue"}))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "no build defines it yet")
	assert.ErrorContains(t, err, `no registered build defines run "daily_revenue"`)
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily_revenue", "x"}}))

	res, err := c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{
		Tenant: "t", Name: "daily_revenue", ParamsJson: `{"day":"2026-10-24","n":10000000000000001}`}))
	require.NoError(t, err)
	got, err := c.GetRun(ctx, connect.NewRequest(&mokshav1.GetRunRequest{RunId: res.Msg.RunId}))
	require.NoError(t, err)
	hl := got.Msg.Run
	assert.Equal(t, state.RunRunning, hl.Status)
	assert.Equal(t, "daily_revenue", hl.Name)
	assert.Contains(t, hl.ParamsJson, `"n":10000000000000001`, "numbers stay exact")
	assert.NotNil(t, hl.CreatedAt)
	assert.Nil(t, hl.FinishedAt)

	_, err = c.GetRun(ctx, connect.NewRequest(&mokshav1.GetRunRequest{RunId: "nope"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	// Its history, in plain words (internal/history tells the rest).
	hist, err := c.GetHistory(ctx, connect.NewRequest(&mokshav1.GetHistoryRequest{RunId: res.Msg.RunId}))
	require.NoError(t, err)
	require.NotEmpty(t, hist.Msg.Entries)
	first := hist.Msg.Entries[0]
	assert.Equal(t, []string{"started", "Started by hand, for day=2026-10-24, n=10000000000000001", "RunStarted"},
		[]string{first.Kind, first.Text, first.EventType})
	assert.NotZero(t, first.EventSeq)
	_, err = c.GetHistory(ctx, connect.NewRequest(&mokshav1.GetHistoryRequest{RunId: "nope"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	_, err = c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{Tenant: "t", Name: "x", ParamsJson: "[1]"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "params must be an object")
	_, err = c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{Name: "x"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "no tenant")
}

func TestSignalRun(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	c := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"x"}}))
	res, err := c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{Tenant: "t", Name: "x"}))
	require.NoError(t, err)
	id := res.Msg.RunId

	sig, err := c.SignalRun(ctx, connect.NewRequest(&mokshav1.SignalRunRequest{RunId: id, Name: "approve", PayloadJson: `{"by":"ana"}`}))
	require.NoError(t, err)
	assert.NotEmpty(t, sig.Msg.SignalId)
	evs, err := s.store.RunEvents(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.EventSignalReceived, evs[len(evs)-1].Type)

	_, err = c.SignalRun(ctx, connect.NewRequest(&mokshav1.SignalRunRequest{RunId: "nope", Name: "approve"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	_, err = c.SignalRun(ctx, connect.NewRequest(&mokshav1.SignalRunRequest{RunId: id, Name: "approve", PayloadJson: "{"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.NoError(t, s.core.CancelRun(ctx, id, "test"))
	_, err = c.SignalRun(ctx, connect.NewRequest(&mokshav1.SignalRunRequest{RunId: id, Name: "approve"}))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "the run is finished")
}

func TestSchedulesOverTheAPI(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	runs := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))

	_, err := s.client.RegisterBuild(ctx, connect.NewRequest(&mokshav1.RegisterBuildRequest{BuildId: "b1", Pipelines: []string{"daily"},
		Schedules: []*mokshav1.PipelineSchedule{{Pipeline: "daily", Cron: "0 3 * * *", Timezone: "Europe/Berlin", Interval: "daily", CatchUp: "all"}}}))
	require.NoError(t, err)
	got, err := runs.ListSchedules(ctx, connect.NewRequest(&mokshav1.ListSchedulesRequest{}))
	require.NoError(t, err)
	require.Len(t, got.Msg.Schedules, 1)
	sc := got.Msg.Schedules[0]
	assert.Equal(t, []string{"daily", "0 3 * * *", "Europe/Berlin", "daily", "all", "b1"},
		[]string{sc.Pipeline, sc.Cron, sc.Timezone, sc.Interval, sc.CatchUp, sc.BuildId})
	assert.True(t, sc.NextRun.AsTime().After(time.Now()))
	// To start one by hand: the latest day that came due, midnight to
	// midnight in Berlin.
	var latest struct {
		Interval struct {
			Iv struct{ Start, End time.Time } `json:"$interval"`
		} `json:"interval"`
	}
	require.NoError(t, json.Unmarshal([]byte(sc.LatestParamsJson), &latest), sc.LatestParamsJson)
	iv := latest.Interval.Iv
	berlin, _ := time.LoadLocation("Europe/Berlin")
	assert.Equal(t, iv.Start.In(berlin).AddDate(0, 0, 1), iv.End.In(berlin))
	assert.Equal(t, "00:00", iv.End.In(berlin).Format("15:04"))
	assert.True(t, iv.End.Before(time.Now()))

	_, err = s.client.RegisterBuild(ctx, connect.NewRequest(&mokshav1.RegisterBuildRequest{BuildId: "b2", Pipelines: []string{"daily"},
		Schedules: []*mokshav1.PipelineSchedule{{Pipeline: "daily", Cron: "nope"}}}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestListPipelines(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	runs := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))
	register := func(id string, info ...*mokshav1.PipelineInfo) error {
		_, err := s.client.RegisterBuild(ctx, connect.NewRequest(&mokshav1.RegisterBuildRequest{
			BuildId: id, Pipelines: []string{"daily", "hourly"}, PipelineInfo: info}))
		return err
	}
	require.NoError(t, register("b1", &mokshav1.PipelineInfo{Pipeline: "daily", Title: "Daily", Description: "old"}))
	require.NoError(t, register("b2", &mokshav1.PipelineInfo{Pipeline: "daily", Title: "Daily revenue", Description: "Orders to a report."}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(register("b3", &mokshav1.PipelineInfo{Pipeline: "weekly", Title: "x"})))

	got, err := runs.ListPipelines(ctx, connect.NewRequest(&mokshav1.ListPipelinesRequest{}))
	require.NoError(t, err)
	require.Len(t, got.Msg.Pipelines, 2)
	assert.Equal(t, []string{"daily", "Daily revenue", "Orders to a report."},
		[]string{got.Msg.Pipelines[0].Name, got.Msg.Pipelines[0].Title, got.Msg.Pipelines[0].Description}, "the newest build decides")
	assert.Equal(t, []string{"hourly", ""}, []string{got.Msg.Pipelines[1].Name, got.Msg.Pipelines[1].Title})
}

func TestListRunsAndEventLog(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	c := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily", "x"}}))
	var ids []string
	for _, name := range []string{"daily", "x", "daily"} {
		res, err := c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{Tenant: "t", Name: name, ParamsJson: `{"n":1}`}))
		require.NoError(t, err)
		ids = append(ids, res.Msg.RunId)
	}
	got, err := c.ListRuns(ctx, connect.NewRequest(&mokshav1.ListRunsRequest{Name: "daily"}))
	require.NoError(t, err)
	require.Len(t, got.Msg.Runs, 2)
	assert.Equal(t, []string{ids[2], ids[0]}, []string{got.Msg.Runs[0].Id, got.Msg.Runs[1].Id}, "newest first")
	got, err = c.ListRuns(ctx, connect.NewRequest(&mokshav1.ListRunsRequest{Statuses: []string{"completed"}}))
	require.NoError(t, err)
	assert.Empty(t, got.Msg.Runs)
	_, err = c.ListRuns(ctx, connect.NewRequest(&mokshav1.ListRunsRequest{Limit: 5000}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	_, err = c.ListRuns(ctx, connect.NewRequest(&mokshav1.ListRunsRequest{Limit: 500, WithSteps: true}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	lb, err := c.GetEventLog(ctx, connect.NewRequest(&mokshav1.GetEventLogRequest{RunId: ids[0]}))
	require.NoError(t, err)
	assert.JSONEq(t, `{"n":1}`, lb.Msg.Run.ParamsJson)
	require.NotEmpty(t, lb.Msg.EventLog)
	assert.Equal(t, string(state.EventRunStarted), lb.Msg.EventLog[0].Type)
	for _, e := range lb.Msg.EventLog {
		assert.NotEqual(t, string(state.EventRunTaskQueued), e.Type, "only what a replay reads")
	}
	_, err = c.GetEventLog(ctx, connect.NewRequest(&mokshav1.GetEventLogRequest{RunId: "nope"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestCancelResumeAndChildren(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	c := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"x"}}))
	res, err := c.StartRun(ctx, connect.NewRequest(&mokshav1.StartRunRequest{Tenant: "t", Name: "x"}))
	require.NoError(t, err)
	id := res.Msg.RunId

	_, err = c.ResumeRun(ctx, connect.NewRequest(&mokshav1.ResumeRunRequest{RunId: id}))
	require.NoError(t, err, "not stuck: a no-op")
	_, err = c.CancelRun(ctx, connect.NewRequest(&mokshav1.CancelRunRequest{RunId: id, By: "ana"}))
	require.NoError(t, err)
	got, err := s.store.GetRun(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.RunCancelled, got.Status)
	assert.Equal(t, "cancelled by ana", got.Message)
	_, err = c.CancelRun(ctx, connect.NewRequest(&mokshav1.CancelRunRequest{RunId: "nope"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))

	put := func(h state.Run) {
		_, err := s.store.Append(ctx, s.store.Term(), nil, func(tx *state.Tx) error { return tx.PutRun(h) })
		require.NoError(t, err)
	}
	put(state.Run{ID: "zc1", Tenant: "t", Name: "x", Queue: "default", Trigger: "run " + id, Status: state.RunRunning, ParentID: id, CreatedAt: time.Now()})
	put(state.Run{ID: "zc2", Tenant: "t", Name: "x", Queue: "default", Trigger: "manual", Status: state.RunRunning, CreatedAt: time.Now()})
	kids, err := c.ListRuns(ctx, connect.NewRequest(&mokshav1.ListRunsRequest{ParentId: id}))
	require.NoError(t, err)
	require.Len(t, kids.Msg.Runs, 1)
	assert.Equal(t, "zc1", kids.Msg.Runs[0].Id)
	older, err := c.ListRuns(ctx, connect.NewRequest(&mokshav1.ListRunsRequest{BeforeId: "zc2"}))
	require.NoError(t, err)
	assert.NotContains(t, []string{older.Msg.Runs[0].Id}, "zc2", "paging: older than zc2")
}

func TestListCode(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewRunHandler(s.core, s.store)
	mux.Handle(path, h)
	c := mokshav1connect.NewRunServiceClient(http.DefaultClient, httptestServer(t, mux))
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily"}, Steps: map[string]string{"load": "py-1"}}))
	res, err := c.ListCode(ctx, connect.NewRequest(&mokshav1.ListCodeRequest{}))
	require.NoError(t, err)
	require.Len(t, res.Msg.Steps, 1)
	assert.Equal(t, []string{"load", "py-1", "b1"}, []string{res.Msg.Steps[0].Name, res.Msg.Steps[0].CodeVersion, res.Msg.Steps[0].BuildId})
	require.Len(t, res.Msg.Pipelines, 1)
	assert.Equal(t, "daily", res.Msg.Pipelines[0].Name)
}
