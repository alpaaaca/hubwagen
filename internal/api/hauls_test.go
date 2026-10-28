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
	"github.com/alper/hubwagen/internal/state"
)

func TestHaulService(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewHaulHandler(s.core, s.store)
	mux.Handle(path, h)
	srv := httptestServer(t, mux)
	c := hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, srv)

	_, err := c.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{Tenant: "t", Name: "daily_revenue"}))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "no build defines it yet")
	assert.ErrorContains(t, err, `no registered build defines haul "daily_revenue"`)
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily_revenue", "x"}}))

	res, err := c.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{
		Tenant: "t", Name: "daily_revenue", ParamsJson: `{"day":"2026-10-24","n":10000000000000001}`}))
	require.NoError(t, err)
	got, err := c.GetHaul(ctx, connect.NewRequest(&hubwagenv1.GetHaulRequest{HaulId: res.Msg.HaulId}))
	require.NoError(t, err)
	hl := got.Msg.Haul
	assert.Equal(t, state.HaulRunning, hl.Status)
	assert.Equal(t, "daily_revenue", hl.Name)
	assert.Contains(t, hl.ParamsJson, `"n":10000000000000001`, "numbers stay exact")
	assert.NotNil(t, hl.CreatedAt)
	assert.Nil(t, hl.FinishedAt)

	_, err = c.GetHaul(ctx, connect.NewRequest(&hubwagenv1.GetHaulRequest{HaulId: "nope"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	_, err = c.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{Tenant: "t", Name: "x", ParamsJson: "[1]"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "params must be an object")
	_, err = c.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{Name: "x"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "no tenant")
}

func TestSignalHaul(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewHaulHandler(s.core, s.store)
	mux.Handle(path, h)
	c := hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, httptestServer(t, mux))
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"x"}}))
	res, err := c.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{Tenant: "t", Name: "x"}))
	require.NoError(t, err)
	id := res.Msg.HaulId

	sig, err := c.SignalHaul(ctx, connect.NewRequest(&hubwagenv1.SignalHaulRequest{HaulId: id, Name: "approve", PayloadJson: `{"by":"ana"}`}))
	require.NoError(t, err)
	assert.NotEmpty(t, sig.Msg.SignalId)
	evs, err := s.store.HaulEvents(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, state.EventSignalReceived, evs[len(evs)-1].Type)

	_, err = c.SignalHaul(ctx, connect.NewRequest(&hubwagenv1.SignalHaulRequest{HaulId: "nope", Name: "approve"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	_, err = c.SignalHaul(ctx, connect.NewRequest(&hubwagenv1.SignalHaulRequest{HaulId: id, Name: "approve", PayloadJson: "{"}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	require.NoError(t, s.core.CancelHaul(ctx, id, "test"))
	_, err = c.SignalHaul(ctx, connect.NewRequest(&hubwagenv1.SignalHaulRequest{HaulId: id, Name: "approve"}))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "the haul is finished")
}
