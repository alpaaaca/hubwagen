package api

import (
	"net/http"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/layout"
	"github.com/alper/moksha/internal/state"
)

func TestCatalogService(t *testing.T) {
	s := start(t)
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Pipelines: []string{"daily_revenue"}}))
	res, err := s.core.StartRun(ctx, core.StartRunRequest{Tenant: "default", Name: "daily_revenue", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"extract", "build"}, s.work(t, res.RunID))

	mux := http.NewServeMux()
	path, h := NewCatalogHandler(s.core, s.store, dataversion.NewStore(s.bucket, layout.New(s.bucket.Base())))
	mux.Handle(path, h)
	c := mokshav1connect.NewCatalogServiceClient(http.DefaultClient, httptestServer(t, mux))

	dss, err := c.ListDatasets(ctx, connect.NewRequest(&mokshav1.ListDatasetsRequest{}))
	require.NoError(t, err)
	var names []string
	for _, d := range dss.Msg.Datasets {
		names = append(names, d.Name)
	}
	assert.ElementsMatch(t, []string{"raw.orders", "mart.revenue"}, names)

	parts, err := c.ListPartitions(ctx, connect.NewRequest(&mokshav1.ListPartitionsRequest{Dataset: "mart.revenue"}))
	require.NoError(t, err)
	require.Len(t, parts.Msg.Partitions, 1)
	built := parts.Msg.Partitions[0].Latest
	assert.Equal(t, int32(1), parts.Msg.Partitions[0].DataVersions)
	assert.Equal(t, state.DataVersionCommitted, built.State)
	assert.Equal(t, res.RunID, built.RunId)

	got, err := c.GetDataVersion(ctx, connect.NewRequest(&mokshav1.GetDataVersionRequest{Id: built.Id}))
	require.NoError(t, err)
	assert.Equal(t, "build", got.Msg.Step)
	require.NotNil(t, got.Msg.Manifest, got.Msg.ManifestError)
	assert.Equal(t, "part-0.csv", got.Msg.Manifest.Files[0].Path)
	assert.JSONEq(t, `{"rows":1}`, got.Msg.Manifest.ResultJson)
	require.Len(t, got.Msg.Upstream.DataVersions, 2, "itself and raw.orders")
	require.Len(t, got.Msg.Upstream.Edges, 1)
	assert.Equal(t, "orders", got.Msg.Upstream.Edges[0].Name)
	raw := got.Msg.Upstream.Edges[0].InputId
	assert.Len(t, got.Msg.Downstream.DataVersions, 1, "nothing was built from it")

	up, err := c.GetDataVersion(ctx, connect.NewRequest(&mokshav1.GetDataVersionRequest{Id: raw}))
	require.NoError(t, err)
	assert.Len(t, up.Msg.Downstream.DataVersions, 2, "mart.revenue was built from it")

	ps, err := c.ListDataVersions(ctx, connect.NewRequest(&mokshav1.ListDataVersionsRequest{Dataset: "raw.orders", States: []string{"committed"}}))
	require.NoError(t, err)
	assert.Len(t, ps.Msg.DataVersions, 1)

	_, err = c.GetDataVersion(ctx, connect.NewRequest(&mokshav1.GetDataVersionRequest{Id: "nope"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	_, err = c.ListDataVersions(ctx, connect.NewRequest(&mokshav1.ListDataVersionsRequest{Limit: 5000}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	noManifests := http.NewServeMux()
	path, h = NewCatalogHandler(nil, s.store, nil)
	noManifests.Handle(path, h)
	got, err = mokshav1connect.NewCatalogServiceClient(http.DefaultClient, httptestServer(t, noManifests)).
		GetDataVersion(ctx, connect.NewRequest(&mokshav1.GetDataVersionRequest{Id: built.Id}))
	require.NoError(t, err)
	assert.Nil(t, got.Msg.Manifest)
	assert.NotEmpty(t, got.Msg.ManifestError)
	_, err = mokshav1connect.NewCatalogServiceClient(http.DefaultClient, httptestServer(t, noManifests)).
		InvalidateDataVersion(ctx, connect.NewRequest(&mokshav1.InvalidateDataVersionRequest{Id: built.Id, Reason: "x"}))
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err), "read only")

	// Invalidating the raw data version reaches what was built from it.
	dry, err := c.InvalidateDataVersion(ctx, connect.NewRequest(&mokshav1.InvalidateDataVersionRequest{Id: raw, Reason: "bad", DryRun: true}))
	require.NoError(t, err)
	assert.Len(t, dry.Msg.DataVersions, 2)
	inv, err := c.InvalidateDataVersion(ctx, connect.NewRequest(&mokshav1.InvalidateDataVersionRequest{Id: raw, Reason: "bad", By: "test"}))
	require.NoError(t, err)
	require.Len(t, inv.Msg.Runs, 1)
	assert.Equal(t, res.RunID, inv.Msg.Runs[0].Id)
	assert.JSONEq(t, `{"day":"2026-10-24"}`, inv.Msg.Runs[0].ParamsJson)
	ps, err = c.ListDataVersions(ctx, connect.NewRequest(&mokshav1.ListDataVersionsRequest{Dataset: "mart.revenue", States: []string{"invalid"}}))
	require.NoError(t, err)
	assert.Len(t, ps.Msg.DataVersions, 1)
	_, err = c.InvalidateDataVersion(ctx, connect.NewRequest(&mokshav1.InvalidateDataVersionRequest{Id: raw}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a reason is required")
}
