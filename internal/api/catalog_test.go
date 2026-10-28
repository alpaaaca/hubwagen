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
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/state"
)

func TestCatalogService(t *testing.T) {
	s := start(t)
	require.NoError(t, s.core.RegisterBuild(ctx, core.BuildRequest{ID: "b1", Hauls: []string{"daily_revenue"}}))
	res, err := s.core.StartHaul(ctx, core.StartHaulRequest{Tenant: "default", Name: "daily_revenue", Params: map[string]any{"day": "2026-10-24"}})
	require.NoError(t, err)
	assert.Equal(t, []string{"extract", "build"}, s.work(t, res.HaulID))

	mux := http.NewServeMux()
	path, h := NewCatalogHandler(s.core, s.store, pallet.NewStore(s.bucket, layout.New(s.bucket.Base())))
	mux.Handle(path, h)
	c := hubwagenv1connect.NewCatalogServiceClient(http.DefaultClient, httptestServer(t, mux))

	dss, err := c.ListDatasets(ctx, connect.NewRequest(&hubwagenv1.ListDatasetsRequest{}))
	require.NoError(t, err)
	var names []string
	for _, d := range dss.Msg.Datasets {
		names = append(names, d.Name)
	}
	assert.ElementsMatch(t, []string{"raw.orders", "mart.revenue"}, names)

	parts, err := c.ListPartitions(ctx, connect.NewRequest(&hubwagenv1.ListPartitionsRequest{Dataset: "mart.revenue"}))
	require.NoError(t, err)
	require.Len(t, parts.Msg.Partitions, 1)
	built := parts.Msg.Partitions[0].Latest
	assert.Equal(t, int32(1), parts.Msg.Partitions[0].Pallets)
	assert.Equal(t, state.PalletCommitted, built.State)
	assert.Equal(t, res.HaulID, built.HaulId)

	got, err := c.GetPallet(ctx, connect.NewRequest(&hubwagenv1.GetPalletRequest{Id: built.Id}))
	require.NoError(t, err)
	assert.Equal(t, "build", got.Msg.Lift)
	require.NotNil(t, got.Msg.Manifest, got.Msg.ManifestError)
	assert.Equal(t, "part-0.csv", got.Msg.Manifest.Files[0].Path)
	assert.JSONEq(t, `{"rows":1}`, got.Msg.Manifest.ResultJson)
	require.Len(t, got.Msg.Upstream.Pallets, 2, "itself and raw.orders")
	require.Len(t, got.Msg.Upstream.Edges, 1)
	assert.Equal(t, "orders", got.Msg.Upstream.Edges[0].Name)
	raw := got.Msg.Upstream.Edges[0].InputId
	assert.Len(t, got.Msg.Downstream.Pallets, 1, "nothing was built from it")

	up, err := c.GetPallet(ctx, connect.NewRequest(&hubwagenv1.GetPalletRequest{Id: raw}))
	require.NoError(t, err)
	assert.Len(t, up.Msg.Downstream.Pallets, 2, "mart.revenue was built from it")

	ps, err := c.ListPallets(ctx, connect.NewRequest(&hubwagenv1.ListPalletsRequest{Dataset: "raw.orders", States: []string{"committed"}}))
	require.NoError(t, err)
	assert.Len(t, ps.Msg.Pallets, 1)

	_, err = c.GetPallet(ctx, connect.NewRequest(&hubwagenv1.GetPalletRequest{Id: "nope"}))
	assert.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	_, err = c.ListPallets(ctx, connect.NewRequest(&hubwagenv1.ListPalletsRequest{Limit: 5000}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))

	noManifests := http.NewServeMux()
	path, h = NewCatalogHandler(nil, s.store, nil)
	noManifests.Handle(path, h)
	got, err = hubwagenv1connect.NewCatalogServiceClient(http.DefaultClient, httptestServer(t, noManifests)).
		GetPallet(ctx, connect.NewRequest(&hubwagenv1.GetPalletRequest{Id: built.Id}))
	require.NoError(t, err)
	assert.Nil(t, got.Msg.Manifest)
	assert.NotEmpty(t, got.Msg.ManifestError)
	_, err = hubwagenv1connect.NewCatalogServiceClient(http.DefaultClient, httptestServer(t, noManifests)).
		InvalidatePallet(ctx, connect.NewRequest(&hubwagenv1.InvalidatePalletRequest{Id: built.Id, Reason: "x"}))
	assert.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err), "read only")

	// Invalidating the raw pallet reaches what was built from it.
	dry, err := c.InvalidatePallet(ctx, connect.NewRequest(&hubwagenv1.InvalidatePalletRequest{Id: raw, Reason: "bad", DryRun: true}))
	require.NoError(t, err)
	assert.Len(t, dry.Msg.Pallets, 2)
	inv, err := c.InvalidatePallet(ctx, connect.NewRequest(&hubwagenv1.InvalidatePalletRequest{Id: raw, Reason: "bad", By: "test"}))
	require.NoError(t, err)
	require.Len(t, inv.Msg.Hauls, 1)
	assert.Equal(t, res.HaulID, inv.Msg.Hauls[0].Id)
	assert.JSONEq(t, `{"day":"2026-10-24"}`, inv.Msg.Hauls[0].ParamsJson)
	ps, err = c.ListPallets(ctx, connect.NewRequest(&hubwagenv1.ListPalletsRequest{Dataset: "mart.revenue", States: []string{"invalid"}}))
	require.NoError(t, err)
	assert.Len(t, ps.Msg.Pallets, 1)
	_, err = c.InvalidatePallet(ctx, connect.NewRequest(&hubwagenv1.InvalidatePalletRequest{Id: raw}))
	assert.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err), "a reason is required")
}
