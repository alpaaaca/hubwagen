package api

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
)

func TestDeployService(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewDeployHandler(s.core, func(map[string]string) error { return nil })
	mux.Handle(path, h)
	path, h = NewHaulHandler(s.core, s.store)
	mux.Handle(path, h)
	srv := httptestServer(t, mux)
	c := hubwagenv1connect.NewDeployServiceClient(http.DefaultClient, srv)
	hauls := hubwagenv1connect.NewHaulServiceClient(http.DefaultClient, srv)

	req := &hubwagenv1.DeployRequest{Project: "revenue", Image: "acme/revenue:b1", By: "ana",
		Build: &hubwagenv1.RegisterBuildRequest{BuildId: "b1", Hauls: []string{"daily"}, LiftVersions: map[string]string{"load": "v1"},
			Schedules: []*hubwagenv1.HaulSchedule{{Haul: "daily", Cron: "0 3 * * *", Interval: "daily"}}},
		Pools: []*hubwagenv1.DeployedPool{{Name: "default", Docks: []string{"default"}, Min: 0, Max: 3, Lifts: 2, IdleAfter: durationpb.New(5 * time.Minute)}}}
	res, err := c.Deploy(ctx, connect.NewRequest(req))
	require.NoError(t, err)
	d := res.Msg.Deployment
	assert.Equal(t, []any{"revenue", int32(1), "b1", []string{"daily"}}, []any{d.Project, d.Version, d.BuildId, d.Pipelines})

	// The build came with it: its pipeline starts, its schedule is in place.
	_, err = hauls.StartHaul(ctx, connect.NewRequest(&hubwagenv1.StartHaulRequest{Tenant: "t", Name: "daily"}))
	require.NoError(t, err)
	scheds, err := hauls.ListSchedules(ctx, connect.NewRequest(&hubwagenv1.ListSchedulesRequest{}))
	require.NoError(t, err)
	require.Len(t, scheds.Msg.Schedules, 1)

	req.Project = "other"
	_, err = c.Deploy(ctx, connect.NewRequest(req))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))

	_, err = c.DeleteProject(ctx, connect.NewRequest(&hubwagenv1.DeleteProjectRequest{Project: "revenue"}))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err), "an open run")
	del, err := c.DeleteProject(ctx, connect.NewRequest(&hubwagenv1.DeleteProjectRequest{Project: "revenue", CancelRuns: true, By: "ana"}))
	require.NoError(t, err)
	assert.Equal(t, int32(1), del.Msg.CancelledRuns)

	list, err := c.ListProjects(ctx, connect.NewRequest(&hubwagenv1.ListProjectsRequest{}))
	require.NoError(t, err)
	assert.Empty(t, list.Msg.Projects)
	list, err = c.ListProjects(ctx, connect.NewRequest(&hubwagenv1.ListProjectsRequest{WithDeleted: true}))
	require.NoError(t, err)
	require.Len(t, list.Msg.Projects, 1)
	assert.NotNil(t, list.Msg.Projects[0].DeletedAt)
}

func TestDeployChecksSecrets(t *testing.T) {
	s := start(t)
	mux := http.NewServeMux()
	path, h := NewDeployHandler(s.core, func(names map[string]string) error {
		if names["DB"] != "revenue-db" {
			return fmt.Errorf("secret %s: no such secret on this server", names["DB"])
		}
		return nil
	})
	mux.Handle(path, h)
	c := hubwagenv1connect.NewDeployServiceClient(http.DefaultClient, httptestServer(t, mux))
	req := &hubwagenv1.DeployRequest{Project: "revenue", Image: "acme/revenue:b1", Secrets: map[string]string{"DB": "nope"},
		Build: &hubwagenv1.RegisterBuildRequest{BuildId: "b1", Hauls: []string{"daily"}},
		Pools: []*hubwagenv1.DeployedPool{{Name: "default", Docks: []string{"default"}, Max: 1, Lifts: 1}}}
	_, err := c.Deploy(ctx, connect.NewRequest(req))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	assert.ErrorContains(t, err, "secret nope: no such secret on this server")
	req.Secrets["DB"] = "revenue-db"
	res, err := c.Deploy(ctx, connect.NewRequest(req))
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"DB": "revenue-db"}, res.Msg.Deployment.Secrets)
}
