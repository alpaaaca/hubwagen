package api

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
)

// DeployServer implements DeployService: projects (docs/projects.md).
type DeployServer struct {
	core *core.Core
}

// NewDeployHandler returns the path and handler to mount DeployService at.
func NewDeployHandler(c *core.Core) (string, http.Handler) {
	return hubwagenv1connect.NewDeployServiceHandler(&DeployServer{core: c})
}

var _ hubwagenv1connect.DeployServiceHandler = (*DeployServer)(nil)

func (s *DeployServer) Deploy(ctx context.Context, req *connect.Request[hubwagenv1.DeployRequest]) (*connect.Response[hubwagenv1.DeployResponse], error) {
	m := req.Msg
	r := core.DeployRequest{Project: m.Project, Build: buildRequest(m.Build), Image: m.Image, Dir: m.Dir, Python: m.Python,
		Env: m.Env, By: m.By}
	for _, p := range m.Pools {
		r.Pools = append(r.Pools, state.DeployedPool{Name: p.Name, Docks: p.Docks, Min: int(p.Min), Max: int(p.Max),
			Lifts: int(p.Lifts), IdleAfter: p.IdleAfter.AsDuration()})
	}
	d, err := s.core.Deploy(ctx, r)
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.DeployResponse{Deployment: deploymentMessage(state.Project{Deployment: d})}), nil
}

func (s *DeployServer) ListProjects(ctx context.Context, req *connect.Request[hubwagenv1.ListProjectsRequest]) (*connect.Response[hubwagenv1.ListProjectsResponse], error) {
	ps, err := s.core.Projects(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListProjectsResponse{}
	for _, p := range ps {
		if p.DeletedAt != nil && !req.Msg.WithDeleted {
			continue
		}
		out.Projects = append(out.Projects, deploymentMessage(p))
	}
	return connect.NewResponse(out), nil
}

func (s *DeployServer) DeleteProject(ctx context.Context, req *connect.Request[hubwagenv1.DeleteProjectRequest]) (*connect.Response[hubwagenv1.DeleteProjectResponse], error) {
	by := req.Msg.By
	if by == "" {
		by = "unknown"
	}
	n, err := s.core.DeleteProject(ctx, req.Msg.Project, by, req.Msg.CancelRuns)
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.DeleteProjectResponse{CancelledRuns: int32(n)}), nil
}

func deploymentMessage(p state.Project) *hubwagenv1.Deployment {
	m := &hubwagenv1.Deployment{Project: p.Project, Version: int32(p.Version), BuildId: p.BuildID, Image: p.Image, Dir: p.Dir,
		Pipelines: p.Hauls, By: p.By, DeployedAt: timestamppb.New(p.DeployedAt)}
	for _, pool := range p.Pools {
		m.Pools = append(m.Pools, &hubwagenv1.DeployedPool{Name: pool.Name, Docks: pool.Docks, Min: int32(pool.Min),
			Max: int32(pool.Max), Lifts: int32(pool.Lifts), IdleAfter: durationpb.New(pool.IdleAfter)})
	}
	if p.DeletedAt != nil {
		m.DeletedAt = timestamppb.New(*p.DeletedAt)
	}
	return m
}
