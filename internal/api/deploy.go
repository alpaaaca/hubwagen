package api

import (
	"context"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/state"
)

// DeployServer implements DeployService: projects (docs/projects.md).
type DeployServer struct {
	core  *core.Core
	check func(*mokshav1.DeployRequest) error
}

// NewDeployHandler returns the path and handler to mount DeployService at.
// check says whether this server can run a deployment: that it holds the
// secrets it names, that it can run its workers in place or needs an
// image.
func NewDeployHandler(c *core.Core, check func(*mokshav1.DeployRequest) error) (string, http.Handler) {
	return mokshav1connect.NewDeployServiceHandler(&DeployServer{core: c, check: check})
}

var _ mokshav1connect.DeployServiceHandler = (*DeployServer)(nil)

func (s *DeployServer) Deploy(ctx context.Context, req *connect.Request[mokshav1.DeployRequest]) (*connect.Response[mokshav1.DeployResponse], error) {
	m := req.Msg
	if err := s.check(m); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	r := core.DeployRequest{Project: m.Project, Build: buildRequest(m.Build), Image: m.Image, Dir: m.Dir, Python: m.Python,
		Env: m.Env, Secrets: m.Secrets, By: m.By}
	for _, p := range m.Pools {
		r.Pools = append(r.Pools, state.DeployedPool{Name: p.Name, Queues: p.Queues, Min: int(p.Min), Max: int(p.Max),
			Steps: int(p.Steps), IdleAfter: p.IdleAfter.AsDuration()})
	}
	d, err := s.core.Deploy(ctx, r)
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.DeployResponse{Deployment: deploymentMessage(state.Project{Deployment: d})}), nil
}

func (s *DeployServer) ListProjects(ctx context.Context, req *connect.Request[mokshav1.ListProjectsRequest]) (*connect.Response[mokshav1.ListProjectsResponse], error) {
	ps, err := s.core.Projects(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.ListProjectsResponse{}
	for _, p := range ps {
		if p.DeletedAt != nil && !req.Msg.WithDeleted {
			continue
		}
		out.Projects = append(out.Projects, deploymentMessage(p))
	}
	return connect.NewResponse(out), nil
}

func (s *DeployServer) DeleteProject(ctx context.Context, req *connect.Request[mokshav1.DeleteProjectRequest]) (*connect.Response[mokshav1.DeleteProjectResponse], error) {
	by := req.Msg.By
	if by == "" {
		by = "unknown"
	}
	n, err := s.core.DeleteProject(ctx, req.Msg.Project, by, req.Msg.CancelRuns)
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.DeleteProjectResponse{CancelledRuns: int32(n)}), nil
}

func deploymentMessage(p state.Project) *mokshav1.Deployment {
	m := &mokshav1.Deployment{Project: p.Project, Version: int32(p.Version), BuildId: p.BuildID, Image: p.Image, Dir: p.Dir,
		Pipelines: p.Pipelines, By: p.By, DeployedAt: timestamppb.New(p.DeployedAt), Secrets: p.Secrets}
	for _, pool := range p.Pools {
		m.Pools = append(m.Pools, &mokshav1.DeployedPool{Name: pool.Name, Queues: pool.Queues, Min: int32(pool.Min),
			Max: int32(pool.Max), Steps: int32(pool.Steps), IdleAfter: durationpb.New(pool.IdleAfter)})
	}
	if p.DeletedAt != nil {
		m.DeletedAt = timestamppb.New(*p.DeletedAt)
	}
	return m
}
