package api

import (
	"context"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/core"
)

// WaitingText tells why a run's work waits, in plain words (pipeline, step,
// not haul, lift), with what to do about it. It starts lowercase: callers
// put "waiting: " or the like in front.
func WaitingText(w core.Waiting) string {
	what := "the pipeline's code"
	if w.Kind == core.TaskLift {
		what = fmt.Sprintf("step %s", w.Name)
	}
	dockFlag, onDock := "", ""
	if w.Dock != core.DefaultDock {
		dockFlag, onDock = fmt.Sprintf(" --dock %s", w.Dock), fmt.Sprintf(" on dock %q", w.Dock)
	}
	switch w.Reason {
	case core.WaitNoWorker:
		return fmt.Sprintf("no worker is connected%s to run %s. Start one with `python -m hubwagen.worker%s <modules>` "+
			"(or `hubwagen workers --config hubwagen.yaml`)", onDock, what, dockFlag)
	case core.WaitNoBuild:
		defines := fmt.Sprintf("pipeline %s", w.Name)
		if w.Kind == core.TaskLift {
			defines = fmt.Sprintf("step %s", w.Name)
		}
		return fmt.Sprintf("%s connected%s, but %s code doesn't define %s (build %s). "+
			"Restart a worker with code that has it", workersAre(w.Workers), onDock, theirIts(w.Workers), defines,
			strings.Join(shortBuilds(w.Builds), ", "))
	case core.WaitStarting:
		return fmt.Sprintf("a worker is starting in pool %s to run %s", w.Pool, what)
	case core.WaitDockLimit:
		return fmt.Sprintf("%s waits for a free slot: dock %q runs as many steps at once as it may", what, w.Dock)
	default:
		return fmt.Sprintf("%s waits for a free worker: the workers that can run it are busy", what)
	}
}

func workersAre(n int) string {
	if n == 1 {
		return "1 worker is"
	}
	return fmt.Sprintf("%d workers are", n)
}

func theirIts(n int) string {
	if n == 1 {
		return "its"
	}
	return "their"
}

func shortBuilds(builds []string) []string {
	out := make([]string, len(builds))
	for i, b := range builds {
		switch {
		case b == "":
			out[i] = "none"
		case len(b) > 12:
			out[i] = b[:12]
		default:
			out[i] = b
		}
	}
	return out
}

func waitingMessage(w core.Waiting) *hubwagenv1.Waiting {
	return &hubwagenv1.Waiting{Reason: string(w.Reason), Message: WaitingText(w), Kind: string(w.Kind), Name: w.Name,
		Seq: int32(w.Seq), Dock: w.Dock, Since: timestamppb.New(w.Since), Workers: int32(w.Workers), Builds: w.Builds, Pool: w.Pool}
}

// addWaiting sets why the running ones among hauls wait, if they do.
func (s *HaulServer) addWaiting(ctx context.Context, hauls ...*hubwagenv1.Haul) error {
	var ids []string
	for _, h := range hauls {
		if h.Status == "running" {
			ids = append(ids, h.Id)
		}
	}
	if len(ids) == 0 {
		return nil
	}
	ws, err := s.core.Waiting(ctx, ids)
	if err != nil {
		return err
	}
	for _, h := range hauls {
		if w, ok := ws[h.Id]; ok {
			h.Waiting = waitingMessage(w)
		}
	}
	return nil
}

func (s *HaulServer) ListWorkers(ctx context.Context, _ *connect.Request[hubwagenv1.ListWorkersRequest]) (*connect.Response[hubwagenv1.ListWorkersResponse], error) {
	ws, err := s.core.Workers(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListWorkersResponse{}
	for _, w := range ws {
		m := &hubwagenv1.Worker{Id: w.ID, BuildId: w.BuildID, Docks: w.Docks, LastSeen: timestamppb.New(w.LastSeen), Polling: w.Polling,
			Running: int32(w.Running)}
		for _, k := range w.Kinds {
			m.Kinds = append(m.Kinds, string(k))
		}
		out.Workers = append(out.Workers, m)
	}
	return connect.NewResponse(out), nil
}

func (s *HaulServer) ListPools(ctx context.Context, _ *connect.Request[hubwagenv1.ListPoolsRequest]) (*connect.Response[hubwagenv1.ListPoolsResponse], error) {
	ps, err := s.core.Pools(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListPoolsResponse{}
	for _, p := range ps {
		m := &hubwagenv1.Pool{Name: p.Name, Docks: p.Docks, Min: int32(p.Min), Max: int32(p.Max), Workers: p.Workers,
			Starting: int32(p.Starting), Decision: p.Decision}
		if !p.DecidedAt.IsZero() {
			m.DecidedAt = timestamppb.New(p.DecidedAt)
		}
		out.Pools = append(out.Pools, m)
	}
	return connect.NewResponse(out), nil
}
