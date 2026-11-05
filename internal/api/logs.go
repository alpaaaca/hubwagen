package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/runlog"
	"github.com/alper/moksha/internal/state"
)

// LogServer serves LogService: lift output, kept apart from the state.
type LogServer struct {
	store *state.Store
	logs  *runlog.Store
}

// NewLogHandler returns the path and handler to mount LogService at.
func NewLogHandler(st *state.Store, logs *runlog.Store) (string, http.Handler) {
	return mokshav1connect.NewLogServiceHandler(&LogServer{store: st, logs: logs})
}

var _ mokshav1connect.LogServiceHandler = (*LogServer)(nil)

const (
	defaultLogLimit = 2000
	maxLogLimit     = 10000
)

func (s *LogServer) AppendLogs(ctx context.Context, req *connect.Request[mokshav1.AppendLogsRequest]) (*connect.Response[mokshav1.AppendLogsResponse], error) {
	a, err := s.store.GetLiftAttempt(ctx, req.Msg.AttemptId)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("lift attempt %s not found", req.Msg.AttemptId))
	}
	if err != nil {
		return nil, connectError(err)
	}
	lines := make([]runlog.Line, 0, len(req.Msg.Lines))
	for _, l := range req.Msg.Lines {
		lines = append(lines, runlog.Line{Time: l.Time.AsTime(), Stream: l.Stream, Level: l.Level, Logger: l.Logger, Text: l.Text})
	}
	if err := s.logs.Append(runlog.Attempt{HaulID: a.HaulID, Seq: a.Seq, Number: a.Number}, lines); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.AppendLogsResponse{}), nil
}

func (s *LogServer) GetLogs(ctx context.Context, req *connect.Request[mokshav1.GetLogsRequest]) (*connect.Response[mokshav1.GetLogsResponse], error) {
	m := req.Msg
	limit := int(m.Limit)
	switch {
	case limit > maxLogLimit:
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit %d: at most %d", limit, maxLogLimit))
	case limit <= 0:
		limit = defaultLogLimit
	}
	if _, err := s.store.GetHaul(ctx, m.HaulId); errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("haul %s not found", m.HaulId))
	} else if err != nil {
		return nil, connectError(err)
	}
	lifts, err := s.store.ListLifts(ctx, m.HaulId)
	if err != nil {
		return nil, connectError(err)
	}
	names := map[int]string{}
	for _, l := range lifts {
		names[l.Seq] = l.Name
	}
	entries, skipped, err := s.logs.Read(m.HaulId, int(m.Seq), limit)
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.GetLogsResponse{Skipped: int32(skipped)}
	for _, e := range entries {
		out.Lines = append(out.Lines, &mokshav1.RunLogLine{Seq: int32(e.Seq), Lift: names[e.Seq], Attempt: int32(e.Number),
			Line: &mokshav1.LogLine{Time: timestamppb.New(e.Time), Stream: e.Stream, Level: e.Level, Logger: e.Logger, Text: e.Text}})
	}
	return connect.NewResponse(out), nil
}
