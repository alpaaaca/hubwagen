package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/state"
)

// DefaultMaxWait caps how long a poll may wait for work.
const DefaultMaxWait = 30 * time.Second

// WorkerServer implements WorkerService on top of the core.
type WorkerServer struct {
	core    *core.Core
	maxWait time.Duration
}

// NewWorkerHandler returns the path and handler to mount WorkerService at.
// maxWait caps long polls (0: DefaultMaxWait).
func NewWorkerHandler(c *core.Core, maxWait time.Duration) (string, http.Handler) {
	if maxWait <= 0 {
		maxWait = DefaultMaxWait
	}
	return hubwagenv1connect.NewWorkerServiceHandler(&WorkerServer{core: c, maxWait: maxWait})
}

var _ hubwagenv1connect.WorkerServiceHandler = (*WorkerServer)(nil)

func (s *WorkerServer) RegisterBuild(ctx context.Context, req *connect.Request[hubwagenv1.RegisterBuildRequest]) (*connect.Response[hubwagenv1.RegisterBuildResponse], error) {
	err := s.core.RegisterBuild(ctx, core.BuildRequest{ID: req.Msg.BuildId, Hauls: req.Msg.Hauls, Lifts: req.Msg.LiftVersions})
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.RegisterBuildResponse{}), nil
}

func (s *WorkerServer) poll(ctx context.Context, worker, build string, docks []string, wait time.Duration, kind core.TaskKind) (core.Task, bool, error) {
	wait = min(max(wait, 0), s.maxWait)
	t, ok, err := s.core.Poll(ctx, core.PollRequest{WorkerID: worker, BuildID: build, Docks: docks, Kinds: []core.TaskKind{kind}, Wait: wait})
	if err != nil {
		return t, false, connectError(err)
	}
	return t, ok, nil
}

func (s *WorkerServer) PollHaulTask(ctx context.Context, req *connect.Request[hubwagenv1.PollHaulTaskRequest]) (*connect.Response[hubwagenv1.PollHaulTaskResponse], error) {
	m := req.Msg
	t, ok, err := s.poll(ctx, m.WorkerId, m.BuildId, m.Docks, m.Wait.AsDuration(), core.TaskHaul)
	if err != nil || !ok {
		return connect.NewResponse(&hubwagenv1.PollHaulTaskResponse{}), err
	}
	out := &hubwagenv1.HaulTask{Id: t.ID, HaulId: t.HaulID, Name: t.Name, Dock: t.Dock}
	for _, e := range t.Logbook {
		if !replayEvents[e.Type] {
			continue
		}
		out.Logbook = append(out.Logbook, &hubwagenv1.Event{
			Seq: e.Seq, Type: string(e.Type), Time: timestamppb.New(e.Time), PayloadJson: string(e.Payload)})
		if e.Type == state.EventHaulStarted {
			var p struct {
				Params json.RawMessage `json:"params"`
			}
			if json.Unmarshal(e.Payload, &p) == nil && len(p.Params) > 0 {
				out.ParamsJson = string(p.Params)
			}
		}
	}
	return connect.NewResponse(&hubwagenv1.PollHaulTaskResponse{Task: out}), nil
}

// replayEvents are the logbook events a replay reads; the rest is
// bookkeeping a worker doesn't need (spike S1: a 1,000-lift logbook shrinks
// about fivefold).
var replayEvents = map[state.EventType]bool{
	state.EventHaulStarted:       true,
	state.EventLiftFinished:      true,
	state.EventTimerFired:        true,
	state.EventQueryAnswered:     true,
	state.EventSignalDelivered:   true,
	state.EventHaulTaskCompleted: true,
}

func (s *WorkerServer) CompleteHaulTask(ctx context.Context, req *connect.Request[hubwagenv1.CompleteHaulTaskRequest]) (*connect.Response[hubwagenv1.CompleteHaulTaskResponse], error) {
	m := req.Msg
	res := core.HaulTaskResult{TaskID: m.TaskId, BuildID: m.BuildId}
	for i, c := range m.Commands {
		cmd, err := command(c)
		if err != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("commands[%d]: %w", i, err))
		}
		res.Commands = append(res.Commands, cmd)
	}
	if o := m.Outcome; o != nil {
		result, err := rawJSON("outcome.result_json", o.ResultJson)
		if err != nil {
			return nil, err
		}
		res.Outcome = &core.HaulOutcome{Result: result, Reason: o.Reason, Message: o.Message}
		switch o.Status {
		case hubwagenv1.HaulOutcome_STATUS_COMPLETED:
			res.Outcome.Status = state.HaulCompleted
		case hubwagenv1.HaulOutcome_STATUS_FAILED:
			res.Outcome.Status = state.HaulFailed
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("outcome.status must be COMPLETED or FAILED"))
		}
	}
	if e := m.Error; e != nil {
		res.Error = &core.TaskError{Reason: e.Reason, Message: e.Message}
	}
	if err := s.core.CompleteHaulTask(ctx, res); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.CompleteHaulTaskResponse{}), nil
}

func command(c *hubwagenv1.Command) (core.Command, error) {
	cmd := core.Command{Seq: int(c.Seq), Source: c.Source}
	switch k := c.Kind.(type) {
	case *hubwagenv1.Command_Lift:
		l := k.Lift
		args, err := rawJSON("lift.args_json", l.ArgsJson)
		if err != nil {
			return cmd, err
		}
		cmd.Kind, cmd.Name, cmd.Dock, cmd.Args, cmd.Memo = core.CommandLift, l.Name, l.Dock, args, l.Memo
		if l.Timeout != nil {
			cmd.Timeout = l.Timeout.AsDuration()
		}
		if r := l.Retry; r != nil {
			cmd.Retry = &core.RetryPolicy{Limit: int(r.Limit), Backoff: r.Backoff}
			if r.Initial != nil {
				cmd.Retry.Initial = r.Initial.AsDuration()
			}
			if r.Max != nil {
				cmd.Retry.Max = r.Max.AsDuration()
			}
		}
		if o := l.Output; o != nil {
			cmd.Output = &core.OutputSpec{Dataset: o.Dataset, Partition: o.Partition, PartitionKey: o.PartitionKey}
		}
		for _, in := range l.Inputs {
			cmd.Inputs = append(cmd.Inputs, core.InputRef{Name: in.Name, Pallet: in.Pallet})
		}
	case *hubwagenv1.Command_Sleep:
		cmd.Kind = core.CommandSleep
		if k.Sleep.Duration != nil {
			cmd.Duration = k.Sleep.Duration.AsDuration()
		}
	case *hubwagenv1.Command_LatestPallet:
		cmd.Kind, cmd.Dataset, cmd.Partition = core.CommandLatestPallet, k.LatestPallet.Dataset, k.LatestPallet.Partition
	case *hubwagenv1.Command_WaitSignal:
		cmd.Kind, cmd.Name = core.CommandWaitSignal, k.WaitSignal.Name
		if k.WaitSignal.Timeout != nil {
			cmd.Duration = k.WaitSignal.Timeout.AsDuration()
		}
	default:
		return cmd, errors.New("a command is a lift, a sleep, a latest_pallet or a wait_signal")
	}
	return cmd, nil
}

func (s *WorkerServer) PollLiftTask(ctx context.Context, req *connect.Request[hubwagenv1.PollLiftTaskRequest]) (*connect.Response[hubwagenv1.PollLiftTaskResponse], error) {
	m := req.Msg
	t, ok, err := s.poll(ctx, m.WorkerId, m.BuildId, m.Docks, m.Wait.AsDuration(), core.TaskLift)
	if err != nil || !ok {
		return connect.NewResponse(&hubwagenv1.PollLiftTaskResponse{}), err
	}
	out := &hubwagenv1.LiftTask{
		Id: t.ID, HaulId: t.HaulID, Seq: int32(t.Seq), Name: t.Name, Dock: t.Dock,
		ArgsJson: string(t.Args), Staging: t.Staging,
	}
	if o := t.Output; o != nil {
		out.Output = &hubwagenv1.Output{Dataset: o.Dataset, Partition: o.Partition, PartitionKey: o.PartitionKey}
	}
	for _, in := range t.Inputs {
		out.Inputs = append(out.Inputs, &hubwagenv1.InputRef{Name: in.Name, Pallet: in.Pallet, Location: in.Location})
	}
	return connect.NewResponse(&hubwagenv1.PollLiftTaskResponse{Task: out}), nil
}

func (s *WorkerServer) CompleteLiftTask(ctx context.Context, req *connect.Request[hubwagenv1.CompleteLiftTaskRequest]) (*connect.Response[hubwagenv1.CompleteLiftTaskResponse], error) {
	m := req.Msg
	result, err := rawJSON("result_json", m.ResultJson)
	if err != nil {
		return nil, err
	}
	res := core.LiftResult{AttemptID: m.AttemptId, Result: result, Reason: m.Reason, Message: m.Message, NoRetry: m.NoRetry}
	switch m.Status {
	case hubwagenv1.CompleteLiftTaskRequest_STATUS_SUCCEEDED:
		res.Status = state.AttemptSucceeded
	case hubwagenv1.CompleteLiftTaskRequest_STATUS_FAILED:
		res.Status = state.AttemptFailed
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("status must be SUCCEEDED or FAILED"))
	}
	if o := m.Output; o != nil {
		out := &core.LiftOutput{}
		if out.Stats, err = jsonObject("output.stats_json", o.StatsJson); err != nil {
			return nil, err
		}
		if out.Meta, err = jsonObject("output.meta_json", o.MetaJson); err != nil {
			return nil, err
		}
		for _, f := range o.Files {
			out.Files = append(out.Files, pallet.File{Path: f.Path, Size: f.Size, SHA256: f.Sha256})
		}
		res.Output = out
	}
	if err := s.core.CompleteLift(ctx, res); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.CompleteLiftTaskResponse{}), nil
}

func (s *WorkerServer) Heartbeat(ctx context.Context, req *connect.Request[hubwagenv1.HeartbeatRequest]) (*connect.Response[hubwagenv1.HeartbeatResponse], error) {
	stop, err := s.core.Heartbeat(ctx, req.Msg.WorkerId, req.Msg.TaskIds)
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.HeartbeatResponse{Stop: stop}), nil
}

// rawJSON checks a *_json field; "" means unset.
func rawJSON(field, s string) (json.RawMessage, error) {
	if s == "" {
		return nil, nil
	}
	if !json.Valid([]byte(s)) {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s is not valid JSON", field))
	}
	return json.RawMessage(s), nil
}

// jsonObject decodes a *_json field holding an object, keeping numbers
// exact (json.Number); "" means unset.
func jsonObject(field, s string) (map[string]any, error) {
	if s == "" {
		return nil, nil
	}
	dec := json.NewDecoder(strings.NewReader(s))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil || dec.More() {
		if err == nil {
			err = errors.New("trailing data")
		}
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("%s must be a JSON object: %v", field, err))
	}
	return m, nil
}

// connectError maps core errors to Connect codes.
func connectError(err error) error {
	switch {
	case errors.Is(err, core.ErrNotFound):
		return connect.NewError(connect.CodeNotFound, err)
	case errors.Is(err, core.ErrInvalid):
		return connect.NewError(connect.CodeInvalidArgument, err)
	case errors.Is(err, core.ErrUnknownHaul), errors.Is(err, core.ErrUnknownBuild), errors.Is(err, core.ErrHaulFinished):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, core.ErrStopped):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeCanceled, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}
