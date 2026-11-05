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

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/state"
)

// DefaultMaxWait caps how long a poll may wait for work.
const DefaultMaxWait = 30 * time.Second

// WorkerServer implements WorkerService on top of the core.
type WorkerServer struct {
	core    *core.Core
	maxWait time.Duration
	data    *Data // nil: no bucket
}

// NewWorkerHandler returns the path and handler to mount WorkerService at.
// maxWait caps long polls (0: DefaultMaxWait). data hands out transfer URLs
// (nil: the server has no bucket).
func NewWorkerHandler(c *core.Core, maxWait time.Duration, data *Data) (string, http.Handler) {
	if maxWait <= 0 {
		maxWait = DefaultMaxWait
	}
	return mokshav1connect.NewWorkerServiceHandler(&WorkerServer{core: c, maxWait: maxWait, data: data})
}

var _ mokshav1connect.WorkerServiceHandler = (*WorkerServer)(nil)

func (s *WorkerServer) RegisterBuild(ctx context.Context, req *connect.Request[mokshav1.RegisterBuildRequest]) (*connect.Response[mokshav1.RegisterBuildResponse], error) {
	err := s.core.RegisterBuild(ctx, buildRequest(req.Msg))
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.RegisterBuildResponse{}), nil
}

// buildRequest is a build as the core takes it (RegisterBuild, Deploy).
func buildRequest(m *mokshav1.RegisterBuildRequest) core.BuildRequest {
	b := core.BuildRequest{ID: m.GetBuildId(), Pipelines: m.GetPipelines(), Steps: m.GetStepVersions()}
	for _, sc := range m.GetSchedules() {
		b.Schedules = append(b.Schedules, core.ScheduleSpec{
			Pipeline: sc.Pipeline, Cron: sc.Cron, Timezone: sc.Timezone, Interval: sc.Interval, CatchUp: sc.CatchUp})
	}
	for _, sub := range m.GetSubscriptions() {
		b.Subscriptions = append(b.Subscriptions, core.SubscriptionSpec{Pipeline: sub.Pipeline, Datasets: sub.Datasets,
			Debounce: sub.Debounce.AsDuration(), MaxWait: sub.MaxWait.AsDuration(), MaxBatch: int(sub.MaxBatch)})
	}
	for _, d := range m.GetDatasets() {
		b.Datasets = append(b.Datasets, core.DatasetSpec{Name: d.Name, Versions: int(d.KeepVersions), Age: d.KeepFor.AsDuration()})
	}
	for _, i := range m.GetPipelineInfo() {
		if b.Info == nil {
			b.Info = map[string]state.RunInfo{}
		}
		b.Info[i.Pipeline] = state.RunInfo{Title: i.Title, Description: i.Description}
	}
	return b
}

func (s *WorkerServer) poll(ctx context.Context, worker, build string, queues []string, wait time.Duration, kind core.TaskKind) (core.Task, bool, error) {
	wait = min(max(wait, 0), s.maxWait)
	t, ok, err := s.core.Poll(ctx, core.PollRequest{WorkerID: worker, BuildID: build, Queues: queues, Kinds: []core.TaskKind{kind}, Wait: wait})
	if err != nil {
		return t, false, connectError(err)
	}
	return t, ok, nil
}

func (s *WorkerServer) PollRunTask(ctx context.Context, req *connect.Request[mokshav1.PollRunTaskRequest]) (*connect.Response[mokshav1.PollRunTaskResponse], error) {
	m := req.Msg
	t, ok, err := s.poll(ctx, m.WorkerId, m.BuildId, m.Queues, m.Wait.AsDuration(), core.TaskRun)
	if err != nil || !ok {
		return connect.NewResponse(&mokshav1.PollRunTaskResponse{}), err
	}
	out := &mokshav1.RunTask{Id: t.ID, RunId: t.RunID, Name: t.Name, Queue: t.Queue}
	for _, e := range t.EventLog {
		if !replayEvents[e.Type] {
			continue
		}
		out.EventLog = append(out.EventLog, &mokshav1.Event{
			Seq: e.Seq, Type: string(e.Type), Time: timestamppb.New(e.Time), PayloadJson: string(e.Payload)})
		if e.Type == state.EventRunStarted {
			var p struct {
				Params json.RawMessage `json:"params"`
			}
			if json.Unmarshal(e.Payload, &p) == nil && len(p.Params) > 0 {
				out.ParamsJson = string(p.Params)
			}
		}
	}
	return connect.NewResponse(&mokshav1.PollRunTaskResponse{Task: out}), nil
}

// replayEvents are the event log events a replay reads; the rest is
// bookkeeping a worker doesn't need (spike S1: a 1,000-step event log shrinks
// about fivefold).
var replayEvents = map[state.EventType]bool{
	state.EventRunStarted:       true,
	state.EventStepFinished:     true,
	state.EventTimerFired:       true,
	state.EventQueryAnswered:    true,
	state.EventSignalDelivered:  true,
	state.EventChildStarted:     true,
	state.EventChildFinished:    true,
	state.EventRunTaskCompleted: true,
}

func (s *WorkerServer) CompleteRunTask(ctx context.Context, req *connect.Request[mokshav1.CompleteRunTaskRequest]) (*connect.Response[mokshav1.CompleteRunTaskResponse], error) {
	m := req.Msg
	res := core.RunTaskResult{TaskID: m.TaskId, BuildID: m.BuildId}
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
		res.Outcome = &core.RunOutcome{Result: result, Reason: o.Reason, Message: o.Message}
		switch o.Status {
		case mokshav1.RunOutcome_STATUS_COMPLETED:
			res.Outcome.Status = state.RunCompleted
		case mokshav1.RunOutcome_STATUS_FAILED:
			res.Outcome.Status = state.RunFailed
		case mokshav1.RunOutcome_STATUS_CONTINUED:
			res.Outcome.Status = state.RunContinued
			if res.Outcome.Params, err = jsonObject("outcome.params_json", o.ParamsJson); err != nil {
				return nil, err
			}
		default:
			return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("outcome.status must be COMPLETED, FAILED or CONTINUED"))
		}
	}
	if e := m.Error; e != nil {
		res.Error = &core.TaskError{Reason: e.Reason, Message: e.Message}
	}
	for _, mk := range m.Markers {
		res.Markers = append(res.Markers, core.Marker{Name: mk.Name, Version: int(mk.Version)})
	}
	if err := s.core.CompleteRunTask(ctx, res); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.CompleteRunTaskResponse{}), nil
}

func command(c *mokshav1.Command) (core.Command, error) {
	cmd := core.Command{Seq: int(c.Seq), Source: c.Source}
	switch k := c.Kind.(type) {
	case *mokshav1.Command_Step:
		l := k.Step
		args, err := rawJSON("step.args_json", l.ArgsJson)
		if err != nil {
			return cmd, err
		}
		cmd.Kind, cmd.Name, cmd.Queue, cmd.Args, cmd.Memo = core.CommandStep, l.Name, l.Queue, args, l.Memo
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
			cmd.Inputs = append(cmd.Inputs, core.InputRef{Name: in.Name, DataVersion: in.DataVersion})
		}
		for _, lim := range l.Limits {
			cmd.Limits = append(cmd.Limits, core.ParallelLimit{Group: lim.Group, Max: int(lim.Max)})
		}
	case *mokshav1.Command_Sleep:
		cmd.Kind = core.CommandSleep
		if k.Sleep.Duration != nil {
			cmd.Duration = k.Sleep.Duration.AsDuration()
		}
	case *mokshav1.Command_LatestDataVersion:
		cmd.Kind, cmd.Dataset, cmd.Partition = core.CommandLatestDataVersion, k.LatestDataVersion.Dataset, k.LatestDataVersion.Partition
	case *mokshav1.Command_StartRun:
		sh := k.StartRun
		params, err := rawJSON("start_run.params_json", sh.ParamsJson)
		if err != nil {
			return cmd, err
		}
		cmd.Kind, cmd.Name, cmd.Args, cmd.Wait, cmd.Queue = core.CommandStartRun, sh.Name, params, sh.Wait, sh.Queue
	case *mokshav1.Command_WaitSignal:
		cmd.Kind, cmd.Name = core.CommandWaitSignal, k.WaitSignal.Name
		if k.WaitSignal.Timeout != nil {
			cmd.Duration = k.WaitSignal.Timeout.AsDuration()
		}
	default:
		return cmd, errors.New("a command is a step, a sleep, a latest_version, a wait_signal or a start_run")
	}
	return cmd, nil
}

func (s *WorkerServer) PollStepTask(ctx context.Context, req *connect.Request[mokshav1.PollStepTaskRequest]) (*connect.Response[mokshav1.PollStepTaskResponse], error) {
	m := req.Msg
	t, ok, err := s.poll(ctx, m.WorkerId, m.BuildId, m.Queues, m.Wait.AsDuration(), core.TaskStep)
	if err != nil || !ok {
		return connect.NewResponse(&mokshav1.PollStepTaskResponse{}), err
	}
	out := &mokshav1.StepTask{
		Id: t.ID, RunId: t.RunID, Seq: int32(t.Seq), Name: t.Name, Queue: t.Queue,
		ArgsJson: string(t.Args), Staging: t.Staging,
	}
	if s.data != nil {
		out.Bucket, out.BucketId = s.data.bucket.Base(), s.data.id
	}
	if o := t.Output; o != nil {
		out.Output = &mokshav1.Output{Dataset: o.Dataset, Partition: o.Partition, PartitionKey: o.PartitionKey}
	}
	for _, in := range t.Inputs {
		out.Inputs = append(out.Inputs, &mokshav1.InputRef{Name: in.Name, DataVersion: in.DataVersion, Location: in.Location})
	}
	return connect.NewResponse(&mokshav1.PollStepTaskResponse{Task: out}), nil
}

func (s *WorkerServer) CompleteStepTask(ctx context.Context, req *connect.Request[mokshav1.CompleteStepTaskRequest]) (*connect.Response[mokshav1.CompleteStepTaskResponse], error) {
	m := req.Msg
	result, err := rawJSON("result_json", m.ResultJson)
	if err != nil {
		return nil, err
	}
	res := core.StepResult{AttemptID: m.AttemptId, Result: result, Reason: m.Reason, Message: m.Message, NoRetry: m.NoRetry}
	switch m.Status {
	case mokshav1.CompleteStepTaskRequest_STATUS_SUCCEEDED:
		res.Status = state.AttemptSucceeded
	case mokshav1.CompleteStepTaskRequest_STATUS_FAILED:
		res.Status = state.AttemptFailed
	default:
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("status must be SUCCEEDED or FAILED"))
	}
	if o := m.Output; o != nil {
		out := &core.StepOutput{}
		if out.Stats, err = jsonObject("output.stats_json", o.StatsJson); err != nil {
			return nil, err
		}
		if out.Meta, err = jsonObject("output.meta_json", o.MetaJson); err != nil {
			return nil, err
		}
		for _, f := range o.Files {
			out.Files = append(out.Files, dataversion.File{Path: f.Path, Size: f.Size, SHA256: f.Sha256})
		}
		res.Output = out
	}
	if err := s.core.CompleteStep(ctx, res); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.CompleteStepTaskResponse{}), nil
}

func (s *WorkerServer) Heartbeat(ctx context.Context, req *connect.Request[mokshav1.HeartbeatRequest]) (*connect.Response[mokshav1.HeartbeatResponse], error) {
	stop, err := s.core.Heartbeat(ctx, req.Msg.WorkerId, req.Msg.TaskIds)
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.HeartbeatResponse{Stop: stop}), nil
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
	case errors.Is(err, core.ErrUnknownPipeline), errors.Is(err, core.ErrUnknownBuild), errors.Is(err, core.ErrRunFinished),
		errors.Is(err, core.ErrConflict):
		return connect.NewError(connect.CodeFailedPrecondition, err)
	case errors.Is(err, core.ErrStopped):
		return connect.NewError(connect.CodeUnavailable, err)
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return connect.NewError(connect.CodeCanceled, err)
	}
	return connect.NewError(connect.CodeInternal, err)
}
