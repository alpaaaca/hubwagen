package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/history"
	"github.com/alper/moksha/internal/schedule"
	"github.com/alper/moksha/internal/state"
)

// RunServer implements RunService: writes go through the core, reads come
// from the store's projections.
type RunServer struct {
	core  *core.Core
	store *state.Store
}

// NewRunHandler returns the path and handler to mount RunService at.
func NewRunHandler(c *core.Core, st *state.Store) (string, http.Handler) {
	return mokshav1connect.NewRunServiceHandler(&RunServer{core: c, store: st})
}

var _ mokshav1connect.RunServiceHandler = (*RunServer)(nil)

func (s *RunServer) StartRun(ctx context.Context, req *connect.Request[mokshav1.StartRunRequest]) (*connect.Response[mokshav1.StartRunResponse], error) {
	m := req.Msg
	params, err := jsonObject("params_json", m.ParamsJson)
	if err != nil {
		return nil, err
	}
	res, err := s.core.StartRun(ctx, core.StartRunRequest{Tenant: m.Tenant, Name: m.Name, Queue: m.Queue, Trigger: m.Trigger,
		Params: params, Reuse: m.Reuse})
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.StartRunResponse{RunId: res.RunID}), nil
}

func (s *RunServer) SignalRun(ctx context.Context, req *connect.Request[mokshav1.SignalRunRequest]) (*connect.Response[mokshav1.SignalRunResponse], error) {
	m := req.Msg
	id, err := s.core.SignalRun(ctx, core.SignalRequest{RunID: m.RunId, Name: m.Name, Payload: json.RawMessage(m.PayloadJson)})
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.SignalRunResponse{SignalId: id}), nil
}

func (s *RunServer) CancelRun(ctx context.Context, req *connect.Request[mokshav1.CancelRunRequest]) (*connect.Response[mokshav1.CancelRunResponse], error) {
	by := req.Msg.By
	if by == "" {
		by = "the API"
	}
	if err := s.core.CancelRun(ctx, req.Msg.RunId, by); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.CancelRunResponse{}), nil
}

func (s *RunServer) ResumeRun(ctx context.Context, req *connect.Request[mokshav1.ResumeRunRequest]) (*connect.Response[mokshav1.ResumeRunResponse], error) {
	if err := s.core.ResumeRun(ctx, req.Msg.RunId); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.ResumeRunResponse{}), nil
}

func (s *RunServer) ListRuns(ctx context.Context, req *connect.Request[mokshav1.ListRunsRequest]) (*connect.Response[mokshav1.ListRunsResponse], error) {
	m := req.Msg
	limit := int(m.Limit)
	if limit > 1000 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit %d: at most 1000", limit))
	}
	if m.WithSteps && limit > 200 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit %d: at most 200 with steps", limit))
	}
	runs, err := s.store.ListRuns(ctx, state.RunFilter{Tenant: m.Tenant, Name: m.Name, Statuses: m.Statuses,
		ParentID: m.ParentId, BeforeID: m.BeforeId, Limit: limit})
	if err != nil {
		return nil, connectError(err)
	}
	ids := make([]string, len(runs))
	for i, h := range runs {
		ids[i] = h.ID
	}
	counts, err := s.store.CountSteps(ctx, ids)
	if err != nil {
		return nil, connectError(err)
	}
	var steps map[string][]state.Step
	if m.WithSteps {
		if steps, err = s.store.ListStepsOf(ctx, ids); err != nil {
			return nil, connectError(err)
		}
	}
	out := &mokshav1.ListRunsResponse{}
	for _, h := range runs {
		hm := runMessage(h)
		hm.StepCounts = stepCountsMessage(counts[h.ID])
		for _, l := range steps[h.ID] {
			lm := &mokshav1.Step{Seq: int32(l.Seq), Name: l.Name, Status: l.Status, Reused: l.Reused,
				Attempts: int32(l.Attempts), ScheduledAt: timestamppb.New(l.ScheduledAt)}
			if l.FinishedAt != nil {
				lm.FinishedAt = timestamppb.New(*l.FinishedAt)
			}
			hm.Steps = append(hm.Steps, lm)
		}
		out.Runs = append(out.Runs, hm)
	}
	if err := s.addWaiting(ctx, out.Runs...); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(out), nil
}

func (s *RunServer) GetEventLog(ctx context.Context, req *connect.Request[mokshav1.GetEventLogRequest]) (*connect.Response[mokshav1.GetEventLogResponse], error) {
	h, err := s.store.GetRun(ctx, req.Msg.RunId)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %s not found", req.Msg.RunId))
	}
	if err != nil {
		return nil, connectError(err)
	}
	evs, err := s.store.RunEvents(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.GetEventLogResponse{Run: runMessage(h)}
	for _, e := range evs {
		if replayEvents[e.Type] {
			out.EventLog = append(out.EventLog, &mokshav1.LoggedEvent{
				Seq: e.Seq, Type: string(e.Type), Time: timestamppb.New(e.Time), PayloadJson: string(e.Payload)})
		}
	}
	return connect.NewResponse(out), nil
}

// why says why a step ran or was reused, for people.
func why(runReason string, reused bool) string {
	if reused {
		return "same inputs and same code as before, so it didn't run again"
	}
	if w := history.WhyItRan(runReason); w != "" {
		return "it ran because " + w
	}
	return ""
}

func (s *RunServer) GetHistory(ctx context.Context, req *connect.Request[mokshav1.GetHistoryRequest]) (*connect.Response[mokshav1.GetHistoryResponse], error) {
	h, err := s.store.GetRun(ctx, req.Msg.RunId)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %s not found", req.Msg.RunId))
	}
	if err != nil {
		return nil, connectError(err)
	}
	evs, err := s.store.RunEvents(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	look := history.Lookup{
		DataVersion: func(id string) (state.DataVersion, bool) {
			p, err := s.store.GetDataVersion(ctx, id)
			return p, err == nil
		},
		Run: func(id string) (state.Run, bool) {
			h, err := s.store.GetRun(ctx, id)
			return h, err == nil
		},
	}
	out := &mokshav1.GetHistoryResponse{}
	for _, e := range history.Tell(evs, look) {
		out.Entries = append(out.Entries, &mokshav1.HistoryEntry{
			Time: timestamppb.New(e.Time), Kind: e.Kind, Seq: int32(e.Seq), Step: e.Step, Text: e.Text, Detail: e.Detail,
			RunId: e.Run, EventSeq: e.Event.Seq, EventType: string(e.Event.Type), PayloadJson: string(e.Event.Payload)})
	}
	return connect.NewResponse(out), nil
}

func (s *RunServer) ListSchedules(ctx context.Context, _ *connect.Request[mokshav1.ListSchedulesRequest]) (*connect.Response[mokshav1.ListSchedulesResponse], error) {
	scheds, err := s.store.ListSchedules(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.ListSchedulesResponse{}
	for _, sc := range scheds {
		v := &mokshav1.Schedule{Pipeline: sc.Pipeline, Cron: sc.Cron, Timezone: sc.Timezone, Interval: sc.Interval,
			CatchUp: sc.CatchUp, BuildId: sc.BuildID, Cursor: timestamppb.New(sc.Cursor)}
		if cal, err := schedule.NewCalendar(schedule.Schedule{Cron: sc.Cron, Timezone: sc.Timezone, Interval: sc.Interval}); err == nil {
			now := time.Now()
			v.NextRun = timestamppb.New(cal.Next(now))
			if tick := cal.Prev(now); !tick.IsZero() {
				b, _ := json.Marshal(map[string]any{"interval": core.IntervalParam(cal.Interval(tick))})
				v.LatestParamsJson = string(b)
			}
		}
		out.Schedules = append(out.Schedules, v)
	}
	subs, err := s.store.ListSubscriptions(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	waiting, err := s.store.ListSubscriptionDataVersions(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	count := map[string]int32{}
	for _, w := range waiting {
		count[w.Pipeline]++
	}
	for _, sub := range subs {
		out.Subscriptions = append(out.Subscriptions, &mokshav1.Subscription{
			Pipeline: sub.Pipeline, Datasets: sub.Datasets, Debounce: durationpb.New(sub.Debounce), MaxWait: durationpb.New(sub.MaxWait),
			MaxBatch: int32(sub.MaxBatch), BuildId: sub.BuildID, Waiting: count[sub.Pipeline]})
	}
	return connect.NewResponse(out), nil
}

func (s *RunServer) GetRun(ctx context.Context, req *connect.Request[mokshav1.GetRunRequest]) (*connect.Response[mokshav1.GetRunResponse], error) {
	h, err := s.store.GetRun(ctx, req.Msg.RunId)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("run %s not found", req.Msg.RunId))
	}
	if err != nil {
		return nil, connectError(err)
	}
	steps, err := s.store.ListSteps(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	// Which data version each step ended with, and whether it was reused, is in
	// its StepFinished event.
	evs, err := s.store.RunEvents(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	type finished struct {
		Seq         int32  `json:"seq"`
		DataVersion string `json:"data_version"`
		Reused      bool   `json:"reused"`
	}
	results := map[int32]finished{}
	for _, e := range evs {
		if e.Type == state.EventStepFinished {
			var f finished
			if json.Unmarshal(e.Payload, &f) == nil {
				results[f.Seq] = f
			}
		}
	}

	// The DAG: a step comes after the steps whose data versions it reads.
	producedBy := map[string]int32{}
	for seq, f := range results {
		if f.DataVersion != "" {
			producedBy[f.DataVersion] = seq
		}
	}
	out := runMessage(h)
	var counts state.StepCounts
	for _, l := range steps {
		f := results[int32(l.Seq)]
		m := &mokshav1.Step{
			Seq: int32(l.Seq), Name: l.Name, Status: l.Status, RunReason: l.RunReason, Why: why(l.RunReason, f.Reused), Reused: f.Reused,
			Attempts: int32(l.Attempts), DataVersion: f.DataVersion, Reason: l.Reason, Message: l.Message,
			ScheduledAt: timestamppb.New(l.ScheduledAt), ResultJson: string(l.Result),
		}
		var inputs []struct {
			DataVersion string `json:"data_version"`
		}
		var output struct {
			Dataset string `json:"dataset"`
		}
		_ = json.Unmarshal(l.Inputs, &inputs) // encoded by the core; nil when it has none
		_ = json.Unmarshal(l.Output, &output)
		for _, in := range inputs {
			if seq, ok := producedBy[in.DataVersion]; ok && seq != m.Seq && !slices.Contains(m.After, seq) {
				m.After = append(m.After, seq)
			}
		}
		slices.Sort(m.After)
		m.Dataset = output.Dataset
		if l.Status == state.StepCompleted && !f.Reused && l.RunReason == core.ReasonNoReuse {
			rep, err := s.store.StepRepeats(ctx, h.Tenant, l, f.DataVersion)
			if err != nil {
				return nil, connectError(err)
			}
			if rep.Others > 0 && rep.Same == rep.Others {
				m.SameResultRuns = int32(rep.Others)
			}
		}
		if l.FinishedAt != nil {
			m.FinishedAt = timestamppb.New(*l.FinishedAt)
		}
		out.Steps = append(out.Steps, m)
		counts.Total++
		switch l.Status {
		case state.StepCompleted:
			counts.Completed++
			if l.Reused {
				counts.Reused++
			}
		case state.StepFailed:
			counts.Failed++
		case state.StepCancelled:
			counts.Cancelled++
		}
	}
	out.StepCounts = stepCountsMessage(counts)
	if err := s.addWaiting(ctx, out); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&mokshav1.GetRunResponse{Run: out}), nil
}

// runMessage is a run without its steps.
func (s *RunServer) ListPipelines(ctx context.Context, _ *connect.Request[mokshav1.ListPipelinesRequest]) (*connect.Response[mokshav1.ListPipelinesResponse], error) {
	ps, err := s.store.ListPipelines(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.ListPipelinesResponse{}
	for _, p := range ps {
		out.Pipelines = append(out.Pipelines, &mokshav1.Pipeline{Name: p.Name, Title: p.Title, Description: p.Description})
	}
	return connect.NewResponse(out), nil
}

func stepCountsMessage(c state.StepCounts) *mokshav1.StepCounts {
	return &mokshav1.StepCounts{Total: int32(c.Total), Completed: int32(c.Completed), Reused: int32(c.Reused),
		Failed: int32(c.Failed), Cancelled: int32(c.Cancelled)}
}

func runMessage(h state.Run) *mokshav1.Run {
	out := &mokshav1.Run{
		Id: h.ID, Tenant: h.Tenant, Name: h.Name, Status: h.Status, Reason: h.Reason, Message: h.Message,
		ResultJson: string(h.Result), CreatedAt: timestamppb.New(h.CreatedAt),
		ParentId: h.ParentID, ContinuedAs: h.ContinuedAs, Trigger: h.Trigger,
	}
	if len(h.Params) > 0 {
		if b, err := json.Marshal(h.Params); err == nil { // a JSON object read from the store
			out.ParamsJson = string(b)
		}
	}
	if h.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*h.FinishedAt)
	}
	return out
}

func (s *RunServer) ListCode(ctx context.Context, _ *connect.Request[mokshav1.ListCodeRequest]) (*connect.Response[mokshav1.ListCodeResponse], error) {
	steps, pipelines, err := s.store.CurrentCode(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	msg := func(c state.Code) *mokshav1.Code {
		return &mokshav1.Code{Name: c.Name, CodeVersion: c.Version, BuildId: c.BuildID, RegisteredAt: timestamppb.New(c.RegisteredAt)}
	}
	out := &mokshav1.ListCodeResponse{}
	for _, c := range steps {
		out.Steps = append(out.Steps, msg(c))
	}
	for _, c := range pipelines {
		out.Pipelines = append(out.Pipelines, msg(c))
	}
	return connect.NewResponse(out), nil
}

func (s *RunServer) PlanRuns(ctx context.Context, req *connect.Request[mokshav1.PlanRunsRequest]) (*connect.Response[mokshav1.PlanRunsResponse], error) {
	m := req.Msg
	out := &mokshav1.PlanRunsResponse{}
	for i, pj := range m.ParamsJson {
		params, err := jsonObject(fmt.Sprintf("params_json[%d]", i), pj)
		if err != nil {
			return nil, err
		}
		p, err := s.core.PlanRun(ctx, m.Tenant, m.Name, params, m.Reuse)
		if err != nil {
			return nil, connectError(err)
		}
		rp := &mokshav1.RunPlan{ParamsJson: pj}
		if p.Earlier != nil {
			rp.EarlierRunId, rp.EarlierStatus, rp.EarlierCreatedAt = p.Earlier.ID, p.Earlier.Status, timestamppb.New(p.Earlier.CreatedAt)
		}
		for _, sp := range p.Steps {
			rp.Steps = append(rp.Steps, &mokshav1.StepPlan{Seq: int32(sp.Seq), Name: sp.Name, Reuse: sp.Reuse, Why: sp.Why})
		}
		out.Plans = append(out.Plans, rp)
	}
	return connect.NewResponse(out), nil
}
