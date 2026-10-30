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

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/history"
	"github.com/alper/hubwagen/internal/schedule"
	"github.com/alper/hubwagen/internal/state"
)

// HaulServer implements HaulService: writes go through the core, reads come
// from the store's projections.
type HaulServer struct {
	core  *core.Core
	store *state.Store
}

// NewHaulHandler returns the path and handler to mount HaulService at.
func NewHaulHandler(c *core.Core, st *state.Store) (string, http.Handler) {
	return hubwagenv1connect.NewHaulServiceHandler(&HaulServer{core: c, store: st})
}

var _ hubwagenv1connect.HaulServiceHandler = (*HaulServer)(nil)

func (s *HaulServer) StartHaul(ctx context.Context, req *connect.Request[hubwagenv1.StartHaulRequest]) (*connect.Response[hubwagenv1.StartHaulResponse], error) {
	m := req.Msg
	params, err := jsonObject("params_json", m.ParamsJson)
	if err != nil {
		return nil, err
	}
	res, err := s.core.StartHaul(ctx, core.StartHaulRequest{Tenant: m.Tenant, Name: m.Name, Dock: m.Dock, Trigger: m.Trigger, Params: params})
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.StartHaulResponse{HaulId: res.HaulID}), nil
}

func (s *HaulServer) SignalHaul(ctx context.Context, req *connect.Request[hubwagenv1.SignalHaulRequest]) (*connect.Response[hubwagenv1.SignalHaulResponse], error) {
	m := req.Msg
	id, err := s.core.SignalHaul(ctx, core.SignalRequest{HaulID: m.HaulId, Name: m.Name, Payload: json.RawMessage(m.PayloadJson)})
	if err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.SignalHaulResponse{SignalId: id}), nil
}

func (s *HaulServer) CancelHaul(ctx context.Context, req *connect.Request[hubwagenv1.CancelHaulRequest]) (*connect.Response[hubwagenv1.CancelHaulResponse], error) {
	by := req.Msg.By
	if by == "" {
		by = "the API"
	}
	if err := s.core.CancelHaul(ctx, req.Msg.HaulId, by); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.CancelHaulResponse{}), nil
}

func (s *HaulServer) ResumeHaul(ctx context.Context, req *connect.Request[hubwagenv1.ResumeHaulRequest]) (*connect.Response[hubwagenv1.ResumeHaulResponse], error) {
	if err := s.core.ResumeHaul(ctx, req.Msg.HaulId); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.ResumeHaulResponse{}), nil
}

func (s *HaulServer) ListHauls(ctx context.Context, req *connect.Request[hubwagenv1.ListHaulsRequest]) (*connect.Response[hubwagenv1.ListHaulsResponse], error) {
	m := req.Msg
	limit := int(m.Limit)
	if limit > 1000 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit %d: at most 1000", limit))
	}
	if m.WithLifts && limit > 200 {
		return nil, connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit %d: at most 200 with lifts", limit))
	}
	hauls, err := s.store.ListHauls(ctx, state.HaulFilter{Tenant: m.Tenant, Name: m.Name, Statuses: m.Statuses,
		ParentID: m.ParentId, BeforeID: m.BeforeId, Limit: limit})
	if err != nil {
		return nil, connectError(err)
	}
	ids := make([]string, len(hauls))
	for i, h := range hauls {
		ids[i] = h.ID
	}
	counts, err := s.store.CountLifts(ctx, ids)
	if err != nil {
		return nil, connectError(err)
	}
	var lifts map[string][]state.Lift
	if m.WithLifts {
		if lifts, err = s.store.ListLiftsOf(ctx, ids); err != nil {
			return nil, connectError(err)
		}
	}
	out := &hubwagenv1.ListHaulsResponse{}
	for _, h := range hauls {
		hm := haulMessage(h)
		hm.LiftCounts = liftCountsMessage(counts[h.ID])
		for _, l := range lifts[h.ID] {
			lm := &hubwagenv1.Lift{Seq: int32(l.Seq), Name: l.Name, Status: l.Status, Reused: l.Reused,
				Attempts: int32(l.Attempts), ScheduledAt: timestamppb.New(l.ScheduledAt)}
			if l.FinishedAt != nil {
				lm.FinishedAt = timestamppb.New(*l.FinishedAt)
			}
			hm.Lifts = append(hm.Lifts, lm)
		}
		out.Hauls = append(out.Hauls, hm)
	}
	if err := s.addWaiting(ctx, out.Hauls...); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(out), nil
}

func (s *HaulServer) GetLogbook(ctx context.Context, req *connect.Request[hubwagenv1.GetLogbookRequest]) (*connect.Response[hubwagenv1.GetLogbookResponse], error) {
	h, err := s.store.GetHaul(ctx, req.Msg.HaulId)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("haul %s not found", req.Msg.HaulId))
	}
	if err != nil {
		return nil, connectError(err)
	}
	evs, err := s.store.HaulEvents(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.GetLogbookResponse{Haul: haulMessage(h)}
	for _, e := range evs {
		if replayEvents[e.Type] {
			out.Logbook = append(out.Logbook, &hubwagenv1.LogbookEvent{
				Seq: e.Seq, Type: string(e.Type), Time: timestamppb.New(e.Time), PayloadJson: string(e.Payload)})
		}
	}
	return connect.NewResponse(out), nil
}

// why says why a lift ran or was reused, for people.
func why(runReason string, reused bool) string {
	if reused {
		return "same inputs and same code as before, so it didn't run again"
	}
	if w := history.WhyItRan(runReason); w != "" {
		return "it ran because " + w
	}
	return ""
}

func (s *HaulServer) GetHistory(ctx context.Context, req *connect.Request[hubwagenv1.GetHistoryRequest]) (*connect.Response[hubwagenv1.GetHistoryResponse], error) {
	h, err := s.store.GetHaul(ctx, req.Msg.HaulId)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("haul %s not found", req.Msg.HaulId))
	}
	if err != nil {
		return nil, connectError(err)
	}
	evs, err := s.store.HaulEvents(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	look := history.Lookup{
		Pallet: func(id string) (state.Pallet, bool) {
			p, err := s.store.GetPallet(ctx, id)
			return p, err == nil
		},
		Haul: func(id string) (state.Haul, bool) {
			h, err := s.store.GetHaul(ctx, id)
			return h, err == nil
		},
	}
	out := &hubwagenv1.GetHistoryResponse{}
	for _, e := range history.Tell(evs, look) {
		out.Entries = append(out.Entries, &hubwagenv1.HistoryEntry{
			Time: timestamppb.New(e.Time), Kind: e.Kind, Seq: int32(e.Seq), Step: e.Step, Text: e.Text, Detail: e.Detail,
			HaulId: e.Run, EventSeq: e.Event.Seq, EventType: string(e.Event.Type), PayloadJson: string(e.Event.Payload)})
	}
	return connect.NewResponse(out), nil
}

func (s *HaulServer) ListSchedules(ctx context.Context, _ *connect.Request[hubwagenv1.ListSchedulesRequest]) (*connect.Response[hubwagenv1.ListSchedulesResponse], error) {
	scheds, err := s.store.ListSchedules(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListSchedulesResponse{}
	for _, sc := range scheds {
		v := &hubwagenv1.Schedule{Haul: sc.Haul, Cron: sc.Cron, Timezone: sc.Timezone, Interval: sc.Interval,
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
	waiting, err := s.store.ListSubscriptionPallets(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	count := map[string]int32{}
	for _, w := range waiting {
		count[w.Haul]++
	}
	for _, sub := range subs {
		out.Subscriptions = append(out.Subscriptions, &hubwagenv1.Subscription{
			Haul: sub.Haul, Datasets: sub.Datasets, Debounce: durationpb.New(sub.Debounce), MaxWait: durationpb.New(sub.MaxWait),
			MaxBatch: int32(sub.MaxBatch), BuildId: sub.BuildID, Waiting: count[sub.Haul]})
	}
	return connect.NewResponse(out), nil
}

func (s *HaulServer) GetHaul(ctx context.Context, req *connect.Request[hubwagenv1.GetHaulRequest]) (*connect.Response[hubwagenv1.GetHaulResponse], error) {
	h, err := s.store.GetHaul(ctx, req.Msg.HaulId)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("haul %s not found", req.Msg.HaulId))
	}
	if err != nil {
		return nil, connectError(err)
	}
	lifts, err := s.store.ListLifts(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	// Which pallet each lift ended with, and whether it was reused, is in
	// its LiftFinished event.
	evs, err := s.store.HaulEvents(ctx, h.ID)
	if err != nil {
		return nil, connectError(err)
	}
	type finished struct {
		Seq    int32  `json:"seq"`
		Pallet string `json:"pallet"`
		Reused bool   `json:"reused"`
	}
	results := map[int32]finished{}
	for _, e := range evs {
		if e.Type == state.EventLiftFinished {
			var f finished
			if json.Unmarshal(e.Payload, &f) == nil {
				results[f.Seq] = f
			}
		}
	}

	// The DAG: a lift comes after the lifts whose pallets it reads.
	producedBy := map[string]int32{}
	for seq, f := range results {
		if f.Pallet != "" {
			producedBy[f.Pallet] = seq
		}
	}
	out := haulMessage(h)
	var counts state.LiftCounts
	for _, l := range lifts {
		f := results[int32(l.Seq)]
		m := &hubwagenv1.Lift{
			Seq: int32(l.Seq), Name: l.Name, Status: l.Status, RunReason: l.RunReason, Why: why(l.RunReason, f.Reused), Reused: f.Reused,
			Attempts: int32(l.Attempts), Pallet: f.Pallet, Reason: l.Reason, Message: l.Message,
			ScheduledAt: timestamppb.New(l.ScheduledAt), ResultJson: string(l.Result),
		}
		var inputs []struct {
			Pallet string `json:"pallet"`
		}
		var output struct {
			Dataset string `json:"dataset"`
		}
		_ = json.Unmarshal(l.Inputs, &inputs) // encoded by the core; nil when it has none
		_ = json.Unmarshal(l.Output, &output)
		for _, in := range inputs {
			if seq, ok := producedBy[in.Pallet]; ok && seq != m.Seq && !slices.Contains(m.After, seq) {
				m.After = append(m.After, seq)
			}
		}
		slices.Sort(m.After)
		m.Dataset = output.Dataset
		if l.FinishedAt != nil {
			m.FinishedAt = timestamppb.New(*l.FinishedAt)
		}
		out.Lifts = append(out.Lifts, m)
		counts.Total++
		switch l.Status {
		case state.LiftCompleted:
			counts.Completed++
			if l.Reused {
				counts.Reused++
			}
		case state.LiftFailed:
			counts.Failed++
		case state.LiftCancelled:
			counts.Cancelled++
		}
	}
	out.LiftCounts = liftCountsMessage(counts)
	if err := s.addWaiting(ctx, out); err != nil {
		return nil, connectError(err)
	}
	return connect.NewResponse(&hubwagenv1.GetHaulResponse{Haul: out}), nil
}

// haulMessage is a haul without its lifts.
func (s *HaulServer) ListPipelines(ctx context.Context, _ *connect.Request[hubwagenv1.ListPipelinesRequest]) (*connect.Response[hubwagenv1.ListPipelinesResponse], error) {
	ps, err := s.store.ListPipelines(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListPipelinesResponse{}
	for _, p := range ps {
		out.Pipelines = append(out.Pipelines, &hubwagenv1.Pipeline{Name: p.Name, Title: p.Title, Description: p.Description})
	}
	return connect.NewResponse(out), nil
}

func liftCountsMessage(c state.LiftCounts) *hubwagenv1.LiftCounts {
	return &hubwagenv1.LiftCounts{Total: int32(c.Total), Completed: int32(c.Completed), Reused: int32(c.Reused),
		Failed: int32(c.Failed), Cancelled: int32(c.Cancelled)}
}

func haulMessage(h state.Haul) *hubwagenv1.Haul {
	out := &hubwagenv1.Haul{
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

func (s *HaulServer) ListCode(ctx context.Context, _ *connect.Request[hubwagenv1.ListCodeRequest]) (*connect.Response[hubwagenv1.ListCodeResponse], error) {
	steps, pipelines, err := s.store.CurrentCode(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	msg := func(c state.Code) *hubwagenv1.Code {
		return &hubwagenv1.Code{Name: c.Name, CodeVersion: c.Version, BuildId: c.BuildID, RegisteredAt: timestamppb.New(c.RegisteredAt)}
	}
	out := &hubwagenv1.ListCodeResponse{}
	for _, c := range steps {
		out.Steps = append(out.Steps, msg(c))
	}
	for _, c := range pipelines {
		out.Pipelines = append(out.Pipelines, msg(c))
	}
	return connect.NewResponse(out), nil
}
