package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/core"
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

	out := &hubwagenv1.Haul{
		Id: h.ID, Tenant: h.Tenant, Name: h.Name, Status: h.Status, Reason: h.Reason, Message: h.Message,
		ResultJson: string(h.Result), CreatedAt: timestamppb.New(h.CreatedAt),
	}
	if len(h.Params) > 0 {
		b, err := json.Marshal(h.Params)
		if err != nil {
			return nil, connectError(err)
		}
		out.ParamsJson = string(b)
	}
	if h.FinishedAt != nil {
		out.FinishedAt = timestamppb.New(*h.FinishedAt)
	}
	for _, l := range lifts {
		f := results[int32(l.Seq)]
		out.Lifts = append(out.Lifts, &hubwagenv1.Lift{
			Seq: int32(l.Seq), Name: l.Name, Status: l.Status, RunReason: l.RunReason, Reused: f.Reused,
			Attempts: int32(l.Attempts), Pallet: f.Pallet, Reason: l.Reason, Message: l.Message,
		})
	}
	return connect.NewResponse(&hubwagenv1.GetHaulResponse{Haul: out}), nil
}
