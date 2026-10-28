package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/state"
)

// ManifestReader reads a pallet's manifest from where it lives;
// *pallet.Store implements it.
type ManifestReader interface {
	Read(ctx context.Context, uri string) (pallet.Manifest, error)
}

// CatalogServer implements CatalogService from the store's projections.
type CatalogServer struct {
	core      *core.Core // writes (invalidation); nil: read only
	store     *state.Store
	manifests ManifestReader // nil: manifests aren't read
}

// NewCatalogHandler returns the path and handler to mount CatalogService
// at. Without a core, InvalidatePallet is unavailable.
func NewCatalogHandler(c *core.Core, st *state.Store, manifests ManifestReader) (string, http.Handler) {
	return hubwagenv1connect.NewCatalogServiceHandler(&CatalogServer{core: c, store: st, manifests: manifests})
}

func (s *CatalogServer) InvalidatePallet(ctx context.Context, req *connect.Request[hubwagenv1.InvalidatePalletRequest]) (*connect.Response[hubwagenv1.InvalidatePalletResponse], error) {
	if s.core == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this server only reads the catalog"))
	}
	m := req.Msg
	res, err := s.core.InvalidatePallet(ctx, core.InvalidateRequest{PalletID: m.Id, Reason: m.Reason, By: m.By, DryRun: m.DryRun})
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.InvalidatePalletResponse{}
	for _, p := range res.Pallets {
		out.Pallets = append(out.Pallets, palletMessage(p))
	}
	for _, h := range res.Hauls {
		out.Hauls = append(out.Hauls, haulRef(h))
	}
	return connect.NewResponse(out), nil
}

var _ hubwagenv1connect.CatalogServiceHandler = (*CatalogServer)(nil)

const maxListLimit = 1000

func tenantOr(t string) string {
	if t == "" {
		return "default"
	}
	return t
}

func checkLimit(limit int32) error {
	if limit < 0 || limit > maxListLimit {
		return connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("limit %d: want 0 to %d", limit, maxListLimit))
	}
	return nil
}

func (s *CatalogServer) ListDatasets(ctx context.Context, req *connect.Request[hubwagenv1.ListDatasetsRequest]) (*connect.Response[hubwagenv1.ListDatasetsResponse], error) {
	dss, err := s.store.ListDatasets(ctx, req.Msg.Tenant)
	if err != nil {
		return nil, connectError(err)
	}
	keep, err := s.store.ListRetention(ctx)
	if err != nil {
		return nil, connectError(err)
	}
	byName := map[string]state.Retention{}
	for _, r := range keep {
		byName[r.Dataset] = r
	}
	out := &hubwagenv1.ListDatasetsResponse{}
	for _, d := range dss {
		v := &hubwagenv1.Dataset{Tenant: d.Tenant, Name: d.Name, PartitionKey: d.PartitionKey, CreatedAt: timestamppb.New(d.CreatedAt)}
		if r, ok := byName[d.Name]; ok {
			v.KeepVersions = int32(r.Versions)
			if r.Age > 0 {
				v.KeepFor = durationpb.New(r.Age)
			}
		}
		out.Datasets = append(out.Datasets, v)
	}
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) ListPartitions(ctx context.Context, req *connect.Request[hubwagenv1.ListPartitionsRequest]) (*connect.Response[hubwagenv1.ListPartitionsResponse], error) {
	m := req.Msg
	if err := checkLimit(m.Limit); err != nil {
		return nil, err
	}
	parts, err := s.store.ListPartitions(ctx, tenantOr(m.Tenant), m.Dataset, m.After, int(m.Limit))
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListPartitionsResponse{}
	for _, p := range parts {
		out.Partitions = append(out.Partitions, &hubwagenv1.Partition{
			Partition: p.Partition, Latest: palletMessage(p.Latest), Pallets: int32(p.Pallets)})
	}
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) ListPallets(ctx context.Context, req *connect.Request[hubwagenv1.ListPalletsRequest]) (*connect.Response[hubwagenv1.ListPalletsResponse], error) {
	m := req.Msg
	if err := checkLimit(m.Limit); err != nil {
		return nil, err
	}
	ps, err := s.store.ListPallets(ctx, state.PalletFilter{Tenant: tenantOr(m.Tenant), Dataset: m.Dataset,
		Partition: m.Partition, States: m.States, BeforeID: m.BeforeId, Limit: int(m.Limit)})
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListPalletsResponse{}
	for _, p := range ps {
		out.Pallets = append(out.Pallets, palletMessage(p))
	}
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) GetPallet(ctx context.Context, req *connect.Request[hubwagenv1.GetPalletRequest]) (*connect.Response[hubwagenv1.GetPalletResponse], error) {
	p, err := s.store.GetPallet(ctx, req.Msg.Id)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("pallet %s not found", req.Msg.Id))
	}
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.GetPalletResponse{Pallet: palletMessage(p)}
	if l, err := s.store.GetLift(ctx, p.HaulID, p.LiftSeq); err == nil {
		out.Lift = l.Name
	}
	switch {
	case s.manifests == nil:
		out.ManifestError = "this server doesn't read manifests"
	case p.State == state.PalletExpired:
		out.ManifestError = "expired: its data was removed"
	default:
		if m, err := s.manifests.Read(ctx, p.Location); err != nil {
			out.ManifestError = err.Error()
		} else {
			mm := &hubwagenv1.Manifest{Data: m.Data, ResultJson: string(m.Result)}
			for _, f := range m.Files {
				mm.Files = append(mm.Files, &hubwagenv1.ManifestFile{Path: f.Path, Size: f.Size, Sha256: f.SHA256})
			}
			out.Manifest = mm
		}
	}
	up, err := s.store.Upstream(ctx, p.ID)
	if err != nil {
		return nil, connectError(err)
	}
	down, err := s.store.Downstream(ctx, p.ID)
	if err != nil {
		return nil, connectError(err)
	}
	out.Upstream, out.Downstream = lineageMessage(up), lineageMessage(down)
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) ExpirePallets(ctx context.Context, req *connect.Request[hubwagenv1.ExpirePalletsRequest]) (*connect.Response[hubwagenv1.ExpirePalletsResponse], error) {
	if s.core == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this server only reads the catalog"))
	}
	expired, err := s.core.Expire(ctx, req.Msg.DryRun)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ExpirePalletsResponse{}
	for _, e := range expired {
		out.Expired = append(out.Expired, &hubwagenv1.ExpiredPallet{Pallet: palletMessage(e.Pallet), Reason: e.Reason})
	}
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) ListStale(ctx context.Context, req *connect.Request[hubwagenv1.ListStaleRequest]) (*connect.Response[hubwagenv1.ListStaleResponse], error) {
	stale, err := s.store.StalePartitions(ctx, tenantOr(req.Msg.Tenant), req.Msg.Dataset)
	if err != nil {
		return nil, connectError(err)
	}
	out := &hubwagenv1.ListStaleResponse{}
	for _, st := range stale {
		v := &hubwagenv1.StalePallet{Pallet: palletMessage(st.Pallet), Lift: st.Lift, Reasons: st.Reasons}
		if h, err := s.store.GetHaul(ctx, st.Pallet.HaulID); err == nil {
			v.Haul = haulRef(h)
		}
		out.Stale = append(out.Stale, v)
	}
	return connect.NewResponse(out), nil
}

func haulRef(h state.Haul) *hubwagenv1.HaulRef {
	return &hubwagenv1.HaulRef{Id: h.ID, Name: h.Name, ParamsJson: haulMessage(h).ParamsJson, Tenant: h.Tenant}
}

func palletMessage(p state.Pallet) *hubwagenv1.Pallet {
	out := &hubwagenv1.Pallet{
		Id: p.ID, Tenant: p.Tenant, Dataset: p.Dataset, Partition: p.Partition, Location: p.Location,
		ContentHash: p.ContentHash, Fingerprint: p.Fingerprint, HaulId: p.HaulID, LiftSeq: int32(p.LiftSeq),
		State: p.State, Reason: p.Reason, CreatedAt: timestamppb.New(p.CreatedAt),
	}
	if len(p.Stats) > 0 {
		b, _ := json.Marshal(p.Stats)
		out.StatsJson = string(b)
	}
	if len(p.Meta) > 0 {
		b, _ := json.Marshal(p.Meta)
		out.MetaJson = string(b)
	}
	if p.CommittedAt != nil {
		out.CommittedAt = timestamppb.New(*p.CommittedAt)
	}
	if p.RetiredAt != nil {
		out.RetiredAt = timestamppb.New(*p.RetiredAt)
	}
	return out
}

func lineageMessage(l state.Lineage) *hubwagenv1.Lineage {
	out := &hubwagenv1.Lineage{}
	for _, p := range l.Pallets {
		out.Pallets = append(out.Pallets, palletMessage(p))
	}
	for _, e := range l.Edges {
		out.Edges = append(out.Edges, &hubwagenv1.LineageEdge{PalletId: e.PalletID, InputId: e.InputID, Name: e.Name})
	}
	return out
}
