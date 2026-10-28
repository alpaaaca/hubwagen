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
	store     *state.Store
	manifests ManifestReader // nil: manifests aren't read
}

// NewCatalogHandler returns the path and handler to mount CatalogService at.
func NewCatalogHandler(st *state.Store, manifests ManifestReader) (string, http.Handler) {
	return hubwagenv1connect.NewCatalogServiceHandler(&CatalogServer{store: st, manifests: manifests})
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
	out := &hubwagenv1.ListDatasetsResponse{}
	for _, d := range dss {
		out.Datasets = append(out.Datasets, &hubwagenv1.Dataset{Tenant: d.Tenant, Name: d.Name,
			PartitionKey: d.PartitionKey, CreatedAt: timestamppb.New(d.CreatedAt)})
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
