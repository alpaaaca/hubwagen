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

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/dataversion"
	"github.com/alper/moksha/internal/state"
)

// ManifestReader reads a data version's manifest from where it lives;
// *dataversion.Store implements it.
type ManifestReader interface {
	Read(ctx context.Context, uri string) (dataversion.Manifest, error)
}

// CatalogServer implements CatalogService from the store's projections.
type CatalogServer struct {
	core      *core.Core // writes (invalidation); nil: read only
	store     *state.Store
	manifests ManifestReader // nil: manifests aren't read
}

// NewCatalogHandler returns the path and handler to mount CatalogService
// at. Without a core, InvalidateDataVersion is unavailable.
func NewCatalogHandler(c *core.Core, st *state.Store, manifests ManifestReader) (string, http.Handler) {
	return mokshav1connect.NewCatalogServiceHandler(&CatalogServer{core: c, store: st, manifests: manifests})
}

func (s *CatalogServer) InvalidateDataVersion(ctx context.Context, req *connect.Request[mokshav1.InvalidateDataVersionRequest]) (*connect.Response[mokshav1.InvalidateDataVersionResponse], error) {
	if s.core == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this server only reads the catalog"))
	}
	m := req.Msg
	res, err := s.core.InvalidateDataVersion(ctx, core.InvalidateRequest{DataVersionID: m.Id, Reason: m.Reason, By: m.By, DryRun: m.DryRun})
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.InvalidateDataVersionResponse{}
	for _, p := range res.DataVersions {
		out.DataVersions = append(out.DataVersions, dataVersionMessage(p))
	}
	for _, h := range res.Runs {
		out.Runs = append(out.Runs, runRef(h))
	}
	return connect.NewResponse(out), nil
}

var _ mokshav1connect.CatalogServiceHandler = (*CatalogServer)(nil)

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

func (s *CatalogServer) ListDatasets(ctx context.Context, req *connect.Request[mokshav1.ListDatasetsRequest]) (*connect.Response[mokshav1.ListDatasetsResponse], error) {
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
	out := &mokshav1.ListDatasetsResponse{}
	for _, d := range dss {
		v := &mokshav1.Dataset{Tenant: d.Tenant, Name: d.Name, PartitionKey: d.PartitionKey, CreatedAt: timestamppb.New(d.CreatedAt)}
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

func (s *CatalogServer) ListPartitions(ctx context.Context, req *connect.Request[mokshav1.ListPartitionsRequest]) (*connect.Response[mokshav1.ListPartitionsResponse], error) {
	m := req.Msg
	if err := checkLimit(m.Limit); err != nil {
		return nil, err
	}
	parts, err := s.store.ListPartitions(ctx, tenantOr(m.Tenant), m.Dataset, m.After, int(m.Limit))
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.ListPartitionsResponse{}
	for _, p := range parts {
		out.Partitions = append(out.Partitions, &mokshav1.Partition{
			Partition: p.Partition, Latest: dataVersionMessage(p.Latest), DataVersions: int32(p.DataVersions)})
	}
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) ListDataVersions(ctx context.Context, req *connect.Request[mokshav1.ListDataVersionsRequest]) (*connect.Response[mokshav1.ListDataVersionsResponse], error) {
	m := req.Msg
	if err := checkLimit(m.Limit); err != nil {
		return nil, err
	}
	ps, err := s.store.ListDataVersions(ctx, state.DataVersionFilter{Tenant: tenantOr(m.Tenant), Dataset: m.Dataset,
		Partition: m.Partition, States: m.States, BeforeID: m.BeforeId, Limit: int(m.Limit)})
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.ListDataVersionsResponse{}
	for _, p := range ps {
		out.DataVersions = append(out.DataVersions, dataVersionMessage(p))
	}
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) GetDataVersion(ctx context.Context, req *connect.Request[mokshav1.GetDataVersionRequest]) (*connect.Response[mokshav1.GetDataVersionResponse], error) {
	p, err := s.store.GetDataVersion(ctx, req.Msg.Id)
	if errors.Is(err, state.ErrNotFound) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("data version %s not found", req.Msg.Id))
	}
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.GetDataVersionResponse{DataVersion: dataVersionMessage(p)}
	if l, err := s.store.GetStep(ctx, p.RunID, p.StepSeq); err == nil {
		out.Step = l.Name
	}
	switch {
	case s.manifests == nil:
		out.ManifestError = "this server doesn't read manifests"
	case p.State == state.DataVersionExpired:
		out.ManifestError = "expired: its data was removed"
	default:
		if m, err := s.manifests.Read(ctx, p.Location); err != nil {
			out.ManifestError = err.Error()
		} else {
			mm := &mokshav1.Manifest{Data: m.Data, ResultJson: string(m.Result)}
			for _, f := range m.Files {
				mm.Files = append(mm.Files, &mokshav1.ManifestFile{Path: f.Path, Size: f.Size, Sha256: f.SHA256})
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

func (s *CatalogServer) ExpireDataVersions(ctx context.Context, req *connect.Request[mokshav1.ExpireDataVersionsRequest]) (*connect.Response[mokshav1.ExpireDataVersionsResponse], error) {
	if s.core == nil {
		return nil, connect.NewError(connect.CodeUnimplemented, errors.New("this server only reads the catalog"))
	}
	expired, err := s.core.Expire(ctx, req.Msg.DryRun)
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.ExpireDataVersionsResponse{}
	for _, e := range expired {
		out.Expired = append(out.Expired, &mokshav1.ExpiredDataVersion{DataVersion: dataVersionMessage(e.DataVersion), Reason: e.Reason})
	}
	return connect.NewResponse(out), nil
}

func (s *CatalogServer) ListStale(ctx context.Context, req *connect.Request[mokshav1.ListStaleRequest]) (*connect.Response[mokshav1.ListStaleResponse], error) {
	stale, err := s.store.StalePartitions(ctx, tenantOr(req.Msg.Tenant), req.Msg.Dataset)
	if err != nil {
		return nil, connectError(err)
	}
	out := &mokshav1.ListStaleResponse{}
	for _, st := range stale {
		v := &mokshav1.StaleDataVersion{DataVersion: dataVersionMessage(st.DataVersion), Step: st.Step, Reasons: st.Reasons}
		if h, err := s.store.GetRun(ctx, st.DataVersion.RunID); err == nil {
			v.Run = runRef(h)
		}
		out.Stale = append(out.Stale, v)
	}
	return connect.NewResponse(out), nil
}

func runRef(h state.Run) *mokshav1.RunRef {
	return &mokshav1.RunRef{Id: h.ID, Name: h.Name, ParamsJson: runMessage(h).ParamsJson, Tenant: h.Tenant}
}

func dataVersionMessage(p state.DataVersion) *mokshav1.DataVersion {
	out := &mokshav1.DataVersion{
		Id: p.ID, Tenant: p.Tenant, Dataset: p.Dataset, Partition: p.Partition, Location: p.Location,
		ContentHash: p.ContentHash, Fingerprint: p.Fingerprint, RunId: p.RunID, StepSeq: int32(p.StepSeq),
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

func lineageMessage(l state.Lineage) *mokshav1.Lineage {
	out := &mokshav1.Lineage{}
	for _, p := range l.DataVersions {
		out.DataVersions = append(out.DataVersions, dataVersionMessage(p))
	}
	for _, e := range l.Edges {
		out.Edges = append(out.Edges, &mokshav1.LineageEdge{DataVersionId: e.DataVersionID, InputId: e.InputID, Name: e.Name})
	}
	return out
}
