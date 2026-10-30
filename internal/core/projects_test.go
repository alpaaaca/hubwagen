package core

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/state"
)

func deployReq(project, build string, hauls ...string) DeployRequest {
	return DeployRequest{Project: project, Image: "acme/" + project + ":" + build, By: "ana",
		Build: BuildRequest{ID: build, Hauls: hauls, Lifts: map[string]string{"load": build}},
		Pools: []state.DeployedPool{{Name: "default", Docks: []string{DefaultDock}, Min: 0, Max: 3, Lifts: 2, IdleAfter: time.Minute}}}
}

func TestDeployRegistersTheBuildAndCountsVersions(t *testing.T) {
	h := newHarness(t)
	d, err := h.core.Deploy(h.ctx, deployReq("revenue", "b1", "weekly"))
	require.NoError(t, err)
	assert.Equal(t, 1, d.Version)
	assert.Equal(t, []string{"weekly"}, d.Hauls)

	// Its pipeline starts with no worker yet: the deploy registered the build.
	_, err = h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: "weekly"})
	require.NoError(t, err)

	d, err = h.core.Deploy(h.ctx, deployReq("revenue", "b2", "weekly"))
	require.NoError(t, err)
	assert.Equal(t, 2, d.Version)
	ps, err := h.core.Projects(h.ctx)
	require.NoError(t, err)
	require.Len(t, ps, 1)
	assert.Equal(t, "b2", ps[0].BuildID)

	evs, err := h.store.EventsSince(h.ctx, 0, "", 1000)
	require.NoError(t, err)
	var deployed int
	for _, e := range evs {
		if e.Type == state.EventProjectDeployed {
			deployed++
		}
	}
	assert.Equal(t, 2, deployed, "recorded as events")
}

func TestDeployRefusesBadRequestsAndClashes(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.Deploy(h.ctx, deployReq("revenue", "b1", "weekly"))
	require.NoError(t, err)

	_, err = h.core.Deploy(h.ctx, deployReq("other", "b9", "weekly"))
	assert.ErrorIs(t, err, ErrConflict)
	assert.ErrorContains(t, err, "pipeline weekly belongs to project revenue")

	bad := deployReq("Bad Name", "b2", "x")
	_, err = h.core.Deploy(h.ctx, bad)
	assert.ErrorIs(t, err, ErrInvalid)
	bad = deployReq("p", "b3", "x")
	bad.Dir = "/srv/p"
	_, err = h.core.Deploy(h.ctx, bad)
	assert.ErrorContains(t, err, "give an image or a directory")
	bad = deployReq("p", "b4", "x")
	bad.Pools[0].Min, bad.Pools[0].Max = 3, 2
	_, err = h.core.Deploy(h.ctx, bad)
	assert.ErrorContains(t, err, "want 0 ≤ min ≤ max")
}

func TestDeleteProject(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.Deploy(h.ctx, deployReq("revenue", "b1", "weekly"))
	require.NoError(t, err)
	run, err := h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: "weekly"})
	require.NoError(t, err)

	_, err = h.core.DeleteProject(h.ctx, "revenue", "ana", false)
	assert.ErrorIs(t, err, ErrConflict)
	assert.ErrorContains(t, err, "1 open runs")

	n, err := h.core.DeleteProject(h.ctx, "revenue", "ana", true)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	got, err := h.store.GetHaul(h.ctx, run.HaulID)
	require.NoError(t, err)
	assert.Equal(t, state.HaulCancelled, got.Status)

	_, err = h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: "weekly"})
	assert.ErrorIs(t, err, ErrUnknownHaul)
	assert.ErrorContains(t, err, "belongs to project revenue, which was deleted")
	_, err = h.core.DeleteProject(h.ctx, "revenue", "ana", false)
	assert.ErrorIs(t, err, ErrNotFound)

	// Deploying it again brings it back, as the next version.
	d, err := h.core.Deploy(h.ctx, deployReq("revenue", "b2", "weekly"))
	require.NoError(t, err)
	assert.Equal(t, 2, d.Version)
	_, err = h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: "weekly"})
	assert.NoError(t, err)
}

func TestProjectsSurviveARestart(t *testing.T) {
	h := newHarness(t)
	_, err := h.core.Deploy(h.ctx, deployReq("revenue", "b1", "weekly"))
	require.NoError(t, err)
	_, err = h.core.Deploy(h.ctx, deployReq("gone", "b2", "old"))
	require.NoError(t, err)
	_, err = h.core.DeleteProject(h.ctx, "gone", "ana", false)
	require.NoError(t, err)
	h.restart()

	ps, err := h.core.Projects(h.ctx)
	require.NoError(t, err)
	require.Len(t, ps, 2)
	assert.Equal(t, "gone", ps[0].Project)
	assert.NotNil(t, ps[0].DeletedAt)
	assert.Equal(t, "acme/revenue:b1", ps[1].Image)
	_, err = h.core.StartHaul(h.ctx, StartHaulRequest{Tenant: "t", Name: "old"})
	assert.ErrorContains(t, err, "which was deleted")
	d, err := h.core.Deploy(h.ctx, deployReq("revenue", "b3", "weekly"))
	require.NoError(t, err)
	assert.Equal(t, 2, d.Version, "versions go on after a restart")
}

func TestDeploySecretReferences(t *testing.T) {
	h := newHarness(t)
	req := deployReq("revenue", "b1", "weekly")
	req.Secrets = map[string]string{"DB_URL": "revenue-db"}
	d, err := h.core.Deploy(h.ctx, req)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"DB_URL": "revenue-db"}, d.Secrets)

	req = deployReq("revenue", "b2", "weekly")
	req.Env = map[string]string{"DB_URL": "x"}
	req.Secrets = map[string]string{"DB_URL": "revenue-db"}
	_, err = h.core.Deploy(h.ctx, req)
	assert.ErrorContains(t, err, "not also in env")
	req.Env, req.Secrets = nil, map[string]string{"DB_URL": "../x"}
	_, err = h.core.Deploy(h.ctx, req)
	assert.ErrorIs(t, err, ErrInvalid)
}
