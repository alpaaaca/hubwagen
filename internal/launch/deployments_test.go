package launch

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/moksha/internal/config"
	"github.com/alper/moksha/internal/launch/kubefake"
)

func TestDeploymentName(t *testing.T) {
	assert.Equal(t, "moksha-revenue-default-v3", DeploymentName("revenue", "default", 3))
	assert.Equal(t, "moksha-my-proj-gpu-pool-v1", DeploymentName("my_proj", "gpu_pool", 1), "_ isn't allowed in a name")
	long := DeploymentName(strings.Repeat("p", 40), strings.Repeat("q", 20), 12)
	assert.LessOrEqual(t, len(long), 52)
	assert.NotEqual(t, long, DeploymentName(strings.Repeat("p", 40), strings.Repeat("q", 20), 13), "versions stay apart")
}

func TestKubeCreatesListsAndDeletesWorkerDeployments(t *testing.T) {
	api := kubefake.New()
	srv := httptest.NewServer(api)
	defer srv.Close()
	k, err := NewKube(config.Kubernetes{APIURL: srv.URL, Namespace: "jobs"}, t.TempDir())
	require.NoError(t, err)
	ctx := context.Background()
	d := WorkerDeployment{Name: "moksha-revenue-default-v1", Instance: "moksha", Project: "revenue", Pool: "revenue-default-v1",
		Image: "ghcr.io/acme/revenue@sha256:ab", Replicas: 2, Server: "http://moksha.jobs.svc:7070", Queues: []string{"default", "gpu"},
		Steps: 3, Env: map[string]string{"LOG_LEVEL": "info"}, Secrets: map[string]string{"DB_URL": "revenue-db"}, PullSecrets: []string{"ghcr"}}
	require.NoError(t, k.CreateDeployment(ctx, "", d))
	require.NoError(t, k.CreateDeployment(ctx, "", d), "it exists: a version's Deployment doesn't change")

	got, err := json.Marshal(api.Deployment("jobs", d.Name))
	require.NoError(t, err)
	assert.JSONEq(t, `{
	  "apiVersion": "apps/v1", "kind": "Deployment",
	  "metadata": {"name": "moksha-revenue-default-v1", "labels": {"app.kubernetes.io/managed-by": "moksha",
	    "moksha.dev/instance": "moksha", "moksha.dev/project": "revenue", "moksha.dev/pool": "revenue-default-v1"}},
	  "spec": {
	    "replicas": 2,
	    "selector": {"matchLabels": {"moksha.dev/instance": "moksha", "moksha.dev/pool": "revenue-default-v1"}},
	    "template": {
	      "metadata": {"labels": {"app.kubernetes.io/managed-by": "moksha", "moksha.dev/instance": "moksha",
	        "moksha.dev/project": "revenue", "moksha.dev/pool": "revenue-default-v1"}},
	      "spec": {
	        "automountServiceAccountToken": false,
	        "terminationGracePeriodSeconds": 600,
	        "imagePullSecrets": [{"name": "ghcr"}],
	        "containers": [{"name": "worker", "image": "ghcr.io/acme/revenue@sha256:ab", "env": [
	          {"name": "MOKSHA_SERVER", "value": "http://moksha.jobs.svc:7070"},
	          {"name": "MOKSHA_WORKER_ID", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
	          {"name": "MOKSHA_QUEUES", "value": "default,gpu"},
	          {"name": "MOKSHA_STEPS", "value": "3"},
	          {"name": "MOKSHA_DATA", "value": "remote"},
	          {"name": "LOG_LEVEL", "value": "info"},
	          {"name": "DB_URL", "valueFrom": {"secretKeyRef": {"name": "revenue-db", "key": "value"}}}
	        ]}]
	      }
	    }
	  }
	}`, string(got))

	api.Put("jobs", map[string]any{"metadata": map[string]any{"name": "theirs", "labels": map[string]any{"app": "x"}}, "spec": map[string]any{}})
	api.Put("jobs", map[string]any{"metadata": map[string]any{"name": "other-moksha", "labels": map[string]any{
		"app.kubernetes.io/managed-by": "moksha", "moksha.dev/instance": "staging"}}, "spec": map[string]any{}})
	names, err := k.WorkerDeployments(ctx, "", "moksha")
	require.NoError(t, err)
	assert.Equal(t, []string{"moksha-revenue-default-v1"}, names, "only this instance's")

	require.NoError(t, k.Scale(ctx, "", d.Name, 4))
	assert.Equal(t, 4, kubefake.Replicas(api.Deployment("jobs", d.Name)))
	require.NoError(t, k.DeleteDeployment(ctx, "", d.Name))
	require.NoError(t, k.DeleteDeployment(ctx, "", d.Name), "gone already is fine")
	assert.Equal(t, []string{"other-moksha", "theirs"}, api.Names("jobs"))
}
