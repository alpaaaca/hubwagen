package launch

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/config"
)

// apiServer answers scale PATCHes like the Kubernetes API server, recording
// them.
type apiServer struct {
	got  []string // method path auth body
	fail bool
}

func (a *apiServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	a.got = append(a.got, r.Method+" "+r.URL.Path+" "+r.Header.Get("Content-Type")+" "+r.Header.Get("Authorization")+" "+string(b))
	if a.fail {
		http.Error(w, `{"kind":"Status","message":"deployments.apps \"x\" is forbidden"}`, http.StatusForbidden)
		return
	}
	_, _ = w.Write([]byte(`{"kind":"Scale"}`))
}

func TestKubeScalesADeploymentThroughAProxy(t *testing.T) {
	api := &apiServer{}
	srv := httptest.NewServer(api)
	defer srv.Close()
	k, err := NewKube(config.Kubernetes{APIURL: srv.URL + "/"}, t.TempDir())
	require.NoError(t, err)
	require.NoError(t, k.Scale(context.Background(), "", "revenue-workers", 3))
	require.NoError(t, k.Scale(context.Background(), "jobs", "gpu", 0))
	assert.Equal(t, []string{
		`PATCH /apis/apps/v1/namespaces/default/deployments/revenue-workers/scale application/merge-patch+json  {"spec":{"replicas":3}}`,
		`PATCH /apis/apps/v1/namespaces/jobs/deployments/gpu/scale application/merge-patch+json  {"spec":{"replicas":0}}`,
	}, api.got)

	api.fail = true
	err = k.Scale(context.Background(), "", "x", 1)
	assert.ErrorContains(t, err, "kubernetes: scale deployment default/x: 403 Forbidden")
	assert.ErrorContains(t, err, "is forbidden")
}

func TestKubeInAPodUsesItsServiceAccount(t *testing.T) {
	api := &apiServer{}
	srv := httptest.NewTLSServer(api)
	defer srv.Close()
	sa := t.TempDir()
	ca := pemOf(t, srv)
	require.NoError(t, os.WriteFile(filepath.Join(sa, "ca.crt"), ca, 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(sa, "token"), []byte("tok1\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(sa, "namespace"), []byte("hubwagen"), 0o600))
	host, port, _ := netSplit(srv.URL)
	t.Setenv("KUBERNETES_SERVICE_HOST", host)
	t.Setenv("KUBERNETES_SERVICE_PORT", port)

	k, err := NewKube(config.Kubernetes{}, sa)
	require.NoError(t, err)
	assert.Equal(t, "hubwagen", k.Namespace)
	require.NoError(t, k.Scale(context.Background(), "", "w", 2))
	require.NoError(t, os.WriteFile(filepath.Join(sa, "token"), []byte("tok2"), 0o600)) // rotated
	require.NoError(t, k.Scale(context.Background(), "", "w", 1))
	assert.Contains(t, api.got[0], "Bearer tok1")
	assert.Contains(t, api.got[1], "Bearer tok2")

	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	_, err = NewKube(config.Kubernetes{}, sa)
	assert.ErrorContains(t, err, "not in a cluster; set kubernetes.api_url")
}
