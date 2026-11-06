package cmd

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/durationpb"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
)

// freeListen returns a loopback address with a port nothing listens on.
func freeListen(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

// deployRecorder deploys project revenue in place to the server on
// listen, with pool as its one pool. Its workers are a fake python3 that
// writes how it was started to out, and when it got SIGTERM.
func deployRecorder(t *testing.T, listen, out string, pool *mokshav1.DeployedPool) {
	t.Helper()
	bin := t.TempDir()
	script := fmt.Sprintf("#!/bin/sh\ntrap 'echo stopped >> %[1]s; exit 0' TERM\necho \"$MOKSHA_SERVER $MOKSHA_WORKER_ID $MOKSHA_QUEUES $MOKSHA_STEPS\" >> %[1]s\nwhile :; do sleep 0.05; done\n", out)
	require.NoError(t, os.WriteFile(filepath.Join(bin, "python3"), []byte(script), 0o755))
	if pool.IdleAfter == nil {
		pool.IdleAfter = durationpb.New(time.Minute)
	}
	_, err := mokshav1connect.NewDeployServiceClient(http.DefaultClient, "http://"+listen).Deploy(context.Background(), connect.NewRequest(&mokshav1.DeployRequest{
		Project: "revenue", Dir: t.TempDir(), Python: filepath.Join(bin, "python3"),
		Build: &mokshav1.RegisterBuildRequest{BuildId: "b1", Pipelines: []string{"daily"}},
		Pools: []*mokshav1.DeployedPool{pool}}))
	require.NoError(t, err)
}

// runServerConfig runs `moksha server` on listen with rest (YAML) after
// the config's header, until healthy.
func runServerConfig(t *testing.T, listen, rest string, args ...string) <-chan error {
	t.Helper()
	dir := t.TempDir()
	cfg := fmt.Sprintf("apiVersion: moksha.dev/v1alpha1\nkind: Config\ndata_dir: data\nserver:\n  listen: %s\n%s", listen, rest)
	path := filepath.Join(dir, "moksha.yaml")
	require.NoError(t, os.WriteFile(path, []byte(cfg), 0o644))
	noWorkers = false
	rootCmd.SetArgs(append([]string{"server", "--config", path}, args...))
	done := make(chan error, 1)
	go func() { done <- rootCmd.ExecuteContext(context.Background()) }()
	require.Eventually(t, func() bool {
		res, err := http.Get("http://" + listen + "/healthz")
		if err != nil {
			return false
		}
		res.Body.Close()
		return res.StatusCode == http.StatusOK
	}, 10*time.Second, 20*time.Millisecond)
	return done
}

func stopServerCmd(t *testing.T, done <-chan error) {
	t.Helper()
	require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM)) // the command catches it
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("server didn't stop")
	}
}

func TestServerStopsItsWorkersFirst(t *testing.T) {
	out, listen := filepath.Join(t.TempDir(), "worker.txt"), freeListen(t)
	done := runServerConfig(t, listen, "")
	deployRecorder(t, listen, out, &mokshav1.DeployedPool{Name: "default", Queues: []string{"default"}, Min: 1, Max: 1, Steps: 4})
	read := func() string { b, _ := os.ReadFile(out); return string(b) }
	require.Eventually(t, func() bool { return read() != "" }, 15*time.Second, 20*time.Millisecond)
	assert.Equal(t, "http://"+listen+" revenue-default-v1-1 default 4\n", read())

	stopServerCmd(t, done)
	assert.True(t, strings.HasSuffix(read(), "stopped\n"), "the worker got SIGTERM before the server stopped: %q", read())
}

func TestServerWithoutWorkers(t *testing.T) {
	out, listen := filepath.Join(t.TempDir(), "worker.txt"), freeListen(t)
	done := runServerConfig(t, listen, "", "--no-workers")
	deployRecorder(t, listen, out, &mokshav1.DeployedPool{Name: "default", Queues: []string{"default"}, Min: 1, Max: 1, Steps: 1})
	time.Sleep(3 * time.Second) // a scaler pass would have started it
	stopServerCmd(t, done)
	_, err := os.Stat(out)
	assert.True(t, os.IsNotExist(err), "no worker started")
}

func TestServerStartsAScaledPoolAtItsMin(t *testing.T) {
	out, listen := filepath.Join(t.TempDir(), "worker.txt"), freeListen(t)
	done := runServerConfig(t, listen, "")
	deployRecorder(t, listen, out, &mokshav1.DeployedPool{Name: "default", Queues: []string{"gpu", "default"}, Min: 1, Max: 3, Steps: 2})
	read := func() string { b, _ := os.ReadFile(out); return string(b) }
	require.Eventually(t, func() bool { return read() != "" }, 15*time.Second, 20*time.Millisecond)
	time.Sleep(3 * time.Second) // a scaler pass: nothing waits, so no more start
	assert.Equal(t, "http://"+listen+" revenue-default-v1-1 gpu,default 2\n", read())
	stopServerCmd(t, done)
}

func TestServerScalesAKubernetesDeployment(t *testing.T) {
	var mu sync.Mutex
	var patches []string
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		patches = append(patches, r.Method+" "+r.URL.Path+" "+string(b))
		mu.Unlock()
		_, _ = w.Write([]byte(`{}`))
	}))
	defer api.Close()
	done := runServerConfig(t, freeListen(t), fmt.Sprintf(
		"kubernetes: {api_url: %q, namespace: jobs}\npools:\n  - {name: k8s, launcher: kubernetes, deployment: revenue-workers, min: 1, max: 4}\n", api.URL))
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(patches) > 0 }, 10*time.Second, 20*time.Millisecond)
	stopServerCmd(t, done)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, `PATCH /apis/apps/v1/namespaces/jobs/deployments/revenue-workers/scale {"spec":{"replicas":1}}`, patches[0], "min")
}

// A project deployed with an image runs as containers of it.
func TestServerRunsADeployedImageAsContainers(t *testing.T) {
	bin, logf := t.TempDir(), filepath.Join(t.TempDir(), "docker.log")
	require.NoError(t, os.WriteFile(filepath.Join(bin, "docker"), []byte(`#!/bin/sh
case "$1" in
run) echo "run $MOKSHA_SERVER $MOKSHA_WORKER_ID $MOKSHA_DATA | $*" >> `+logf+`
     trap 'exit 0' TERM; while :; do sleep 0.05; done ;;
esac
`), 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	listen := freeListen(t)
	done := runServerConfig(t, listen, "projects: {runtime: docker}\n")
	_, err := mokshav1connect.NewDeployServiceClient(http.DefaultClient, "http://"+listen).Deploy(context.Background(), connect.NewRequest(&mokshav1.DeployRequest{
		Project: "revenue", Image: "acme/revenue:1",
		Build: &mokshav1.RegisterBuildRequest{BuildId: "b1", Pipelines: []string{"daily"}},
		Pools: []*mokshav1.DeployedPool{{Name: "c", Queues: []string{"default"}, Min: 1, Max: 2, Steps: 1, IdleAfter: durationpb.New(time.Minute)}}}))
	require.NoError(t, err)
	read := func() string { b, _ := os.ReadFile(logf); return string(b) }
	require.Eventually(t, func() bool { return read() != "" }, 15*time.Second, 20*time.Millisecond)
	stopServerCmd(t, done)
	_, port, _ := net.SplitHostPort(listen)
	assert.True(t, strings.HasPrefix(read(), "run http://host.docker.internal:"+port+" revenue-c-v1-1 remote | run --rm --name moksha-revenue-c-v1-1 "), read())
	assert.True(t, strings.HasSuffix(read(), " acme/revenue:1\n"), read())
}

// A server runs what is deployed to it: here in place, with a fake python3
// that records how each worker was started and when it got SIGTERM.
func TestServerRunsDeployedProjects(t *testing.T) {
	bin, logf, dir := t.TempDir(), filepath.Join(t.TempDir(), "workers.log"), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "python3"), []byte(`#!/bin/sh
echo "start $MOKSHA_WORKER_ID $MOKSHA_QUEUES $MOKSHA_STEPS $GREETING $(pwd)" >> `+logf+`
trap 'echo "stop $MOKSHA_WORKER_ID" >> `+logf+`; exit 0' TERM
while :; do sleep 0.05; done
`), 0o755))
	listen := freeListen(t)
	done := runServerConfig(t, listen, "")
	c := mokshav1connect.NewDeployServiceClient(http.DefaultClient, "http://"+listen)
	deploy := func(build string) {
		t.Helper()
		_, err := c.Deploy(context.Background(), connect.NewRequest(&mokshav1.DeployRequest{
			Project: "revenue", Dir: dir, Python: filepath.Join(bin, "python3"), Env: map[string]string{"GREETING": "hi"},
			Build: &mokshav1.RegisterBuildRequest{BuildId: build, Pipelines: []string{"daily"}},
			Pools: []*mokshav1.DeployedPool{{Name: "default", Queues: []string{"default"}, Min: 1, Max: 2, Steps: 3, IdleAfter: durationpb.New(time.Minute)}}}))
		require.NoError(t, err)
	}
	read := func() string { b, _ := os.ReadFile(logf); return string(b) }
	wait := func(s string) {
		t.Helper()
		require.Eventually(t, func() bool { return strings.Contains(read(), s) }, 15*time.Second, 50*time.Millisecond, read())
	}
	real, _ := filepath.EvalSymlinks(dir)

	deploy("b1")
	wait("start revenue-default-v1-1 default 3 hi " + real)
	deploy("b2") // the rollout: v2 starts, v1 stops
	wait("start revenue-default-v2-1")
	wait("stop revenue-default-v1-1")
	_, err := c.DeleteProject(context.Background(), connect.NewRequest(&mokshav1.DeleteProjectRequest{Project: "revenue", By: "test"}))
	require.NoError(t, err)
	wait("stop revenue-default-v2-1")
	stopServerCmd(t, done)
}

// A deployed project's secret reaches its workers' environment, read from
// the server's secrets directory when the worker starts.
func TestServerGivesDeployedWorkersTheirSecrets(t *testing.T) {
	bin, logf, dir, secrets := t.TempDir(), filepath.Join(t.TempDir(), "workers.log"), t.TempDir(), t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, "python3"), []byte(`#!/bin/sh
echo "start $MOKSHA_WORKER_ID db=$DB_PASSWORD args=$*" >> `+logf+`
trap 'exit 0' TERM
while :; do sleep 0.05; done
`), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(secrets, "revenue-db"), []byte("s3cr3t\n"), 0o600))
	listen := freeListen(t)
	done := runServerConfig(t, listen, fmt.Sprintf("projects: {secrets_dir: %q}\n", secrets))
	c := mokshav1connect.NewDeployServiceClient(http.DefaultClient, "http://"+listen)
	req := &mokshav1.DeployRequest{Project: "revenue", Dir: dir, Python: filepath.Join(bin, "python3"),
		Secrets: map[string]string{"DB_PASSWORD": "revenue-db"},
		Build:   &mokshav1.RegisterBuildRequest{BuildId: "b1", Pipelines: []string{"daily"}},
		Pools:   []*mokshav1.DeployedPool{{Name: "default", Queues: []string{"default"}, Min: 1, Max: 1, Steps: 1, IdleAfter: durationpb.New(time.Minute)}}}
	_, err := c.Deploy(context.Background(), connect.NewRequest(req))
	require.NoError(t, err)
	read := func() string { b, _ := os.ReadFile(logf); return string(b) }
	require.Eventually(t, func() bool { return strings.Contains(read(), "start revenue-default-v1-1") }, 15*time.Second, 50*time.Millisecond)
	assert.Contains(t, read(), "db=s3cr3t args=-m moksha.worker\n", "the value in its environment, not its arguments")

	// A secret the server doesn't have: refused at deploy.
	req.Secrets["DB_PASSWORD"] = "nope"
	_, err = c.Deploy(context.Background(), connect.NewRequest(req))
	assert.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	stopServerCmd(t, done)
}
