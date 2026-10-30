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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

// runServerCmd runs `hubwagen server` in this process with a config whose
// one worker is a shell script that writes what it got to out, and stops
// on SIGTERM. It returns the listen address and the command's result.
func runServerCmd(t *testing.T, out string, args ...string) (string, <-chan error) {
	t.Helper()
	return runServerCmdWith(t, out, "", args...)
}

// runServerCmdWith is runServerCmd with more settings for the worker group
// (YAML lines indented for it).
func runServerCmdWith(t *testing.T, out, group string, args ...string) (string, <-chan error) {
	t.Helper()
	listen := freeListen(t)
	script := fmt.Sprintf(`trap 'echo stopped >> %[1]s; exit 0' TERM; echo "$HUBWAGEN_SERVER $HUBWAGEN_WORKER_ID $HUBWAGEN_DOCKS $HUBWAGEN_LIFTS" >> %[1]s; echo hello; while :; do sleep 0.05; done`, out)
	return listen, runServerConfig(t, listen, fmt.Sprintf("workers:\n  - name: sh\n    command: [/bin/sh, -c, %q]\n%s", script, group), args...)
}

// runServerConfig runs `hubwagen server` on listen with rest (YAML) after
// the config's header, until healthy.
func runServerConfig(t *testing.T, listen, rest string, args ...string) <-chan error {
	t.Helper()
	dir := t.TempDir()
	cfg := fmt.Sprintf("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\ndata_dir: data\nserver:\n  listen: %s\n%s", listen, rest)
	path := filepath.Join(dir, "hubwagen.yaml")
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

func TestServerStartsAndStopsTheConfigsWorkers(t *testing.T) {
	out := filepath.Join(t.TempDir(), "worker.txt")
	listen, done := runServerCmd(t, out)
	read := func() string { b, _ := os.ReadFile(out); return string(b) }
	require.Eventually(t, func() bool { return read() != "" }, 10*time.Second, 20*time.Millisecond)
	assert.Equal(t, "http://"+listen+" sh-1 default 4\n", read())

	stopServerCmd(t, done)
	assert.True(t, strings.HasSuffix(read(), "stopped\n"), "the worker got SIGTERM before the server stopped: %q", read())
}

func TestServerWithoutWorkers(t *testing.T) {
	out := filepath.Join(t.TempDir(), "worker.txt")
	_, done := runServerCmd(t, out, "--no-workers")
	time.Sleep(300 * time.Millisecond)
	stopServerCmd(t, done)
	_, err := os.Stat(out)
	assert.True(t, os.IsNotExist(err), "no worker started")
}

func TestServerStartsAScaledPoolAtItsMin(t *testing.T) {
	out := filepath.Join(t.TempDir(), "worker.txt")
	listen, done := runServerCmdWith(t, out, "    min: 1\n    max: 3\n    docks: [gpu, default]\n    lifts: 2\n")
	read := func() string { b, _ := os.ReadFile(out); return string(b) }
	require.Eventually(t, func() bool { return read() != "" }, 10*time.Second, 20*time.Millisecond)
	time.Sleep(3 * time.Second) // a scaler pass: nothing waits, so no more start
	assert.Equal(t, "http://"+listen+" sh-1 gpu,default 2\n", read())
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
		"kubernetes: {api_url: %q, namespace: jobs}\nworkers:\n  - {name: k8s, launcher: kubernetes, deployment: revenue-workers, min: 1, max: 4}\n", api.URL))
	require.Eventually(t, func() bool { mu.Lock(); defer mu.Unlock(); return len(patches) > 0 }, 10*time.Second, 20*time.Millisecond)
	stopServerCmd(t, done)
	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, `PATCH /apis/apps/v1/namespaces/jobs/deployments/revenue-workers/scale {"spec":{"replicas":1}}`, patches[0], "min")
}

func TestServerRunsAContainerPool(t *testing.T) {
	bin, logf := t.TempDir(), filepath.Join(t.TempDir(), "docker.log")
	require.NoError(t, os.WriteFile(filepath.Join(bin, "docker"), []byte(`#!/bin/sh
case "$1" in
run) echo "run $HUBWAGEN_SERVER $HUBWAGEN_WORKER_ID $HUBWAGEN_DATA | $*" >> `+logf+`
     trap 'exit 0' TERM; while :; do sleep 0.05; done ;;
esac
`), 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	listen := freeListen(t)
	done := runServerConfig(t, listen, "workers:\n  - {name: c, launcher: container, runtime: docker, image: acme/revenue:1, min: 1, max: 2}\n")
	read := func() string { b, _ := os.ReadFile(logf); return string(b) }
	require.Eventually(t, func() bool { return read() != "" }, 10*time.Second, 20*time.Millisecond)
	stopServerCmd(t, done)
	_, port, _ := net.SplitHostPort(listen)
	assert.True(t, strings.HasPrefix(read(), "run http://host.docker.internal:"+port+" c-1 remote | run --rm --name hubwagen-c-1 "), read())
	assert.True(t, strings.HasSuffix(read(), " acme/revenue:1\n"), read())
}
