package cmd

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
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
	dir := t.TempDir()
	listen := freeListen(t)
	script := fmt.Sprintf(`trap 'echo stopped >> %[1]s; exit 0' TERM; echo "$HUBWAGEN_SERVER $HUBWAGEN_WORKER_ID" >> %[1]s; echo hello; while :; do sleep 0.05; done`, out)
	cfg := fmt.Sprintf("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\ndata_dir: data\nserver:\n  listen: %s\nworkers:\n  - name: sh\n    command: [/bin/sh, -c, %q]\n", listen, script)
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
	return listen, done
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
	assert.Equal(t, "http://"+listen+" sh-1\n", read())

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
