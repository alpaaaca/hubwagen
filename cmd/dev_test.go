package cmd

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/api/gen/moksha/v1/mokshav1connect"
)

// runDevCmd runs `moksha dev` in this process with args, until healthy;
// what it prints goes to the file out.
func runDevCmd(t *testing.T, listen, out string, args ...string) <-chan error {
	t.Helper()
	f, err := os.OpenFile(out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	require.NoError(t, err)
	stdout := os.Stdout
	os.Stdout = f
	t.Cleanup(func() { os.Stdout = stdout; f.Close() })
	noWorkers, devEvents = false, false
	rootCmd.SetArgs(append([]string{"dev", "--listen", listen}, args...))
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

// In a project's directory, dev deploys the project in place, deploys
// again when its code changes, and not again when it starts with the
// code as it was.
func TestDevDeploysTheProjectAndRedeploysOnChange(t *testing.T) {
	dir, bin := t.TempDir(), t.TempDir()
	logf, out := filepath.Join(t.TempDir(), "workers.log"), filepath.Join(t.TempDir(), "dev.out")
	// The worker: --build-json makes the build from code.py; else it runs
	// until SIGTERM, writing when it starts and stops.
	python := filepath.Join(bin, "python3")
	require.NoError(t, os.WriteFile(python, []byte(`#!/bin/sh
if [ "$3" = "--build-json" ]; then
  b=$(cat code.py)
  echo "{\"buildId\": \"$b\", \"pipelines\": [\"daily\"], \"stepVersions\": {\"load\": \"py-$b\"}}"
  exit 0
fi
echo "start $MOKSHA_WORKER_ID" >> `+logf+`
trap 'echo "stop $MOKSHA_WORKER_ID" >> `+logf+`; exit 0' TERM
while :; do sleep 0.05; done
`), 0o755))
	project := filepath.Join(dir, "moksha.yaml")
	require.NoError(t, os.WriteFile(project, []byte("apiVersion: moksha.dev/v1alpha1\nkind: Project\nname: revenue\npools: [{name: default, min: 1, max: 1}]\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "code.py"), []byte("b1"), 0o644))
	devWatchEvery = 20 * time.Millisecond
	t.Cleanup(func() { devWatchEvery = time.Second })

	read := func(f string) string { b, _ := os.ReadFile(f); return string(b) }
	wait := func(f, s string) {
		t.Helper()
		require.Eventually(t, func() bool { return strings.Contains(read(f), s) }, 15*time.Second, 20*time.Millisecond, read(f))
	}
	listen := freeListen(t)
	args := []string{"--config", project, "--python", python}
	done := runDevCmd(t, listen, out, args...)
	wait(out, "project revenue v1 deployed (build b1, in place): 1 step, 1 pipeline; its workers start")
	wait(logf, "start revenue-default-v1-1")
	_, err := os.Stat(filepath.Join(dir, ".moksha", "state.db"))
	assert.NoError(t, err, "the data is next to the project file")

	require.NoError(t, os.WriteFile(filepath.Join(dir, "code.py"), []byte("b2"), 0o644))
	wait(out, "project revenue v2 deployed (build b2, in place): load runs again (its code changed)")
	wait(logf, "start revenue-default-v2-1")
	wait(logf, "stop revenue-default-v1-1")
	stopServerCmd(t, done)

	done = runDevCmd(t, listen, out, args...)
	wait(out, "project revenue v2: deployed already (build b2)")
	ps, err := mokshav1connect.NewDeployServiceClient(http.DefaultClient, "http://"+listen).ListProjects(context.Background(),
		connect.NewRequest(&mokshav1.ListProjectsRequest{}))
	require.NoError(t, err)
	require.Len(t, ps.Msg.Projects, 1)
	assert.Equal(t, int32(2), ps.Msg.Projects[0].Version)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "code.py"), []byte("b3 is not json\""), 0o644))
	wait(out, "can't deploy moksha.yaml")
	stopServerCmd(t, done)
}
