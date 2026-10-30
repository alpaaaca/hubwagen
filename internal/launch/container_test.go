package launch

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/supervise"
)

func TestContainerServerURL(t *testing.T) {
	docker := config.Worker{Runtime: "docker"}
	podman := config.Worker{Runtime: "podman"}
	assert.Equal(t, "http://host.docker.internal:7070", ContainerServerURL(docker, "http://127.0.0.1:7070"))
	assert.Equal(t, "http://host.containers.internal:7070", ContainerServerURL(podman, "http://127.0.0.1:7070"))
	assert.Equal(t, "http://10.0.0.5:7070", ContainerServerURL(docker, "http://10.0.0.5:7070"), "reachable as is")
	docker.ServerURL = "http://hubwagen:7070"
	assert.Equal(t, "http://hubwagen:7070", ContainerServerURL(docker, "http://127.0.0.1:7070"))
}

func TestContainerCommand(t *testing.T) {
	w := config.Worker{Name: "py", Launcher: config.LauncherContainer, Runtime: "docker", Image: "acme/revenue:1",
		Command: []string{"python3", "-m", "hubwagen.worker"}, Dir: "/srv"}
	p := Container(w, "py-2", "http://127.0.0.1:7070", "/srv/.hubwagen", []string{"HUBWAGEN_WORKER_ID=py-2", "SECRET=s3cr3t"})
	assert.Equal(t, []string{"docker", "run", "--rm", "--name", "hubwagen-py-2", "--label", "hubwagen.pool=py",
		"--label", "hubwagen.data_dir=/srv/.hubwagen",
		"--add-host=host.docker.internal:host-gateway",
		"-e", "HUBWAGEN_DATA", "-e", "HUBWAGEN_SERVER", "-e", "HUBWAGEN_WORKER_ID", "-e", "SECRET",
		"acme/revenue:1", "python3", "-m", "hubwagen.worker"}, p.Command)
	assert.NotContains(t, strings.Join(p.Command, " "), "s3cr3t", "values stay off the command line")
	assert.Contains(t, p.Env, "HUBWAGEN_SERVER=http://host.docker.internal:7070")
	assert.Contains(t, p.Env, "HUBWAGEN_DATA=remote")
	assert.Equal(t, []string{"docker", "rm", "-f", "hubwagen-py-2"}, p.Cleanup)
}

// A fake docker on PATH: run records its arguments and two variables and
// waits for SIGTERM; rm records what it removes.
func TestContainerRunsUnderASupervisor(t *testing.T) {
	bin, logf := t.TempDir(), filepath.Join(t.TempDir(), "docker.log")
	script := `#!/bin/sh
case "$1" in
run) echo "run $* | $HUBWAGEN_SERVER $HUBWAGEN_WORKER_ID" >> ` + logf + `
     trap 'echo "term" >> ` + logf + `; exit 0' TERM
     echo "worker up"
     while :; do sleep 0.05; done ;;
rm)  echo "rm $3" >> ` + logf + ` ;;
esac
`
	require.NoError(t, os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))

	w := config.Worker{Name: "py", Launcher: config.LauncherContainer, Runtime: "docker", Image: "img", Dir: t.TempDir()}
	ctx, cancel := context.WithCancel(context.Background())
	g := supervise.NewGroup(ctx, supervise.Options{Out: io.Discard})
	g.Start(Container(w, "py-1", "http://127.0.0.1:7070", "/data", []string{"HUBWAGEN_WORKER_ID=py-1"}))
	read := func() string { b, _ := os.ReadFile(logf); return string(b) }
	require.Eventually(t, func() bool { return strings.Contains(read(), "run ") }, 5*time.Second, 10*time.Millisecond)
	g.Stop("py-1")
	cancel()
	g.Wait()

	lines := strings.Split(strings.TrimSpace(read()), "\n")
	require.Len(t, lines, 4, read())
	assert.Equal(t, "rm hubwagen-py-1", lines[0], "a leftover container goes first")
	assert.True(t, strings.HasSuffix(lines[1], "| http://host.docker.internal:7070 py-1"), lines[1])
	assert.Equal(t, "term", lines[2], "SIGTERM reaches the runtime, which passes it on")
	assert.Equal(t, "rm hubwagen-py-1", lines[3])
}

func TestRemoveLeftovers(t *testing.T) {
	bin, logf := t.TempDir(), filepath.Join(t.TempDir(), "docker.log")
	script := `#!/bin/sh
echo "$*" >> ` + logf + `
case "$1" in
ps) echo abc123; echo def456 ;;
esac
`
	require.NoError(t, os.WriteFile(filepath.Join(bin, "docker"), []byte(script), 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
	n, err := RemoveLeftovers(context.Background(), "docker", "/data", "py")
	require.NoError(t, err)
	assert.Equal(t, 2, n)
	b, _ := os.ReadFile(logf)
	assert.Equal(t, "ps -aq --filter label=hubwagen.data_dir=/data --filter label=hubwagen.pool=py\nrm -f abc123 def456\n", string(b))
}

func TestContainerAuthFile(t *testing.T) {
	w := config.Worker{Name: "py", Launcher: config.LauncherContainer, Image: "registry.acme.dev/revenue@sha256:abc", AuthFile: "/etc/hubwagen/auth/config.json"}
	w.Runtime = "podman"
	p := Container(w, "py-1", "http://127.0.0.1:7070", "/data", nil)
	assert.Contains(t, strings.Join(p.Command, " "), " --authfile /etc/hubwagen/auth/config.json ")
	w.Runtime = "docker"
	p = Container(w, "py-1", "http://127.0.0.1:7070", "/data", nil)
	assert.NotContains(t, strings.Join(p.Command, " "), "authfile")
	assert.Contains(t, p.Env, "DOCKER_CONFIG=/etc/hubwagen/auth", "docker's own variable")
	assert.NotContains(t, strings.Join(p.Command, " "), "DOCKER_CONFIG", "not passed into the container")
}

func TestSecrets(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "revenue-db"), []byte("s3cr3t\n"), 0o600))
	s := Secrets{Dir: dir}
	v, err := s.Get("revenue-db")
	require.NoError(t, err)
	assert.Equal(t, "s3cr3t", v, "without the trailing newline")
	_, err = s.Get("../etc/passwd")
	assert.ErrorContains(t, err, "not a secret name")
	err = s.Check(map[string]string{"DB": "revenue-db", "A": "missing-a", "B": "missing-b"})
	assert.ErrorContains(t, err, "secret missing-a: no such secret on this server")
	assert.ErrorContains(t, err, "secret missing-b", "every problem")
	_, err = Secrets{}.Get("x")
	assert.ErrorContains(t, err, "set projects.secrets_dir")
}
