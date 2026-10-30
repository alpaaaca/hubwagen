// Package launch builds what starts a pool's workers when hubwagen doesn't
// run them as plain processes: a container per worker (Container), or a
// Kubernetes Deployment whose replicas the server sets (Kube). Pools
// (internal/pools) decide how many; this package only knows how.
package launch

import (
	"context"
	"fmt"
	"maps"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"

	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/supervise"
)

// Runtime is the container runtime for w: its runtime field, else docker
// if it is on PATH, else podman.
func Runtime(w config.Worker) string {
	if w.Runtime != "" {
		return w.Runtime
	}
	if _, err := exec.LookPath("docker"); err == nil {
		return "docker"
	}
	return "podman"
}

// ContainerServerURL is the API's URL as w's containers reach it: its
// server_url, else server (the host's URL) with a loopback or unspecified
// host replaced by the name the runtime gives the host.
func ContainerServerURL(w config.Worker, server string) string {
	if w.ServerURL != "" {
		return w.ServerURL
	}
	u, err := url.Parse(server)
	if err != nil {
		return server
	}
	host, port := u.Hostname(), u.Port()
	if ip := net.ParseIP(host); host == "localhost" || (ip != nil && (ip.IsLoopback() || ip.IsUnspecified())) {
		name := "host.docker.internal"
		if Runtime(w) == "podman" {
			name = "host.containers.internal"
		}
		u.Host = net.JoinHostPort(name, port)
	}
	return u.String()
}

// Labels on every container: its pool, and the server's data directory,
// so a server finds its own leftovers and no other server's.
const (
	LabelPool    = "hubwagen.pool"
	LabelDataDir = "hubwagen.data_dir"
)

// Container is worker id of w as a container: `<runtime> run` in the
// foreground, so the supervisor keeps it running, prefixes its output and
// stops it with SIGTERM (which the runtime passes on), and `<runtime> rm
// -f` as its cleanup. env (NAME=value) goes into the container, by name
// only on the command line; the container reaches the server at
// ContainerServerURL and moves data through the server (HUBWAGEN_DATA=
// remote: it shares no files with it). dataDir is the server's, for the
// labels.
func Container(w config.Worker, id, server, dataDir string, env []string) supervise.Process {
	rt := Runtime(w)
	name := "hubwagen-" + id
	env = append(append([]string(nil), env...),
		config.EnvServer+"="+ContainerServerURL(w, server), "HUBWAGEN_DATA=remote")
	args := []string{rt, "run", "--rm", "--name", name, "--label", LabelPool + "=" + w.Name, "--label", LabelDataDir + "=" + dataDir}
	if rt == "docker" {
		args = append(args, "--add-host=host.docker.internal:host-gateway") // Linux; Docker Desktop has it already
	}
	runtimeEnv := os.Environ()
	if w.AuthFile != "" {
		if rt == "podman" {
			args = append(args, "--authfile", w.AuthFile)
		} else {
			// Docker reads <DOCKER_CONFIG>/config.json; the runtime's own
			// variable, not the container's.
			runtimeEnv = append(runtimeEnv, "DOCKER_CONFIG="+filepath.Dir(w.AuthFile))
		}
	}
	names := map[string]bool{}
	for _, kv := range env {
		k, _, _ := strings.Cut(kv, "=")
		names[k] = true
	}
	for _, k := range slices.Sorted(maps.Keys(names)) {
		args = append(args, "-e", k) // its value comes from the runtime's environment, the last one set
	}
	args = append(args, w.Image)
	args = append(args, w.Command...)
	return supervise.Process{ID: id, Command: args, Dir: w.Dir, Env: append(runtimeEnv, env...),
		Cleanup: []string{rt, "rm", "-f", name}}
}

// RemoveLeftovers removes the containers a hubwagen on dataDir started and
// didn't stop (of pool, or of every pool if pool is ""): one killed with
// SIGKILL leaves them running (their runtime process is in its own process
// group), still polling, and nothing would stop them. Run it before any
// pool starts.
func RemoveLeftovers(ctx context.Context, rt, dataDir, pool string) (int, error) {
	args := []string{"ps", "-aq", "--filter", "label=" + LabelDataDir + "=" + dataDir}
	if pool != "" {
		args = append(args, "--filter", "label="+LabelPool+"="+pool)
	}
	out, err := exec.CommandContext(ctx, rt, args...).Output()
	if err != nil {
		return 0, fmt.Errorf("%s ps: %w", rt, err)
	}
	ids := strings.Fields(string(out))
	if len(ids) == 0 {
		return 0, nil
	}
	if b, err := exec.CommandContext(ctx, rt, append([]string{"rm", "-f"}, ids...)...).CombinedOutput(); err != nil {
		return 0, fmt.Errorf("%s rm: %w: %s", rt, err, strings.TrimSpace(string(b)))
	}
	return len(ids), nil
}
