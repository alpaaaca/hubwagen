package deploycli

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
	"google.golang.org/protobuf/types/known/timestamppb"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/config"
)

var t0 = time.Date(2026, 10, 30, 18, 42, 0, 0, time.UTC)

func code(name, version, build string, at time.Time) *hubwagenv1.Code {
	return &hubwagenv1.Code{Name: name, CodeVersion: version, BuildId: build, RegisteredAt: timestamppb.New(at)}
}

func project(t *testing.T, yaml string) *config.Project {
	t.Helper()
	p, err := config.ParseProject([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Project\nname: revenue\n"+yaml), t.TempDir())
	require.NoError(t, err)
	return p
}

const versionsJSON = `{"build": "b-new", "hauls": {"daily": {"version": "py-h1"}, "weekly": {"version": "py-h2"}},
  "lifts": {"extract": {"version": "py-1"}, "build": {"version": "py-2b"}, "report": {"version": "py-9"}}}`

func TestCompareAndReport(t *testing.T) {
	var v Versions
	require.NoError(t, jsonUnmarshal(versionsJSON, &v))
	server := &hubwagenv1.ListCodeResponse{
		Steps: []*hubwagenv1.Code{
			code("build", "py-2a", "b-old", t0),
			code("extract", "py-1", "b-old", t0),
			code("legacy", "py-7", "b-older", t0.Add(-time.Hour)),
		},
		Pipelines: []*hubwagenv1.Code{code("daily", "", "b-old", t0), code("gone", "", "b-older", t0.Add(-time.Hour))},
	}
	d := Compare(v, server)
	assert.Equal(t, map[Change]int{Changed: 1, Unchanged: 1, Added: 1, Removed: 1}, d.Counts())
	assert.Equal(t, "b-old", d.Newest.BuildId)

	p := project(t, "pools:\n  - {name: default, min: 1, max: 6, lifts: 2}\n  - {name: gpu, docks: [gpu], min: 2, max: 2, lifts: 1}\n")
	assert.Equal(t, `Project revenue: build b-new (from image ghcr.io/acme/revenue:dry-run)
The server's newest build: b-old, registered 2026-10-30 18:42 UTC

Steps: 1 runs again, 1 new, 1 unchanged, 1 no longer in the code
  build     code version changed (py-2a → py-2b): runs again, also for inputs it ran with before
  extract   unchanged: reused for inputs it ran with before
  report    new: runs
  legacy    not in this code (the server has it from b-older)
  (A code version covers the step's code, the packages it uses and the Python version.
  Steps that read what a changed step writes run again too, when it writes something new.)

Pipelines
  weekly   new
  gone     not in this code: no new runs of it once deployed

Pools
  default   1–6 workers, 2 steps at once each, docks default
  gpu       2 workers, 1 step at once each, docks gpu
`, Report(p, Source{Image: "ghcr.io/acme/revenue:dry-run"}, d))
}

func TestReportForAServerWithoutBuilds(t *testing.T) {
	var v Versions
	require.NoError(t, jsonUnmarshal(versionsJSON, &v))
	out := Report(project(t, "pools: [{name: default}]\n"), Source{}, Compare(v, &hubwagenv1.ListCodeResponse{}))
	assert.Contains(t, out, "(from its code in place)")
	assert.Contains(t, out, "The server has no builds yet: every step and pipeline is new.")
	assert.Contains(t, out, "Steps: 3 new")
}

// fake writes an executable script named name into a directory on PATH.
func fake(t *testing.T, name, script string) {
	t.Helper()
	bin := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(bin, name), []byte("#!/bin/sh\n"+script), 0o755))
	t.Setenv("PATH", bin+":"+os.Getenv("PATH"))
}

func TestBuildVersionsInPlace(t *testing.T) {
	fake(t, "python3", `[ "$*" = "-m hubwagen.worker --versions" ] || exit 9; echo "in $(pwd)" >&2; cat <<'J'
`+versionsJSON+`
J
`)
	p := project(t, "pools: [{name: default}]\n")
	var log strings.Builder
	v, src, err := BuildVersions(context.Background(), p, Options{Out: &log})
	require.NoError(t, err)
	assert.Equal(t, "b-new", v.Build)
	assert.Equal(t, "py-2b", v.Lifts["build"].Version)
	assert.Equal(t, Source{}, src)
	assert.Contains(t, log.String(), "in ", "the worker's stderr goes to the log")
}

func TestBuildVersionsInTheImage(t *testing.T) {
	logf := filepath.Join(t.TempDir(), "docker.log")
	fake(t, "docker", `echo "$*" >> `+logf+`
case "$1" in
run) cat <<'J'
`+versionsJSON+`
J
;;
esac
`)
	p := project(t, "build: {dockerfile: Dockerfile, image: ghcr.io/acme/revenue, args: {WORKER_IMAGE: hubwagen-worker:dev}}\npools: [{name: default}]\n")
	v, src, err := BuildVersions(context.Background(), p, Options{Runtime: "docker", Out: io.Discard})
	require.NoError(t, err)
	assert.Equal(t, "b-new", v.Build)
	assert.Equal(t, Source{Image: "ghcr.io/acme/revenue:dry-run"}, src)
	b, _ := os.ReadFile(logf)
	assert.Equal(t, "build -t ghcr.io/acme/revenue:dry-run -f "+p.Build.Dockerfile+" --build-arg WORKER_IMAGE=hubwagen-worker:dev "+p.Dir+"\n"+
		"run --rm ghcr.io/acme/revenue:dry-run python3 -m hubwagen.worker --versions\n", string(b))
}

func TestBuildVersionsSaysWhatWentWrong(t *testing.T) {
	fake(t, "python3", "echo 'Traceback: no module named hubwagen'\n")
	_, _, err := BuildVersions(context.Background(), project(t, "pools: [{name: default}]\n"), Options{Out: io.Discard})
	assert.ErrorContains(t, err, "python3 -m hubwagen.worker --versions: not a build's versions: Traceback: no module named hubwagen")
}

func TestDeployRequest(t *testing.T) {
	reg := &hubwagenv1.RegisterBuildRequest{BuildId: "b-new", Hauls: []string{"daily"}}
	p := project(t, "build: {dockerfile: Dockerfile, image: localhost/revenue}\npools:\n  - {name: default, min: 1, max: 4, lifts: 2, idle_after: 90s}\nenv: {LOG_LEVEL: info}\n")
	req, err := DeployRequest(p, Source{Image: "localhost/revenue:dry-run"}, reg, "python3", "ana@laptop")
	require.NoError(t, err)
	assert.Equal(t, "localhost/revenue:b-new", req.Image, "the build's tag")
	assert.Empty(t, req.Dir)
	assert.Equal(t, map[string]string{"LOG_LEVEL": "info"}, req.Env)
	require.Len(t, req.Pools, 1)
	assert.Equal(t, []any{"default", []string{"default"}, int32(1), int32(4), int32(2), 90 * time.Second},
		[]any{req.Pools[0].Name, req.Pools[0].Docks, req.Pools[0].Min, req.Pools[0].Max, req.Pools[0].Lifts, req.Pools[0].IdleAfter.AsDuration()})

	// In place: the directory, the interpreter found on PATH, PYTHONPATH as here.
	fake(t, "python3", "exit 0\n")
	t.Setenv("PYTHONPATH", "/opt/sdk")
	req, err = DeployRequest(p, Source{}, reg, "python3", "ana@laptop")
	require.NoError(t, err)
	assert.Equal(t, p.Dir, req.Dir)
	assert.True(t, strings.HasSuffix(req.Python, "/python3"))
	assert.Equal(t, "/opt/sdk", req.Env["PYTHONPATH"])
	assert.Equal(t, 5*time.Minute, project(t, "pools: [{name: default}]\n").Pools[0].Idle(), "the default idle_after")

	req, err = DeployRequest(project(t, "pools: [{name: default}]\nenv: {DB: {secret: revenue-db}, LEVEL: info}\n"), Source{}, reg, "python3", "ana")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"DB": "revenue-db"}, req.Secrets, "the secret's name only")
	assert.Equal(t, "info", req.Env["LEVEL"])
	assert.NotContains(t, req.Env, "DB")
}

func TestRegistrationInPlace(t *testing.T) {
	fake(t, "python3", `[ "$*" = "-m hubwagen.worker --build-json" ] || exit 9
echo '{"buildId": "b-new", "hauls": ["daily"], "liftVersions": {"load": "py-1"}, "schedules": [{"haul": "daily", "cron": "0 3 * * *", "interval": "daily", "catchUp": "latest"}]}'
`)
	reg, err := Registration(context.Background(), project(t, "pools: [{name: default}]\n"), Source{}, Options{Out: io.Discard})
	require.NoError(t, err)
	assert.Equal(t, "b-new", reg.BuildId)
	assert.Equal(t, "py-1", reg.LiftVersions["load"])
	require.Len(t, reg.Schedules, 1)
	assert.Equal(t, "latest", reg.Schedules[0].CatchUp)

	fake(t, "python3", "echo 'usage: …' >&2; echo 'python -m hubwagen.worker: error: unrecognized arguments: --build-json' >&2; exit 2\n")
	_, err = Registration(context.Background(), project(t, "pools: [{name: default}]\n"), Source{}, Options{Out: io.Discard})
	assert.ErrorContains(t, err, "its hubwagen SDK is older than this hubwagen")
}

func TestProjectsTable(t *testing.T) {
	at := timestamppb.New(t0)
	var b strings.Builder
	require.NoError(t, Projects(&b, []*hubwagenv1.Deployment{
		{Project: "revenue", Version: 3, BuildId: "b-new", Image: "localhost/revenue:b-new", By: "ana@laptop", DeployedAt: at,
			Pools: []*hubwagenv1.DeployedPool{{Name: "default", Min: 0, Max: 3}, {Name: "gpu", Min: 1, Max: 1}}},
		{Project: "local", Version: 1, BuildId: "b-x", Dir: "/Users/ana/code/examples/local", By: "ana@laptop", DeployedAt: at,
			Pools: []*hubwagenv1.DeployedPool{{Name: "default", Min: 0, Max: 1}}},
	}, []*hubwagenv1.Pool{{Name: "revenue-default-v3", Workers: []string{"revenue-default-v3-1", "revenue-default-v3-2"}}, {Name: "revenue-default-v2", Workers: []string{"x"}}}))
	assert.Equal(t, `PROJECT   VERSION   BUILD   RUNS                            POOLS                            DEPLOYED               BY
revenue   v3        b-new   image localhost/revenue:b-new   default 2 of 0–3, gpu 0 of 1–1   2026-10-30 18:42 UTC   ana@laptop
local     v1        b-x     in place, …/examples/local      default 0 of 0–1                 2026-10-30 18:42 UTC   ana@laptop
`, b.String())
}

func TestPushReturnsTheDigest(t *testing.T) {
	p := project(t, "build: {dockerfile: Dockerfile, image: registry.acme.dev/revenue}\npools: [{name: default}]\n")
	logf := filepath.Join(t.TempDir(), "rt.log")

	fake(t, "podman", `echo "$*" >> `+logf+`
[ "$1" = push ] && [ "$2" = --digestfile ] && echo "sha256:abc123" > "$3"
`)
	ref, err := Push(context.Background(), p, "b-new", Options{Runtime: "podman", Out: io.Discard})
	require.NoError(t, err)
	assert.Equal(t, "registry.acme.dev/revenue@sha256:abc123", ref)

	fake(t, "docker", `echo "$*" >> `+logf+`
[ "$1" = inspect ] && echo '["other/revenue@sha256:zzz","registry.acme.dev/revenue@sha256:def456"]'
true
`)
	ref, err = Push(context.Background(), p, "b-new", Options{Runtime: "docker", Out: io.Discard})
	require.NoError(t, err)
	assert.Equal(t, "registry.acme.dev/revenue@sha256:def456", ref)
	b, _ := os.ReadFile(logf)
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	require.Len(t, lines, 3)
	assert.True(t, strings.HasSuffix(lines[0], " registry.acme.dev/revenue:b-new"), lines[0])
	assert.Equal(t, "push registry.acme.dev/revenue:b-new", lines[1])
	assert.Equal(t, "inspect --format {{json .RepoDigests}} registry.acme.dev/revenue:b-new", lines[2])

	fake(t, "docker", "exit 1\n")
	_, err = Push(context.Background(), p, "b-new", Options{Runtime: "docker", Out: io.Discard})
	assert.ErrorContains(t, err, "docker push registry.acme.dev/revenue:b-new")
}
