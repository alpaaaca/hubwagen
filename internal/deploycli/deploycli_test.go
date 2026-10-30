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
