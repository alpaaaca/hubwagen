package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/hubwagen/internal/blob"
)

func TestParse(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.Mkdir(filepath.Join(dir, "app"), 0o755))
	c, err := Parse([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
data_dir: state
server:
  listen: 0.0.0.0:7171
workers:
  - name: py
    command: [python3, -m, hubwagen.worker, app.hauls]
    replicas: 3
    dir: app
    env: {PYTHONPATH: src}
  - name: off
    command: [true]
    replicas: 0
  - name: one
    command: [true]
`), dir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "state"), c.DataDir)
	assert.Equal(t, "0.0.0.0:7171", c.Server.Listen)
	require.Len(t, c.Workers, 3)
	assert.Equal(t, 3, c.Workers[0].Count())
	assert.Equal(t, filepath.Join(dir, "app"), c.Workers[0].Dir)
	assert.Equal(t, map[string]string{"PYTHONPATH": "src"}, c.Workers[0].Env)
	assert.Equal(t, 0, c.Workers[1].Count())
	assert.Equal(t, 1, c.Workers[2].Count(), "one replica by default")
	assert.Equal(t, dir, c.Workers[2].Dir, "the config's directory by default")
}

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\n"), "/srv")
	require.NoError(t, err)
	assert.Equal(t, "/srv/.hubwagen", c.DataDir)
	assert.Equal(t, DefaultListen, c.Server.Listen)
	assert.Empty(t, c.Workers)
}

func TestParseReportsAllProblems(t *testing.T) {
	_, err := Parse([]byte(`
apiVersion: v0
kind: Config
server: {listen: "7070"}
workers:
  - name: Py
    command: []
  - name: w
    command: [x]
    replicas: -1
    dir: nowhere
  - name: w
    command: [x]
    env: {HUBWAGEN_SERVER: x, HUBWAGEN_SUPERVISOR_PID: "1"}
`), t.TempDir())
	require.Error(t, err)
	for _, want := range []string{
		`apiVersion: must be hubwagen.dev/v1alpha1, got "v0"`,
		"server.listen:",
		`workers[0].name: "Py" must be lower-case`,
		"workers[0].command: required",
		"workers[1].replicas: must be 0 or more",
		"workers[1].dir:",
		`workers[2].name: "w" is used twice`,
		"workers[2].env.HUBWAGEN_SERVER: set by hubwagen",
		"workers[2].env.HUBWAGEN_SUPERVISOR_PID: set by hubwagen",
	} {
		assert.Contains(t, err.Error(), want)
	}
}

func TestParseIsStrict(t *testing.T) {
	_, err := Parse([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\nworkerz: []\n"), t.TempDir())
	assert.ErrorContains(t, err, "field workerz not found")
}

func TestFindAndLoad(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("HUBWAGEN_CONFIG", "")
	path, err := Find("")
	require.NoError(t, err)
	assert.Empty(t, path, "no file: defaults")
	c, err := Load("")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, DefaultDataDir), c.DataDir)

	require.NoError(t, os.WriteFile(FileName, []byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\n"), 0o644))
	path, err = Find("")
	require.NoError(t, err)
	assert.Equal(t, FileName, path)

	_, err = Find("missing.yaml")
	assert.Error(t, err, "an explicit file must exist")
	t.Setenv("HUBWAGEN_CONFIG", "missing.yaml")
	_, err = Find("")
	assert.Error(t, err)

	require.NoError(t, os.WriteFile("bad.yaml", []byte("kind: Config\n"), 0o644))
	_, err = Load("bad.yaml")
	assert.ErrorContains(t, err, "bad.yaml: apiVersion")
}

func TestServerURL(t *testing.T) {
	assert.Equal(t, "http://127.0.0.1:7070", ServerURL("0.0.0.0:7070"))
	assert.Equal(t, "http://127.0.0.1:7070", ServerURL(":7070"))
	assert.Equal(t, "http://127.0.0.1:7070", ServerURL("[::]:7070"))
	assert.Equal(t, "http://10.0.0.5:7070", ServerURL("10.0.0.5:7070"))
}

func TestBucket(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "key_id"), []byte("AKIA123\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "secret"), []byte("s3cr3t"), 0o600))
	c, err := Parse([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
bucket:
  url: s3://acme-hubwagen/prod
  endpoint: http://minio.internal:9000
  public_endpoint: https://s3.acme.example
  region: eu-central-1
  path_style: true
  access_key_id_file: key_id
  secret_access_key_file: secret
  transfers: server
`), dir)
	require.NoError(t, err)
	s3, err := c.Bucket.S3()
	require.NoError(t, err)
	assert.Equal(t, &blob.S3Config{URL: "s3://acme-hubwagen/prod", Endpoint: "http://minio.internal:9000",
		PublicEndpoint: "https://s3.acme.example", Region: "eu-central-1", PathStyle: true,
		AccessKeyID: "AKIA123", SecretAccessKey: "s3cr3t"}, s3, "credential files relative to the config, newline trimmed")
	assert.Equal(t, TransfersServer, c.Bucket.Transfers)

	none, err := (Bucket{}).S3()
	require.NoError(t, err)
	assert.Nil(t, none, "no url: the file bucket under data_dir")

	_, err = (Bucket{URL: "s3://b", AccessKeyIDFile: filepath.Join(dir, "missing"), SecretAccessKeyFile: filepath.Join(dir, "secret")}).S3()
	assert.ErrorContains(t, err, "bucket.access_key_id_file")

	_, err = Parse([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
bucket:
  url: gs://acme
  endpoint: minio:9000
  access_key_id_file: key_id
  transfers: sometimes
`), dir)
	require.Error(t, err)
	for _, want := range []string{"bucket.url", "bucket.endpoint", "both access_key_id_file and secret_access_key_file", "bucket.transfers"} {
		assert.ErrorContains(t, err, want)
	}
	_, err = Parse([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\nbucket:\n  region: x\n"), dir)
	assert.ErrorContains(t, err, "bucket.url: required")
	_, err = Parse([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\nbucket:\n  secret_access_key: x\n"), dir)
	assert.ErrorContains(t, err, "secret_access_key not found", "secret values themselves aren't fields")
}

func TestPools(t *testing.T) {
	dir := t.TempDir()
	c, err := Parse([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
workers:
  - name: fixed
    command: [w]
    replicas: 2
  - name: gpu
    command: [w]
    min: 0
    max: 3
    docks: [gpu, default]
    lifts: 1
    idle_after: 90s
  - name: atleast
    command: [w]
    min: 2
`), dir)
	require.NoError(t, err)
	type bounds struct {
		lo, hi int
		scaled bool
	}
	b := func(i int) bounds { lo, hi := c.Workers[i].Bounds(); return bounds{lo, hi, c.Workers[i].Scaled()} }
	assert.Equal(t, bounds{2, 2, false}, b(0))
	assert.Equal(t, bounds{0, 3, true}, b(1))
	assert.Equal(t, bounds{2, 2, false}, b(2), "max defaults to min")
	assert.Equal(t, 3, c.Workers[1].Count(), "hubwagen workers runs max")
	assert.Equal(t, []string{"gpu", "default"}, c.Workers[1].DockList())
	assert.Equal(t, []string{"default"}, c.Workers[0].DockList())
	assert.Equal(t, 1, c.Workers[1].LiftCount())
	assert.Equal(t, DefaultLifts, c.Workers[0].LiftCount())
	assert.Equal(t, 90*time.Second, c.Workers[1].Idle())
	assert.Equal(t, DefaultIdleAfter, c.Workers[0].Idle())

	_, err = Parse([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
workers:
  - {name: a, command: [w], replicas: 1, max: 2}
  - {name: b, command: [w], min: 3, max: 2}
  - {name: c, command: [w], lifts: 0, docks: [""]}
  - {name: d, command: [w], env: {HUBWAGEN_DOCKS: x}}
`), dir)
	require.Error(t, err)
	for _, want := range []string{
		"workers[0]: give replicas (a fixed count) or min and max, not both",
		"workers[1]: min 3 is more than max 2",
		"workers[2].lifts: must be 1 or more",
		"workers[2].docks[0]: empty",
		"workers[3].env.HUBWAGEN_DOCKS: set by hubwagen",
	} {
		assert.Contains(t, err.Error(), want)
	}

	_, err = Parse([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\nworkers:\n  - {name: a, command: [w], idle_after: soon}\n"), dir)
	assert.ErrorContains(t, err, `"soon": want a duration such as 5m or 90s`)
}

func TestExternalPools(t *testing.T) {
	c, err := Parse([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\nworkers:\n  - {name: k8s, launcher: external, min: 0, max: 5, docks: [gpu]}\n"), t.TempDir())
	require.NoError(t, err)
	assert.True(t, c.Workers[0].External())
	assert.True(t, c.Workers[0].Scaled())

	_, err = Parse([]byte("apiVersion: hubwagen.dev/v1alpha1\nkind: Config\nworkers:\n"+
		"  - {name: a, launcher: external, command: [w]}\n  - {name: b, launcher: docker, command: [w]}\n"), t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "workers[0]: with launcher: external its workers are started elsewhere: no command or env")
	assert.Contains(t, err.Error(), `workers[1].launcher: must be process, container, kubernetes or external, got "docker"`)
}

func TestContainerAndKubernetesPools(t *testing.T) {
	c, err := Parse([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
kubernetes: {api_url: "http://127.0.0.1:8001", namespace: jobs}
workers:
  - {name: c, launcher: container, image: "ghcr.io/acme/revenue:1", min: 0, max: 2}
  - {name: k, launcher: kubernetes, deployment: revenue-workers, min: 0, max: 5}
`), t.TempDir())
	require.NoError(t, err)
	assert.False(t, c.Workers[0].External(), "hubwagen runs the containers")
	assert.True(t, c.Workers[1].External(), "a desired count, applied to the Deployment")
	assert.Equal(t, Kubernetes{APIURL: "http://127.0.0.1:8001", Namespace: "jobs"}, c.Kubernetes)

	_, err = Parse([]byte(`
apiVersion: hubwagen.dev/v1alpha1
kind: Config
workers:
  - {name: a, launcher: container, runtime: lxc}
  - {name: b, launcher: kubernetes}
  - {name: c, command: [w], image: x, deployment: y}
`), t.TempDir())
	require.Error(t, err)
	for _, want := range []string{
		"workers[0].image: required with launcher: container",
		`workers[0].runtime: must be docker or podman, got "lxc"`,
		"workers[1].deployment: required with launcher: kubernetes",
		"workers[2]: image, runtime, server_url and auth_file are for launcher: container",
		"workers[2]: deployment and namespace are for launcher: kubernetes",
	} {
		assert.Contains(t, err.Error(), want)
	}
}
