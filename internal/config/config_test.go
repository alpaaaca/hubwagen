package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/alper/moksha/internal/blob"
)

func TestParse(t *testing.T) {
	dir := t.TempDir()
	c, err := Parse([]byte(`
apiVersion: moksha.dev/v1alpha1
kind: Config
data_dir: state
server:
  listen: 0.0.0.0:7171
pools:
  - {name: revenue, launcher: external, min: 0, max: 5, queues: [gpu]}
`), dir)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, "state"), c.DataDir)
	assert.Equal(t, "0.0.0.0:7171", c.Server.Listen)
	require.Len(t, c.Pools, 1)
	assert.Equal(t, LauncherExternal, c.Pools[0].Launcher)
	assert.Equal(t, []string{"gpu"}, c.Pools[0].QueueList())
}

func TestDefaults(t *testing.T) {
	c, err := Parse([]byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\n"), "/srv")
	require.NoError(t, err)
	assert.Equal(t, "/srv/.moksha", c.DataDir)
	assert.Equal(t, DefaultListen, c.Server.Listen)
	assert.Empty(t, c.Pools)
}

func TestParseReportsAllProblems(t *testing.T) {
	_, err := Parse([]byte(`
apiVersion: v0
kind: Config
server: {listen: "7070"}
pools:
  - {name: Py, launcher: external}
  - {name: w, launcher: external, min: -1}
  - {name: w, launcher: external}
`), t.TempDir())
	require.Error(t, err)
	for _, want := range []string{
		`apiVersion: must be moksha.dev/v1alpha1, got "v0"`,
		"server.listen:",
		`pools[0].name: "Py" must be lower-case`,
		"pools[1]: min and max must be 0 or more",
		`pools[2].name: "w" is used twice`,
	} {
		assert.Contains(t, err.Error(), want)
	}
}

// A config from before pools: says what to write instead of workers:.
func TestParseSaysWorkersIsGone(t *testing.T) {
	_, err := Parse([]byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\nworkers:\n  - {name: py, command: [python3, -m, moksha.worker]}\n"), t.TempDir())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "line 4: workers: is gone")
	assert.Contains(t, err.Error(), "kind: Project and pools: [{name: default, min: 1, max: 3}]: moksha dev in its directory deploys it")
}

func TestParseIsStrict(t *testing.T) {
	_, err := Parse([]byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\nworkerz: []\n"), t.TempDir())
	assert.ErrorContains(t, err, "field workerz not found")
}

func TestFindAndLoad(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	t.Setenv("MOKSHA_CONFIG", "")
	path, err := Find("")
	require.NoError(t, err)
	assert.Empty(t, path, "no file: defaults")
	c, err := Load("")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(dir, DefaultDataDir), c.DataDir)

	require.NoError(t, os.WriteFile(FileName, []byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\n"), 0o644))
	path, err = Find("")
	require.NoError(t, err)
	assert.Equal(t, FileName, path)

	_, err = Find("missing.yaml")
	assert.Error(t, err, "an explicit file must exist")
	t.Setenv("MOKSHA_CONFIG", "missing.yaml")
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
apiVersion: moksha.dev/v1alpha1
kind: Config
bucket:
  url: s3://acme-moksha/prod
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
	assert.Equal(t, &blob.S3Config{URL: "s3://acme-moksha/prod", Endpoint: "http://minio.internal:9000",
		PublicEndpoint: "https://s3.acme.example", Region: "eu-central-1", PathStyle: true,
		AccessKeyID: "AKIA123", SecretAccessKey: "s3cr3t"}, s3, "credential files relative to the config, newline trimmed")
	assert.Equal(t, TransfersServer, c.Bucket.Transfers)

	none, err := (Bucket{}).S3()
	require.NoError(t, err)
	assert.Nil(t, none, "no url: the file bucket under data_dir")

	_, err = (Bucket{URL: "s3://b", AccessKeyIDFile: filepath.Join(dir, "missing"), SecretAccessKeyFile: filepath.Join(dir, "secret")}).S3()
	assert.ErrorContains(t, err, "bucket.access_key_id_file")

	_, err = Parse([]byte(`
apiVersion: moksha.dev/v1alpha1
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
	_, err = Parse([]byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\nbucket:\n  region: x\n"), dir)
	assert.ErrorContains(t, err, "bucket.url: required")
	_, err = Parse([]byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\nbucket:\n  secret_access_key: x\n"), dir)
	assert.ErrorContains(t, err, "secret_access_key not found", "secret values themselves aren't fields")
}

func TestPools(t *testing.T) {
	c, err := Parse([]byte(`
apiVersion: moksha.dev/v1alpha1
kind: Config
kubernetes: {api_url: "http://127.0.0.1:8001", namespace: jobs}
pools:
  - {name: gpu, launcher: external, max: 3, queues: [gpu, default], steps: 1, idle_after: 90s}
  - {name: k, launcher: kubernetes, deployment: revenue-workers, min: 2}
`), t.TempDir())
	require.NoError(t, err)
	type bounds struct{ lo, hi int }
	b := func(i int) bounds { lo, hi := c.Pools[i].Bounds(); return bounds{lo, hi} }
	assert.Equal(t, bounds{0, 3}, b(0))
	assert.Equal(t, bounds{2, 2}, b(1), "max defaults to min")
	assert.Equal(t, []string{"gpu", "default"}, c.Pools[0].QueueList())
	assert.Equal(t, []string{"default"}, c.Pools[1].QueueList())
	assert.Equal(t, 1, c.Pools[0].StepCount())
	assert.Equal(t, DefaultSteps, c.Pools[1].StepCount())
	assert.Equal(t, 90*time.Second, c.Pools[0].Idle())
	assert.Equal(t, DefaultIdleAfter, c.Pools[1].Idle())
	assert.Equal(t, "revenue-workers", c.Pools[1].Deployment)
	assert.Equal(t, Kubernetes{APIURL: "http://127.0.0.1:8001", Namespace: "jobs"}, c.Kubernetes)

	_, err = Parse([]byte(`
apiVersion: moksha.dev/v1alpha1
kind: Config
pools:
  - {name: a}
  - {name: b, launcher: container}
  - {name: c, launcher: docker}
  - {name: d, launcher: kubernetes}
  - {name: e, launcher: external, deployment: y, min: 3, max: 2}
  - {name: f, launcher: external, steps: 0, queues: [""]}
`), t.TempDir())
	require.Error(t, err)
	for _, want := range []string{
		"pools[0].launcher: must be external or kubernetes; pools of processes or containers come from a project's file",
		"pools[1].launcher: must be external or kubernetes; pools of processes",
		`pools[2].launcher: must be external or kubernetes, got "docker"`,
		"pools[3].deployment: required with launcher: kubernetes",
		"pools[4]: deployment and namespace are for launcher: kubernetes",
		"pools[4]: min 3 is more than max 2",
		"pools[5].steps: must be 1 or more",
		"pools[5].queues[0]: empty",
	} {
		assert.Contains(t, err.Error(), want)
	}
	_, err = Parse([]byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\npools:\n  - {name: a, launcher: external, command: [w]}\n"), t.TempDir())
	assert.ErrorContains(t, err, "field command not found", "a pool here has no command: it is started elsewhere")

	_, err = Parse([]byte("apiVersion: moksha.dev/v1alpha1\nkind: Config\npools:\n  - {name: a, launcher: external, idle_after: soon}\n"), t.TempDir())
	assert.ErrorContains(t, err, `"soon": want a duration such as 5m or 90s`)
}
