package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
    env: {HUBWAGEN_SERVER: x}
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
