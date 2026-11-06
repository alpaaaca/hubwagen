package config

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseExpandsVariables(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("DEMO_LISTEN", "127.0.0.1:7171")
	t.Setenv("DEMO_WORKERS", "2")
	t.Setenv("DEMO_EMPTY", "")
	c, err := Parse([]byte(`
apiVersion: moksha.dev/v1alpha1
kind: Config
# A comment may mention ${ANYTHING} without it being set.
data_dir: ${DEMO_EMPTY:-state}    # empty: the default
server:
  listen: ${DEMO_LISTEN}
bucket:
  url: ${DEMO_BUCKET:-}          # unset with an empty default: no bucket
  endpoint: ${DEMO_EMPTY}        # set, empty
pools:
  - name: py
    launcher: kubernetes
    max: ${DEMO_WORKERS}
    deployment: ${DEMO_DEPLOYMENT:-revenue-workers}
    queues: ["$HOME", "$${NOT_A_VARIABLE}"]
`), dir)
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:7171", c.Server.Listen)
	assert.Equal(t, Bucket{}, c.Bucket)
	assert.Equal(t, dir+"/state", c.DataDir)
	require.Len(t, c.Pools, 1)
	_, hi := c.Pools[0].Bounds()
	assert.Equal(t, 2, hi, "a number after expansion is a number")
	assert.Equal(t, "revenue-workers", c.Pools[0].Deployment)
	assert.Equal(t, []string{"$HOME", "${NOT_A_VARIABLE}"}, c.Pools[0].Queues, "only ${…} is a variable; $${ is a literal ${")
}

func TestExpandReportsAllProblemsByLine(t *testing.T) {
	env := map[string]string{"MULTI": "a\nb"}
	lookup := func(k string) (string, bool) { v, ok := env[k]; return v, ok }
	_, err := expand([]byte("a: ${UNSET}\nb: ${bad name}\nc: ${MULTI}\nd: ${OPEN\ne: ok\n"), lookup)
	require.Error(t, err)
	assert.Equal(t, "line 1: ${UNSET} is not set (give it a default: ${UNSET:-…})\n"+
		"line 2: ${bad name}: want ${NAME} or ${NAME:-default}\n"+
		"line 3: ${MULTI}: the value has a line break\n"+
		"line 4: ${ without a closing }", err.Error())
}

func TestVariableErrorsNameTheFile(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/moksha.yaml"
	require.NoError(t, writeFile(path, "apiVersion: moksha.dev/v1alpha1\nkind: Config\ndata_dir: ${MOKSHA_TEST_UNSET_DIR}\n"))
	_, err := Load(path)
	assert.EqualError(t, err, path+": line 3: ${MOKSHA_TEST_UNSET_DIR} is not set (give it a default: ${MOKSHA_TEST_UNSET_DIR:-…})")
}

func writeFile(path, s string) error { return os.WriteFile(path, []byte(s), 0o644) }
