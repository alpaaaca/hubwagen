package devmode

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// What Watch looks at: the project's Python files, pyproject.toml and the
// project file, not data, caches, hidden directories or virtualenvs.
func TestCodeStateSeesCodeNotData(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, s string) {
		t.Helper()
		p := filepath.Join(dir, rel)
		require.NoError(t, os.MkdirAll(filepath.Dir(p), 0o755))
		require.NoError(t, os.WriteFile(p, []byte(s), 0o644))
	}
	write("moksha.yaml", "kind: Project")
	write("pyproject.toml", "[tool.moksha]")
	write("revenue/pipelines.py", "a")
	write("venv/pyvenv.cfg", "")

	at := codeState(dir, "moksha.yaml")
	for _, rel := range []string{"data/orders.csv", "revenue/__pycache__/pipelines.pyc", ".moksha/state.db",
		".venv/lib/x.py", "venv/lib/y.py", "node_modules/z.py", "README.md"} {
		write(rel, "changed")
		assert.Equal(t, at, codeState(dir, "moksha.yaml"), rel)
	}
	for _, rel := range []string{"revenue/pipelines.py", "revenue/new.py", "pyproject.toml", "moksha.yaml"} {
		write(rel, "changed at "+time.Now().String())
		now := codeState(dir, "moksha.yaml")
		assert.NotEqual(t, at, now, rel)
		at = now
	}
}
