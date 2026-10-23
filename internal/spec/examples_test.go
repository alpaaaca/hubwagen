package spec

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Every example in the repository must keep compiling.
func TestExamplesCompile(t *testing.T) {
	root := filepath.Join("..", "..", "pipelines", "examples")
	var n int
	err := filepath.WalkDir(root, func(p string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(p) != ".yaml" {
			return err
		}
		n++
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		_, err = CompileFile(context.Background(), p, data, Options{})
		assert.NoError(t, err, p)
		return nil
	})
	require.NoError(t, err)
	assert.Greater(t, n, 0)
}
