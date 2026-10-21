package layout

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayout(t *testing.T) {
	l := New("gs://bucket/")
	gen := l.OutputGeneration("t", "p", "extract", "rows", "t/p/20261020T000000Z_20261021T000000Z", 2)
	assert.Equal(t, "gs://bucket/data/t/p/extract/rows/t%2Fp%2F20261020T000000Z_20261021T000000Z/gen-2/", gen)
	assert.Equal(t, gen+"attempt-3/", Staging(gen, 3))
	assert.Equal(t, gen+"_COMMITTED", CommitMarker(gen))
	assert.Equal(t, "gs://bucket/logs/t/p/run1/extract/3/", l.Logs("t", "p", "run1", "extract", 3))
	assert.Equal(t, "gs://bucket/results/t/run1/extract/3.json", l.Result("t", "run1", "extract", 3))
}
