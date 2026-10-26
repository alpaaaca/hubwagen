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
	assert.Equal(t, "gs://bucket/commits/t/p/extract/t%2Fp%2Fk/gen-2.json", l.TaskCommit("t", "p", "extract", "t/p/k", 2))
	assert.Equal(t, "gs://bucket/logs/t/p/run1/extract/3/", l.Logs("t", "p", "run1", "extract", 3))
	assert.Equal(t, "gs://bucket/results/t/run1/extract/3.json", l.Result("t", "run1", "extract", 3))
	assert.Equal(t, "gs://bucket/lifts/t/h1/2/a1/", l.LiftStaging("t", "h1", 2, "a1"))
	assert.Equal(t, "gs://bucket/pallets/t/raw.orders/2026-10-24/p1.json", l.PalletManifest("t", "raw.orders", "2026-10-24", "p1"))
	assert.Equal(t, "gs://bucket/pallets/t/cfg/_/p2.json", l.PalletManifest("t", "cfg", "", "p2"))
}
