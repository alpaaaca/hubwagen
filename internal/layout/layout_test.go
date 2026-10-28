package layout

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayout(t *testing.T) {
	l := New("gs://bucket/")
	assert.Equal(t, "gs://bucket/data/t/raw.orders/2026-10-24/a1/", l.LiftStaging("t", "raw.orders", "2026-10-24", "a1"))
	assert.Equal(t, "gs://bucket/data/t/lift:frame/_/a1/", l.LiftStaging("t", "lift:frame", "", "a1"), "unpartitioned")
	assert.Equal(t, "gs://bucket/data/t/odd%2Fname/a%20b/a1/", l.LiftStaging("t", "odd/name", "a b", "a1"), "escaped")
	assert.Equal(t, "gs://bucket/lifts/t/h1/2/a1/", l.LegacyLiftStaging("t", "h1", 2, "a1"))
	assert.Equal(t, "gs://bucket/pallets/t/raw.orders/2026-10-24/p1.json", l.PalletManifest("t", "raw.orders", "2026-10-24", "p1"))
	assert.Equal(t, "gs://bucket/pallets/t/cfg/_/p2.json", l.PalletManifest("t", "cfg", "", "p2"))
}
