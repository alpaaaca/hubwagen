package layout

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayout(t *testing.T) {
	l := New("gs://bucket/")
	assert.Equal(t, "gs://bucket/lifts/t/h1/2/a1/", l.LiftStaging("t", "h1", 2, "a1"))
	assert.Equal(t, "gs://bucket/pallets/t/raw.orders/2026-10-24/p1.json", l.PalletManifest("t", "raw.orders", "2026-10-24", "p1"))
	assert.Equal(t, "gs://bucket/pallets/t/cfg/_/p2.json", l.PalletManifest("t", "cfg", "", "p2"))
}
