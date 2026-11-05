package layout

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestLayout(t *testing.T) {
	l := New("gs://bucket/")
	assert.Equal(t, "gs://bucket/data/t/raw.orders/2026-10-24/a1/", l.StepStaging("t", "raw.orders", "2026-10-24", "a1"))
	assert.Equal(t, "gs://bucket/data/t/step:frame/_/a1/", l.StepStaging("t", "step:frame", "", "a1"), "unpartitioned")
	assert.Equal(t, "gs://bucket/data/t/odd%2Fname/a%20b/a1/", l.StepStaging("t", "odd/name", "a b", "a1"), "escaped")
	assert.Equal(t, "gs://bucket/steps/t/h1/2/a1/", l.LegacyStepStaging("t", "h1", 2, "a1"))
	assert.Equal(t, "gs://bucket/data_versions/t/raw.orders/2026-10-24/p1.json", l.DataVersionManifest("t", "raw.orders", "2026-10-24", "p1"))
	assert.Equal(t, "gs://bucket/data_versions/t/cfg/_/p2.json", l.DataVersionManifest("t", "cfg", "", "p2"))
}
