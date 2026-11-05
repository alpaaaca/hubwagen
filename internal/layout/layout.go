// Package layout computes where things live in the bucket (or the local
// directory standing in for one): step staging prefixes and data version
// manifests. The core hands these URIs to workers.
package layout

import (
	"fmt"
	"net/url"
	"strings"
)

// Layout is rooted at a base URI such as "file:///var/lib/moksha/bucket"
// or "gs://acme-moksha". Every URI it returns for a directory ends in "/".
type Layout struct {
	base string
}

func New(base string) Layout {
	return Layout{base: strings.TrimSuffix(base, "/")}
}

func (l Layout) Base() string { return l.base }

// StepStaging is where one step attempt writes its output: under the
// dataset and partition it goes to, so the bucket reads like the catalog
// (data/<tenant>/<dataset>/<partition>/<attempt>/; partition "_" when
// unpartitioned). Once the attempt's data version is committed, its manifest
// points here, so this becomes the data's home; attempts that fail are
// discarded.
func (l Layout) StepStaging(tenant, dataset, partition, attemptID string) string {
	if partition == "" {
		partition = "_"
	}
	return fmt.Sprintf("%s/data/%s/%s/%s/%s/", l.base, tenant, url.PathEscape(dataset), url.PathEscape(partition), attemptID)
}

// LegacyStepStaging is where attempts dispatched before StepStaging went by
// dataset wrote (steps/<tenant>/<run>/<seq>/<attempt>/). Data versions committed
// there keep their paths; only those attempts' cleanup needs it.
func (l Layout) LegacyStepStaging(tenant, runID string, seq int, attemptID string) string {
	return fmt.Sprintf("%s/steps/%s/%s/%d/%s/", l.base, tenant, runID, seq, attemptID)
}

// DataVersionManifest is a data version's manifest: written once, create-if-absent, by
// the promote that makes the data version visible.
func (l Layout) DataVersionManifest(tenant, dataset, partition, dataVersionID string) string {
	if partition == "" {
		partition = "_"
	}
	return fmt.Sprintf("%s/data_versions/%s/%s/%s/%s.json", l.base,
		tenant, url.PathEscape(dataset), url.PathEscape(partition), dataVersionID)
}
