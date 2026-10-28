// Package layout computes where things live in the bucket (or the local
// directory standing in for one): lift staging prefixes and pallet
// manifests. The core hands these URIs to workers.
package layout

import (
	"fmt"
	"net/url"
	"strings"
)

// Layout is rooted at a base URI such as "file:///var/lib/hubwagen/bucket"
// or "gs://acme-hubwagen". Every URI it returns for a directory ends in "/".
type Layout struct {
	base string
}

func New(base string) Layout {
	return Layout{base: strings.TrimSuffix(base, "/")}
}

func (l Layout) Base() string { return l.base }

// LiftStaging is where one lift attempt writes its output: under the
// dataset and partition it goes to, so the bucket reads like the catalog
// (data/<tenant>/<dataset>/<partition>/<attempt>/; partition "_" when
// unpartitioned). Once the attempt's pallet is committed, its manifest
// points here, so this becomes the data's home; attempts that fail are
// discarded.
func (l Layout) LiftStaging(tenant, dataset, partition, attemptID string) string {
	if partition == "" {
		partition = "_"
	}
	return fmt.Sprintf("%s/data/%s/%s/%s/%s/", l.base, tenant, url.PathEscape(dataset), url.PathEscape(partition), attemptID)
}

// LegacyLiftStaging is where attempts dispatched before LiftStaging went by
// dataset wrote (lifts/<tenant>/<haul>/<seq>/<attempt>/). Pallets committed
// there keep their paths; only those attempts' cleanup needs it.
func (l Layout) LegacyLiftStaging(tenant, haulID string, seq int, attemptID string) string {
	return fmt.Sprintf("%s/lifts/%s/%s/%d/%s/", l.base, tenant, haulID, seq, attemptID)
}

// PalletManifest is a pallet's manifest: written once, create-if-absent, by
// the promote that makes the pallet visible.
func (l Layout) PalletManifest(tenant, dataset, partition, palletID string) string {
	if partition == "" {
		partition = "_"
	}
	return fmt.Sprintf("%s/pallets/%s/%s/%s/%s.json", l.base,
		tenant, url.PathEscape(dataset), url.PathEscape(partition), palletID)
}
