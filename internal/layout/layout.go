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

// LiftStaging is where one lift attempt writes its output. Once the attempt's
// pallet is committed, its manifest points here, so this becomes the data's
// home; attempts that fail are discarded.
func (l Layout) LiftStaging(tenant, haulID string, seq int, attemptID string) string {
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
