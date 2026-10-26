// Package layout computes where things live in the bucket (or the local
// directory standing in for one). It is shared by the core, which hands URIs
// to tasks, and the task wrapper, which writes to them. See the bucket layout
// in docs/architecture.md.
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

// OutputGeneration is the directory holding every attempt's staging data for
// one output of one task in one run generation, plus its commit marker.
func (l Layout) OutputGeneration(tenant, pipeline, task, output, runKey string, generation int) string {
	return fmt.Sprintf("%s/data/%s/%s/%s/%s/%s/gen-%d/", l.base,
		tenant, pipeline, task, output, url.PathEscape(runKey), generation)
}

// TaskCommit is the create-if-absent marker that decides which attempt of a
// task (in one run generation) won. It lists every output, so one attempt
// wins all of them. Per-output CommitMarkers are written after it, for
// people and tools browsing the data.
func (l Layout) TaskCommit(tenant, pipeline, task, runKey string, generation int) string {
	return fmt.Sprintf("%s/commits/%s/%s/%s/%s/gen-%d.json", l.base,
		tenant, pipeline, task, url.PathEscape(runKey), generation)
}

// Staging is one attempt's private directory for an output.
func Staging(outputGeneration string, attempt int) string {
	return fmt.Sprintf("%sattempt-%d/", outputGeneration, attempt)
}

// CommitMarker marks an output directory as committed (written after the
// TaskCommit, best effort).
func CommitMarker(outputGeneration string) string {
	return outputGeneration + "_COMMITTED"
}

// Logs is the directory an attempt's log chunks are written to.
func (l Layout) Logs(tenant, pipeline, runID, taskID string, attempt int) string {
	return fmt.Sprintf("%s/logs/%s/%s/%s/%s/%d/", l.base, tenant, pipeline, runID, taskID, attempt)
}

// Result is where the wrapper stores an attempt's result file.
func (l Layout) Result(tenant, runID, taskID string, attempt int) string {
	return fmt.Sprintf("%s/results/%s/%s/%s/%d.json", l.base, tenant, runID, taskID, attempt)
}

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
