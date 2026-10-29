// Package catalogcli is what `hubwagen datasets`, `pallets` and `pallet` do,
// apart from flag parsing: read the catalog over CatalogService and print
// it for people.
package catalogcli

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/api/gen/hubwagen/v1/hubwagenv1connect"
)

type Client struct {
	catalog hubwagenv1connect.CatalogServiceClient
}

func NewClient(server string) *Client {
	return &Client{catalog: hubwagenv1connect.NewCatalogServiceClient(http.DefaultClient, strings.TrimSuffix(server, "/"))}
}

// maxPartitions is how many partitions one listing reads.
const maxPartitions = 1000

// Datasets writes every dataset of tenant with how many partitions it has
// and its newest partition.
func (c *Client) Datasets(ctx context.Context, w io.Writer, tenant string) error {
	res, err := c.catalog.ListDatasets(ctx, connect.NewRequest(&hubwagenv1.ListDatasetsRequest{Tenant: tenant}))
	if err != nil {
		return err
	}
	if len(res.Msg.Datasets) == 0 {
		_, err := fmt.Fprintln(w, "no datasets yet (a step with output=hw.Dataset(...) or one returning a dataframe makes one)")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "DATASET\tPARTITIONED BY\tPARTITIONS\tLATEST\tKEEP")
	for _, d := range res.Msg.Datasets {
		parts, err := c.catalog.ListPartitions(ctx, connect.NewRequest(&hubwagenv1.ListPartitionsRequest{
			Tenant: d.Tenant, Dataset: d.Name, Limit: maxPartitions}))
		if err != nil {
			return err
		}
		key, latest := d.PartitionKey, "-"
		if key == "" {
			key = "-"
		}
		if ps := parts.Msg.Partitions; len(ps) > 0 {
			newest := ps[0].Latest
			for _, p := range ps {
				if p.Latest.CommittedAt.AsTime().After(newest.CommittedAt.AsTime()) {
					newest = p.Latest
				}
			}
			latest = fmt.Sprintf("%s %s", describePartition(newest.Partition), when(newest.CommittedAt.AsTime()))
		}
		count := fmt.Sprint(len(parts.Msg.Partitions))
		if len(parts.Msg.Partitions) == maxPartitions {
			count += "+"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", d.Name, key, count, latest, describeKeep(d))
	}
	return tw.Flush()
}

func describeKeep(d *hubwagenv1.Dataset) string {
	var keep []string
	if d.KeepVersions > 0 {
		keep = append(keep, fmt.Sprintf("%d version%s", d.KeepVersions, plural(int(d.KeepVersions))))
	}
	if d.KeepFor != nil {
		keep = append(keep, d.KeepFor.AsDuration().String())
	}
	if len(keep) == 0 {
		return "all"
	}
	return strings.Join(keep, " or ")
}

// GC expires what retention no longer keeps, or with dryRun shows it.
func (c *Client) GC(ctx context.Context, w io.Writer, dryRun bool) error {
	res, err := c.catalog.ExpirePallets(ctx, connect.NewRequest(&hubwagenv1.ExpirePalletsRequest{DryRun: dryRun}))
	if err != nil {
		return err
	}
	ex := res.Msg.Expired
	if len(ex) == 0 {
		_, err := fmt.Fprintln(w, "nothing to expire")
		return err
	}
	verb := "expired"
	if dryRun {
		verb = "would expire"
	}
	fmt.Fprintf(w, "%s %d version%s:\n", verb, len(ex), plural(len(ex)))
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	for _, e := range ex {
		p := e.Pallet
		fmt.Fprintf(tw, "  %s\t%s\t%s\tsaved %s\n", p.Id, p.Dataset, describePartition(p.Partition), when(p.CommittedAt.AsTime()))
	}
	return tw.Flush()
}

// PalletsQuery is what `hubwagen versions` lists.
type PalletsQuery struct {
	Tenant    string
	Dataset   string
	Partition *string  // only this partition
	States    []string // only these states
	All       bool     // every pallet, not each partition's latest
	Limit     int
}

// Pallets writes a dataset's partitions with their latest pallets or, for
// one partition (or All), its pallets newest first.
func (c *Client) Pallets(ctx context.Context, w io.Writer, q PalletsQuery) error {
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	if q.Partition == nil && !q.All && len(q.States) == 0 {
		res, err := c.catalog.ListPartitions(ctx, connect.NewRequest(&hubwagenv1.ListPartitionsRequest{
			Tenant: q.Tenant, Dataset: q.Dataset, Limit: maxPartitions}))
		if err != nil {
			return err
		}
		if len(res.Msg.Partitions) == 0 {
			_, err := fmt.Fprintf(w, "%s has no saved versions\n", q.Dataset)
			return err
		}
		fmt.Fprintln(tw, "PARTITION\tLATEST VERSION\tSAVED\tVERSIONS")
		for _, p := range res.Msg.Partitions {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%d\n", describePartition(p.Partition), p.Latest.Id,
				when(p.Latest.CommittedAt.AsTime()), p.Pallets)
		}
		return tw.Flush()
	}
	res, err := c.catalog.ListPallets(ctx, connect.NewRequest(&hubwagenv1.ListPalletsRequest{
		Tenant: q.Tenant, Dataset: q.Dataset, Partition: q.Partition, States: q.States, Limit: int32(q.Limit)}))
	if err != nil {
		return err
	}
	if len(res.Msg.Pallets) == 0 {
		_, err := fmt.Fprintln(w, "no versions match")
		return err
	}
	fmt.Fprintln(tw, "VERSION\tPARTITION\tSTATE\tSAVED\tWRITTEN BY RUN")
	for _, p := range res.Msg.Pallets {
		committed := "-"
		if p.CommittedAt != nil {
			committed = when(p.CommittedAt.AsTime())
		}
		state := p.State
		if p.Reason != "" {
			state += " (" + p.Reason + ")"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", p.Id, describePartition(p.Partition), state, committed, p.HaulId)
	}
	return tw.Flush()
}

// Pallet writes one pallet, its files and its lineage as two trees.
func (c *Client) Pallet(ctx context.Context, w io.Writer, id string) error {
	res, err := c.catalog.GetPallet(ctx, connect.NewRequest(&hubwagenv1.GetPalletRequest{Id: id}))
	if err != nil {
		return err
	}
	m := res.Msg
	p := m.Pallet
	fmt.Fprintf(w, "data version %s, %s\n", p.Id, p.State)
	if p.Reason != "" {
		fmt.Fprintf(w, "  reason     %s\n", p.Reason)
	}
	fmt.Fprintf(w, "  dataset    %s, partition %s\n", p.Dataset, describePartition(p.Partition))
	built := fmt.Sprintf("run %s, step %d", p.HaulId, p.LiftSeq)
	if m.Lift != "" {
		built = fmt.Sprintf("step %s in run %s", m.Lift, p.HaulId)
	}
	fmt.Fprintf(w, "  written by %s\n", built)
	if p.CommittedAt != nil {
		fmt.Fprintf(w, "  saved      %s\n", when(p.CommittedAt.AsTime()))
	}
	if p.ContentHash != "" {
		fmt.Fprintf(w, "  content    %s\n", p.ContentHash)
	}
	if p.MetaJson != "" {
		fmt.Fprintf(w, "  meta       %s\n", p.MetaJson)
	}
	if mf := m.Manifest; mf != nil {
		var files []string
		for _, f := range mf.Files {
			files = append(files, fmt.Sprintf("%s (%s)", f.Path, size(f.Size)))
		}
		if len(files) == 0 {
			files = []string{"none"}
		}
		fmt.Fprintf(w, "  files      %s\n             under %s\n", strings.Join(files, ", "), mf.Data)
		if mf.ResultJson != "" {
			fmt.Fprintf(w, "  result     %s\n", mf.ResultJson)
		}
	} else if m.ManifestError != "" {
		fmt.Fprintf(w, "  files      can't be read: %s\n", m.ManifestError)
	}
	fmt.Fprintln(w, "\nbuilt from:")
	tree(w, p.Id, m.Upstream, func(e *hubwagenv1.LineageEdge) (string, string) { return e.PalletId, e.InputId })
	fmt.Fprintln(w, "\nbuilt into:")
	tree(w, p.Id, m.Downstream, func(e *hubwagenv1.LineageEdge) (string, string) { return e.InputId, e.PalletId })
	return nil
}

// InvalidateQuery is what `hubwagen data invalidate` does. Confirm is
// asked, after the dry run is shown, before anything is marked.
type InvalidateQuery struct {
	ID, Reason, By string
	DryRun         bool
	Confirm        func(n int) bool
}

// Invalidate shows what invalidating a pallet marks, marks it unless
// DryRun (or Confirm says no), and prints the starts that recompute it.
func (c *Client) Invalidate(ctx context.Context, w io.Writer, q InvalidateQuery) error {
	req := &hubwagenv1.InvalidatePalletRequest{Id: q.ID, Reason: q.Reason, By: q.By, DryRun: true}
	res, err := c.catalog.InvalidatePallet(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	ps := res.Msg.Pallets
	if len(ps) == 0 {
		_, err := fmt.Fprintf(w, "nothing to mark: %s and what was built from it are already invalid or expired\n", q.ID)
		return err
	}
	verb := "marks"
	if q.DryRun {
		verb = "would mark"
	}
	fmt.Fprintf(w, "invalidating %s %s %d version%s invalid:\n", q.ID, verb, len(ps), plural(len(ps)))
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	for _, p := range ps {
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", p.Id, p.Dataset, describePartition(p.Partition), p.State)
	}
	tw.Flush()
	if q.DryRun {
		return replayHint(w, res.Msg.Hauls, "would recompute them")
	}
	if q.Confirm != nil && !q.Confirm(len(ps)) {
		_, err := fmt.Fprintln(w, "nothing changed")
		return err
	}
	req.DryRun = false
	res, err = c.catalog.InvalidatePallet(ctx, connect.NewRequest(req))
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "marked %d version%s invalid\n", len(res.Msg.Pallets), plural(len(res.Msg.Pallets)))
	return replayHint(w, res.Msg.Hauls, "recompute them")
}

// Stale writes the partitions whose latest pallet is out of date, why, and
// the starts that refresh them.
func (c *Client) Stale(ctx context.Context, w io.Writer, tenant, dataset string) error {
	res, err := c.catalog.ListStale(ctx, connect.NewRequest(&hubwagenv1.ListStaleRequest{Tenant: tenant, Dataset: dataset}))
	if err != nil {
		return err
	}
	if len(res.Msg.Stale) == 0 {
		_, err := fmt.Fprintln(w, "nothing is stale")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "DATASET\tPARTITION\tLATEST VERSION\tWHY")
	var hauls []*hubwagenv1.HaulRef
	for _, st := range res.Msg.Stale {
		p := st.Pallet
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", p.Dataset, describePartition(p.Partition), p.Id, strings.Join(st.Reasons, "; "))
		if st.Haul != nil {
			hauls = append(hauls, st.Haul)
		}
	}
	tw.Flush()
	return replayHint(w, hauls, "refresh them")
}

// replayHint prints a start per haul that built the pallets: memoization
// reuses what is still good, so those recompute only what isn't.
func replayHint(w io.Writer, hauls []*hubwagenv1.HaulRef, what string) error {
	if len(hauls) == 0 {
		return nil
	}
	fmt.Fprintf(w, "\nthese %s (memoized steps whose code and inputs didn't change are reused):\n", what)
	printed := map[string]bool{}
	for _, h := range hauls {
		var params map[string]json.RawMessage
		_ = json.Unmarshal([]byte(h.ParamsJson), &params)
		line := "  hubwagen start " + h.Name
		if h.Tenant != "" && h.Tenant != "default" {
			line += " --tenant " + h.Tenant
		}
		for _, k := range slices.Sorted(maps.Keys(params)) {
			v := string(params[k])
			var str string
			if json.Unmarshal(params[k], &str) == nil && !json.Valid([]byte(str)) {
				v = str // a plain string: --param takes it as is
			}
			line += " --param " + shellQuote(k+"="+v)
		}
		if !printed[line] { // hauls with the same params: one start does
			printed[line] = true
			fmt.Fprintln(w, line)
		}
	}
	return nil
}

func shellQuote(s string) string {
	if strings.IndexFunc(s, func(r rune) bool {
		return !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("-_.=:/,+@", r))
	}) < 0 {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}

// tree prints a lineage from root, following edges as from → to.
func tree(w io.Writer, root string, l *hubwagenv1.Lineage, dir func(*hubwagenv1.LineageEdge) (from, to string)) {
	pallets := map[string]*hubwagenv1.Pallet{}
	for _, p := range l.Pallets {
		pallets[p.Id] = p
	}
	next := map[string][]*hubwagenv1.LineageEdge{}
	for _, e := range l.Edges {
		from, _ := dir(e)
		next[from] = append(next[from], e)
	}
	for _, es := range next {
		sort.Slice(es, func(i, j int) bool { _, a := dir(es[i]); _, b := dir(es[j]); return a < b })
	}
	if len(next[root]) == 0 {
		fmt.Fprintln(w, "  nothing")
		return
	}
	seen := map[string]bool{}
	var walk func(id string, depth int)
	walk = func(id string, depth int) {
		for _, e := range next[id] {
			_, to := dir(e)
			p := pallets[to]
			line := fmt.Sprintf("%s%s  %s %s", strings.Repeat("  ", depth+1), to, p.GetDataset(), describePartition(p.GetPartition()))
			if e.Name != "" {
				line += fmt.Sprintf("  (as %s)", e.Name)
			}
			if st := p.GetState(); st != "committed" {
				line += "  " + st
			}
			if seen[to] {
				fmt.Fprintln(w, line+"  (shown above)")
				continue
			}
			seen[to] = true
			fmt.Fprintln(w, line)
			walk(to, depth+1)
		}
	}
	walk(root, 0)
}

func describePartition(p string) string {
	if p == "" {
		return "-"
	}
	return p
}

func when(t time.Time) string { return t.Local().Format("2006-01-02 15:04:05") }

func size(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GiB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d B", n)
}
