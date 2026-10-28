// Package catalogcli is what `hubwagen datasets`, `pallets` and `pallet` do,
// apart from flag parsing: read the catalog over CatalogService and print
// it for people.
package catalogcli

import (
	"context"
	"fmt"
	"io"
	"net/http"
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
		_, err := fmt.Fprintln(w, "no datasets yet (a lift with output=hw.Dataset(...) or one returning a dataframe makes one)")
		return err
	}
	tw := tabwriter.NewWriter(w, 0, 0, 3, ' ', 0)
	fmt.Fprintln(tw, "DATASET\tPARTITIONED BY\tPARTITIONS\tLATEST")
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
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\n", d.Name, key, count, latest)
	}
	return tw.Flush()
}

// PalletsQuery is what `hubwagen pallets` lists.
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
			_, err := fmt.Fprintf(w, "%s has no committed pallets\n", q.Dataset)
			return err
		}
		fmt.Fprintln(tw, "PARTITION\tLATEST PALLET\tCOMMITTED\tVERSIONS")
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
		_, err := fmt.Fprintln(w, "no pallets match")
		return err
	}
	fmt.Fprintln(tw, "PALLET\tPARTITION\tSTATE\tCOMMITTED\tBUILT BY HAUL")
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
	fmt.Fprintf(w, "pallet %s, %s\n", p.Id, p.State)
	if p.Reason != "" {
		fmt.Fprintf(w, "  reason     %s\n", p.Reason)
	}
	fmt.Fprintf(w, "  dataset    %s, partition %s\n", p.Dataset, describePartition(p.Partition))
	built := fmt.Sprintf("haul %s, command %d", p.HaulId, p.LiftSeq)
	if m.Lift != "" {
		built = fmt.Sprintf("lift %s in %s", m.Lift, built)
	}
	fmt.Fprintf(w, "  built by   %s\n", built)
	if p.CommittedAt != nil {
		fmt.Fprintf(w, "  committed  %s\n", when(p.CommittedAt.AsTime()))
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
