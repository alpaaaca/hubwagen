package cmd

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/catalogcli"
)

var (
	catalogTenant    string
	palletsPartition string
	palletsStates    []string
	palletsAll       bool
	palletsLimit     int
)

var datasetsCmd = &cobra.Command{
	Use:   "datasets",
	Short: "List the datasets in the catalog",
	Long: `Datasets lists the catalog's datasets: what they are partitioned by, how many
partitions have a committed pallet, and the newest one.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return serverError(catalogcli.NewClient(haulServer).Datasets(cmd.Context(), os.Stdout, catalogTenant))
	},
}

var palletsCmd = &cobra.Command{
	Use:   "pallets <dataset>",
	Short: "List a dataset's partitions, or its pallets",
	Long: `Pallets lists a dataset's partitions, each with its latest pallet and how many
versions it has. With --partition, --state or --all it lists the pallets
themselves, newest first.

  hubwagen pallets mart.daily_revenue
  hubwagen pallets mart.daily_revenue --partition 2026-10-01
  hubwagen pallets raw.orders --state invalid`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		q := catalogcli.PalletsQuery{Tenant: catalogTenant, Dataset: args[0], States: palletsStates, All: palletsAll, Limit: palletsLimit}
		if cmd.Flags().Changed("partition") {
			q.Partition = &palletsPartition
		}
		return serverError(catalogcli.NewClient(haulServer).Pallets(cmd.Context(), os.Stdout, q))
	},
}

var palletCmd = &cobra.Command{
	Use:   "pallet <id>",
	Short: "Show a pallet, its files and its lineage",
	Long: `Pallet shows one pallet: its dataset and partition, the lift and haul that
built it, its files, and its lineage both ways: everything it was built
from, and everything built from it.`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return serverError(catalogcli.NewClient(haulServer).Pallet(cmd.Context(), os.Stdout, args[0]))
	},
}

func init() {
	for _, c := range []*cobra.Command{datasetsCmd, palletsCmd, palletCmd} {
		c.Flags().StringVar(&haulServer, "server", "http://127.0.0.1:7070", "hubwagen API to talk to (hubwagen dev serves it)")
		if c != palletCmd {
			c.Flags().StringVar(&catalogTenant, "tenant", "default", "tenant")
		}
		rootCmd.AddCommand(c)
	}
	palletsCmd.Flags().StringVar(&palletsPartition, "partition", "", "only this partition")
	palletsCmd.Flags().StringSliceVar(&palletsStates, "state", nil, "only pallets in these states (committed, pending, invalid, expired)")
	palletsCmd.Flags().BoolVar(&palletsAll, "all", false, "every pallet, not each partition's latest")
	palletsCmd.Flags().IntVar(&palletsLimit, "limit", 50, "at most this many pallets")
}
