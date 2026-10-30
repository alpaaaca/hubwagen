package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

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
partitions have a saved version, and the newest one.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return serverError(catalogcli.NewClient(haulServer).Datasets(cmd.Context(), os.Stdout, catalogTenant))
	},
}

var palletsCmd = &cobra.Command{
	Use:     "versions <dataset>",
	Aliases: []string{"pallets"},
	Short:   "List a dataset's partitions, or its versions",
	Long: `Versions lists a dataset's partitions, each with its latest version and how
many versions it has. With --partition, --state or --all it lists the
versions themselves, newest first. (The engine calls a data version a
pallet; "pallets" works too.)

  hubwagen versions mart.daily_revenue
  hubwagen versions mart.daily_revenue --partition 2026-10-01
  hubwagen versions raw.orders --state invalid`,
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
	Use:     "data <version-id>",
	Aliases: []string{"pallet"},
	Short:   "Show a data version, its files and its lineage",
	Long: `Data shows one version of a dataset: its dataset and partition, the step and
run that wrote it, its files, and its lineage both ways: everything it was
built from, and everything built from it. ("pallet" works too.)`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		return serverError(catalogcli.NewClient(haulServer).Pallet(cmd.Context(), os.Stdout, args[0]))
	},
}

var staleCmd = &cobra.Command{
	Use:   "stale [dataset]",
	Short: "List the partitions whose latest version is out of date",
	Long: `Stale lists the partitions whose latest version is out of date, and why: one of
its inputs' partitions has a newer version with other content, or the newest
build has another code version of the step that wrote it. Then it prints
the runs to start again: their unchanged steps are reused.

  hubwagen stale
  hubwagen stale mart.daily_revenue`,
	Args:         cobra.MaximumNArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		dataset := ""
		if len(args) == 1 {
			dataset = args[0]
		}
		return serverError(catalogcli.NewClient(haulServer).Stale(cmd.Context(), os.Stdout, catalogTenant, dataset))
	},
}

var gcDryRun bool

var gcCmd = &cobra.Command{
	Use:   "gc",
	Short: "Expire data versions that retention no longer keeps",
	Long: `GC expires, now, what the leader otherwise expires every hour: per dataset with
a retention (hw.Dataset(keep=...)), the versions beyond what it keeps, never a
partition's latest or anything an open run may read. They are recorded as
expired, then their data is deleted; their manifests and lineage stay.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return serverError(catalogcli.NewClient(haulServer).GC(cmd.Context(), os.Stdout, gcDryRun))
	},
}

var (
	invalidateReason string
	invalidateDryRun bool
	invalidateYes    bool
)

var invalidateCmd = &cobra.Command{
	Use:   "invalidate <version-id>",
	Short: "Mark a data version and everything built from it invalid",
	Long: `Invalidate marks a data version and every version built from it,
transitively, invalid: bad data that must not be reused or read. It shows
what it will mark and asks first (--yes skips the question, --dry-run only
shows), then prints the runs to start again: their memoized steps are
reused, so only the invalid ones run.

  hubwagen data invalidate 01M6... --reason "orders export was truncated"`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if invalidateReason == "" {
			return errors.New("--reason is required: it is recorded with every version marked")
		}
		q := catalogcli.InvalidateQuery{ID: args[0], Reason: invalidateReason, By: os.Getenv("USER"), DryRun: invalidateDryRun}
		if !invalidateYes {
			if fi, err := os.Stdin.Stat(); err != nil || fi.Mode()&os.ModeCharDevice == 0 {
				q.Confirm = func(int) bool {
					fmt.Fprintln(os.Stderr, "not a terminal: pass --yes to invalidate")
					return false
				}
			} else {
				q.Confirm = func(n int) bool {
					fmt.Printf("mark these %d invalid? [y/N] ", n)
					answer, _ := bufio.NewReader(os.Stdin).ReadString('\n')
					return strings.EqualFold(strings.TrimSpace(answer), "y")
				}
			}
		}
		return serverError(catalogcli.NewClient(haulServer).Invalidate(cmd.Context(), os.Stdout, q))
	},
}

func init() {
	invalidateCmd.Flags().StringVar(&haulServer, "server", "http://127.0.0.1:7070", "hubwagen API to talk to (hubwagen dev or hubwagen server)")
	invalidateCmd.Flags().StringVar(&invalidateReason, "reason", "", "why the data is bad (required)")
	invalidateCmd.Flags().BoolVar(&invalidateDryRun, "dry-run", false, "only show what would be marked")
	invalidateCmd.Flags().BoolVarP(&invalidateYes, "yes", "y", false, "don't ask")
	palletCmd.AddCommand(invalidateCmd)
	gcCmd.Flags().StringVar(&haulServer, "server", "http://127.0.0.1:7070", "hubwagen API to talk to (hubwagen dev or hubwagen server)")
	gcCmd.Flags().BoolVar(&gcDryRun, "dry-run", false, "only show what would expire")
	rootCmd.AddCommand(gcCmd)

	for _, c := range []*cobra.Command{datasetsCmd, palletsCmd, palletCmd, staleCmd} {
		c.Flags().StringVar(&haulServer, "server", "http://127.0.0.1:7070", "hubwagen API to talk to (hubwagen dev or hubwagen server)")
		if c != palletCmd {
			c.Flags().StringVar(&catalogTenant, "tenant", "default", "tenant")
		}
		rootCmd.AddCommand(c)
	}
	palletsCmd.Flags().StringVar(&palletsPartition, "partition", "", "only this partition")
	palletsCmd.Flags().StringSliceVar(&palletsStates, "state", nil, "only versions in these states (committed, pending, invalid, expired)")
	palletsCmd.Flags().BoolVar(&palletsAll, "all", false, "every version, not each partition's latest")
	palletsCmd.Flags().IntVar(&palletsLimit, "limit", 50, "at most this many versions")
}
