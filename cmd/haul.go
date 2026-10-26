package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	hubwagenv1 "github.com/alper/hubwagen/internal/api/gen/hubwagen/v1"
	"github.com/alper/hubwagen/internal/haulcli"
)

// Flags shared by start and replay.
var (
	haulServer string
	haulTenant string
	haulDock   string
	haulParams []string
	startWait  bool
)

func addHaulFlags(c *cobra.Command) {
	c.Flags().StringVar(&haulServer, "server", "http://127.0.0.1:7070", "hubwagen API to talk to (hubwagen dev serves it)")
	c.Flags().StringVar(&haulTenant, "tenant", "default", "tenant the haul belongs to")
	c.Flags().StringVar(&haulDock, "dock", "", "dock its haul tasks go to (default: default)")
	c.Flags().StringArrayVar(&haulParams, "param", nil, "haul parameter as name=value (repeatable); values are JSON if they parse, else strings")
}

var startCmd = &cobra.Command{
	Use:   "start <haul>",
	Short: "Start a haul on a running server",
	Long: `Start starts one haul on the server (hubwagen dev by default) and prints its ID.
Workers registered with the server run it. With --wait it waits for the haul
to end, prints what each lift did, and exits non-zero unless it completed.

  hubwagen start daily_revenue --param day=2026-10-24 --wait`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		sets, err := haulcli.ExpandParams(haulParams)
		if err != nil {
			return err
		}
		if len(sets) != 1 {
			return fmt.Errorf("start takes no ranges (a..b); use hubwagen replay")
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		c := haulcli.NewClient(haulServer)
		id, err := c.Start(ctx, haulcli.HaulRequest{Tenant: haulTenant, Name: args[0], Dock: haulDock, Params: sets[0]})
		if err != nil {
			return serverError(err)
		}
		fmt.Println(id)
		if !startWait {
			return nil
		}
		return waitAndReport(ctx, c, []string{id})
	},
}

var replayCmd = &cobra.Command{
	Use:   "replay <haul>",
	Short: "Run a haul for a range of parameters and report what was reused",
	Long: `Replay starts one haul per parameter set, waits for all of them, and reports
per lift how many were reused and how many ran, and why. A --param value
a..b is a range: dates (daily) or integers, inclusive; several ranges
multiply. Memoized lifts whose code, inputs and parameters are unchanged
are reused, so replays and backfills only run what changed.

  hubwagen replay daily_revenue --param day=2026-03-01..2026-03-31

Exits non-zero unless every haul completed. Ctrl-C stops waiting; the
hauls keep running.`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		sets, err := haulcli.ExpandParams(haulParams)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		c := haulcli.NewClient(haulServer)
		var ids []string
		for _, params := range sets {
			id, err := c.Start(ctx, haulcli.HaulRequest{Tenant: haulTenant, Name: args[0], Dock: haulDock, Params: params})
			if err != nil {
				return serverError(err)
			}
			ids = append(ids, id)
		}
		fmt.Printf("%d hauls started\n", len(ids))
		return waitAndReport(ctx, c, ids)
	},
}

func waitAndReport(ctx context.Context, c *haulcli.Client, ids []string) error {
	hauls, err := c.Wait(ctx, ids, 500*time.Millisecond)
	if errors.Is(err, context.Canceled) {
		fmt.Println("stopped waiting; the hauls keep running")
		return nil
	}
	if err != nil {
		return serverError(err)
	}
	fmt.Print(haulcli.Report(hauls))
	for _, h := range hauls {
		if h.Status != "completed" {
			return fmt.Errorf("%d of %d hauls did not complete", countNot(hauls, "completed"), len(hauls))
		}
	}
	return nil
}

func countNot(hauls []*hubwagenv1.Haul, status string) int {
	n := 0
	for _, h := range hauls {
		if h.Status != status {
			n++
		}
	}
	return n
}

// serverError explains the usual reason a call fails: no server.
func serverError(err error) error {
	if connect.CodeOf(err) == connect.CodeUnavailable {
		return fmt.Errorf("%w (is hubwagen dev running at %s?)", err, haulServer)
	}
	return err
}

func init() {
	addHaulFlags(startCmd)
	startCmd.Flags().BoolVar(&startWait, "wait", false, "wait for the haul to end and report what its lifts did")
	addHaulFlags(replayCmd)
	rootCmd.AddCommand(startCmd, replayCmd)
}
