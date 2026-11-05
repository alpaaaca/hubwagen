package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/spf13/cobra"

	mokshav1 "github.com/alper/moksha/internal/api/gen/moksha/v1"
	"github.com/alper/moksha/internal/runcli"
)

// Flags shared by start and replay.
var (
	apiServer string
	runTenant string
	runQueue  string
	runParams []string
	startWait bool
)

func addRunFlags(c *cobra.Command) {
	c.Flags().StringVar(&apiServer, "server", "http://127.0.0.1:7070", "moksha API to talk to (moksha dev or moksha server)")
	c.Flags().StringVar(&runTenant, "tenant", "default", "tenant the run belongs to")
	c.Flags().StringVar(&runQueue, "queue", "", "queue its pipeline code runs from (default: default)")
	c.Flags().StringArrayVar(&runParams, "param", nil, "an input as name=value (repeatable); values are JSON if they parse, else strings")
}

var startCmd = &cobra.Command{
	Use:   "start <pipeline>",
	Short: "Start a run of a pipeline on a running server",
	Long: `Start starts one run of a pipeline on the server (moksha dev or moksha server)
and prints its ID. Workers registered with the server run it. With --wait it
waits for the run to end (following continue_as_new to the last run), prints
what each step did, and exits non-zero unless it completed.

  moksha start daily_revenue --param day=2026-10-24 --wait`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		sets, err := runcli.ExpandParams(runParams)
		if err != nil {
			return err
		}
		if len(sets) != 1 {
			return fmt.Errorf("start takes no ranges (a..b); use moksha replay")
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		c := runcli.NewClient(apiServer)
		id, err := c.Start(ctx, runcli.RunRequest{Tenant: runTenant, Name: args[0], Queue: runQueue, Params: sets[0]})
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
	Use:   "replay <pipeline>",
	Short: "Run a pipeline for a range of inputs and report what was reused",
	Long: `Replay starts one run of a pipeline per set of inputs, waits for all of them,
and reports per step how many were reused and how many ran, and why. A
--param value a..b is a range: dates (daily) or integers, inclusive; several
ranges multiply. Memoized steps whose code, inputs and arguments are
unchanged are reused, so replays and backfills only run what changed.

  moksha replay daily_revenue --param day=2026-03-01..2026-03-31

Exits non-zero unless every run completed. Ctrl-C stops waiting; the runs
keep running.`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		sets, err := runcli.ExpandParams(runParams)
		if err != nil {
			return err
		}
		ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		c := runcli.NewClient(apiServer)
		var ids []string
		for _, params := range sets {
			id, err := c.Start(ctx, runcli.RunRequest{Tenant: runTenant, Name: args[0], Queue: runQueue, Params: params})
			if err != nil {
				return serverError(err)
			}
			ids = append(ids, id)
		}
		fmt.Printf("%d run%s started\n", len(ids), map[bool]string{true: "", false: "s"}[len(ids) == 1])
		return waitAndReport(ctx, c, ids)
	},
}

func waitAndReport(ctx context.Context, c *runcli.Client, ids []string) error {
	c.Hint = func(s string) { fmt.Println(s) }
	runs, err := c.Wait(ctx, ids, 500*time.Millisecond)
	if errors.Is(err, context.Canceled) {
		fmt.Println("stopped waiting; the runs keep going")
		return nil
	}
	if err != nil {
		return serverError(err)
	}
	fmt.Print(runcli.Report(runs))
	for _, h := range runs {
		if h.Status != "completed" {
			return fmt.Errorf("%d of %d runs did not complete", countNot(runs, "completed"), len(runs))
		}
	}
	return nil
}

func countNot(runs []*mokshav1.Run, status string) int {
	n := 0
	for _, h := range runs {
		if h.Status != status {
			n++
		}
	}
	return n
}

// serverError explains the usual reason a call fails: no server.
func serverError(err error) error {
	if connect.CodeOf(err) == connect.CodeUnavailable {
		return fmt.Errorf("%w (is moksha dev running at %s?)", err, apiServer)
	}
	return err
}

var schedulesCmd = &cobra.Command{
	Use:   "schedules",
	Short: "List the pipelines that run on a schedule or on new data",
	Long: `Schedules lists the pipelines that run on a schedule: their cron, timezone,
the data interval each run covers, what happens to runs missed while no
server ran (catch-up), and the next run. Then the pipelines that new data
versions start: their datasets, batching and how many new versions wait. A
pipeline gets either from its code, @mk.pipeline(schedule=...) or
subscribe=...; the newest build that defines the pipeline decides.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		return serverError(runcli.NewClient(apiServer).Schedules(cmd.Context(), os.Stdout))
	},
}

var signalData string

var signalCmd = &cobra.Command{
	Use:   "signal <run-id> <name>",
	Short: "Send a signal to a run that is going on",
	Long: `Signal sends a signal to a run, which gets it from ctx.wait_signal(name). A
signal the run isn't waiting for yet is kept until it waits for that name;
each signal answers one wait. --data is the payload, as JSON.

  moksha signal 01M6... approve --data '{"by": "ana"}'`,
	Args:         cobra.ExactArgs(2),
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		if signalData != "" && !json.Valid([]byte(signalData)) {
			return fmt.Errorf("--data is not JSON: %s", signalData)
		}
		id, err := runcli.NewClient(apiServer).Signal(cmd.Context(), args[0], args[1], signalData)
		if err != nil {
			return serverError(err)
		}
		fmt.Println(id)
		return nil
	},
}

func init() {
	signalCmd.Flags().StringVar(&apiServer, "server", "http://127.0.0.1:7070", "moksha API to talk to (moksha dev or moksha server)")
	signalCmd.Flags().StringVar(&signalData, "data", "", "payload, as JSON (default: null)")
	rootCmd.AddCommand(signalCmd)
	schedulesCmd.Flags().StringVar(&apiServer, "server", "http://127.0.0.1:7070", "moksha API to talk to (moksha dev or moksha server)")
	rootCmd.AddCommand(schedulesCmd)
	addRunFlags(startCmd)
	startCmd.Flags().BoolVar(&startWait, "wait", false, "wait for the run to end and report what its steps did")
	addRunFlags(replayCmd)
	rootCmd.AddCommand(startCmd, replayCmd)
}
