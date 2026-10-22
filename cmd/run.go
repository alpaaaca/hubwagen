package cmd

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/devmode"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
)

var (
	runPipeline string
	runDate     string
	runInterval string
	runParams   []string
	runRerun    bool
	runQuiet    bool
	runDryRun   bool
)

var runCmd = &cobra.Command{
	Use:   "run <pipeline-file>",
	Short: "Run a pipeline once on this machine and wait for it",
	Long: `Run compiles a pipeline file, publishes it to a local engine in --data-dir,
triggers one run, streams the task logs, and exits non-zero if the run did not
succeed. Schedules in the file are ignored. Ctrl-C cancels the run.

The data interval comes from --date (one day in the pipeline's timezone) or
--interval START/END (dates or RFC 3339). Without either the run is ad hoc,
and templates that use .Interval fail.

Running the same interval again returns the earlier run; use --rerun for a
new generation.`,
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE:         runOnce,
}

func init() {
	runCmd.Flags().StringVar(&runPipeline, "pipeline", "", "pipeline to run when the file defines several (tenant/name or name)")
	runCmd.Flags().StringVar(&runDate, "date", "", "data interval: this day (YYYY-MM-DD) in the pipeline's timezone")
	runCmd.Flags().StringVar(&runInterval, "interval", "", "data interval: START/END, dates or RFC 3339")
	runCmd.Flags().StringArrayVar(&runParams, "param", nil, "param value as name=value (repeatable)")
	runCmd.Flags().BoolVar(&runRerun, "rerun", false, "run the interval again even if it already ran (new generation)")
	runCmd.Flags().BoolVarP(&runQuiet, "quiet", "q", false, "don't stream task logs")
	runCmd.Flags().BoolVar(&runDryRun, "dry-run", false, "compile and show the tasks in order; run nothing")
	addDataDirFlag(runCmd)
	rootCmd.AddCommand(runCmd)
}

func runOnce(cmd *cobra.Command, args []string) error {
	data, err := os.ReadFile(args[0])
	if err != nil {
		return err
	}
	compiled, err := spec.CompileFile(cmd.Context(), args[0], data, spec.Options{})
	if err != nil {
		return err
	}
	target, err := pickPipeline(compiled, runPipeline)
	if err != nil {
		return err
	}
	s := target.Spec

	if runDryRun {
		fmt.Printf("%s/%s (version %s)\n", s.Tenant, s.Name, target.Hash[:12])
		for _, t := range s.Tasks {
			deps := "-"
			if len(t.DependsOn) > 0 {
				deps = strings.Join(t.DependsOn, ", ")
			}
			fmt.Printf("  %-24s after: %s\n", t.ID, deps)
		}
		return nil
	}

	interval, err := parseInterval(s, runDate, runInterval)
	if err != nil {
		return err
	}
	params, err := parseParams(s, runParams)
	if err != nil {
		return err
	}

	cfg := devmode.Config{DataDir: dataDir, DisableSchedules: true}
	if !runQuiet {
		cfg.Echo = os.Stdout
	}
	ctx := context.Background()
	e, err := devmode.Start(ctx, cfg)
	if err != nil {
		return err
	}
	defer e.Close()

	if _, err := e.Core.Publish(ctx, core.PublishRequest{SpecJSON: target.Canonical, PublishedBy: "run:" + args[0]}); err != nil {
		return err
	}
	res, err := e.Core.Trigger(ctx, core.TriggerRequest{
		Tenant: s.Tenant, Pipeline: s.Name, Interval: interval, Params: params, Rerun: runRerun, By: "cli",
	})
	if err != nil {
		return err
	}
	switch {
	case res.Skipped:
		return fmt.Errorf("not started: %s", res.Reason)
	case res.Existing:
		fmt.Printf("this interval already has run %s (generation %d); showing it. Use --rerun to run it again.\n", res.RunID, res.Generation)
	}

	// First Ctrl-C cancels the run; the engine stops once it has finished.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sig)
	go func() {
		if _, ok := <-sig; ok {
			fmt.Fprintln(os.Stderr, "cancelling run…")
			_ = e.Core.CancelRun(ctx, res.RunID, "cli")
		}
	}()

	run, err := e.WaitRun(ctx, res.RunID)
	if err != nil {
		return err
	}
	if err := printSummary(ctx, e, run); err != nil {
		return err
	}
	if run.Status != state.RunSucceeded {
		return fmt.Errorf("run %s", run.Status)
	}
	return nil
}

func pickPipeline(compiled []spec.Compiled, want string) (spec.Compiled, error) {
	if want == "" {
		if len(compiled) == 1 {
			return compiled[0], nil
		}
		var names []string
		for _, c := range compiled {
			names = append(names, c.Spec.Tenant+"/"+c.Spec.Name)
		}
		return spec.Compiled{}, fmt.Errorf("the file defines several pipelines (%s); choose one with --pipeline", strings.Join(names, ", "))
	}
	for _, c := range compiled {
		if want == c.Spec.Name || want == c.Spec.Tenant+"/"+c.Spec.Name {
			return c, nil
		}
	}
	return spec.Compiled{}, fmt.Errorf("pipeline %q is not defined in the file", want)
}

// parseInterval reads --date or --interval in the pipeline's timezone.
func parseInterval(s *spec.Spec, date, interval string) (*spec.Interval, error) {
	if date != "" && interval != "" {
		return nil, errors.New("use either --date or --interval, not both")
	}
	loc := time.UTC
	if s.Schedule != nil {
		var err error
		if loc, err = time.LoadLocation(s.Schedule.Timezone); err != nil {
			return nil, err
		}
	}
	parse := func(v string) (time.Time, error) {
		if t, err := time.ParseInLocation("2006-01-02", v, loc); err == nil {
			return t, nil
		}
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return t, fmt.Errorf("%q is neither YYYY-MM-DD nor RFC 3339", v)
		}
		return t, nil
	}
	switch {
	case date != "":
		start, err := time.ParseInLocation("2006-01-02", date, loc)
		if err != nil {
			return nil, fmt.Errorf("--date %q: want YYYY-MM-DD", date)
		}
		return &spec.Interval{Start: start, End: start.AddDate(0, 0, 1)}, nil
	case interval != "":
		a, b, ok := strings.Cut(interval, "/")
		if !ok {
			return nil, fmt.Errorf("--interval %q: want START/END", interval)
		}
		start, err := parse(a)
		if err != nil {
			return nil, fmt.Errorf("--interval start: %w", err)
		}
		end, err := parse(b)
		if err != nil {
			return nil, fmt.Errorf("--interval end: %w", err)
		}
		return &spec.Interval{Start: start, End: end}, nil
	}
	return nil, nil
}

// parseParams converts name=value flags using the declared param types.
func parseParams(s *spec.Spec, flags []string) (map[string]any, error) {
	types := map[string]string{}
	for _, p := range s.Params {
		types[p.Name] = p.Type
	}
	out := map[string]any{}
	for _, f := range flags {
		name, raw, ok := strings.Cut(f, "=")
		if !ok {
			return nil, fmt.Errorf("--param %q: want name=value", f)
		}
		switch types[name] {
		case "int":
			n, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("--param %s: %q is not an int", name, raw)
			}
			out[name] = n
		case "bool":
			b, err := strconv.ParseBool(raw)
			if err != nil {
				return nil, fmt.Errorf("--param %s: %q is not a bool", name, raw)
			}
			out[name] = b
		default: // string, or unknown (the core reports unknown params)
			out[name] = raw
		}
	}
	return out, nil
}

func printSummary(ctx context.Context, e *devmode.Engine, run state.Run) error {
	tasks, err := e.Store.ListTaskRuns(ctx, run.ID)
	if err != nil {
		return err
	}
	took := ""
	if run.StartedAt != nil && run.FinishedAt != nil {
		took = " in " + run.FinishedAt.Sub(*run.StartedAt).Round(time.Millisecond).String()
	}
	fmt.Printf("\nrun %s (%s/%s) %s%s\n", run.ID, run.Tenant, run.Pipeline, run.Status, took)
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	for _, t := range tasks {
		d := ""
		if t.StartedAt != nil && t.FinishedAt != nil {
			d = t.FinishedAt.Sub(*t.StartedAt).Round(time.Millisecond).String()
		}
		reason := ""
		if t.Status == state.TaskFailed {
			if atts, err := e.Store.ListAttempts(ctx, run.ID, t.TaskID); err == nil && len(atts) > 0 {
				reason = atts[len(atts)-1].Reason
			}
		}
		fmt.Fprintf(w, "  %s\t%s\t%s\t%d attempt(s)\t%s\n", t.TaskID, t.Status, d, t.Attempts, reason)
	}
	return w.Flush()
}
