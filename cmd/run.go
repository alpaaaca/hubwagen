package cmd

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"time"

	"github.com/alper/hubwagen/internal/dag"
	"github.com/alper/hubwagen/internal/executor"
	internallog "github.com/alper/hubwagen/internal/log"
	"github.com/alper/hubwagen/internal/scheduler"
	"github.com/alper/hubwagen/internal/store"
	"github.com/spf13/cobra"
)

var dryRun bool

var runCmd = &cobra.Command{
	Use:          "run <dag-file>",
	Short:        "Run a DAG once from a YAML file",
	Args:         cobra.ExactArgs(1),
	SilenceUsage: true,
	RunE:         runDAG,
}

func init() {
	runCmd.Flags().BoolVar(&dryRun, "dry-run", false, "parse and validate only; do not execute")
	rootCmd.AddCommand(runCmd)
}

func runDAG(_ *cobra.Command, args []string) error {
	path := args[0]

	d, err := dag.ParseFile(path)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}

	if errs := dag.Validate(d); len(errs) > 0 {
		for _, e := range errs {
			fmt.Fprintf(os.Stderr, "validation error: %v\n", e)
		}
		return fmt.Errorf("DAG %q failed validation", d.Name)
	}

	if dryRun {
		return printExecutionPlan(d)
	}

	st, err := store.Open(dbPath)
	if err != nil {
		return fmt.Errorf("opening store: %w", err)
	}
	defer st.Close()

	ctx := context.Background()

	if err := st.MarkInterruptedRunsFailed(ctx); err != nil {
		return fmt.Errorf("recovering interrupted runs: %w", err)
	}

	rawYAML, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading DAG file for hashing: %w", err)
	}
	yamlHash := fmt.Sprintf("%x", sha256.Sum256(rawYAML))

	mw := &internallog.MultiWriter{}
	mw.Add(os.Stdout)

	executors := map[string]executor.Executor{
		"shell": &executor.ShellExecutor{},
		"noop":  &executor.NoopExecutor{},
	}

	discovered := dag.Discovered{Path: path, DAG: d, YAMLHash: yamlHash}

	start := time.Now()
	result, runErr := scheduler.TriggerManual(ctx, st, discovered, executors, mw, "manual")
	elapsed := time.Since(start)

	if result.Skipped {
		fmt.Printf("⧗ DAG %s skipped: %s\n", d.Name, result.Reason)
		return nil
	}

	if runErr != nil {
		fmt.Fprintf(os.Stderr, "✗ DAG %s failed in %.1fs (run: %s)\n", d.Name, elapsed.Seconds(), result.RunID)
		return runErr
	}

	fmt.Printf("✓ DAG %s completed in %.1fs (run: %s)\n", d.Name, elapsed.Seconds(), result.RunID)
	return nil
}

// printExecutionPlan prints the wave-by-wave step order --dry-run resolves
// to, without opening the DB or executing anything.
func printExecutionPlan(d *dag.DAG) error {
	waves, err := dag.TopologicalWaves(d.Steps)
	if err != nil {
		return fmt.Errorf("topological sort: %w", err)
	}

	fmt.Printf("✓ DAG %q is valid (%d steps, %d waves)\n\n", d.Name, len(d.Steps), len(waves))
	for i, wave := range waves {
		fmt.Printf("Wave %d:\n", i+1)
		for _, stepID := range wave {
			fmt.Printf("  - %s\n", stepID)
		}
	}
	return nil
}
