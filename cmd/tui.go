package cmd

import (
	"context"
	"fmt"
	"os"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/executor"
	"github.com/alper/hubwagen/internal/store"
	"github.com/alper/hubwagen/internal/tui"
)

// dagsDir is the directory hubwagen tui (and, later, the scheduler daemon)
// scans for DAG definitions. No existing command needed a *directory* of
// DAGs before this — `run`/`validate` both take one explicit file path —
// so this is genuinely new CLI surface. The default
// matches the documented hubwagen.yaml config default.
var dagsDir string

var tuiCmd = &cobra.Command{
	Use:          "tui",
	Short:        "Open the terminal dashboard",
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         runTUI,
}

func init() {
	tuiCmd.Flags().StringVar(&dagsDir, "dags-dir", "./dags", "directory to scan for DAG YAML files")
	rootCmd.AddCommand(tuiCmd)
}

func runTUI(_ *cobra.Command, _ []string) error {
	// Fail fast and clearly rather than hang or render garbage when launched
	// without an interactive terminal (spec Edge Cases).
	if !isatty.IsTerminal(os.Stdin.Fd()) {
		return fmt.Errorf("hubwagen tui requires an interactive terminal (none attached)")
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

	if _, err := os.Stat(dagsDir); err != nil {
		return fmt.Errorf("dags directory %q: %w", dagsDir, err)
	}

	executors := map[string]executor.Executor{
		"shell": &executor.ShellExecutor{},
		"noop":  &executor.NoopExecutor{},
	}

	app := tui.NewApp(ctx, st, dagsDir, executors)
	p := tea.NewProgram(app, tea.WithAltScreen())
	_, err = p.Run()
	return err
}
