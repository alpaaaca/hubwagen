package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/mattn/go-isatty"
	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/devmode"
	"github.com/alper/hubwagen/internal/tui"
)

var (
	dataDir     string
	devSlots    int
	devPools    []string
	devHeadless bool
)

func addDataDirFlag(c *cobra.Command) {
	c.Flags().StringVar(&dataDir, "data-dir", ".hubwagen", "directory for the engine's state, logs and outputs")
}

var devCmd = &cobra.Command{
	Use:     "dev [pipeline-file-or-dir]...",
	Aliases: []string{"tui"},
	Short:   "Run the scheduler locally with a terminal dashboard",
	Long: `Dev runs the whole engine in this process, against --data-dir: pipelines from
the given files and directories (default: dags) are compiled and published,
and reloaded when they change; schedules run; tasks run as host processes in
the "local" pool.

On a terminal it opens the dashboard (pipelines, runs, task status and logs;
t triggers a run). With --headless, or without a terminal, it prints events
and task logs instead.

Only one process can use a data directory at a time.`,
	SilenceUsage: true,
	RunE:         runDev,
}

func init() {
	addDataDirFlag(devCmd)
	devCmd.Flags().IntVar(&devSlots, "slots", 4, "tasks that may run at the same time")
	devCmd.Flags().StringSliceVar(&devPools, "pools", []string{"local"}, "agent pools to serve with host processes")
	devCmd.Flags().BoolVar(&devHeadless, "headless", false, "no dashboard; print events and task logs")
	rootCmd.AddCommand(devCmd)
}

func runDev(cmd *cobra.Command, args []string) error {
	paths := args
	if len(paths) == 0 {
		paths = []string{"dags"}
	}
	dashboard := !devHeadless && isatty.IsTerminal(os.Stdout.Fd()) && isatty.IsTerminal(os.Stdin.Fd())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := devmode.Config{DataDir: dataDir, Slots: devSlots, Pools: devPools}
	if dashboard {
		// Anything written to the terminal would corrupt the dashboard.
		logFile, err := openDevLog(dataDir)
		if err != nil {
			return err
		}
		defer logFile.Close()
		cfg.Logger = slog.New(slog.NewTextHandler(logFile, nil))
		slog.SetDefault(cfg.Logger)
	} else {
		cfg.Echo = os.Stdout
	}
	e, err := devmode.Start(ctx, cfg)
	if err != nil {
		return err
	}
	defer e.Close()

	loader := devmode.NewLoader(e.Core, paths)
	from := e.LastSeq(ctx)
	loaded, err := loader.Load(ctx)
	if err != nil {
		return err
	}

	if dashboard {
		go loader.Watch(ctx, 2*time.Second, nil) // the dashboard shows file errors itself
		p := tea.NewProgram(tui.NewApp(ctx, tui.EngineSource{Engine: e, Loader: loader}), tea.WithAltScreen(), tea.WithContext(ctx))
		if _, err := p.Run(); err != nil && ctx.Err() == nil {
			return err
		}
		fmt.Println("stopping: terminating running tasks…")
		return e.Close()
	}

	for _, f := range loaded {
		printFileStatus(f)
	}
	go loader.Watch(ctx, 2*time.Second, func(fs []devmode.FileStatus) {
		for _, f := range fs {
			printFileStatus(f)
		}
	})
	fmt.Printf("hubwagen dev: data in %s; Ctrl-C to stop\n", e.DataDir)
	go e.Follow(ctx, from, func(line string) { fmt.Println("·", line) })

	select {
	case <-ctx.Done():
		fmt.Println("stopping: terminating running tasks…")
		return e.Close()
	case err := <-e.Done():
		return fmt.Errorf("engine stopped: %w", err)
	}
}

func openDevLog(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(filepath.Join(dir, "dev.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
}

func printFileStatus(f devmode.FileStatus) {
	if f.Err != "" {
		fmt.Fprintf(os.Stderr, "✗ %s: %s\n", f.Path, f.Err)
		return
	}
	fmt.Printf("✓ %s: %v\n", f.Path, f.Pipelines)
}
