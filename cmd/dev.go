package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/devmode"
	"github.com/alper/hubwagen/internal/supervise"
)

var (
	dataDir   string
	devListen string
	noWorkers bool
)

var devCmd = &cobra.Command{
	Use:   "dev",
	Short: "Run a hubwagen server and its workers on this machine",
	Long: `Dev runs the whole server in this process, with its state, logbooks and
pallets in --data-dir, and serves the API on --listen. Workers connect to it
and run the hauls that hubwagen start and hubwagen replay start.

Settings come from hubwagen.yaml (--config, $HUBWAGEN_CONFIG, else
./hubwagen.yaml if present); flags override it. The worker processes listed
under workers: are started next to the server, their output prefixed with
their ID, and restarted if they exit. --no-workers starts the server alone
(run them with hubwagen workers, or by hand:
python -m hubwagen.worker app.hauls).

It prints a line for every notable event: hauls started, stuck and
finished, lifts scheduled (and why), reused, retried and finished, builds
registered. Ctrl-C stops the workers (running lifts finish first), then the server.
Work that workers outside dev are running carries on and is picked up on
the next start.

Only one process can use a data directory at a time.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         runDev,
}

func init() {
	devCmd.Flags().StringVar(&dataDir, "data-dir", config.DefaultDataDir, "directory for the server's state, logbooks and pallets (default: from the config)")
	devCmd.Flags().StringVar(&devListen, "listen", config.DefaultListen, "address to serve the API on (default: from the config)")
	devCmd.Flags().BoolVar(&noWorkers, "no-workers", false, "don't start the workers from the config")
	addConfigFlag(devCmd)
	rootCmd.AddCommand(devCmd)
}

func runDev(cmd *cobra.Command, _ []string) error {
	cfg, err := loadConfig(cmd)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	e, err := devmode.Start(ctx, devmode.Config{DataDir: cfg.DataDir, Listen: cfg.Server.Listen})
	if err != nil {
		return err
	}
	defer e.Close()
	from := e.LastSeq(ctx)
	fmt.Printf("hubwagen dev: config %s\n", configName(cfg))
	fmt.Printf("hubwagen dev: data in %s; Ctrl-C to stop\n", e.DataDir)
	fmt.Printf("hubwagen dev: API on http://%s\n", e.APIAddr)
	go e.Follow(ctx, from, func(line string) { fmt.Println("·", line) })

	// Workers stop before the server, so running lifts can still report.
	workersCtx, stopWorkers := context.WithCancel(context.Background())
	workersDone := make(chan struct{})
	var procs []supervise.Process
	if !noWorkers {
		procs = workerProcesses(cfg, config.ServerURL(e.APIAddr))
	}
	kill := make(chan struct{})
	go func() {
		defer close(workersDone)
		supervise.Run(workersCtx, procs, supervise.Options{Out: os.Stdout, Kill: kill})
	}()
	defer func() {
		stopWorkers()
		<-workersDone
	}()

	select {
	case <-ctx.Done():
		if len(procs) > 0 {
			fmt.Println("stopping workers: running lifts finish, polls end within their wait; Ctrl-C again to kill them")
			killOnSignal(kill)
		}
		stopWorkers()
		<-workersDone
		fmt.Println("stopping")
		return e.Close()
	case err := <-e.Done():
		return fmt.Errorf("server stopped: %w", err)
	}
}
