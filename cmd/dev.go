package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/alper/moksha/internal/config"
	"github.com/alper/moksha/internal/devmode"
	"github.com/alper/moksha/internal/supervise"
	"github.com/alper/moksha/internal/webui"
)

var (
	dataDir    string
	listenAddr string
	noWorkers  bool
	devEvents  bool
)

var devCmd = &cobra.Command{
	Use:   "dev",
	Short: "Run a moksha server and its workers on this machine",
	Long: `Dev runs the whole server in this process, with its state, event_logs and
data_versions in --data-dir, and serves the API on --listen. Workers connect to it
and run the runs that moksha start and moksha replay start.

Settings come from moksha.yaml (--config, $MOKSHA_CONFIG, else
./moksha.yaml if present); flags override it. The worker processes listed
under workers: are started next to the server, their output prefixed with
their ID, and restarted if they exit. --no-workers starts the server alone
(run them with moksha workers, or by hand:
python -m moksha.worker app.pipelines).

It prints a line when a run starts and when it ends, and in between what
its steps print and log, each line marked with the run's inputs and the
step. --events prints the engine's events instead (steps scheduled and
why, dispatched, reused, retried; builds registered), for debugging
moksha itself. Ctrl-C stops the workers (running steps finish first), then the server.
Work that workers outside dev are running carries on and is picked up on
the next start.

Only one process can use a data directory at a time.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE:         runDev,
}

func init() {
	devCmd.Flags().StringVar(&dataDir, "data-dir", config.DefaultDataDir, "directory for the server's state, event logs and data versions (default: from the config)")
	devCmd.Flags().StringVar(&listenAddr, "listen", config.DefaultListen, "address to serve the API on (default: from the config)")
	devCmd.Flags().BoolVar(&noWorkers, "no-workers", false, "don't start the workers from the config")
	devCmd.Flags().BoolVar(&devEvents, "events", false, "print the engine's events (steps scheduled, dispatched, ...) instead of runs, for debugging moksha")
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

	scfg, err := serverConfig(cfg)
	if err != nil {
		return err
	}
	e, err := devmode.Start(ctx, scfg)
	if err != nil {
		return err
	}
	defer e.Close()
	from := e.LastSeq(ctx)
	fmt.Printf("moksha dev: config %s\n", configName(cfg))
	fmt.Printf("moksha dev: data in %s; Ctrl-C to stop\n", e.DataDir)
	if scfg.S3 != nil {
		fmt.Printf("moksha dev: bucket %s\n", e.Bucket.Base())
	}
	fmt.Printf("moksha dev: API on http://%s\n", e.APIAddr)
	if webui.Built() {
		fmt.Printf("moksha dev: web UI on http://%s/\n", e.APIAddr)
	} else {
		fmt.Println("moksha dev: no web UI in this binary (go generate ./internal/webui, then go build)")
	}
	go e.Console(ctx, from, devEvents, func(line string) { fmt.Println(line) })

	// Workers stop before the server, so running steps can still report.
	workersCtx, stopWorkers := context.WithCancel(context.Background())
	workersDone := make(chan struct{})
	kill := make(chan struct{})
	g := supervise.NewGroup(workersCtx, supervise.Options{Out: os.Stdout, Kill: kill})
	groups := 0
	if !noWorkers {
		// dev prints what steps print, from the server: workers needn't echo it.
		if groups, err = startWorkers(workersCtx, g, cfg, e.Core, config.ServerURL(e.APIAddr), slog.Default(), "MOKSHA_ECHO_LOGS=0"); err != nil {
			stopWorkers() // it started none
			return err
		}
	}
	go func() {
		defer close(workersDone)
		g.Wait()
	}()
	defer func() {
		stopWorkers()
		<-workersDone
	}()

	select {
	case <-ctx.Done():
		if groups > 0 {
			fmt.Println("stopping workers: running steps finish, polls end within their wait; Ctrl-C again to kill them")
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
