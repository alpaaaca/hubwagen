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
	devPython  string
)

// devWatchEvery is how often dev looks for changes to a project's code.
var devWatchEvery = time.Second

var devCmd = &cobra.Command{
	Use:   "dev",
	Short: "Run a moksha server and its workers on this machine",
	Long: `Dev runs the whole server in this process, with its state, event_logs and
data_versions in --data-dir, and serves the API on --listen. Workers connect to it
and run the runs that moksha start and moksha replay start.

In a project's directory (a moksha.yaml or moksha.project.yaml with
kind: Project, docs/projects.md), dev deploys the project to itself in
place: its pools' workers run python3 -m moksha.worker (--python) in that
directory, with PYTHONPATH as it is here. When the project's Python files,
pyproject.toml or project file change, it deploys again: the new version's
workers start and the old ones finish what they run. A deploy that fails
(a syntax error) is printed, and the next change tries again.

Settings come from moksha.yaml (--config, $MOKSHA_CONFIG, else
./moksha.yaml if present); flags override it. When that file is the
project file, the server's settings are the defaults, with its data in
.moksha next to it. Deployed workers' output is prefixed with their ID;
one that exits is restarted. --no-workers starts the server alone and
deploys nothing (run workers by hand:
python -m moksha.worker app.pipelines, with MOKSHA_SERVER set).

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
	devCmd.Flags().BoolVar(&noWorkers, "no-workers", false, "start no workers and deploy nothing (they run elsewhere)")
	devCmd.Flags().StringVar(&devPython, "python", "python3", "the interpreter a project's workers run in")
	devCmd.Flags().BoolVar(&devEvents, "events", false, "print the engine's events (steps scheduled, dispatched, ...) instead of runs, for debugging moksha")
	addConfigFlag(devCmd)
	rootCmd.AddCommand(devCmd)
}

func runDev(cmd *cobra.Command, _ []string) error {
	cfg, project, err := devConfig(cmd)
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
	if project != "" && cfg.Path == "" {
		fmt.Printf("moksha dev: project %s (server settings: the defaults)\n", project)
	} else {
		fmt.Printf("moksha dev: config %s\n", configName(cfg))
	}
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
	if !noWorkers {
		// dev prints what steps print, from the server: workers needn't echo it.
		if err = startWorkers(workersCtx, g, cfg, e.Core, config.ServerURL(e.APIAddr), slog.Default(), "MOKSHA_ECHO_LOGS=0"); err != nil {
			stopWorkers() // it started none
			return err
		}
	}
	go func() {
		defer close(workersDone)
		g.Wait()
	}()
	if project != "" && noWorkers {
		fmt.Printf("moksha dev: not deploying %s (--no-workers)\n", project)
	} else if project != "" {
		d := &devmode.Deployer{Path: project, Server: config.ServerURL(e.APIAddr), Python: devPython, By: deployer(),
			Print: func(line string) { fmt.Println("moksha dev: " + line) }, Log: os.Stdout}
		if err := d.Deploy(ctx); err != nil && ctx.Err() == nil {
			fmt.Printf("moksha dev: can't deploy %s: %v (dev tries again when it changes)\n", project, err)
		}
		go d.Watch(ctx, devWatchEvery)
	}
	defer func() {
		stopWorkers()
		<-workersDone
	}()

	select {
	case <-ctx.Done():
		if !noWorkers {
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

// devConfig is dev's config and the project it deploys, if any: the
// config file itself when it is a project file (then the server's settings
// are the defaults next to it), else a project file in the current
// directory.
func devConfig(cmd *cobra.Command) (*config.Config, string, error) {
	path, err := config.Find(configPath)
	if err != nil {
		return nil, "", err
	}
	if path != "" && config.IsProject(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return nil, "", err
		}
		cfg := config.Defaults(filepath.Dir(abs))
		applyFlags(cmd, cfg)
		return cfg, abs, nil
	}
	cfg, err := loadConfig(cmd)
	if err != nil {
		return nil, "", err
	}
	wd, err := os.Getwd()
	if err != nil {
		return nil, "", err
	}
	project, err := config.FindProject("", wd)
	if err != nil {
		project = "" // no project here: the config's workers only
	}
	return cfg, project, nil
}
