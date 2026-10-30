package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/pools"
	"github.com/alper/hubwagen/internal/supervise"
)

var (
	configPath    string
	workersServer string
)

var workersCmd = &cobra.Command{
	Use:   "workers",
	Short: "Run the workers from hubwagen.yaml against a running server",
	Long: `Workers starts the worker processes listed under workers: in hubwagen.yaml,
the same ones hubwagen dev starts, but without a server: they connect to
--server (default: the config's server.listen). Use it to restart the server
while the workers keep running, or to run workers on another machine.

Each process gets HUBWAGEN_SERVER, HUBWAGEN_WORKER_ID (<name>-<n>) and
HUBWAGEN_SUPERVISOR_PID (this process; workers stop when it is gone). One
that exits is restarted with backoff; Ctrl-C stops them all.`,
	Args:         cobra.NoArgs,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, _ []string) error {
		cfg, err := loadConfig(cmd)
		if err != nil {
			return err
		}
		procs := workerProcesses(cfg, workersServer)
		if len(procs) == 0 {
			return fmt.Errorf("no workers configured (%s)", configName(cfg))
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		fmt.Printf("hubwagen workers: %d processes against %s; Ctrl-C to stop\n", len(procs), workersServer)
		kill := make(chan struct{})
		go func() {
			<-ctx.Done()
			fmt.Println("stopping: running steps finish, polls end within their wait; Ctrl-C again to kill them")
			killOnSignal(kill)
		}()
		supervise.Run(ctx, procs, supervise.Options{Out: os.Stdout, Kill: kill})
		return nil
	},
}

func init() {
	addConfigFlag(workersCmd)
	workersCmd.Flags().StringVar(&workersServer, "server", "", "hubwagen API the workers connect to (default: from the config)")
	rootCmd.AddCommand(workersCmd)
}

func addConfigFlag(c *cobra.Command) {
	c.Flags().StringVar(&configPath, "config", "", "config file (default: $HUBWAGEN_CONFIG, else ./"+config.FileName+" if present)")
}

// loadConfig loads the config and applies the command's flags on top.
func loadConfig(cmd *cobra.Command) (*config.Config, error) {
	path, err := config.Find(configPath)
	if err != nil {
		return nil, err
	}
	cfg, err := config.Load(path)
	if err != nil {
		return nil, err
	}
	f := cmd.Flags()
	if f.Changed("data-dir") {
		cfg.DataDir = dataDir
	}
	if f.Changed("listen") {
		cfg.Server.Listen = listenAddr
	}
	if f.Lookup("server") != nil && !f.Changed("server") {
		workersServer = config.ServerURL(cfg.Server.Listen)
	}
	return cfg, nil
}

func configName(cfg *config.Config) string {
	if cfg.Path == "" {
		return "no config file"
	}
	return cfg.Path
}

// workerProcesses expands the config's worker groups into processes, max
// of each: without a server, nothing scales them.
func workerProcesses(cfg *config.Config, server string, extraEnv ...string) []supervise.Process {
	var procs []supervise.Process
	for _, w := range cfg.Workers {
		if w.External() {
			continue // started by its own launcher
		}
		for n := 1; n <= w.Count(); n++ {
			procs = append(procs, workerProcess(w, w.Name+"-"+strconv.Itoa(n), server, extraEnv...))
		}
	}
	return procs
}

// workerProcess is one process of a worker group.
func workerProcess(w config.Worker, id, server string, extraEnv ...string) supervise.Process {
	env := os.Environ()
	for k, v := range w.Env {
		env = append(env, k+"="+v)
	}
	env = append(env, config.EnvServer+"="+server, config.EnvWorkerID+"="+id,
		config.EnvSupervisorPID+"="+strconv.Itoa(os.Getpid()),
		config.EnvDocks+"="+strings.Join(w.DockList(), ","), config.EnvLifts+"="+strconv.Itoa(w.LiftCount()))
	env = append(env, extraEnv...)
	return supervise.Process{ID: id, Command: w.Command, Dir: w.Dir, Env: env}
}

// startWorkers starts the config's worker groups in g for a server: fixed
// groups at their count, and scaled ones (min below max) from min, then by
// a scaler that runs until ctx ends. It returns how many groups it started.
func startWorkers(ctx context.Context, g *supervise.Group, cfg *config.Config, c *core.Core, server string, log *slog.Logger, extraEnv ...string) int {
	groups := map[string]config.Worker{}
	var scaled []pools.Pool
	for _, w := range cfg.Workers {
		groups[w.Name] = w
		lo, hi := w.Bounds()
		if !w.Scaled() {
			for n := 1; n <= hi; n++ {
				g.Start(workerProcess(w, w.Name+"-"+strconv.Itoa(n), server, extraEnv...))
			}
			continue
		}
		scaled = append(scaled, pools.Pool{Name: w.Name, Docks: w.DockList(), Lifts: w.LiftCount(), Min: lo, Max: hi,
			IdleAfter: w.Idle(), External: w.External()})
		msg := "scaled by the work waiting on its docks"
		if w.External() {
			msg = "started elsewhere; its desired count is at GET /v1/pools/" + w.Name
		}
		log.Info("worker pool "+w.Name+": "+msg, "pool", w.Name, "min", lo, "max", hi,
			"docks", strings.Join(w.DockList(), ","), "idle_after", w.Idle())
	}
	if len(scaled) > 0 {
		launch := groupLauncher{g: g, start: func(pool, id string) supervise.Process {
			return workerProcess(groups[pool], id, server, extraEnv...)
		}}
		go pools.New(scaled, launch, c, clock.Real{}, log).Run(ctx)
	}
	return len(cfg.Workers)
}

// groupLauncher starts and stops a pool's workers as processes of g.
type groupLauncher struct {
	g     *supervise.Group
	start func(pool, id string) supervise.Process
}

func (l groupLauncher) Start(pool, id string) { l.g.Start(l.start(pool, id)) }
func (l groupLauncher) Stop(id string)        { l.g.Stop(id) }

// killOnSignal closes kill on the next interrupt or SIGTERM.
func killOnSignal(kill chan struct{}) {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		signal.Stop(sig)
		close(kill)
	}()
}
