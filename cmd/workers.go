package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/config"
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
		cfg.Server.Listen = devListen
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

// workerProcesses expands the config's worker groups into processes.
func workerProcesses(cfg *config.Config, server string, extraEnv ...string) []supervise.Process {
	var procs []supervise.Process
	for _, w := range cfg.Workers {
		for n := 1; n <= w.Count(); n++ {
			id := w.Name + "-" + strconv.Itoa(n)
			env := os.Environ()
			for k, v := range w.Env {
				env = append(env, k+"="+v)
			}
			env = append(env, config.EnvServer+"="+server, config.EnvWorkerID+"="+id,
				config.EnvSupervisorPID+"="+strconv.Itoa(os.Getpid()))
			env = append(env, extraEnv...)
			procs = append(procs, supervise.Process{ID: id, Command: w.Command, Dir: w.Dir, Env: env})
		}
	}
	return procs
}

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
