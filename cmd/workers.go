package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"os/exec"
	"os/signal"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/config"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/launch"
	"github.com/alper/hubwagen/internal/pools"
	"github.com/alper/hubwagen/internal/state"
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
	removeLeftovers(context.Background(), cfg, slog.Default())
	var procs []supervise.Process
	for _, w := range cfg.Workers {
		if w.External() {
			continue // started by its own launcher
		}
		for n := 1; n <= w.Count(); n++ {
			procs = append(procs, workerProcess(w, w.Name+"-"+strconv.Itoa(n), server, cfg.DataDir, extraEnv...))
		}
	}
	return procs
}

// workerProcess is one process of a worker group: the group's command,
// or for launcher: container, a container of its image.
func workerProcess(w config.Worker, id, server, dataDir string, extraEnv ...string) supervise.Process {
	var env []string
	for k, v := range w.Env {
		env = append(env, k+"="+v)
	}
	env = append(env, config.EnvWorkerID+"="+id,
		config.EnvDocks+"="+strings.Join(w.DockList(), ","), config.EnvLifts+"="+strconv.Itoa(w.LiftCount()))
	env = append(env, extraEnv...)
	if w.Launcher == config.LauncherContainer {
		// No supervisor PID: in the container's PID namespace it means
		// nothing. A container outliving hubwagen is removed when the
		// worker starts again (its cleanup).
		return launch.Container(w, id, server, dataDir, env)
	}
	env = append(env, config.EnvServer+"="+server, config.EnvSupervisorPID+"="+strconv.Itoa(os.Getpid()))
	return supervise.Process{ID: id, Command: w.Command, Dir: w.Dir, Env: append(os.Environ(), env...)}
}

// startWorkers starts the config's worker groups in g for a server: fixed
// groups at their count, and scaled ones (min below max) from min, then by
// a scaler that runs until ctx ends. The scaler also runs the projects
// deployed to the server (followProjects). It returns how many groups the
// config has. Kubernetes pools need the cluster: an error if it can't be
// reached, before anything starts.
func startWorkers(ctx context.Context, g *supervise.Group, cfg *config.Config, c *core.Core, server string, log *slog.Logger, extraEnv ...string) (int, error) {
	var kube *launch.Kube
	for _, w := range cfg.Workers { // the cluster first: an error starts nothing
		if w.Launcher == config.LauncherKubernetes && kube == nil {
			k, err := launch.NewKube(cfg.Kubernetes, launch.ServiceAccountDir)
			if err != nil {
				return 0, fmt.Errorf("workers %s: %w", w.Name, err)
			}
			kube = k
		}
	}
	removeLeftovers(ctx, cfg, log)
	launcher := &groupLauncher{g: g, start: map[string]func(id string) supervise.Process{}}
	var static []pools.Pool
	for _, w := range cfg.Workers {
		lo, hi := w.Bounds()
		if !w.Scaled() {
			for n := 1; n <= hi; n++ {
				g.Start(workerProcess(w, w.Name+"-"+strconv.Itoa(n), server, cfg.DataDir, extraEnv...))
			}
			continue
		}
		p := pools.Pool{Name: w.Name, Docks: w.DockList(), Lifts: w.LiftCount(), Min: lo, Max: hi,
			IdleAfter: w.Idle(), External: w.External()}
		msg := "scaled by the work waiting on its docks"
		switch w.Launcher {
		case config.LauncherExternal:
			msg = "started elsewhere; its desired count is at GET /v1/pools/" + w.Name
		case config.LauncherKubernetes:
			ns, deployment := w.Namespace, w.Deployment
			p.Apply = func(ctx context.Context, n int) error { return kube.Scale(ctx, ns, deployment, n) }
			msg = "the replicas of Deployment " + or(ns, kube.Namespace) + "/" + deployment
		case config.LauncherContainer:
			msg = "containers of " + w.Image + ", scaled by the work waiting on its docks"
		}
		static = append(static, p)
		launcher.set(w.Name, func(id string) supervise.Process { return workerProcess(w, id, server, cfg.DataDir, extraEnv...) })
		log.Info("worker pool "+w.Name+": "+msg, "pool", w.Name, "min", lo, "max", hi,
			"docks", strings.Join(w.DockList(), ","), "idle_after", w.Idle())
	}
	sc := pools.New(static, launcher, c, clock.Real{}, log)
	go sc.Run(ctx)
	go followProjects(ctx, c, sc, static, launcher, cfg, server, log, extraEnv)
	return len(cfg.Workers), nil
}

// followProjects runs the projects deployed to the server (docs/projects.md).
// Every pass, each live project's current version becomes pools of the
// scaler, named <project>-<pool>-v<version>, next to the config's own: a new
// version is a new pool, and the old version's goes (its workers stop and
// finish what they run).
func followProjects(ctx context.Context, c *core.Core, sc *pools.Scaler, static []pools.Pool, launcher *groupLauncher,
	cfg *config.Config, server string, log *slog.Logger, extraEnv []string) {
	t := time.NewTicker(pools.Every)
	defer t.Stop()
	running := map[string]int{} // project → version its pools run
	first := true
	for {
		ps, err := c.Projects(ctx)
		if err != nil && ctx.Err() == nil {
			log.Warn("reading projects", "err", err)
		}
		want := map[string]int{}
		all := slices.Clone(static)
		for _, p := range ps {
			if p.DeletedAt != nil {
				continue
			}
			want[p.Project] = p.Version
			for _, dp := range p.Pools {
				name := fmt.Sprintf("%s-%s-v%d", p.Project, dp.Name, p.Version)
				w := projectWorker(p, dp, name, cfg)
				launcher.set(name, func(id string) supervise.Process { return workerProcess(w, id, server, cfg.DataDir, extraEnv...) })
				all = append(all, pools.Pool{Name: name, Docks: dp.Docks, Lifts: dp.Lifts, Min: dp.Min, Max: dp.Max, IdleAfter: dp.IdleAfter})
			}
		}
		if err == nil && (first || !maps.Equal(want, running)) {
			for _, name := range slices.Sorted(maps.Keys(want)) {
				if v, ok := running[name]; !ok || v != want[name] {
					how := "starts"
					if ok {
						how = fmt.Sprintf("replaces v%d, whose workers finish what they run and stop", v)
					}
					log.Info(fmt.Sprintf("project %s v%d: %s", name, want[name], how), "project", name, "version", want[name])
				}
			}
			for _, name := range slices.Sorted(maps.Keys(running)) {
				if _, ok := want[name]; !ok {
					log.Info(fmt.Sprintf("project %s: deleted; its workers finish what they run and stop", name), "project", name)
				}
			}
			sc.SetPools(all)
			launcher.keep(all)
			running, first = want, false
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// projectWorker is the worker group a deployed pool runs as: containers of
// the deployment's image, or processes in its directory.
func projectWorker(p state.Project, dp state.DeployedPool, name string, cfg *config.Config) config.Worker {
	lifts := dp.Lifts
	w := config.Worker{Name: name, Docks: dp.Docks, Lifts: &lifts, Env: p.Env}
	if p.Image != "" {
		w.Launcher, w.Image, w.Runtime, w.ServerURL = config.LauncherContainer, p.Image, cfg.Projects.Runtime, cfg.Projects.ServerURL
		return w
	}
	w.Command, w.Dir = []string{or(p.Python, "python3"), "-m", "hubwagen.worker"}, p.Dir
	return w
}

func or(s, def string) string {
	if s == "" {
		return def
	}
	return s
}

// groupLauncher starts and stops pools' workers as processes of g, each
// pool's the way set says.
type groupLauncher struct {
	g     *supervise.Group
	mu    sync.Mutex
	start map[string]func(id string) supervise.Process
}

func (l *groupLauncher) set(pool string, start func(id string) supervise.Process) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.start[pool] = start
}

// keep forgets how to start pools not in ps.
func (l *groupLauncher) keep(ps []pools.Pool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for name := range l.start {
		if !slices.ContainsFunc(ps, func(p pools.Pool) bool { return p.Name == name }) {
			delete(l.start, name)
		}
	}
}

func (l *groupLauncher) Start(pool, id string) {
	l.mu.Lock()
	start := l.start[pool]
	l.mu.Unlock()
	if start != nil {
		l.g.Start(start(id))
	}
}

func (l *groupLauncher) Stop(id string) { l.g.Stop(id) }

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

// removeLeftovers removes the containers an earlier hubwagen on the same
// data directory left running (it was killed): nothing else would stop
// them. With a container runtime on PATH (the projects' or a container
// pool's), before any pool starts.
func removeLeftovers(ctx context.Context, cfg *config.Config, log *slog.Logger) {
	runtimes := map[string]bool{launch.Runtime(config.Worker{Runtime: cfg.Projects.Runtime}): true}
	for _, w := range cfg.Workers {
		if w.Launcher == config.LauncherContainer {
			runtimes[launch.Runtime(w)] = true
		}
	}
	for _, rt := range slices.Sorted(maps.Keys(runtimes)) {
		if _, err := exec.LookPath(rt); err != nil {
			continue
		}
		n, err := launch.RemoveLeftovers(ctx, rt, cfg.DataDir, "")
		switch {
		case err != nil:
			log.Warn("can't look for containers left running", "runtime", rt, "err", err)
		case n > 0:
			log.Info(fmt.Sprintf("removed %d container(s) an earlier hubwagen left running", n), "runtime", rt)
		}
	}
}
