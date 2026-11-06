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

	"github.com/alper/moksha/internal/clock"
	"github.com/alper/moksha/internal/config"
	"github.com/alper/moksha/internal/core"
	"github.com/alper/moksha/internal/launch"
	"github.com/alper/moksha/internal/pools"
	"github.com/alper/moksha/internal/state"
	"github.com/alper/moksha/internal/supervise"
)

var configPath string

func addConfigFlag(c *cobra.Command) {
	c.Flags().StringVar(&configPath, "config", "", "config file (default: $MOKSHA_CONFIG, else ./"+config.FileName+" if present)")
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
	applyFlags(cmd, cfg)
	return cfg, nil
}

// applyFlags puts the command's flags that were passed on top of cfg.
func applyFlags(cmd *cobra.Command, cfg *config.Config) {
	f := cmd.Flags()
	if f.Changed("data-dir") {
		cfg.DataDir = dataDir
	}
	if f.Changed("listen") {
		cfg.Server.Listen = listenAddr
	}
}

func configName(cfg *config.Config) string {
	if cfg.Path == "" {
		return "no config file"
	}
	return cfg.Path
}

// workerProcess is one worker of a deployed pool: a process of its
// command, or for launcher: container, a container of its image.
func workerProcess(w config.Worker, id, server, dataDir string, extraEnv ...string) supervise.Process {
	var env []string
	for k, v := range w.Env {
		env = append(env, k+"="+v)
	}
	env = append(env, config.EnvWorkerID+"="+id,
		config.EnvQueues+"="+strings.Join(w.QueueList(), ","), config.EnvSteps+"="+strconv.Itoa(w.StepCount()))
	env = append(env, extraEnv...)
	if w.Launcher == config.LauncherContainer {
		// No supervisor PID: in the container's PID namespace it means
		// nothing. A container outliving moksha is removed when the
		// worker starts again (its cleanup).
		return launch.Container(w, id, server, dataDir, env)
	}
	env = append(env, config.EnvServer+"="+server, config.EnvSupervisorPID+"="+strconv.Itoa(os.Getpid()))
	return supervise.Process{ID: id, Command: w.Command, Dir: w.Dir, Env: append(os.Environ(), env...)}
}

// startWorkers scales the config's pools (launcher: external or
// kubernetes) and runs the projects deployed to the server
// (followProjects), their workers in g, until ctx ends. Kubernetes pools
// need the cluster: an error if it can't be reached, before anything
// starts.
func startWorkers(ctx context.Context, g *supervise.Group, cfg *config.Config, c *core.Core, server string, log *slog.Logger, extraEnv ...string) error {
	var kube *launch.Kube
	if cfg.Projects.Launcher == config.LauncherKubernetes { // the cluster first: an error starts nothing
		k, err := launch.NewKube(cfg.Kubernetes, launch.ServiceAccountDir)
		if err != nil {
			return fmt.Errorf("projects.launcher: %w", err)
		}
		kube = k
	}
	for _, p := range cfg.Pools {
		if p.Launcher == config.LauncherKubernetes && kube == nil {
			k, err := launch.NewKube(cfg.Kubernetes, launch.ServiceAccountDir)
			if err != nil {
				return fmt.Errorf("pools %s: %w", p.Name, err)
			}
			kube = k
		}
	}
	removeLeftovers(ctx, cfg, log)
	launcher := &groupLauncher{g: g, log: log, start: map[string]func(id string) (supervise.Process, error){}}
	var static []pools.Pool
	for _, cp := range cfg.Pools {
		lo, hi := cp.Bounds()
		p := pools.Pool{Name: cp.Name, Queues: cp.QueueList(), Steps: cp.StepCount(), Min: lo, Max: hi,
			IdleAfter: cp.Idle(), External: true}
		msg := "started elsewhere; its desired count is at GET /v1/pools/" + cp.Name
		if cp.Launcher == config.LauncherKubernetes {
			ns, deployment := cp.Namespace, cp.Deployment
			p.Apply = func(ctx context.Context, n int) error { return kube.Scale(ctx, ns, deployment, n) }
			msg = "the replicas of Deployment " + or(ns, kube.Namespace) + "/" + deployment
		}
		static = append(static, p)
		log.Info("worker pool "+cp.Name+": "+msg, "pool", cp.Name, "min", lo, "max", hi,
			"queues", strings.Join(cp.QueueList(), ","), "idle_after", cp.Idle())
	}
	sc := pools.New(static, launcher, c, clock.Real{}, log)
	go sc.Run(ctx)
	var onKube *kubeProjects
	if cfg.Projects.Launcher == config.LauncherKubernetes {
		onKube = &kubeProjects{kube: kube, cfg: cfg, log: log, created: map[string]bool{}}
	}
	go followProjects(ctx, c, sc, static, launcher, onKube, cfg, server, log, extraEnv)
	return nil
}

// followProjects runs the projects deployed to the server (docs/projects.md).
// Every pass, each live project's current version becomes pools of the
// scaler, named <project>-<pool>-v<version>, next to the config's own: a new
// version is a new pool, and the old version's goes (its workers stop and
// finish what they run). With onKube, the pools are Deployments.
func followProjects(ctx context.Context, c *core.Core, sc *pools.Scaler, static []pools.Pool, launcher *groupLauncher,
	onKube *kubeProjects, cfg *config.Config, server string, log *slog.Logger, extraEnv []string) {
	t := time.NewTicker(pools.Every)
	defer t.Stop()
	running := map[string]int{} // project → version its pools run
	first, prune := true, false // prune: Deployments to delete (onKube)
	for {
		ps, err := c.Projects(ctx)
		if err != nil && ctx.Err() == nil {
			log.Warn("reading projects", "err", err)
		}
		want := map[string]int{}
		all := slices.Clone(static)
		deployments := map[string]bool{} // onKube: the Deployments the pools are
		for _, p := range ps {
			if p.DeletedAt != nil {
				continue
			}
			want[p.Project] = p.Version
			for _, dp := range p.Pools {
				name := fmt.Sprintf("%s-%s-v%d", p.Project, dp.Name, p.Version)
				if onKube != nil {
					all = append(all, onKube.pool(p, dp, name))
					deployments[launch.DeploymentName(p.Project, dp.Name, p.Version)] = true
					continue
				}
				w, secrets := projectWorker(p, dp, name, cfg), p.Secrets
				launcher.set(name, func(id string) (supervise.Process, error) {
					// Secrets are read now, as the worker starts: their values
					// live only in its environment.
					if len(secrets) > 0 {
						w.Env = maps.Clone(w.Env)
						if w.Env == nil {
							w.Env = map[string]string{}
						}
						store := launch.Secrets{Dir: cfg.Projects.SecretsDir}
						for k, name := range secrets {
							v, err := store.Get(name)
							if err != nil {
								return supervise.Process{}, err
							}
							w.Env[k] = v
						}
					}
					return workerProcess(w, id, server, cfg.DataDir, extraEnv...), nil
				})
				all = append(all, pools.Pool{Name: name, Queues: dp.Queues, Steps: dp.Steps, Min: dp.Min, Max: dp.Max, IdleAfter: dp.IdleAfter,
					Registered: true}) // deploying registered its build
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
			running, first, prune = want, false, onKube != nil
		}
		if prune && err == nil {
			prune = !onKube.prune(ctx, deployments) // again on the next pass if it failed
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// kubeProjects runs deployed projects' pools as Deployments
// (projects.launcher: kubernetes; internal/launch/deployments.go).
type kubeProjects struct {
	kube *launch.Kube
	cfg  *config.Config
	log  *slog.Logger

	mu      sync.Mutex
	created map[string]bool // Deployments this server made or found
}

// pool is a deployed pool for the scaler: external, its desired count
// applied to its Deployment, which the first count creates.
func (k *kubeProjects) pool(p state.Project, dp state.DeployedPool, name string) pools.Pool {
	d := launch.WorkerDeployment{Name: launch.DeploymentName(p.Project, dp.Name, p.Version), Instance: k.cfg.Kubernetes.InstanceName(),
		Project: p.Project, Pool: name, Image: p.Image, Server: k.cfg.Projects.ServerURL, Queues: dp.Queues, Steps: dp.Steps,
		Env: p.Env, Secrets: p.Secrets, PullSecrets: k.cfg.Kubernetes.ImagePullSecrets}
	return pools.Pool{Name: name, Queues: dp.Queues, Steps: dp.Steps, Min: dp.Min, Max: dp.Max, IdleAfter: dp.IdleAfter,
		External: true, Registered: true, // deploying registered its build
		Apply: func(ctx context.Context, n int) error {
			k.mu.Lock()
			made := k.created[d.Name]
			k.mu.Unlock()
			if !made {
				d.Replicas = n
				if err := k.kube.CreateDeployment(ctx, "", d); err != nil {
					return err
				}
				k.mu.Lock()
				k.created[d.Name] = true
				k.mu.Unlock()
				k.log.Info("worker pool "+name+": Deployment "+k.kube.Namespace+"/"+d.Name, "pool", name, "replicas", n)
			}
			return k.kube.Scale(ctx, "", d.Name, n) // it may have existed, at another count
		}}
}

// prune deletes the Deployments this server made that aren't in keep
// (old versions, deleted projects, leftovers from before a restart):
// their pods get SIGTERM and finish what they run. It reports whether
// it could do all of it.
func (k *kubeProjects) prune(ctx context.Context, keep map[string]bool) bool {
	names, err := k.kube.WorkerDeployments(ctx, "", k.cfg.Kubernetes.InstanceName())
	if err != nil {
		k.log.Warn("can't list the projects' Deployments", "err", err)
		return false
	}
	ok := true
	for _, name := range names {
		if keep[name] {
			continue
		}
		if err := k.kube.DeleteDeployment(ctx, "", name); err != nil {
			k.log.Warn("can't delete Deployment "+name, "err", err)
			ok = false
			continue
		}
		k.mu.Lock()
		delete(k.created, name)
		k.mu.Unlock()
		k.log.Info("deleted Deployment "+k.kube.Namespace+"/"+name+": its workers finish what they run and stop", "deployment", name)
	}
	return ok
}

// projectWorker is the worker group a deployed pool runs as: containers of
// the deployment's image, or processes in its directory.
func projectWorker(p state.Project, dp state.DeployedPool, name string, cfg *config.Config) config.Worker {
	steps := dp.Steps
	w := config.Worker{Name: name, Queues: dp.Queues, Steps: &steps, Env: p.Env}
	if p.Image != "" {
		w.Launcher, w.Image, w.Runtime, w.ServerURL = config.LauncherContainer, p.Image, cfg.Projects.Runtime, cfg.Projects.ServerURL
		w.AuthFile = cfg.Projects.AuthFile
		return w
	}
	w.Command, w.Dir = []string{or(p.Python, "python3"), "-m", "moksha.worker"}, p.Dir
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
	log   *slog.Logger
	mu    sync.Mutex
	start map[string]func(id string) (supervise.Process, error)
}

func (l *groupLauncher) set(pool string, start func(id string) (supervise.Process, error)) {
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

// Start starts worker id of pool; if it can't (a secret is missing), it
// says so and starts nothing: the scaler tries again on a later pass.
func (l *groupLauncher) Start(pool, id string) error {
	l.mu.Lock()
	start := l.start[pool]
	l.mu.Unlock()
	if start == nil {
		return fmt.Errorf("no way to start pool %s", pool)
	}
	p, err := start(id)
	if err != nil {
		l.log.Error("worker pool "+pool+": can't start "+id, "pool", pool, "err", err)
		return err
	}
	l.g.Start(p)
	return nil
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

// removeLeftovers removes the containers an earlier moksha on the same
// data directory left running for its projects (it was killed): nothing
// else would stop them. With the projects' container runtime on PATH,
// before any pool starts.
func removeLeftovers(ctx context.Context, cfg *config.Config, log *slog.Logger) {
	rt := launch.Runtime(config.Worker{Runtime: cfg.Projects.Runtime})
	if _, err := exec.LookPath(rt); err != nil {
		return
	}
	n, err := launch.RemoveLeftovers(ctx, rt, cfg.DataDir, "")
	switch {
	case err != nil:
		log.Warn("can't look for containers left running", "runtime", rt, "err", err)
	case n > 0:
		log.Info(fmt.Sprintf("removed %d container(s) an earlier moksha left running", n), "runtime", rt)
	}
}
