// Package localrt is the dev-mode runtime for agent tasks: an in-process
// agent that runs assignments as host processes through internal/taskexec,
// with a fixed number of slots.
//
// It serves a configured set of pools (default: "local"). Tasks for any other
// pool fail at once with reason no_runtime rather than waiting forever for
// agents that will never exist.
package localrt

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/state"
	"github.com/alper/hubwagen/internal/taskexec"
)

// Reporter receives attempt reports; *core.Core implements it.
type Reporter interface {
	Report(core.Report)
}

type Config struct {
	Pools   []string // default ["local"]
	Slots   int      // concurrent attempts, default 4
	AgentID string   // default "local"
	Exec    taskexec.Options
	Logger  *slog.Logger
}

type Runtime struct {
	cfg      Config
	reporter Reporter
	pools    map[string]bool
	log      *slog.Logger

	mu      sync.Mutex
	queue   []core.Assignment
	running map[string]context.CancelFunc
	closed  bool
	wake    chan struct{}
	wg      sync.WaitGroup
	stop    context.CancelFunc
}

// New starts the runtime's workers. Call Close to stop them.
func New(r Reporter, cfg Config) *Runtime {
	if len(cfg.Pools) == 0 {
		cfg.Pools = []string{"local"}
	}
	if cfg.Slots <= 0 {
		cfg.Slots = 4
	}
	if cfg.AgentID == "" {
		cfg.AgentID = "local"
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	ctx, stop := context.WithCancel(context.Background())
	rt := &Runtime{
		cfg: cfg, reporter: r, pools: map[string]bool{}, log: cfg.Logger.With("component", "localrt"),
		running: map[string]context.CancelFunc{}, wake: make(chan struct{}, 1), stop: stop,
	}
	for _, p := range cfg.Pools {
		rt.pools[p] = true
	}
	for i := 0; i < cfg.Slots; i++ {
		rt.wg.Add(1)
		go rt.worker(ctx)
	}
	return rt
}

// Dispatch queues an assignment. It never blocks (the core calls it).
func (rt *Runtime) Dispatch(a core.Assignment) {
	if !rt.pools[a.Runtime.Pool] {
		served := append([]string(nil), rt.cfg.Pools...)
		sort.Strings(served)
		rt.reporter.Report(core.Report{AttemptID: a.AttemptID, Status: state.AttemptFailed, Reason: "no_runtime",
			Message: fmt.Sprintf("no agents serve pool %q here (local runtime serves: %s)", a.Runtime.Pool, strings.Join(served, ", "))})
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return // the core treats it as lost after a restart
	}
	rt.queue = append(rt.queue, a)
	select {
	case rt.wake <- struct{}{}:
	default:
	}
}

// Cancel stops an attempt: a queued one is dropped and reported cancelled, a
// running one has its process group terminated.
func (rt *Runtime) Cancel(attemptID, reason string) {
	rt.mu.Lock()
	for i, a := range rt.queue {
		if a.AttemptID == attemptID {
			rt.queue = append(rt.queue[:i], rt.queue[i+1:]...)
			rt.mu.Unlock()
			rt.reporter.Report(core.Report{AttemptID: attemptID, Status: state.AttemptCancelled, Reason: "cancelled", Message: reason})
			return
		}
	}
	cancel := rt.running[attemptID]
	rt.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Close stops taking work, terminates running attempts and waits for them.
// Their final reports still reach the reporter.
func (rt *Runtime) Close() {
	rt.mu.Lock()
	rt.closed = true
	rt.queue = nil
	rt.mu.Unlock()
	rt.stop()
	rt.wg.Wait()
}

func (rt *Runtime) worker(ctx context.Context) {
	defer rt.wg.Done()
	for {
		a, attemptCtx, ok := rt.next(ctx)
		if !ok {
			return
		}
		rt.run(attemptCtx, a)
	}
}

// next takes the oldest queued assignment, waiting until there is one. The
// attempt is registered as running before the lock is released, so a Cancel
// can never fall between "dequeued" and "running".
func (rt *Runtime) next(ctx context.Context) (core.Assignment, context.Context, bool) {
	for {
		rt.mu.Lock()
		if len(rt.queue) > 0 {
			a := rt.queue[0]
			rt.queue = rt.queue[1:]
			attemptCtx, cancel := context.WithCancel(ctx)
			rt.running[a.AttemptID] = cancel
			more := len(rt.queue) > 0
			rt.mu.Unlock()
			if more { // let another idle worker pick up the rest
				select {
				case rt.wake <- struct{}{}:
				default:
				}
			}
			return a, attemptCtx, true
		}
		rt.mu.Unlock()
		select {
		case <-ctx.Done():
			return core.Assignment{}, nil, false
		case <-rt.wake:
		}
	}
}

func (rt *Runtime) run(ctx context.Context, a core.Assignment) {
	defer func() {
		rt.mu.Lock()
		cancel := rt.running[a.AttemptID]
		delete(rt.running, a.AttemptID)
		rt.mu.Unlock()
		cancel()
	}()

	rt.reporter.Report(core.Report{AttemptID: a.AttemptID, Status: state.AttemptRunning, AgentID: rt.cfg.AgentID})
	rep := taskexec.Run(ctx, a, rt.cfg.Exec)
	rep.AgentID = rt.cfg.AgentID
	rt.log.Debug("attempt finished", "attempt", a.AttemptID, "task", a.TaskID, "status", rep.Status, "reason", rep.Reason)
	rt.reporter.Report(rep)
}
