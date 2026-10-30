// Package supervise runs a set of long-lived processes (workers, in dev) and
// keeps them running: output is prefixed with the process's ID, a process
// that exits is restarted with backoff, and on shutdown each gets SIGTERM
// and some time to finish before it is killed.
//
// The processes run in their own process groups, so a Ctrl-C in the
// terminal reaches only hubwagen, which then stops them in order.
package supervise

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"slices"
	"sync"
	"syscall"
	"time"
)

type Process struct {
	ID      string   // shown in front of every output line, e.g. py-1
	Command []string // program and arguments
	Dir     string
	Env     []string // the whole environment
	// Cleanup, if set, runs before each start and after each exit, e.g.
	// removing a container the process's command left behind.
	Cleanup []string
}

type Options struct {
	Out io.Writer // output lines and supervisor notes, as "<id> | <text>"
	// Line, if set, gets each line instead of Out: a process's output, or
	// with note set, the supervisor's (started, exited, restarting).
	Line func(id, text string, note bool)
	// StopTimeout is how long a process gets after SIGTERM before it is
	// killed. Default 30s.
	StopTimeout time.Duration
	// MinBackoff and MaxBackoff bound the wait before a restart; it doubles
	// on each quick exit and resets once a process ran for StableAfter.
	// Defaults 1s, 30s, 10s.
	MinBackoff, MaxBackoff, StableAfter time.Duration
	// Kill, if set, kills every process at once when it is closed (a second
	// Ctrl-C), instead of waiting out StopTimeout.
	Kill <-chan struct{}
	// OnStart, if set, is called with each process's PID when it starts.
	OnStart func(id string, pid int)
}

// Run runs procs until ctx ends, then stops them and returns.
func Run(ctx context.Context, procs []Process, o Options) {
	g := NewGroup(ctx, o)
	for _, p := range procs {
		g.Start(p)
	}
	g.Wait()
}

// Group runs a changing set of processes, each kept running like Run's:
// Start adds one, Stop takes one away for good. When the group's context
// ends, every process is stopped.
type Group struct {
	ctx   context.Context
	o     Options
	out   *lockedWriter
	wg    sync.WaitGroup
	mu    sync.Mutex
	procs map[string]*running // started, not being stopped
	// closed: Wait is waiting; Start adds no more (a WaitGroup can't grow
	// while waited on).
	closed bool
}

type running struct{ cancel context.CancelFunc }

// NewGroup returns an empty group whose processes stop when ctx ends.
func NewGroup(ctx context.Context, o Options) *Group {
	o = o.withDefaults()
	return &Group{ctx: ctx, o: o, out: &lockedWriter{w: o.Out, fn: o.Line}, procs: map[string]*running{}}
}

// Start starts p and keeps it running until Stop(p.ID) or the group's
// context ends. An ID already running is left as is.
func (g *Group) Start(p Process) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.procs[p.ID]; ok || g.closed || g.ctx.Err() != nil {
		return
	}
	ctx, cancel := context.WithCancel(g.ctx)
	r := &running{cancel: cancel}
	g.procs[p.ID] = r
	g.wg.Go(func() {
		keep(ctx, p, g.o, g.out)
		cancel()
		g.mu.Lock()
		if g.procs[p.ID] == r { // not a newer process with the same ID
			delete(g.procs, p.ID)
		}
		g.mu.Unlock()
	})
}

// Stop stops the process id as on shutdown (SIGTERM, then a kill after
// StopTimeout) and doesn't restart it. It returns at once.
func (g *Group) Stop(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if r, ok := g.procs[id]; ok {
		r.cancel()
		delete(g.procs, id)
	}
}

// Running returns the IDs of the processes started and not stopped.
func (g *Group) Running() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	ids := make([]string, 0, len(g.procs))
	for id := range g.procs {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// Wait returns once the group's context has ended and every process has
// stopped.
func (g *Group) Wait() {
	<-g.ctx.Done()
	g.mu.Lock()
	g.closed = true
	g.mu.Unlock()
	g.wg.Wait()
}

func (o Options) withDefaults() Options {
	if o.StopTimeout == 0 {
		o.StopTimeout = 30 * time.Second
	}
	if o.MinBackoff == 0 {
		o.MinBackoff = time.Second
	}
	if o.MaxBackoff == 0 {
		o.MaxBackoff = 30 * time.Second
	}
	if o.StableAfter == 0 {
		o.StableAfter = 10 * time.Second
	}
	return o
}

func keep(ctx context.Context, p Process, o Options, out *lockedWriter) {
	backoff := o.MinBackoff
	for ctx.Err() == nil {
		began := time.Now()
		err := runOnce(ctx, p, o, out)
		if ctx.Err() != nil {
			out.note(p.ID, "stopped")
			return
		}
		if time.Since(began) >= o.StableAfter {
			backoff = o.MinBackoff
		}
		out.note(p.ID, fmt.Sprintf("%s; restarting in %s", exitText(err), backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, o.MaxBackoff)
	}
}

func runOnce(ctx context.Context, p Process, o Options, out *lockedWriter) error {
	cleanup(p, out)
	defer cleanup(p, out)
	cmd := exec.CommandContext(ctx, p.Command[0], p.Command[1:]...)
	cmd.Dir = p.Dir
	cmd.Env = p.Env
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGTERM) }
	cmd.WaitDelay = o.StopTimeout
	lines := &prefixWriter{id: p.ID, out: out}
	cmd.Stdout, cmd.Stderr = lines, lines
	if err := cmd.Start(); err != nil {
		return err
	}
	out.note(p.ID, fmt.Sprintf("started (pid %d)", cmd.Process.Pid))
	if o.OnStart != nil {
		o.OnStart(p.ID, cmd.Process.Pid)
	}
	exited := make(chan struct{})
	defer close(exited)
	if o.Kill != nil {
		go func() {
			select {
			case <-o.Kill:
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			case <-exited:
			}
		}()
	}
	err := cmd.Wait()
	lines.flush()
	// Whatever is left of the group (children the process started) goes too.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	return err
}

// cleanup runs p's Cleanup, if any, whether or not the group is stopping.
func cleanup(p Process, out *lockedWriter) {
	if len(p.Cleanup) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, p.Cleanup[0], p.Cleanup[1:]...)
	cmd.Env = p.Env
	if b, err := cmd.CombinedOutput(); err != nil {
		out.note(p.ID, fmt.Sprintf("cleanup: %v: %s", err, bytes.TrimSpace(b)))
	}
}

func exitText(err error) string {
	var exit *exec.ExitError
	switch {
	case err == nil:
		return "exited"
	case errors.As(err, &exit):
		if ws, ok := exit.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
			return "ended by signal: " + ws.Signal().String()
		}
		return fmt.Sprintf("exited with status %d", exit.ExitCode())
	}
	return err.Error()
}

// lockedWriter writes whole lines from many processes without interleaving.
type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
	fn func(id, text string, note bool)
}

func (l *lockedWriter) line(id string, text []byte) { l.write(id, string(text), false) }

func (l *lockedWriter) note(id, text string) { l.write(id, text, true) }

func (l *lockedWriter) write(id, text string, note bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fn != nil {
		l.fn(id, text, note)
		return
	}
	if note {
		text = "▸ " + text
	}
	fmt.Fprintf(l.w, "%s | %s\n", id, text)
}

// prefixWriter splits a process's output into lines.
type prefixWriter struct {
	id  string
	out *lockedWriter
	buf []byte
}

func (w *prefixWriter) Write(b []byte) (int, error) {
	w.buf = append(w.buf, b...)
	for {
		i := bytes.IndexByte(w.buf, '\n')
		if i < 0 {
			break
		}
		w.out.line(w.id, bytes.TrimRight(w.buf[:i], "\r"))
		w.buf = w.buf[i+1:]
	}
	return len(b), nil
}

func (w *prefixWriter) flush() {
	if len(w.buf) > 0 {
		w.out.line(w.id, w.buf)
		w.buf = nil
	}
}
