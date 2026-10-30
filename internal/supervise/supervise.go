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
	"sync"
	"syscall"
	"time"
)

type Process struct {
	ID      string   // shown in front of every output line, e.g. py-1
	Command []string // program and arguments
	Dir     string
	Env     []string // the whole environment
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
	out := &lockedWriter{w: o.Out, fn: o.Line}
	var wg sync.WaitGroup
	for _, p := range procs {
		wg.Go(func() { keep(ctx, p, o, out) })
	}
	wg.Wait()
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
