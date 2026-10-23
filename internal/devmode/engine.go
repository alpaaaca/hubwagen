// Package devmode wires the whole engine into one process for the dev
// profile: the state store and a file:// bucket under one data directory,
// the core, the local runtime, and a loader that compiles and publishes
// pipeline files (and, optionally, watches them for changes).
//
// One process owns a data directory at a time; a lock file enforces that, so
// a second `hubwagen dev` or `hubwagen run` on the same directory fails with
// a clear message instead of fencing the first one out.
package devmode

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/runtime/localrt"
	"github.com/alper/hubwagen/internal/spec"
	"github.com/alper/hubwagen/internal/state"
	"github.com/alper/hubwagen/internal/taskexec"
)

type Config struct {
	DataDir string   // holds state.db, bucket/ and the lock; created if missing
	Slots   int      // concurrent tasks (default 4)
	Pools   []string // agent pools served by host processes (default local)
	// Echo receives every task log line, prefixed with the task ID (nil: none).
	Echo io.Writer
	// DisableSchedules: no cron runs (one-shot `hubwagen run`).
	DisableSchedules bool
	Logger           *slog.Logger
}

type Engine struct {
	Store   *state.Store
	Bucket  blob.Bucket
	Core    *core.Core
	Runtime *localrt.Runtime
	DataDir string

	lock   *os.File
	cancel context.CancelFunc
	done   chan error
	closed bool
}

// ErrLocked means another process is using the data directory.
var ErrLocked = errors.New("data directory is in use by another hubwagen process")

// Start opens the data directory and runs the engine until Close.
func Start(ctx context.Context, cfg Config) (*Engine, error) {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	dir, err := filepath.Abs(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}

	e := &Engine{DataDir: dir, lock: lock}
	fail := func(err error) (*Engine, error) {
		e.release()
		return nil, err
	}

	if e.Store, err = state.Open(filepath.Join(dir, "state.db")); err != nil {
		return fail(fmt.Errorf("opening state: %w", err))
	}
	term, err := e.Store.StartTerm(ctx, 0)
	if err != nil {
		return fail(err)
	}
	if e.Bucket, err = blob.Open(blob.FileURI(filepath.Join(dir, "bucket"))); err != nil {
		return fail(err)
	}

	e.Core = core.New(core.Config{
		Store: e.Store, Term: term, Clock: clock.Real{}, Layout: layout.New(e.Bucket.Base()),
		Recovery:          taskexec.Recovery{Bucket: e.Bucket},
		AutoCreateTenants: true, DisableSchedules: cfg.DisableSchedules,
		Logger: cfg.Logger,
	})
	e.Runtime = localrt.New(e.Core, localrt.Config{
		Pools: cfg.Pools, Slots: cfg.Slots, Logger: cfg.Logger,
		Exec: taskexec.Options{Bucket: e.Bucket, Secrets: taskexec.EnvSecrets{Environ: os.Environ()}, Echo: cfg.Echo},
	})
	e.Core.SetRuntime(spec.RuntimeAgent, e.Runtime)

	runCtx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.done = make(chan error, 1)
	go func() { e.done <- e.Core.Run(runCtx) }()
	if err := e.Core.Barrier(ctx); err != nil {
		e.Close()
		return nil, fmt.Errorf("starting core: %w", err)
	}
	return e, nil
}

// Done is closed-with-error when the core stops on its own (a failed commit).
func (e *Engine) Done() <-chan error { return e.done }

// Close stops running tasks (they are terminated and reported), then the
// core, and releases the data directory.
func (e *Engine) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	e.Runtime.Close()
	// Let the core record the final reports of the tasks just stopped.
	_ = e.Core.Barrier(context.Background())
	e.cancel()
	err := <-e.done
	e.release()
	return err
}

func (e *Engine) release() {
	if e.Store != nil {
		e.Store.Close()
	}
	if e.lock != nil {
		syscall.Flock(int(e.lock.Fd()), syscall.LOCK_UN)
		e.lock.Close()
	}
}

func lockDir(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s (is `hubwagen dev` running there?)", ErrLocked, dir)
		}
		return nil, err
	}
	return f, nil
}
