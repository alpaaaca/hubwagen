// Package core is the scheduler: one goroutine that owns all scheduling
// state, turns inputs (API commands, runtime reports, timers) into events,
// commits them through state.Store, and only then acts on them.
//
// The loop takes everything waiting in the mailbox, processes it against the
// in-memory model (which records events and marks changed objects), commits
// the whole batch in one Store.Append, and then performs side effects
// (dispatching, cancelling, arming timers) and answers commands. A failed
// commit leaves memory ahead of the database, so Run returns the error and
// the leader must restart and reload rather than continue.
package core

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/state"
	"github.com/oklog/ulid/v2"
)

var (
	ErrNotFound = errors.New("not found")
	ErrInvalid  = errors.New("invalid request")
	// ErrUnknownBuild: a worker polled with a build the leader doesn't know;
	// it should register it (again).
	ErrUnknownBuild = errors.New("unknown build")
	// ErrUnknownHaul: no registered build defines the haul.
	ErrUnknownHaul = errors.New("unknown haul")
	// ErrStopped is returned to callers when the core is not running.
	ErrStopped = errors.New("core stopped")
)

type Config struct {
	Store  *state.Store
	Term   int64 // from Store.StartTerm
	Clock  clock.Clock
	Layout layout.Layout
	// Pallets promotes and discards lift outputs. Without it, lifts that
	// declare an output fail (reason no_pallet_store).
	Pallets PalletStore
	// HeartbeatTimeout, if set, is how long a dispatched lift attempt or haul
	// task survives without a heartbeat from its worker (see leases.go).
	HeartbeatTimeout time.Duration
	// Docks limits docks by name; a dock not listed has no limits.
	Docks  map[string]DockConfig
	NewID  func() string // default: ULID
	Logger *slog.Logger
}

type Core struct {
	cfg   Config
	log   *slog.Logger
	inbox mailbox

	// Everything below is owned by the Run goroutine.
	hauls        map[string]*haulState // open hauls
	haulTasks    map[string]*haulTaskState
	liftAttempts map[string]*liftAttemptState // dispatched, unfinished
	docks        map[string]*dockState
	datasets     map[string]string            // tenant\x00name → partition key
	builds       map[string]map[string]string // build ID → lift → code version
	buildHauls   map[string]map[string]bool   // build ID → hauls it defines (none recorded: any)
	haulNames    map[string]bool              // hauls some build defines; StartHaul accepts only these
	promotions   map[string]*promotion        // by pallet ID, while its promote runs
	flights      map[string]*flight           // running memoized lifts, by tenant\x00fingerprint
	schedules    map[string]*scheduleState    // by haul name (ScheduleTenant)
	bg           sync.WaitGroup               // background bucket I/O
	pollers      []*poller                    // waiting, oldest first
	b            *batch
	// queueOrder numbers queued tasks so the oldest across docks goes first.
	queueOrder uint64
	ctx        context.Context // Run's context, for store reads inside the loop
}

func New(cfg Config) *Core {
	if cfg.Clock == nil {
		cfg.Clock = clock.Real{}
	}
	if cfg.NewID == nil {
		cfg.NewID = func() string { return ulid.Make().String() }
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Core{
		cfg:          cfg,
		log:          cfg.Logger.With("component", "core", "term", cfg.Term),
		inbox:        newMailbox(),
		hauls:        map[string]*haulState{},
		haulTasks:    map[string]*haulTaskState{},
		liftAttempts: map[string]*liftAttemptState{},
		docks:        map[string]*dockState{},
		datasets:     map[string]string{},
		builds:       map[string]map[string]string{},
		buildHauls:   map[string]map[string]bool{},
		haulNames:    map[string]bool{},
		promotions:   map[string]*promotion{},
		flights:      map[string]*flight{},
		schedules:    map[string]*scheduleState{},
	}
}

// Run loads state, recovers in-flight attempts, and processes inputs until
// ctx is cancelled (nil error) or a commit fails.
func (c *Core) Run(ctx context.Context) error {
	defer c.bg.Wait() // last: background I/O reports into a closed inbox
	defer c.inbox.close()
	defer c.stopPollers() // before the inbox closes: both answer pollers
	c.ctx = ctx

	if err := c.restore(ctx); err != nil {
		return fmt.Errorf("restore: %w", err)
	}
	for {
		inputs, ok := c.inbox.take(ctx)
		if !ok {
			return nil
		}
		if err := c.process(ctx, inputs); err != nil {
			return err
		}
	}
}

// process handles one batch of inputs and commits it.
func (c *Core) process(ctx context.Context, inputs []input) error {
	c.b = newBatch()
	for _, in := range inputs {
		in.apply(c)
	}
	c.matchDocks()
	return c.commit(ctx)
}

func (c *Core) commit(ctx context.Context) error {
	b := c.b
	c.b = nil
	if !b.empty() {
		if _, err := c.cfg.Store.Append(ctx, c.cfg.Term, b.events, b.write); err != nil {
			err = fmt.Errorf("commit: %w", err)
			for _, r := range b.replies {
				r(err)
			}
			return err
		}
	}
	for _, f := range b.effects {
		f()
	}
	for _, r := range b.replies {
		r(nil)
	}
	return nil
}

func (c *Core) now() time.Time { return c.cfg.Clock.Now() }

// --- inputs and mailbox ---

type input interface{ apply(c *Core) }

// mailbox is an unbounded FIFO queue, so senders (timers, runtimes, API
// handlers) never block on the core.
type mailbox struct {
	mu     sync.Mutex
	items  []input
	signal chan struct{}
	closed bool
}

func newMailbox() mailbox { return mailbox{signal: make(chan struct{}, 1)} }

func (m *mailbox) put(in input) bool {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return false
	}
	m.items = append(m.items, in)
	m.mu.Unlock()
	select {
	case m.signal <- struct{}{}:
	default:
	}
	return true
}

// take waits for inputs and returns all of them.
func (m *mailbox) take(ctx context.Context) ([]input, bool) {
	for {
		m.mu.Lock()
		if len(m.items) > 0 {
			items := m.items
			m.items = nil
			m.mu.Unlock()
			return items, true
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, false
		case <-m.signal:
		}
	}
}

func (m *mailbox) close() {
	m.mu.Lock()
	m.closed = true
	items := m.items
	m.items = nil
	m.mu.Unlock()
	for _, in := range items {
		if cmd, ok := in.(interface{ reject(error) }); ok {
			cmd.reject(ErrStopped)
		}
	}
}

// command is an input with a reply, sent once the batch it was processed in
// has been committed (or has failed to commit).
type command[R any] struct {
	run    func(c *Core) (R, error)
	result R
	err    error
	done   chan struct{}
}

func (cmd *command[R]) apply(c *Core) {
	cmd.result, cmd.err = cmd.run(c)
	c.b.replies = append(c.b.replies, func(commitErr error) {
		if commitErr != nil {
			cmd.err = commitErr
		}
		close(cmd.done)
	})
}

func (cmd *command[R]) reject(err error) {
	cmd.err = err
	close(cmd.done)
}

func submit[R any](ctx context.Context, c *Core, run func(c *Core) (R, error)) (R, error) {
	cmd := &command[R]{run: run, done: make(chan struct{})}
	if !c.inbox.put(cmd) {
		var zero R
		return zero, ErrStopped
	}
	select {
	case <-cmd.done:
		return cmd.result, cmd.err
	case <-ctx.Done():
		var zero R
		return zero, ctx.Err()
	}
}

// Barrier returns once every input submitted before it has been processed
// and committed. Tests use it after Report or advancing a fake clock.
func (c *Core) Barrier(ctx context.Context) error {
	_, err := submit(ctx, c, func(*Core) (struct{}, error) { return struct{}{}, nil })
	return err
}

// funcInput adapts internal events (timers) to the mailbox.
type funcInput func(c *Core)

func (f funcInput) apply(c *Core) { f(c) }

// after arms a timer, effective once the current batch commits, that feeds f
// back into the core.
func (c *Core) after(d time.Duration, f func(c *Core)) *timerRef {
	ref := &timerRef{}
	c.b.effects = append(c.b.effects, func() {
		if ref.stopped {
			return
		}
		ref.t = c.cfg.Clock.AfterFunc(d, func() { c.inbox.put(funcInput(f)) })
	})
	return ref
}

// timerRef lets a timer be stopped before or after it has been armed.
type timerRef struct {
	t       clock.Timer
	stopped bool
}

func (r *timerRef) stop() {
	if r == nil {
		return
	}
	r.stopped = true
	if r.t != nil {
		r.t.Stop()
	}
}
