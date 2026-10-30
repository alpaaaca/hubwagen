// Package server wires the server into one process, for `hubwagen server`
// and `hubwagen dev`: the state store and the run logs under one data
// directory, the bucket (file:// under it, or s3://), the core, and the API
// (the Connect services, the data endpoint, the web UI and /healthz).
//
// One process owns a data directory at a time; a lock file enforces that, so
// a second server on the same directory fails with a clear message instead
// of fencing the first one out.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/alper/hubwagen/internal/api"
	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/clock"
	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/alper/hubwagen/internal/pallet"
	"github.com/alper/hubwagen/internal/runlog"
	"github.com/alper/hubwagen/internal/state"
	"github.com/alper/hubwagen/internal/webui"
)

type Config struct {
	DataDir string // holds state.db, bucket/, logs/ and the lock; created if missing
	// S3, if set, is the bucket; else it is file://<DataDir>/bucket.
	S3 *blob.S3Config
	// ServeData sends workers' data through the server even when the bucket
	// could give them presigned URLs.
	ServeData bool
	// Listen, if set, is the address the API (worker and haul services) is
	// served on, e.g. 127.0.0.1:7070. Port 0 picks a free port (see
	// Engine.APIAddr).
	Listen string
	Logger *slog.Logger
}

// HeartbeatTimeout is how long a worker's task survives without a heartbeat.
const HeartbeatTimeout = time.Minute

type Engine struct {
	Store  *state.Store
	Bucket blob.Bucket
	// BucketID is a file:// bucket's ID, in its .hubwagen-bucket file:
	// workers that find it there share the bucket's filesystem.
	BucketID string
	Logs     *runlog.Store // what lifts printed and logged
	Core     *core.Core
	DataDir  string
	// APIAddr is the API's address, if it is served.
	APIAddr string

	api       *http.Server
	serveData bool
	lock      *os.File
	cancel    context.CancelFunc
	done      chan error
	stopped   atomic.Bool // the core has stopped
	closed    bool
}

// ErrLocked means another process is using the data directory.
var ErrLocked = errors.New("data directory is in use by another hubwagen process")

// Start opens the data directory and runs the server until Close.
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
	fileBucket := blob.FileURI(filepath.Join(dir, "bucket"))
	if cfg.S3 != nil {
		e.Bucket, err = blob.OpenS3(ctx, *cfg.S3)
	} else {
		e.Bucket, err = blob.Open(fileBucket)
	}
	if err != nil {
		return fail(fmt.Errorf("opening the bucket: %w", err))
	}
	claimed, err := e.Store.ClaimBucket(ctx, e.Bucket.Base(), fileBucket)
	if err != nil {
		return fail(err)
	}
	if claimed != e.Bucket.Base() {
		return fail(fmt.Errorf("data directory %s keeps its data versions in %s, not %s: a data directory works with one bucket", dir, claimed, e.Bucket.Base()))
	}
	if _, local := e.Bucket.LocalPath(e.Bucket.Base()); local {
		if e.BucketID, err = bucketID(ctx, e.Bucket); err != nil {
			return fail(err)
		}
	}
	e.serveData = cfg.ServeData
	if e.Logs, err = runlog.Open(filepath.Join(dir, "logs")); err != nil {
		return fail(err)
	}

	lay := layout.New(e.Bucket.Base())
	e.Core = core.New(core.Config{
		Store: e.Store, Term: term, Clock: clock.Real{}, Layout: lay,
		Pallets: pallet.NewStore(e.Bucket, lay), HeartbeatTimeout: HeartbeatTimeout,
		Logger: cfg.Logger,
	})

	runCtx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	e.done = make(chan error, 1)
	go func() {
		err := e.Core.Run(runCtx)
		e.stopped.Store(true)
		e.done <- err
	}()
	if err := e.Core.Barrier(ctx); err != nil {
		e.Close()
		return nil, fmt.Errorf("starting core: %w", err)
	}
	if cfg.Listen != "" {
		if err := e.serveAPI(cfg.Listen, cfg.Logger); err != nil {
			e.Close()
			return nil, err
		}
	}
	return e, nil
}

// bucketMarker names the file in a file:// bucket that holds its ID.
const bucketMarker = ".hubwagen-bucket"

// bucketID reads a file bucket's ID, making one the first time.
func bucketID(ctx context.Context, b blob.Bucket) (string, error) {
	uri := b.Base() + "/" + bucketMarker
	err := b.PutIfAbsent(ctx, uri, []byte(ulid.Make().String()))
	if err != nil && !errors.Is(err, blob.ErrExists) {
		return "", err
	}
	id, err := b.Get(ctx, uri)
	return strings.TrimSpace(string(id)), err
}

// serveAPI starts the API. Long polls end when the engine closes.
func (e *Engine) serveAPI(addr string, log *slog.Logger) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("API: %w", err)
	}
	mux := http.NewServeMux()
	manifests := pallet.NewStore(e.Bucket, layout.New(e.Bucket.Base()))
	data := api.NewData(e.Bucket, e.BucketID, manifests, e.serveData, log)
	path, h := api.NewWorkerHandler(e.Core, 0, data)
	mux.Handle(path, h)
	mux.Handle(api.DataPath, data.Handler())
	path, h = api.NewHaulHandler(e.Core, e.Store)
	mux.Handle(path, h)
	path, h = api.NewLogHandler(e.Store, e.Logs)
	mux.Handle(path, h)
	mux.HandleFunc("GET /healthz", e.health)
	mux.Handle("/", webui.Handler()) // the Connect services' paths above are more specific
	path, h = api.NewCatalogHandler(e.Core, e.Store, manifests)
	mux.Handle(path, h)
	e.api = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	e.APIAddr = ln.Addr().String()
	go func() {
		if err := e.api.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("API stopped", "err", err)
		}
	}()
	return nil
}

// health answers /healthz: 200 while the core runs, 503 once it has
// stopped (a failed commit, or shutting down), for a service manager or a
// load balancer to check.
func (e *Engine) health(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	if e.stopped.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintln(w, "stopped")
		return
	}
	fmt.Fprintln(w, "ok")
}

// Done is closed-with-error when the core stops on its own (a failed commit).
func (e *Engine) Done() <-chan error { return e.done }

// Close stops the API and the core and releases the data directory. Work
// that workers are running carries on; the next start picks it up.
func (e *Engine) Close() error {
	if e.closed {
		return nil
	}
	e.closed = true
	if e.api != nil {
		// Don't wait for long polls: closing the core answers them.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		_ = e.api.Shutdown(ctx)
		cancel()
		e.api.Close()
	}
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
			return nil, fmt.Errorf("%w: %s (is `hubwagen dev` or `hubwagen server` running there?)", ErrLocked, dir)
		}
		return nil, err
	}
	return f, nil
}
