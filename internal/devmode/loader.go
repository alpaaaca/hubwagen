package devmode

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alper/hubwagen/internal/core"
	"github.com/alper/hubwagen/internal/spec"
)

// FileStatus is the outcome of loading one pipeline file.
type FileStatus struct {
	Path      string
	Pipelines []string // tenant/name of every pipeline the file defines
	Err       string   // compile or publish error; empty when loaded
}

// Loader compiles pipeline files and publishes them to the core. Publishing
// is idempotent per version, so reloading an unchanged file does nothing.
// Removing a file does not unpublish its pipelines.
type Loader struct {
	core  *core.Core
	paths []string

	mu     sync.Mutex
	files  map[string]FileStatus
	mtimes map[string]time.Time
}

func NewLoader(c *core.Core, paths []string) *Loader {
	return &Loader{core: c, paths: paths, files: map[string]FileStatus{}, mtimes: map[string]time.Time{}}
}

// Load compiles and publishes every *.yaml / *.yml under the loader's paths
// that changed since the last load. It returns the files it (re)loaded.
func (l *Loader) Load(ctx context.Context) ([]FileStatus, error) {
	files, err := findYAML(l.paths)
	if err != nil {
		return nil, err
	}
	var changed []FileStatus
	for _, f := range files {
		info, err := os.Stat(f)
		if err != nil {
			continue
		}
		l.mu.Lock()
		prev, seen := l.mtimes[f]
		l.mu.Unlock()
		if seen && prev.Equal(info.ModTime()) {
			continue
		}
		st := l.loadFile(ctx, f)
		l.mu.Lock()
		l.mtimes[f] = info.ModTime()
		l.files[f] = st
		l.mu.Unlock()
		changed = append(changed, st)
	}
	return changed, nil
}

func (l *Loader) loadFile(ctx context.Context, path string) FileStatus {
	st := FileStatus{Path: path}
	data, err := os.ReadFile(path)
	if err != nil {
		st.Err = err.Error()
		return st
	}
	// No digest resolution in dev mode: no registry access needed, and
	// image tags are allowed.
	compiled, err := spec.CompileFile(ctx, path, data, spec.Options{})
	if err != nil {
		st.Err = strings.ReplaceAll(err.Error(), path+": ", "")
		return st
	}
	var errs []string
	for _, c := range compiled {
		res, err := l.core.Publish(ctx, core.PublishRequest{SpecJSON: c.Canonical, PublishedBy: "dev:" + path})
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		st.Pipelines = append(st.Pipelines, res.Tenant+"/"+res.Pipeline)
	}
	st.Err = strings.Join(errs, "; ")
	return st
}

// Files returns the status of every file loaded so far, by path.
func (l *Loader) Files() []FileStatus {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]FileStatus, 0, len(l.files))
	for _, f := range l.files {
		out = append(out, f)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out
}

// Watch reloads changed files every interval until ctx ends, calling report
// with the files it reloaded.
func (l *Loader) Watch(ctx context.Context, interval time.Duration, report func([]FileStatus)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			changed, err := l.Load(ctx)
			if err != nil {
				report([]FileStatus{{Err: err.Error()}})
				continue
			}
			if len(changed) > 0 && report != nil {
				report(changed)
			}
		}
	}
}

func findYAML(paths []string) ([]string, error) {
	var out []string
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		if !info.IsDir() {
			out = append(out, p)
			continue
		}
		err = filepath.WalkDir(p, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && path != p && strings.HasPrefix(d.Name(), ".") {
				return filepath.SkipDir
			}
			if !d.IsDir() && (strings.HasSuffix(path, ".yaml") || strings.HasSuffix(path, ".yml")) {
				out = append(out, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	// No pipeline files is fine: v2 hauls come from workers, not YAML.
	sort.Strings(out)
	return out, nil
}
