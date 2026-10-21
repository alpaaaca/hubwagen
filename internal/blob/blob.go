// Package blob is hubwagen's object storage: the bucket that holds logs,
// task outputs, commit markers and results. Objects are addressed by full
// URIs as produced by internal/layout.
//
// Only file:// is implemented so far (dev mode and single-host standalone).
// Object stores (gs://, s3://) come with the profiles that need them; their
// create-if-absent semantics are spike R2 in docs/roadmap.md.
package blob

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	ErrExists   = errors.New("blob: object already exists")
	ErrNotExist = errors.New("blob: object does not exist")
)

type Bucket interface {
	// Base is the bucket's root URI, without a trailing slash.
	Base() string
	Put(ctx context.Context, uri string, data []byte) error
	// PutIfAbsent atomically creates uri with data, or returns ErrExists if
	// it already exists. Concurrent callers: exactly one succeeds. This is
	// what makes output commits exactly-once.
	PutIfAbsent(ctx context.Context, uri string, data []byte) error
	Get(ctx context.Context, uri string) ([]byte, error)
	// List returns the URIs of all objects under prefix (a "directory" URI
	// ending in /), sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	// LocalPath maps a URI to a filesystem path when the bucket is local, so
	// host-process tasks can be handed plain paths.
	LocalPath(uri string) (string, bool)
}

// Open opens the bucket rooted at base.
func Open(base string) (Bucket, error) {
	u, err := url.Parse(base)
	if err != nil {
		return nil, fmt.Errorf("blob: %q: %w", base, err)
	}
	switch u.Scheme {
	case "file":
		if u.Host != "" || !filepath.IsAbs(u.Path) {
			return nil, fmt.Errorf("blob: %q: want file:///absolute/path", base)
		}
		if err := os.MkdirAll(u.Path, 0o755); err != nil {
			return nil, err
		}
		return &fileBucket{base: strings.TrimSuffix(base, "/"), root: filepath.Clean(u.Path)}, nil
	default:
		return nil, fmt.Errorf("blob: scheme %q is not supported yet (only file://)", u.Scheme)
	}
}

// FileURI turns an absolute directory path into a file:// base URI.
func FileURI(path string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path)}).String()
}

type fileBucket struct {
	base string
	root string
}

func (b *fileBucket) Base() string { return b.base }

// path maps a URI under the base to a filesystem path. URI escapes (run keys
// contain %2F) are decoded, consistently in both directions.
func (b *fileBucket) path(uri string) (string, error) {
	if uri != b.base && !strings.HasPrefix(uri, b.base+"/") {
		return "", fmt.Errorf("blob: %q is outside bucket %q", uri, b.base)
	}
	u, err := url.Parse(uri)
	if err != nil {
		return "", fmt.Errorf("blob: %q: %w", uri, err)
	}
	p := filepath.Clean(filepath.FromSlash(u.Path))
	if p != b.root && !strings.HasPrefix(p, b.root+string(filepath.Separator)) {
		return "", fmt.Errorf("blob: %q escapes bucket root", uri)
	}
	return p, nil
}

func (b *fileBucket) LocalPath(uri string) (string, bool) {
	p, err := b.path(uri)
	return p, err == nil
}

func (b *fileBucket) Put(_ context.Context, uri string, data []byte) error {
	p, err := b.path(uri)
	if err != nil {
		return err
	}
	tmp, err := writeTemp(p, data)
	if err != nil {
		return err
	}
	if err := os.Rename(tmp, p); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(filepath.Dir(p))
}

// PutIfAbsent writes a complete temp file and then hard-links it into place:
// link(2) fails with EEXIST if the target exists, so readers never see a
// partial marker and only one writer can win.
func (b *fileBucket) PutIfAbsent(_ context.Context, uri string, data []byte) error {
	p, err := b.path(uri)
	if err != nil {
		return err
	}
	tmp, err := writeTemp(p, data)
	if err != nil {
		return err
	}
	defer os.Remove(tmp)
	if err := os.Link(tmp, p); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return ErrExists
		}
		return err
	}
	return syncDir(filepath.Dir(p))
}

func (b *fileBucket) Get(_ context.Context, uri string) ([]byte, error) {
	p, err := b.path(uri)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, ErrNotExist
	}
	return data, err
}

func (b *fileBucket) List(_ context.Context, prefix string) ([]string, error) {
	if !strings.HasSuffix(prefix, "/") {
		return nil, fmt.Errorf("blob: list prefix %q must end in /", prefix)
	}
	dir, err := b.path(strings.TrimSuffix(prefix, "/"))
	if err != nil {
		return nil, err
	}
	var out []string
	err = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if errors.Is(err, fs.ErrNotExist) && p == dir {
			return filepath.SkipAll
		}
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		rel, err := filepath.Rel(dir, p)
		if err != nil {
			return err
		}
		out = append(out, prefix+filepath.ToSlash(rel))
		return nil
	})
	sort.Strings(out)
	return out, err
}

// writeTemp writes data to a fsynced temp file next to p.
func writeTemp(p string, data []byte) (string, error) {
	dir := filepath.Dir(p)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	f, err := os.CreateTemp(dir, ".tmp-"+filepath.Base(p)+"-*")
	if err != nil {
		return "", err
	}
	_, err = f.Write(data)
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		os.Remove(f.Name())
		return "", err
	}
	return f.Name(), nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
