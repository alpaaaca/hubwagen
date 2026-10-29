package pallet

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/alper/hubwagen/internal/blob"
	"github.com/alper/hubwagen/internal/layout"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var ctx = context.Background()

func open(t *testing.T) (*Store, blob.Bucket, layout.Layout) {
	t.Helper()
	b, err := blob.Open(blob.FileURI(filepath.Join(t.TempDir(), "bucket")))
	require.NoError(t, err)
	l := layout.New(b.Base())
	return NewStore(b, l), b, l
}

// write puts a file under prefix and describes it.
func write(t *testing.T, b blob.Bucket, prefix, path, content string) File {
	t.Helper()
	require.NoError(t, b.Put(ctx, prefix+path, []byte(content)))
	sum := sha256.Sum256([]byte(content))
	return File{Path: path, Size: int64(len(content)), SHA256: hex.EncodeToString(sum[:])}
}

func TestContentHashIgnoresOrderAndLocation(t *testing.T) {
	a := File{Path: "a.csv", Size: 1, SHA256: strings.Repeat("a", 64)}
	b := File{Path: "b.csv", Size: 2, SHA256: strings.Repeat("b", 64)}
	assert.Equal(t, ContentHash([]File{a, b}, nil), ContentHash([]File{b, a}, nil))
	assert.True(t, strings.HasPrefix(ContentHash([]File{a}, nil), "sha256:"))

	changed := b
	changed.SHA256 = strings.Repeat("c", 64)
	assert.NotEqual(t, ContentHash([]File{a, b}, nil), ContentHash([]File{a, changed}, nil))
	renamed := b
	renamed.Path = "c.csv"
	assert.NotEqual(t, ContentHash([]File{a, b}, nil), ContentHash([]File{a, renamed}, nil))

	m1, err := New("p1", "t", "raw", "", "file:///x/a1/", []File{b, a}, nil)
	require.NoError(t, err)
	m2, err := New("p2", "t", "raw", "", "file:///y/a2/", []File{a, b}, nil)
	require.NoError(t, err)
	assert.Equal(t, m1.ContentHash, m2.ContentHash, "same files, different pallets and places")
	assert.Equal(t, []File{a, b}, m1.Files, "sorted by path")
}

func TestContentHashCoversTheResult(t *testing.T) {
	a := File{Path: "a.csv", Size: 1, SHA256: strings.Repeat("a", 64)}
	files := ContentHash([]File{a}, nil)
	assert.Equal(t, files, ContentHash([]File{a}, json.RawMessage("null")), "null is no result")
	assert.Equal(t, files, ContentHash([]File{a}, json.RawMessage(" ")), "nor is nothing")
	one := ContentHash([]File{a}, json.RawMessage(`{"rows":1}`))
	assert.NotEqual(t, files, one)
	assert.NotEqual(t, one, ContentHash([]File{a}, json.RawMessage(`{"rows":2}`)))
	assert.NotEqual(t, ContentHash(nil, json.RawMessage(`1`)), ContentHash(nil, json.RawMessage(`2`)),
		"pallets without files differ by their results")

	m, err := New("p", "t", "d", "", "file:///x/", []File{a}, json.RawMessage(`{"rows":1}`))
	require.NoError(t, err)
	assert.Equal(t, one, m.ContentHash)
	assert.JSONEq(t, `{"rows":1}`, string(m.Result))
	m, err = New("p", "t", "d", "", "file:///x/", []File{a}, json.RawMessage("null"))
	require.NoError(t, err)
	assert.Nil(t, m.Result, "a null result is left out of the manifest")
	_, err = New("p", "t", "d", "", "file:///x/", nil, json.RawMessage("{"))
	assert.Error(t, err)
}

func TestNewRejectsBadFiles(t *testing.T) {
	ok := strings.Repeat("0", 64)
	for name, files := range map[string][]File{
		"absolute":   {{Path: "/etc/passwd", SHA256: ok}},
		"escapes":    {{Path: "a/../../b", SHA256: ok}},
		"empty path": {{Path: "", SHA256: ok}},
		"twice":      {{Path: "a", SHA256: ok}, {Path: "a", SHA256: ok}},
		"negative":   {{Path: "a", Size: -1, SHA256: ok}},
		"bad sum":    {{Path: "a", SHA256: "abc"}},
	} {
		_, err := New("p", "t", "d", "", "file:///x/", files, nil)
		assert.Error(t, err, name)
	}
	_, err := New("p", "t", "d", "", "file:///x", nil, nil)
	assert.Error(t, err, "data prefix must be a directory")
}

func TestPromoteWritesManifestOnce(t *testing.T) {
	s, b, l := open(t)
	staging := l.LiftStaging("t", "raw", "", "a1")
	files := []File{write(t, b, staging, "part-0.csv", "id,n\n1,2\n"), write(t, b, staging, "sub/part-1.csv", "3,4\n")}
	m, err := New("p1", "t", "raw.orders", "2026-10-24", staging, files, nil)
	require.NoError(t, err)

	uri, err := s.Promote(ctx, m)
	require.NoError(t, err)
	assert.Equal(t, l.PalletManifest("t", "raw.orders", "2026-10-24", "p1"), uri)
	got, err := s.Read(ctx, uri)
	require.NoError(t, err)
	assert.Equal(t, m, got)

	again, err := s.Promote(ctx, m)
	require.NoError(t, err, "promoting the same manifest again is a no-op")
	assert.Equal(t, uri, again)

	other := m
	other.Files = files[:1]
	other.ContentHash = ContentHash(other.Files, nil)
	_, err = s.Promote(ctx, other)
	assert.ErrorIs(t, err, ErrConflict)
}

func TestPromoteChecksFilesArePresent(t *testing.T) {
	s, b, l := open(t)
	staging := l.LiftStaging("t", "raw", "", "a1")
	present := write(t, b, staging, "a.csv", "x")
	m, err := New("p1", "t", "raw", "", staging, []File{present, {Path: "b.csv", Size: 1, SHA256: present.SHA256}}, nil)
	require.NoError(t, err)

	_, err = s.Promote(ctx, m)
	assert.ErrorIs(t, err, ErrMissingFiles)
	assert.ErrorContains(t, err, `"b.csv"`)
	_, err = b.Get(ctx, l.PalletManifest("t", "raw", "", "p1"))
	assert.ErrorIs(t, err, blob.ErrNotExist, "nothing was promoted")
}

// remove deletes an object behind hubwagen's back, as someone could.
func remove(t *testing.T, b blob.Bucket, uri string) {
	t.Helper()
	path, ok := b.LocalPath(uri)
	require.True(t, ok)
	require.NoError(t, os.Remove(path))
}

func TestCheckFindsDeletedData(t *testing.T) {
	s, b, l := open(t)
	staging := l.LiftStaging("t", "raw", "", "a1")
	m, err := New("p1", "t", "raw", "", staging, []File{write(t, b, staging, "a.csv", "x"), write(t, b, staging, "b.csv", "y")}, nil)
	require.NoError(t, err)
	uri, err := s.Promote(ctx, m)
	require.NoError(t, err)
	require.NoError(t, s.Check(ctx, uri))

	remove(t, b, staging+"b.csv")
	err = s.Check(ctx, uri)
	assert.ErrorIs(t, err, ErrMissingFiles)
	assert.ErrorContains(t, err, `"b.csv"`)

	remove(t, b, uri)
	err = s.Check(ctx, uri)
	assert.ErrorIs(t, err, ErrMissingFiles)
	assert.ErrorContains(t, err, "manifest")
}

func TestConcurrentPromotesAgree(t *testing.T) {
	s, b, l := open(t)
	staging := l.LiftStaging("t", "raw", "", "a1")
	m, err := New("p1", "t", "raw", "", staging, []File{write(t, b, staging, "a.csv", "x")}, nil)
	require.NoError(t, err)

	// Two leaders (a deposed one and its successor) promote the same pallet.
	var wg sync.WaitGroup
	errs := make([]error, 8)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, errs[i] = s.Promote(ctx, m)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		assert.NoError(t, err)
	}
}

func TestDiscard(t *testing.T) {
	s, b, l := open(t)
	keep, drop := l.LiftStaging("t", "raw", "", "a2"), l.LiftStaging("t", "raw", "", "a1")
	write(t, b, drop, "a.csv", "x")
	write(t, b, keep, "a.csv", "y")
	require.NoError(t, s.Discard(ctx, drop))
	left, err := b.List(ctx, b.Base()+"/data/")
	require.NoError(t, err)
	assert.Equal(t, []string{keep + "a.csv"}, left)
}
