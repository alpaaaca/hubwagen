// Package pallet is the storage adapter for pallets kept as files in the
// bucket. A lift attempt writes its files to its own staging prefix; the
// promote writes a manifest listing them, create-if-absent, and from then on
// readers resolve the pallet through the manifest. The files never move, so
// the promote is one atomic write, and every committed version stays
// readable until retention removes it.
package pallet

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/alper/hubwagen/internal/blob"
)

var (
	// ErrMissingFiles: the manifest lists files that aren't in the bucket.
	ErrMissingFiles = errors.New("pallet: files missing")
	// ErrConflict: a different manifest already exists for this pallet.
	ErrConflict = errors.New("pallet: a different manifest exists")
)

// File is one data file of a pallet.
type File struct {
	Path   string `json:"path"` // relative to the manifest's Data prefix
	Size   int64  `json:"size"`
	SHA256 string `json:"sha256"` // hex
}

// Manifest is what a promote writes. Its bytes are a pure function of its
// fields, so writing it twice (a retried promote, a deposed leader racing
// the new one) produces identical objects.
type Manifest struct {
	Pallet    string `json:"pallet"`
	Tenant    string `json:"tenant"`
	Dataset   string `json:"dataset"`
	Partition string `json:"partition,omitempty"`
	Data      string `json:"data"` // prefix URI the files are under, ending in /
	Files     []File `json:"files"`
	// Result is what the lift returned, as canonical JSON; omitted when it
	// returned nothing (null).
	Result      json.RawMessage `json:"result,omitempty"`
	ContentHash string          `json:"content_hash"`
}

// ContentHash identifies a pallet's data independent of where it lives: the
// sha256 of every file's path, size and checksum, in path order, then the
// lift's result (canonical JSON) unless it is empty or null. Two pallets
// with the same files and result have the same hash; an empty or null
// result leaves the hash of the files alone.
func ContentHash(files []File, result json.RawMessage) string {
	sorted := append([]File(nil), files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	h := sha256.New()
	for _, f := range sorted {
		fmt.Fprintf(h, "%s\x00%d\x00%s\n", f.Path, f.Size, f.SHA256)
	}
	if !noResult(result) {
		fmt.Fprintf(h, "\x00result\x00%s\n", result)
	}
	return "sha256:" + hex.EncodeToString(h.Sum(nil))
}

func noResult(r json.RawMessage) bool {
	t := bytes.TrimSpace(r)
	return len(t) == 0 || bytes.Equal(t, []byte("null"))
}

// New fills in a manifest: files sorted by path, content hash computed.
// result must be canonical JSON (object keys sorted), so equal results hash
// equally.
func New(palletID, tenant, dataset, partition, data string, files []File, result json.RawMessage) (Manifest, error) {
	if !strings.HasSuffix(data, "/") {
		return Manifest{}, fmt.Errorf("pallet: data prefix %q must end in /", data)
	}
	seen := map[string]bool{}
	sorted := append([]File(nil), files...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })
	for i, f := range sorted {
		switch {
		case f.Path == "" || strings.HasPrefix(f.Path, "/") || strings.Contains("/"+f.Path+"/", "/../"):
			return Manifest{}, fmt.Errorf("pallet: files[%d]: path %q must be relative and inside the data prefix", i, f.Path)
		case seen[f.Path]:
			return Manifest{}, fmt.Errorf("pallet: files[%d]: %q listed twice", i, f.Path)
		case f.Size < 0:
			return Manifest{}, fmt.Errorf("pallet: files[%d]: negative size", i)
		case len(f.SHA256) != 64:
			return Manifest{}, fmt.Errorf("pallet: files[%d]: sha256 must be 64 hex digits", i)
		}
		seen[f.Path] = true
	}
	if noResult(result) {
		result = nil
	} else if !json.Valid(result) {
		return Manifest{}, errors.New("pallet: result is not JSON")
	}
	return Manifest{
		Pallet: palletID, Tenant: tenant, Dataset: dataset, Partition: partition,
		Data: data, Files: sorted, Result: result, ContentHash: ContentHash(sorted, result),
	}, nil
}

// URIs locates manifests; layout.Layout implements it.
type URIs interface {
	PalletManifest(tenant, dataset, partition, palletID string) string
}

// Store promotes and discards pallets in one bucket.
type Store struct {
	bucket blob.Bucket
	uris   URIs
}

func NewStore(b blob.Bucket, uris URIs) *Store { return &Store{bucket: b, uris: uris} }

// Promote makes m visible by writing its manifest, after checking that every
// listed file is in the bucket. It is idempotent: if the identical manifest
// already exists, Promote succeeds. It returns the manifest's URI.
func (s *Store) Promote(ctx context.Context, m Manifest) (string, error) {
	present, err := s.bucket.List(ctx, m.Data)
	if err != nil {
		return "", err
	}
	have := map[string]bool{}
	for _, uri := range present {
		have[strings.TrimPrefix(uri, m.Data)] = true
	}
	var missing []string
	for _, f := range m.Files {
		if !have[f.Path] {
			missing = append(missing, strconv.Quote(f.Path))
		}
	}
	if len(missing) > 0 {
		return "", fmt.Errorf("%w under %s: %s", ErrMissingFiles, m.Data, strings.Join(missing, ", "))
	}

	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	uri := s.uris.PalletManifest(m.Tenant, m.Dataset, m.Partition, m.Pallet)
	err = s.bucket.PutIfAbsent(ctx, uri, data)
	if errors.Is(err, blob.ErrExists) {
		existing, gerr := s.bucket.Get(ctx, uri)
		if gerr != nil {
			return "", gerr
		}
		if !bytes.Equal(existing, data) {
			return "", fmt.Errorf("%w: %s", ErrConflict, uri)
		}
		return uri, nil // promoted before (retry, or another leader)
	}
	if err != nil {
		return "", err
	}
	return uri, nil
}

// Read returns the manifest at uri.
func (s *Store) Read(ctx context.Context, uri string) (Manifest, error) {
	data, err := s.bucket.Get(ctx, uri)
	if err != nil {
		return Manifest{}, err
	}
	var m Manifest
	err = json.Unmarshal(data, &m)
	return m, err
}

// Discard removes a staging prefix that will never be promoted.
func (s *Store) Discard(ctx context.Context, prefix string) error {
	return s.bucket.DeletePrefix(ctx, prefix)
}
