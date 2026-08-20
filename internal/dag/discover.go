package dag

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// Discovered is one *.yaml/*.yml file found while walking a DAGs directory,
// successfully parsed or not. Exactly one of DAG/ParseErr is set.
type Discovered struct {
	Path     string
	DAG      *DAG   // nil if ParseErr != nil
	ParseErr error  // non-nil if the file failed to parse or validate
	YAMLHash string // sha256 hex of the file's raw bytes; empty if ParseErr != nil
}

// DiscoverDir walks dir recursively for *.yaml/*.yml files, parsing and
// validating each with ParseFile + Validate. A per-file parse or validation
// failure is captured on that file's Discovered entry, not returned as an
// error — discovery of the remaining files continues. The returned error is
// non-nil only when dir itself cannot be walked (missing, not a directory,
// permission denied).
func DiscoverDir(dir string) ([]Discovered, error) {
	var results []Discovered

	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		ext := strings.ToLower(filepath.Ext(path))
		if ext != ".yaml" && ext != ".yml" {
			return nil
		}

		results = append(results, discoverFile(path))
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walking dags directory %q: %w", dir, err)
	}

	return results, nil
}

func discoverFile(path string) Discovered {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Discovered{Path: path, ParseErr: fmt.Errorf("reading file: %w", err)}
	}

	d, err := ParseFile(path)
	if err != nil {
		return Discovered{Path: path, ParseErr: err}
	}

	if errs := Validate(d); len(errs) > 0 {
		return Discovered{Path: path, ParseErr: errors.Join(errs...)}
	}

	hash := fmt.Sprintf("%x", sha256.Sum256(raw))
	return Discovered{Path: path, DAG: d, YAMLHash: hash}
}
