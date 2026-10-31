package launch

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Secrets are values a server holds for deployed projects' workers
// ({secret: name} in a project file): a file per secret in Dir, its name
// the secret's, trailing newlines not part of the value. A deployment
// names them; the server reads them only when it starts a worker, and the
// values never reach events, state, the API or a command line.
type Secrets struct{ Dir string }

// Get returns secret name's value.
func (s Secrets) Get(name string) (string, error) {
	if s.Dir == "" {
		return "", errors.New("this server has no secrets: set projects.secrets_dir in its config")
	}
	if name == "" || strings.ContainsAny(name, `/\`) || name == "." || name == ".." {
		return "", fmt.Errorf("secret %q: not a secret name", name)
	}
	b, err := os.ReadFile(filepath.Join(s.Dir, name))
	if errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("secret %s: no such secret on this server (a file %s)", name, filepath.Join(s.Dir, name))
	}
	if err != nil {
		return "", fmt.Errorf("secret %s: %w", name, err)
	}
	return strings.TrimRight(string(b), "\r\n"), nil
}

// Check reports whether every secret in names (env name → secret name)
// exists, all problems at once.
func (s Secrets) Check(names map[string]string) error {
	var errs []error
	for _, name := range names {
		if _, err := s.Get(name); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
