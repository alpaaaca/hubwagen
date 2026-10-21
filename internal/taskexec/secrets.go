package taskexec

import (
	"context"
	"fmt"
	"strings"

	"github.com/alper/hubwagen/internal/spec"
)

// SecretProvider resolves a task's secret references to environment
// variables. It runs where the task runs (agent, Job), never in the leader.
type SecretProvider interface {
	Resolve(ctx context.Context, tenant string, refs []spec.Secret) (map[string]string, error)
}

// EnvSecrets reads secrets from the runner's own environment:
// HUBWAGEN_SECRET_<TENANT>_<NAME>_<KEY>, upper-cased, with every character
// other than A-Z and 0-9 replaced by _. Meant for dev mode; the variables
// themselves are never passed on to tasks.
type EnvSecrets struct {
	Environ []string
}

func (e EnvSecrets) Resolve(_ context.Context, tenant string, refs []spec.Secret) (map[string]string, error) {
	have := map[string]string{}
	for _, kv := range e.Environ {
		if k, v, ok := strings.Cut(kv, "="); ok {
			have[k] = v
		}
	}
	out := map[string]string{}
	for _, r := range refs {
		name := "HUBWAGEN_SECRET_" + envSuffix(tenant) + "_" + envSuffix(r.Name) + "_" + envSuffix(r.Key)
		v, ok := have[name]
		if !ok {
			return nil, fmt.Errorf("secret %s/%s not found: set %s", r.Name, r.Key, name)
		}
		out[r.Env] = v
	}
	return out, nil
}

// envSuffix upper-cases s and replaces anything but A-Z and 0-9 with _.
func envSuffix(s string) string {
	b := []byte(strings.ToUpper(s))
	for i, c := range b {
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			b[i] = '_'
		}
	}
	return string(b)
}

func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}
