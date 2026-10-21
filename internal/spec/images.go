package spec

import (
	"context"
	"fmt"
	"strings"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/go-containerregistry/pkg/v1/remote"
)

// ImageResolver pins an image reference to a digest, so a compiled version
// always means the same image bytes.
type ImageResolver interface {
	Resolve(ctx context.Context, ref string) (string, error)
}

// RegistryResolver resolves tags against a container registry, using the
// local Docker credential helpers / config for auth. With Mirror set, lookups
// go to that registry instead (air-gapped installs), but the resolved
// reference keeps the original registry name.
type RegistryResolver struct {
	Mirror string
}

func (r RegistryResolver) Resolve(ctx context.Context, ref string) (string, error) {
	parsed, err := name.ParseReference(ref)
	if err != nil {
		return "", fmt.Errorf("image %q: %w", ref, err)
	}
	if _, ok := parsed.(name.Digest); ok {
		return ref, nil
	}
	lookup, err := mirrorRef(parsed, r.Mirror)
	if err != nil {
		return "", err
	}
	desc, err := remote.Head(lookup, remote.WithContext(ctx), remote.WithAuthFromKeychain(authn.DefaultKeychain))
	if err != nil {
		return "", fmt.Errorf("resolving image %q: %w", ref, err)
	}
	return repoWithoutTag(ref) + "@" + desc.Digest.String(), nil
}

// mirrorRef rewrites ref's registry to mirror (if set) for the lookup.
func mirrorRef(ref name.Reference, mirror string) (name.Reference, error) {
	if mirror == "" {
		return ref, nil
	}
	repo := ref.Context().RepositoryStr()
	m, err := name.ParseReference(strings.TrimSuffix(mirror, "/") + "/" + repo + ":" + ref.Identifier())
	if err != nil {
		return nil, fmt.Errorf("registry mirror %q: %w", mirror, err)
	}
	return m, nil
}

// repoWithoutTag strips a ":tag" suffix (but not a registry port).
func repoWithoutTag(ref string) string {
	slash := strings.LastIndex(ref, "/")
	if colon := strings.LastIndex(ref, ":"); colon > slash {
		return ref[:colon]
	}
	return ref
}

// isDigestRef reports whether ref is already pinned by digest.
func isDigestRef(ref string) bool {
	return strings.Contains(ref, "@sha256:")
}
