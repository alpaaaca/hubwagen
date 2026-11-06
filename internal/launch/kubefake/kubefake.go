// Package kubefake is the part of the Kubernetes API server that moksha
// uses, in memory, for tests: Deployments in namespaces (create, get, list
// by label selector, delete) and their scale subresource. It keeps what
// was created as JSON and records every request.
package kubefake

import (
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
)

// API is the fake API server; serve it with httptest.
type API struct {
	mu          sync.Mutex
	deployments map[string]map[string]any // namespace/name → the Deployment
	requests    []string                  // "METHOD path"
}

func New() *API { return &API{deployments: map[string]map[string]any{}} }

// Put adds a Deployment as if something made it before.
func (a *API) Put(namespace string, d map[string]any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.deployments[namespace+"/"+name(d)] = d
}

// Deployment is namespace/name's Deployment, or nil.
func (a *API) Deployment(namespace, name string) map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.deployments[namespace+"/"+name]
}

// Names is the Deployments in namespace, sorted.
func (a *API) Names(namespace string) []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for k := range a.deployments {
		if ns, n, _ := strings.Cut(k, "/"); ns == namespace {
			out = append(out, n)
		}
	}
	slices.Sort(out)
	return out
}

// Requests is what was asked so far, "METHOD path".
func (a *API) Requests() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.requests)
}

func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.requests = append(a.requests, r.Method+" "+r.URL.Path)
	// /apis/apps/v1/namespaces/<ns>/deployments[/<name>[/scale]]
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/apis/apps/v1/namespaces/"), "/")
	if len(parts) < 2 || parts[1] != "deployments" || !strings.HasPrefix(r.URL.Path, "/apis/apps/v1/namespaces/") {
		status(w, http.StatusNotFound, "NotFound")
		return
	}
	ns := parts[0]
	body, _ := io.ReadAll(r.Body)
	switch {
	case len(parts) == 2 && r.Method == http.MethodPost:
		var d map[string]any
		if err := json.Unmarshal(body, &d); err != nil || name(d) == "" {
			status(w, http.StatusBadRequest, "BadRequest")
			return
		}
		if a.deployments[ns+"/"+name(d)] != nil {
			status(w, http.StatusConflict, "AlreadyExists")
			return
		}
		a.deployments[ns+"/"+name(d)] = d
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(d)
	case len(parts) == 2 && r.Method == http.MethodGet:
		items := []any{}
		for k, d := range a.deployments {
			if strings.HasPrefix(k, ns+"/") && matches(d, r.URL.Query().Get("labelSelector")) {
				items = append(items, d)
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "DeploymentList", "items": items})
	case len(parts) == 3 && r.Method == http.MethodDelete:
		if a.deployments[ns+"/"+parts[2]] == nil {
			status(w, http.StatusNotFound, "NotFound")
			return
		}
		delete(a.deployments, ns+"/"+parts[2])
		status(w, http.StatusOK, "Success")
	case len(parts) == 4 && parts[3] == "scale" && r.Method == http.MethodPatch:
		d := a.deployments[ns+"/"+parts[2]]
		if d == nil {
			status(w, http.StatusNotFound, "NotFound")
			return
		}
		var patch struct {
			Spec struct {
				Replicas int `json:"replicas"`
			} `json:"spec"`
		}
		_ = json.Unmarshal(body, &patch)
		d["spec"].(map[string]any)["replicas"] = float64(patch.Spec.Replicas)
		_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Scale"})
	default:
		status(w, http.StatusMethodNotAllowed, "MethodNotAllowed")
	}
}

// Replicas is a Deployment's spec.replicas.
func Replicas(d map[string]any) int {
	n, _ := d["spec"].(map[string]any)["replicas"].(float64)
	return int(n)
}

func name(d map[string]any) string {
	n, _ := d["metadata"].(map[string]any)["name"].(string)
	return n
}

// matches reports whether d's labels satisfy a selector of k=v pairs.
func matches(d map[string]any, selector string) bool {
	labels, _ := d["metadata"].(map[string]any)["labels"].(map[string]any)
	for _, kv := range strings.Split(selector, ",") {
		if kv == "" {
			continue
		}
		k, v, _ := strings.Cut(kv, "=")
		if labels[k] != v {
			return false
		}
	}
	return true
}

func status(w http.ResponseWriter, code int, reason string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "Status", "reason": reason, "code": code})
}
