package api

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/alper/hubwagen/internal/core"
)

// PoolsPath serves the worker pools as plain JSON, for autoscalers that
// poll an HTTP endpoint (KEDA's metrics-api trigger, a Prometheus
// exporter): GET /v1/pools, and GET /v1/pools/<name> for one, whose
// "desired" is how many workers it wants. The same as ListPools.
const PoolsPath = "/v1/pools"

type poolJSON struct {
	Name     string   `json:"name"`
	Launcher string   `json:"launcher"` // process or external
	Docks    []string `json:"docks"`
	Min      int      `json:"min"`
	Max      int      `json:"max"`
	Desired  int      `json:"desired"`
	Workers  int      `json:"workers"`
	Decision string   `json:"decision,omitempty"`
	// DecidedAt is UTC, RFC 3339.
	DecidedAt string `json:"decided_at,omitempty"`
}

// PoolsHandler serves PoolsPath.
func PoolsHandler(c *core.Core) http.Handler {
	mux := http.NewServeMux()
	list := func(w http.ResponseWriter, r *http.Request) ([]poolJSON, bool) {
		ps, err := c.Pools(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return nil, false
		}
		out := []poolJSON{}
		for _, p := range ps {
			j := poolJSON{Name: p.Name, Launcher: "process", Docks: p.Docks, Min: p.Min, Max: p.Max, Desired: p.Desired,
				Workers: len(p.Workers), Decision: p.Decision}
			if p.External {
				j.Launcher = "external"
			}
			if !p.DecidedAt.IsZero() {
				j.DecidedAt = p.DecidedAt.UTC().Format(time.RFC3339)
			}
			out = append(out, j)
		}
		return out, true
	}
	mux.HandleFunc("GET "+PoolsPath, func(w http.ResponseWriter, r *http.Request) {
		if ps, ok := list(w, r); ok {
			writeJSON(w, map[string]any{"pools": ps})
		}
	})
	mux.HandleFunc("GET "+PoolsPath+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		ps, ok := list(w, r)
		if !ok {
			return
		}
		for _, p := range ps {
			if p.Name == r.PathValue("name") {
				writeJSON(w, p)
				return
			}
		}
		http.Error(w, "no worker pool "+r.PathValue("name")+" (a workers: group with min and max, or launcher: external)", http.StatusNotFound)
	})
	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
