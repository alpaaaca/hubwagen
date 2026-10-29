// Package webui serves the web UI (ui/, TypeScript) from the binary. The
// build is embedded from dist/, which `go generate ./internal/webui` fills
// (it needs Node); without it, a page says how to build it. The UI is a
// single page: paths that aren't files get index.html.
package webui

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:generate sh build.sh

//go:embed all:dist
var dist embed.FS

// Built reports whether the binary has the UI.
func Built() bool {
	_, err := fs.Stat(dist, "dist/index.html")
	return err == nil
}

// Handler serves the UI, or the placeholder page if it wasn't built.
func Handler() http.Handler {
	if !Built() {
		return http.HandlerFunc(placeholder)
	}
	sub, err := fs.Sub(dist, "dist")
	if err != nil {
		panic(err) // dist/ is embedded
	}
	return serve(sub)
}

// serve serves a built UI from sub.
func serve(sub fs.FS) http.Handler {
	files := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := strings.TrimPrefix(r.URL.Path, "/")
		if path == "" || path == "index.html" {
			serveIndex(w, r, sub)
			return
		}
		if _, err := fs.Stat(sub, path); err != nil {
			serveIndex(w, r, sub) // a client-side route
			return
		}
		if strings.HasPrefix(path, "assets/") { // file names carry a content hash
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		files.ServeHTTP(w, r)
	})
}

func serveIndex(w http.ResponseWriter, r *http.Request, sub fs.FS) {
	b, err := fs.ReadFile(sub, "index.html")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	if r.Method != http.MethodHead {
		_, _ = w.Write(b)
	}
}

func placeholder(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(`<!doctype html><title>hubwagen</title>
<body style="font-family: system-ui; max-width: 40em; margin: 4em auto">
<h1>hubwagen</h1>
<p>The API is up, but this binary was built without the web UI.</p>
<p>Build it (needs Node and npm), then the binary:</p>
<pre>go generate ./internal/webui
go build -o hubwagen .</pre>
<p>Or work on the UI against this server: <code>cd ui &amp;&amp; npm install &amp;&amp; npm run dev</code>.</p>
</body>`))
}
