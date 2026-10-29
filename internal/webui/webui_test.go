package webui

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/assert"
)

func get(h http.Handler, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestServeTheBuiltUI(t *testing.T) {
	h := serve(fstest.MapFS{
		"index.html":          {Data: []byte("<div id=root></div>")},
		"assets/index-abc.js": {Data: []byte("console.log(1)")},
		"favicon.svg":         {Data: []byte("<svg/>")},
	})
	w := get(h, "/")
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "<div id=root></div>", w.Body.String())
	assert.Equal(t, "no-cache", w.Header().Get("Cache-Control"))

	w = get(h, "/assets/index-abc.js")
	assert.Equal(t, "console.log(1)", w.Body.String())
	assert.Contains(t, w.Header().Get("Cache-Control"), "immutable", "hashed asset names never change")

	assert.Equal(t, "<svg/>", get(h, "/favicon.svg").Body.String())
	assert.Equal(t, "<div id=root></div>", get(h, "/hauls/01M6").Body.String(), "a client-side route gets the page")
}

func TestPlaceholderWithoutABuild(t *testing.T) {
	w := httptest.NewRecorder()
	placeholder(w, httptest.NewRequest(http.MethodGet, "/", nil))
	assert.Contains(t, w.Body.String(), "go generate ./internal/webui")
}
