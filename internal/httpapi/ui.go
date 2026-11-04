package httpapi

import (
	"io/fs"
	"net/http"
)

// registerUI serves the embedded web UI: index.html at exactly "/" plus its static assets.
func (a *api) registerUI(mux *http.ServeMux) {
	if a.UI == nil {
		return
	}
	serve := func(name string) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			// no-cache makes the browser revalidate, so edits show up without a hard reload.
			w.Header().Set("Cache-Control", "no-cache")
			http.ServeFileFS(w, r, a.UI, name)
		}
	}
	mux.HandleFunc("GET /{$}", serve("index.html"))
	for _, name := range []string{"app.js", "style.css"} {
		if _, err := fs.Stat(a.UI, name); err == nil {
			mux.HandleFunc("GET /"+name, serve(name))
		}
	}
}
