package httpapi

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"
)

const checkTimeout = 2 * time.Second

// readyz probes every dependency concurrently, each bounded by checkTimeout. It answers 200 when
// all succeed and 503 naming the failing ones otherwise (their error text is logged, not returned).
func (a *api) readyz(w http.ResponseWriter, r *http.Request) {
	results := make(map[string]string, len(a.Checks))
	details := make(map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	ready := true
	for name, check := range a.Checks {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(r.Context(), checkTimeout)
			defer cancel()
			res, detail := "ok", ""
			if err := check(ctx); err != nil {
				// The response says only that a dependency is down; the cause is logged.
				res, detail = "unavailable", err.Error()
			}
			mu.Lock()
			results[name] = res
			if res != "ok" {
				ready = false
				details[name] = detail
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if !ready {
		loggerFrom(r.Context(), a.Logger).Warn("not ready", "checks", details)
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"status": "unavailable", "checks": results})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ready", "checks": results})
}

// jsonErrors turns the mux's plain-text 404 (unknown path) and 405 (wrong method) responses into
// JSON. Responses that handlers already write as JSON pass through untouched.
func jsonErrors(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&jsonErrorWriter{ResponseWriter: w}, r)
	})
}

type jsonErrorWriter struct {
	http.ResponseWriter
	wroteHeader bool
	swallow     bool // the JSON body has been written; drop the plain-text one
}

func (w *jsonErrorWriter) WriteHeader(code int) {
	if w.wroteHeader {
		return
	}
	w.wroteHeader = true
	isJSON := strings.HasPrefix(w.Header().Get("Content-Type"), "application/json")
	if (code == http.StatusNotFound || code == http.StatusMethodNotAllowed) && !isJSON {
		msg := "not found"
		if code == http.StatusMethodNotAllowed {
			msg = "method not allowed"
		}
		h := w.Header()
		h.Set("Content-Type", "application/json")
		h.Del("Content-Length")
		w.ResponseWriter.WriteHeader(code)
		_, _ = w.ResponseWriter.Write([]byte(`{"error":"` + msg + `"}` + "\n"))
		w.swallow = true
		return
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *jsonErrorWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	if w.swallow {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

func (w *jsonErrorWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
