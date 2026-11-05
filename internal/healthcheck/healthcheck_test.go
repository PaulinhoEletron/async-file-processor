package healthcheck

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestURL(t *testing.T) {
	for in, want := range map[string]string{
		":8080":          "http://127.0.0.1:8080/readyz",
		"0.0.0.0:9091":   "http://127.0.0.1:9091/readyz",
		"127.0.0.1:9091": "http://127.0.0.1:9091/readyz",
	} {
		if got := URL(in, "/readyz"); got != want {
			t.Errorf("URL(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestProbe(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/ok" {
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	if err := Probe(srv.URL+"/ok", time.Second); err != nil {
		t.Fatalf("healthy endpoint: %v", err)
	}
	if err := Probe(srv.URL+"/bad", time.Second); err == nil {
		t.Fatal("503 must fail the probe")
	}
	srv.Close()
	if err := Probe(srv.URL+"/ok", time.Second); err == nil {
		t.Fatal("closed server must fail the probe")
	}
}
