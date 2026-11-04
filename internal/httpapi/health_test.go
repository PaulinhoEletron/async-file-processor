package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"async-file-processor/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func readyHandler(checks map[string]Checker) http.Handler {
	return NewHandler(Deps{Store: &fakeStore{}, Jobs: &fakeJobs{}, Queue: &fakeQueue{}, Logger: slog.New(slog.DiscardHandler), MaxUploadBytes: 1024, Checks: checks})
}

func ok(context.Context) error { return nil }

func TestReadyzAllOK(t *testing.T) {
	h := readyHandler(map[string]Checker{"db": ok, "s3": ok, "sqs": ok})
	rec := do(h, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body %s", rec.Code, rec.Body)
	}
	var body struct {
		Status string
		Checks map[string]string
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "ready" || len(body.Checks) != 3 || body.Checks["db"] != "ok" || body.Checks["s3"] != "ok" || body.Checks["sqs"] != "ok" {
		t.Fatalf("unexpected body: %s", rec.Body)
	}
}

func TestReadyzOneFailing(t *testing.T) {
	h := readyHandler(map[string]Checker{
		"db":  ok,
		"s3":  func(context.Context) error { return errors.New("bucket unreachable") },
		"sqs": ok,
	})
	rec := do(h, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	var body struct {
		Status string
		Checks map[string]string
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Status != "unavailable" || body.Checks["s3"] != "bucket unreachable" || body.Checks["db"] != "ok" || body.Checks["sqs"] != "ok" {
		t.Fatalf("unexpected body: %s", rec.Body)
	}
}

func TestReadyzSlowCheckTimesOut(t *testing.T) {
	h := readyHandler(map[string]Checker{
		"db": func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() },
	})
	start := time.Now()
	rec := do(h, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
	if time.Since(start) > 4*time.Second {
		t.Fatal("check was not bounded by its timeout")
	}
}

func TestUnknownRoutesReturnJSON(t *testing.T) {
	h := newEnv(1024).h
	rec := do(h, httptest.NewRequest(http.MethodGet, "/nope", nil))
	if rec.Code != http.StatusNotFound || errorBody(t, rec) != "not found" {
		t.Fatalf("404: status %d body %q", rec.Code, rec.Body)
	}
	rec = do(h, httptest.NewRequest(http.MethodDelete, "/api/jobs", nil))
	if rec.Code != http.StatusMethodNotAllowed || errorBody(t, rec) != "method not allowed" {
		t.Fatalf("405: status %d body %q", rec.Code, rec.Body)
	}
	if rec.Header().Get("Allow") == "" {
		t.Fatal("405 must keep the Allow header")
	}
	// A handler's own JSON 404 is untouched.
	rec = do(h, httptest.NewRequest(http.MethodGet, "/api/jobs/6f1c2c1e-8a3b-4d2a-9d55-0a7d9e1f3b11", nil))
	if rec.Code != http.StatusNotFound || errorBody(t, rec) != "job not found" {
		t.Fatalf("job 404: status %d body %q", rec.Code, rec.Body)
	}
}

func TestMetricsEndpointMovesAfterUpload(t *testing.T) {
	m := metrics.New()
	store, jobsRepo, q := &fakeStore{}, &fakeJobs{}, &fakeQueue{}
	h := NewHandler(Deps{Store: store, Jobs: jobsRepo, Queue: q, Logger: slog.New(slog.DiscardHandler), MaxUploadBytes: 1 << 20, Metrics: m})

	content := "id,name\n1,a\n"
	if rec := do(h, upload(t, "file", "d.csv", content)); rec.Code != http.StatusAccepted {
		t.Fatalf("upload status = %d: %s", rec.Code, rec.Body)
	}
	// Rejected uploads must not count.
	if rec := do(h, upload(t, "file", "d.txt", content)); rec.Code != http.StatusBadRequest {
		t.Fatalf("txt upload status = %d", rec.Code)
	}

	if got := testutil.ToFloat64(m.JobsTotal.WithLabelValues(metrics.StatusCreated)); got != 1 {
		t.Fatalf("jobs_total{created} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.UploadBytes); got != float64(len(content)) {
		t.Fatalf("upload_bytes_total = %v, want %d", got, len(content))
	}

	rec := do(h, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`jobs_total{status="created"} 1`,
		`jobs_total{status="completed"} 0`,
		"upload_bytes_total 12",
		"worker_busy 0",
		"job_duration_seconds_bucket",
		"rows_processed_total 0",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %q", want)
		}
	}
}
