// Package httpapi serves the file processor's HTTP API.
package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/metrics"
)

// Store reads and writes objects.
type Store interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader) error
	Delete(ctx context.Context, key string) error
}

// Jobs persists job records.
type Jobs interface {
	Create(ctx context.Context, n jobs.NewJob) (jobs.Job, error)
	Get(ctx context.Context, id string) (jobs.Job, error)
	// List returns the newest jobs created by the owner with the given token hash.
	List(ctx context.Context, ownerHash []byte, limit int) ([]jobs.Job, error)
	Fail(ctx context.Context, id, msg string) error
	// Unfinished counts PENDING and PROCESSING jobs: all of them and those of one client.
	Unfinished(ctx context.Context, clientKey string) (total, forClient int, err error)
}

// Queue publishes work for the worker.
type Queue interface {
	Send(ctx context.Context, body string) error
}

// Checker probes one dependency for /readyz and returns an error when it is unavailable.
type Checker func(ctx context.Context) error

// Deps are the collaborators and settings the handler needs.
type Deps struct {
	Store          Store
	Jobs           Jobs
	Queue          Queue
	Logger         *slog.Logger
	MaxUploadBytes int64
	// UI holds the embedded web UI files (index.html, app.js, style.css). Nil disables the UI routes.
	UI fs.FS
	// Checks are the dependency probes behind /readyz, keyed by name (for example "db").
	Checks map[string]Checker
	// Metrics receives upload and job counters. They are not served by this handler; expose
	// them on a separate, internal listener. Nil uses a private set.
	Metrics *metrics.Metrics
	// UploadRatePerMin is the sustained POST /api/jobs rate allowed per client and UploadRateBurst
	// the largest burst. A rate of 0 disables the limit.
	UploadRatePerMin float64
	UploadRateBurst  int
	// TrustCFConnectingIP keys per-client limits on the CF-Connecting-IP header instead of the
	// connection's remote address. Set it only when the API is reachable solely via Cloudflare.
	TrustCFConnectingIP bool
	// CookieSecure marks the owner cookie Secure. Enable it when the site is served over HTTPS.
	CookieSecure bool
	// UploadIdleTimeout cuts off an upload that sends no data for this long, and UploadMaxDuration
	// bounds the whole upload. Zero disables each.
	UploadIdleTimeout time.Duration
	UploadMaxDuration time.Duration
	// MaxConcurrentUploads caps uploads streaming at once (503 beyond it). Zero disables.
	MaxConcurrentUploads int
	// MaxUnfinishedJobs and MaxUnfinishedPerClient cap PENDING+PROCESSING jobs overall (503) and
	// per client (429). Zero disables each.
	MaxUnfinishedJobs      int
	MaxUnfinishedPerClient int
}

type api struct {
	Deps
	slots chan struct{} // semaphore for concurrent uploads; nil when uncapped
}

// NewHandler returns the API router wrapped in request id, logging and recovery middleware.
func NewHandler(d Deps) http.Handler {
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	if d.Metrics == nil {
		d.Metrics = metrics.New()
	}
	a := &api{Deps: d}
	if d.MaxConcurrentUploads > 0 {
		a.slots = make(chan struct{}, d.MaxConcurrentUploads)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("GET /readyz", a.readyz)
	// Admission order for uploads: rate limit, then the concurrent-upload slot, then the
	// queue caps, and only then is the body read.
	create := a.queueCaps(a.createJob)
	create = a.uploadSlots(create)
	if d.UploadRatePerMin > 0 {
		burst := max(d.UploadRateBurst, 1)
		create = rateLimit(newRateLimiter(d.UploadRatePerMin, burst), a.clientKey, create)
	}
	mux.HandleFunc("POST /api/jobs", create)
	mux.HandleFunc("GET /api/jobs/{id}", a.getJob)
	mux.HandleFunc("GET /api/jobs", a.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}/report", a.getReport)
	mux.HandleFunc("GET /api/jobs/{id}/errors.csv", a.getErrorsCSV)
	mux.HandleFunc("GET /api/sample.csv", a.getSampleCSV)
	a.registerUI(mux)
	return requestID(securityHeaders(logRequests(d.Logger, recoverPanics(d.Logger, jsonErrors(mux)))))
}

// jobResponse is the public JSON form of a job. Internal object keys are not exposed.
type jobResponse struct {
	ID          string      `json:"id"`
	Filename    string      `json:"filename"`
	Status      jobs.Status `json:"status"`
	TotalRows   int64       `json:"total_rows"`
	ValidRows   int64       `json:"valid_rows"`
	InvalidRows int64       `json:"invalid_rows"`
	Attempts    int         `json:"attempts"`
	ErrorMsg    *string     `json:"error_msg"`
	CreatedAt   time.Time   `json:"created_at"`
	UpdatedAt   time.Time   `json:"updated_at"`
	StartedAt   *time.Time  `json:"started_at"`
	FinishedAt  *time.Time  `json:"finished_at"`
}

func newJobResponse(j jobs.Job) jobResponse {
	return jobResponse{
		ID: j.ID, Filename: j.Filename, Status: j.Status,
		TotalRows: j.TotalRows, ValidRows: j.ValidRows, InvalidRows: j.InvalidRows,
		Attempts: j.Attempts, ErrorMsg: j.ErrorMsg,
		CreatedAt: j.CreatedAt, UpdatedAt: j.UpdatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt,
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
