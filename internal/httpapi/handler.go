// Package httpapi serves the file processor's HTTP API.
package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"async-file-processor/internal/jobs"
)

// Store reads and writes objects.
type Store interface {
	Get(ctx context.Context, key string) (io.ReadCloser, error)
	Put(ctx context.Context, key string, r io.Reader) error
	Delete(ctx context.Context, key string) error
}

// Jobs persists job records.
type Jobs interface {
	Create(ctx context.Context, id, filename, inputKey string) (jobs.Job, error)
	Get(ctx context.Context, id string) (jobs.Job, error)
	List(ctx context.Context, limit int) ([]jobs.Job, error)
	Fail(ctx context.Context, id, msg string) error
}

// Queue publishes work for the worker.
type Queue interface {
	Send(ctx context.Context, body string) error
}

// Deps are the collaborators and settings the handler needs.
type Deps struct {
	Store          Store
	Jobs           Jobs
	Queue          Queue
	Logger         *slog.Logger
	MaxUploadBytes int64
}

type api struct {
	Deps
}

// NewHandler returns the API router wrapped in request id, logging and recovery middleware.
func NewHandler(d Deps) http.Handler {
	if d.Logger == nil {
		d.Logger = slog.New(slog.DiscardHandler)
	}
	a := &api{d}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("POST /api/jobs", a.createJob)
	mux.HandleFunc("GET /api/jobs/{id}", a.getJob)
	mux.HandleFunc("GET /api/jobs", a.listJobs)
	mux.HandleFunc("GET /api/jobs/{id}/report", a.getReport)
	mux.HandleFunc("GET /api/jobs/{id}/errors.csv", a.getErrorsCSV)
	return requestID(logRequests(d.Logger, recoverPanics(d.Logger, mux)))
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
