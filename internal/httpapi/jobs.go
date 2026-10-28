package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"async-file-processor/internal/jobs"

	"github.com/google/uuid"
)

const (
	defaultListLimit = 50
	maxListLimit     = 200
	uploadField      = "file"
)

// createJob streams the multipart "file" part straight into object storage, records a PENDING
// job and enqueues it. The body is never buffered or parsed into memory.
func (a *api) createJob(w http.ResponseWriter, r *http.Request) {
	log := loggerFrom(r.Context(), a.Logger)
	r.Body = http.MaxBytesReader(w, r.Body, a.MaxUploadBytes)
	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "request must be multipart/form-data")
		return
	}
	part, err := filePart(mr)
	if err != nil {
		a.uploadError(w, r, err, "read upload")
		return
	}
	if part == nil {
		writeError(w, http.StatusBadRequest, `missing multipart field "file"`)
		return
	}
	filename := path.Base(strings.ReplaceAll(part.FileName(), `\`, "/"))
	if !strings.EqualFold(path.Ext(filename), ".csv") {
		writeError(w, http.StatusBadRequest, "file must have a .csv extension")
		return
	}

	id := uuid.NewString()
	key := id + "/input.csv"
	if err := a.Store.Put(r.Context(), key, part); err != nil {
		a.uploadError(w, r, err, "store upload")
		return
	}

	// Persist and enqueue with a context that survives a client disconnect, so a stored
	// object is not left without a job record.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()
	job, err := a.Jobs.Create(ctx, id, filename, key)
	if err != nil {
		log.Error("create job", "error", err, "job_id", id)
		if delErr := a.Store.Delete(ctx, key); delErr != nil {
			log.Error("remove orphaned upload", "error", delErr, "key", key)
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	body, _ := json.Marshal(map[string]string{"job_id": id})
	if err := a.Queue.Send(ctx, string(body)); err != nil {
		log.Error("enqueue job", "error", err, "job_id", id)
		if failErr := a.Jobs.Fail(ctx, id, "failed to enqueue job"); failErr != nil {
			log.Error("mark job failed", "error", failErr, "job_id", id)
		}
		writeError(w, http.StatusServiceUnavailable, "could not queue job for processing")
		return
	}
	log.Info("job created", "job_id", id, "filename", filename)
	writeJSON(w, http.StatusAccepted, newJobResponse(job))
}

// filePart advances to the part named "file". It returns nil when there is none.
func filePart(mr *multipart.Reader) (*multipart.Part, error) {
	for {
		p, err := mr.NextPart()
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if p.FormName() == uploadField && p.FileName() != "" {
			return p, nil
		}
	}
}

// uploadError maps a failure while reading or storing the upload to a response.
func (a *api) uploadError(w http.ResponseWriter, r *http.Request, err error, what string) {
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("file exceeds the %d byte upload limit", a.MaxUploadBytes))
		return
	}
	if r.Context().Err() != nil {
		return // client went away; nothing to send
	}
	loggerFrom(r.Context(), a.Logger).Error(what, "error", err)
	if what == "read upload" {
		writeError(w, http.StatusBadRequest, "malformed multipart body")
		return
	}
	writeError(w, http.StatusInternalServerError, "internal error")
}

// getJob returns one job. A malformed id is a 400; an unknown id is a 404.
func (a *api) getJob(w http.ResponseWriter, r *http.Request) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return
	}
	job, err := a.Jobs.Get(r.Context(), id.String())
	if errors.Is(err, jobs.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return
	}
	if err != nil {
		loggerFrom(r.Context(), a.Logger).Error("get job", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	writeJSON(w, http.StatusOK, newJobResponse(job))
}

// listJobs returns the newest jobs first. limit defaults to 50 and is capped at 200.
func (a *api) listJobs(w http.ResponseWriter, r *http.Request) {
	limit := defaultListLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be a positive integer")
			return
		}
		limit = min(n, maxListLimit)
	}
	list, err := a.Jobs.List(r.Context(), limit)
	if err != nil {
		loggerFrom(r.Context(), a.Logger).Error("list jobs", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	out := make([]jobResponse, 0, len(list))
	for _, j := range list {
		out = append(out, newJobResponse(j))
	}
	writeJSON(w, http.StatusOK, out)
}
