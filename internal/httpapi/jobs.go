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
	"unicode"
	"unicode/utf8"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/metrics"

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
	rc := http.NewResponseController(w)
	body := &deadlineReader{r: r.Body, setDead: rc.SetReadDeadline, idle: a.UploadIdleTimeout}
	if a.UploadMaxDuration > 0 {
		body.hard = time.Now().Add(a.UploadMaxDuration)
	}
	r.Body = http.MaxBytesReader(w, body, a.MaxUploadBytes)
	// Clear the deadline once the body has been consumed: a leftover one would later break the
	// server's detection of a client that disconnects.
	defer func() { _ = rc.SetReadDeadline(time.Time{}) }()

	mr, err := r.MultipartReader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "request must be multipart/form-data")
		return
	}
	part, err := filePart(mr)
	if err != nil {
		a.uploadError(w, r, body, err, "read upload")
		return
	}
	if part == nil {
		writeError(w, http.StatusBadRequest, `missing multipart field "file"`)
		return
	}
	filename, err := cleanFilename(part.FileName())
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	id := uuid.NewString()
	key := id + "/input.csv"
	counted := &countingReader{r: part}
	if err := a.Store.Put(r.Context(), key, counted); err != nil {
		a.uploadError(w, r, body, err, "store upload")
		return
	}
	_ = rc.SetReadDeadline(time.Time{})
	a.Metrics.UploadBytes.Add(float64(counted.n))

	// The browser keeps the cookie it already has; a new one is issued on its first upload.
	owner, hasOwner := ownerFromRequest(r)
	var token string
	if !hasOwner {
		if token, owner, err = newOwnerToken(); err != nil {
			log.Error("mint owner token", "error", err)
			a.removeUpload(r, key)
			writeError(w, http.StatusInternalServerError, "internal error")
			return
		}
	}

	// Persist and enqueue with a context that survives a client disconnect, so a stored
	// object is not left without a job record.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 30*time.Second)
	defer cancel()
	job, err := a.Jobs.Create(ctx, jobs.NewJob{
		ID: id, Filename: filename, InputKey: key, OwnerHash: owner, ClientKey: a.hashedClientKey(r),
	})
	if err != nil {
		log.Error("create job", "error", err, "job_id", id)
		if delErr := a.Store.Delete(ctx, key); delErr != nil {
			log.Error("remove orphaned upload", "error", delErr, "key", key)
		}
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	msg, _ := json.Marshal(map[string]string{"job_id": id})
	if err := a.Queue.Send(ctx, string(msg)); err != nil {
		log.Error("enqueue job", "error", err, "job_id", id)
		if failErr := a.Jobs.Fail(ctx, id, "failed to enqueue job"); failErr != nil {
			log.Error("mark job failed", "error", failErr, "job_id", id)
		}
		writeError(w, http.StatusServiceUnavailable, "could not queue job for processing")
		return
	}
	a.Metrics.JobsTotal.WithLabelValues(metrics.StatusCreated).Inc()
	log.Info("job created", "job_id", id, "filename", filename, "bytes", counted.n)
	if !hasOwner {
		a.setOwnerCookie(w, token)
	}
	writeJSON(w, http.StatusAccepted, newJobResponse(job))
}

func (a *api) removeUpload(r *http.Request, key string) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), 10*time.Second)
	defer cancel()
	if err := a.Store.Delete(ctx, key); err != nil {
		loggerFrom(r.Context(), a.Logger).Error("remove orphaned upload", "error", err, "key", key)
	}
}

const maxFilenameRunes = 255

// cleanFilename reduces an uploaded file name to a safe display name: any directory part is
// dropped, control and bidirectional-override characters are removed, and the result must be
// valid UTF-8, at most 255 characters and end in .csv.
func cleanFilename(raw string) (string, error) {
	if !utf8.ValidString(raw) {
		return "", errors.New("filename must be valid UTF-8")
	}
	if len(raw) > 8*maxFilenameRunes*4 { // cheap bound before any per-rune work
		return "", errors.New("filename too long")
	}
	name := strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || isBidiControl(r) {
			return -1
		}
		return r
	}, path.Base(strings.ReplaceAll(raw, `\`, "/")))
	if utf8.RuneCountInString(name) > maxFilenameRunes {
		return "", errors.New("filename too long")
	}
	if !strings.EqualFold(path.Ext(name), ".csv") {
		return "", errors.New("file must have a .csv extension")
	}
	return name, nil
}

// isBidiControl reports Unicode directional marks, embeddings, overrides and isolates, which can
// make a file name display differently from what it contains.
func isBidiControl(r rune) bool {
	return r == 0x200E || r == 0x200F || (r >= 0x202A && r <= 0x202E) || (r >= 0x2066 && r <= 0x2069)
}

// filePart advances to the part named "file". It returns nil when the body ends cleanly without
// one. A body that ends without the closing boundary is an error, not "no file".
func filePart(mr *multipart.Reader) (*multipart.Part, error) {
	for {
		p, err := mr.NextPart()
		if err == io.EOF { // only a clean end of the stream; a truncated body wraps the error
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
func (a *api) uploadError(w http.ResponseWriter, r *http.Request, body *deadlineReader, err error, what string) {
	var tooBig *http.MaxBytesError
	switch {
	case body.timedOut.Load():
		w.Header().Set("Connection", "close")
		writeError(w, http.StatusRequestTimeout, "upload timed out")
	case errors.As(err, &tooBig):
		writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("file exceeds the %d byte upload limit", a.MaxUploadBytes))
	case r.Context().Err() != nil:
		// client went away; nothing to send
	case errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF):
		writeError(w, http.StatusBadRequest, "incomplete upload")
	case what == "read upload":
		loggerFrom(r.Context(), a.Logger).Warn(what, "error", err)
		writeError(w, http.StatusBadRequest, "malformed multipart body")
	default:
		loggerFrom(r.Context(), a.Logger).Error(what, "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
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

// listJobs returns the caller's own jobs, newest first. limit defaults to 50 and is capped at 200.
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
	// The list is private to the browser that uploaded the jobs. Without a valid owner cookie
	// there is nothing to show. A job's own id remains a capability link for its detail,
	// report and errors.csv.
	owner, ok := ownerFromRequest(r)
	if !ok {
		writeJSON(w, http.StatusOK, []jobResponse{})
		return
	}
	list, err := a.Jobs.List(r.Context(), owner, limit)
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

// countingReader counts the bytes read through it. The upload reads it from a single goroutine
// at a time and the count is read only after Put returns.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}
