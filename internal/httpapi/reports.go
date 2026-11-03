package httpapi

import (
	"errors"
	"io"
	"net/http"
	"path"
	"strings"

	"async-file-processor/internal/jobs"

	"github.com/google/uuid"
)

// getReport returns the stored summary.json of a completed job.
func (a *api) getReport(w http.ResponseWriter, r *http.Request) {
	job, ok := a.completedJob(w, r)
	if !ok || !a.requireKey(w, job.ReportKey) {
		return
	}
	a.proxy(w, r, *job.ReportKey, "application/json", "")
}

// getErrorsCSV streams the errors file of a completed job as a download.
func (a *api) getErrorsCSV(w http.ResponseWriter, r *http.Request) {
	job, ok := a.completedJob(w, r)
	if !ok || !a.requireKey(w, job.ErrorsKey) {
		return
	}
	a.proxy(w, r, *job.ErrorsKey, "text/csv; charset=utf-8", errorsFilename(job.Filename))
}

// completedJob loads the job named in the path. It writes the response and returns false when
// the id is malformed, unknown, or the job is not COMPLETED yet (409).
func (a *api) completedJob(w http.ResponseWriter, r *http.Request) (jobs.Job, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid job id")
		return jobs.Job{}, false
	}
	job, err := a.Jobs.Get(r.Context(), id.String())
	if errors.Is(err, jobs.ErrNotFound) {
		writeError(w, http.StatusNotFound, "job not found")
		return jobs.Job{}, false
	}
	if err != nil {
		loggerFrom(r.Context(), a.Logger).Error("get job", "error", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return jobs.Job{}, false
	}
	if job.Status != jobs.StatusCompleted {
		writeError(w, http.StatusConflict, "job is "+string(job.Status)+"; report is not available")
		return jobs.Job{}, false
	}
	return job, true
}

func (a *api) requireKey(w http.ResponseWriter, key *string) bool {
	if key == nil || *key == "" {
		writeError(w, http.StatusConflict, "report is not available")
		return false
	}
	return true
}

// proxy streams an object from storage to the client. Locally the API proxies downloads so
// clients never need to reach the object store; on AWS these would be presigned S3 URLs.
func (a *api) proxy(w http.ResponseWriter, r *http.Request, key, contentType, filename string) {
	body, err := a.Store.Get(r.Context(), key)
	if err != nil {
		loggerFrom(r.Context(), a.Logger).Error("read report object", "error", err, "key", key)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	defer body.Close()
	w.Header().Set("Content-Type", contentType)
	if filename != "" {
		w.Header().Set("Content-Disposition", `attachment; filename="`+filename+`"`)
	}
	if _, err := io.Copy(w, body); err != nil {
		// Headers are already sent; all that is left is to log the truncated download.
		loggerFrom(r.Context(), a.Logger).Warn("stream report object", "error", err, "key", key)
	}
}

// errorsFilename derives the download name from the uploaded file name, falling back to
// "errors.csv" when nothing safe remains. Only ASCII letters, digits, '.', '-' and '_' are kept
// so the value is safe inside a quoted header parameter.
func errorsFilename(original string) string {
	base := strings.TrimSuffix(original, path.Ext(original))
	var b strings.Builder
	for _, c := range base {
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '.', c == '-', c == '_':
			b.WriteRune(c)
		case c == ' ':
			b.WriteByte('_')
		}
	}
	name := strings.Trim(b.String(), ".")
	if name == "" {
		return "errors.csv"
	}
	return name + "-errors.csv"
}
