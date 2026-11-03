package httpapi

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"async-file-processor/internal/jobs"

	"github.com/google/uuid"
)

const (
	testSummary   = `{"job_id":"x","total_rows":3,"error_count":2}`
	testErrorsCSV = "row,field,error,raw\n2,email,must be a valid email address,\"2,B,bad,1,2024-01-01,US\"\n"
)

func strPtr(s string) *string { return &s }

// addJob stores a job with the given status. Completed jobs get report and errors keys.
func addJob(e *env, status jobs.Status, filename string) string {
	id := uuid.NewString()
	j := jobs.Job{ID: id, Filename: filename, Status: status}
	if status == jobs.StatusCompleted {
		j.ReportKey, j.ErrorsKey = strPtr(id+"/summary.json"), strPtr(id+"/errors.csv")
		e.store.objects = map[string]string{id + "/summary.json": testSummary, id + "/errors.csv": testErrorsCSV}
	}
	e.jobs.jobs = map[string]jobs.Job{id: j}
	return id
}

func TestReportEndpoints(t *testing.T) {
	e := newEnv(1024)
	id := addJob(e, jobs.StatusCompleted, "My Data (1).csv")

	rec := do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+id+"/report", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != testSummary {
		t.Fatalf("report: %d %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("report Content-Type = %q", ct)
	}

	rec = do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+id+"/errors.csv", nil))
	if rec.Code != http.StatusOK || rec.Body.String() != testErrorsCSV {
		t.Fatalf("errors.csv: %d %q", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/csv; charset=utf-8" {
		t.Fatalf("errors.csv Content-Type = %q", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); cd != `attachment; filename="My_Data_1-errors.csv"` {
		t.Fatalf("Content-Disposition = %q", cd)
	}
}

func TestReportNotFoundAndInvalidID(t *testing.T) {
	e := newEnv(1024)
	for _, suffix := range []string{"/report", "/errors.csv"} {
		rec := do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+uuid.NewString()+suffix, nil))
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s unknown id: status %d", suffix, rec.Code)
		}
		errorBody(t, rec)
		rec = do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/not-a-uuid"+suffix, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s malformed id: status %d", suffix, rec.Code)
		}
	}
}

func TestReportNotReady(t *testing.T) {
	for _, status := range []jobs.Status{jobs.StatusPending, jobs.StatusProcessing, jobs.StatusFailed} {
		for _, suffix := range []string{"/report", "/errors.csv"} {
			t.Run(string(status)+suffix, func(t *testing.T) {
				e := newEnv(1024)
				id := addJob(e, status, "a.csv")
				rec := do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+id+suffix, nil))
				if rec.Code != http.StatusConflict {
					t.Fatalf("status = %d", rec.Code)
				}
				errorBody(t, rec)
			})
		}
	}
}

func TestReportCompletedWithoutKey(t *testing.T) {
	e := newEnv(1024)
	id := uuid.NewString()
	e.jobs.jobs = map[string]jobs.Job{id: {ID: id, Status: jobs.StatusCompleted}}
	for _, suffix := range []string{"/report", "/errors.csv"} {
		rec := do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+id+suffix, nil))
		if rec.Code != http.StatusConflict {
			t.Fatalf("%s: status = %d", suffix, rec.Code)
		}
		errorBody(t, rec)
	}
}

func TestReportStorageFailure(t *testing.T) {
	e := newEnv(1024)
	id := addJob(e, jobs.StatusCompleted, "a.csv")
	e.store.getErr = errors.New("s3 down")
	for _, suffix := range []string{"/report", "/errors.csv"} {
		rec := do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+id+suffix, nil))
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s: status = %d", suffix, rec.Code)
		}
		if got := errorBody(t, rec); got != "internal error" {
			t.Fatalf("error = %q", got)
		}
	}
}

func TestErrorsFilename(t *testing.T) {
	for in, want := range map[string]string{
		"data.csv":          "data-errors.csv",
		"My Data.CSV":       "My_Data-errors.csv",
		`a"b;c\d.csv`:       "abcd-errors.csv",
		"résumé.csv":        "rsum-errors.csv",
		"ключ.csv":          "errors.csv",
		"..csv":             "errors.csv",
		"line\r\nbreak.csv": "linebreak-errors.csv",
		"":                  "errors.csv",
	} {
		if got := errorsFilename(in); got != want {
			t.Errorf("errorsFilename(%q) = %q, want %q", in, got, want)
		}
	}
}
