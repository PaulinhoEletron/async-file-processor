package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"async-file-processor/internal/jobs"

	"github.com/google/uuid"
)

type fakeStore struct {
	objects map[string]string
	putErr  error
	deleted []string
}

func (f *fakeStore) Put(_ context.Context, key string, r io.Reader) error {
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	if f.putErr != nil {
		return f.putErr
	}
	if f.objects == nil {
		f.objects = map[string]string{}
	}
	f.objects[key] = string(b)
	return nil
}

func (f *fakeStore) Delete(_ context.Context, key string) error {
	f.deleted = append(f.deleted, key)
	return nil
}

type fakeJobs struct {
	jobs      map[string]jobs.Job
	order     []string
	failed    map[string]string
	createErr error
	listLimit int
}

func (f *fakeJobs) Create(_ context.Context, id, filename, key string) (jobs.Job, error) {
	if f.createErr != nil {
		return jobs.Job{}, f.createErr
	}
	j := jobs.Job{ID: id, Filename: filename, InputKey: key, Status: jobs.StatusPending, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if f.jobs == nil {
		f.jobs = map[string]jobs.Job{}
	}
	f.jobs[id] = j
	f.order = append(f.order, id)
	return j, nil
}

func (f *fakeJobs) Get(_ context.Context, id string) (jobs.Job, error) {
	j, ok := f.jobs[id]
	if !ok {
		return jobs.Job{}, jobs.ErrNotFound
	}
	return j, nil
}

func (f *fakeJobs) List(_ context.Context, limit int) ([]jobs.Job, error) {
	f.listLimit = limit
	var out []jobs.Job
	for i := len(f.order) - 1; i >= 0 && len(out) < limit; i-- {
		out = append(out, f.jobs[f.order[i]])
	}
	return out, nil
}

func (f *fakeJobs) Fail(_ context.Context, id, msg string) error {
	if f.failed == nil {
		f.failed = map[string]string{}
	}
	f.failed[id] = msg
	j := f.jobs[id]
	j.Status = jobs.StatusFailed
	f.jobs[id] = j
	return nil
}

type fakeQueue struct {
	bodies []string
	err    error
}

func (f *fakeQueue) Send(_ context.Context, body string) error {
	if f.err != nil {
		return f.err
	}
	f.bodies = append(f.bodies, body)
	return nil
}

type env struct {
	store *fakeStore
	jobs  *fakeJobs
	queue *fakeQueue
	h     http.Handler
}

func newEnv(maxBytes int64) *env {
	e := &env{store: &fakeStore{}, jobs: &fakeJobs{}, queue: &fakeQueue{}}
	e.h = NewHandler(Deps{Store: e.store, Jobs: e.jobs, Queue: e.queue, Logger: slog.New(slog.DiscardHandler), MaxUploadBytes: maxBytes})
	return e
}

func upload(t *testing.T, field, filename, content string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	fw, err := mw.CreateFormFile(field, filename)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fw.Write([]byte(content))
	_ = mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func do(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func errorBody(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] == "" {
		t.Fatalf("not a JSON error: %q (%v)", rec.Body.String(), err)
	}
	return body["error"]
}

func TestHealth(t *testing.T) {
	h := newEnv(1024).h
	for _, tc := range []struct {
		method string
		path   string
		status int
	}{
		{http.MethodGet, "/healthz", http.StatusOK},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed},
		{http.MethodGet, "/missing", http.StatusNotFound},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			rec := do(h, httptest.NewRequest(tc.method, tc.path, nil))
			if rec.Code != tc.status {
				t.Fatalf("status = %d, want %d", rec.Code, tc.status)
			}
			if tc.status == http.StatusOK {
				if rec.Body.String() != `{"status":"ok"}` {
					t.Fatalf("unexpected health response: %s", rec.Body.String())
				}
				if rec.Header().Get("Content-Type") != "application/json" {
					t.Fatal("health response must be JSON")
				}
			}
		})
	}
}

func TestUploadSuccess(t *testing.T) {
	e := newEnv(1 << 20)
	content := "id,name\n1,a\n"
	rec := do(e.h, upload(t, "file", `C:\temp\..\data.CSV`, content))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
	}
	var got jobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if _, err := uuid.Parse(got.ID); err != nil {
		t.Fatalf("bad id %q", got.ID)
	}
	if got.Status != jobs.StatusPending || got.Filename != "data.CSV" {
		t.Fatalf("unexpected job: %+v", got)
	}
	if e.store.objects[got.ID+"/input.csv"] != content {
		t.Fatalf("stored objects: %v", e.store.objects)
	}
	if e.jobs.jobs[got.ID].InputKey != got.ID+"/input.csv" {
		t.Fatal("job input key mismatch")
	}
	if len(e.queue.bodies) != 1 || e.queue.bodies[0] != `{"job_id":"`+got.ID+`"}` {
		t.Fatalf("queue bodies: %v", e.queue.bodies)
	}
}

func TestUploadRejects(t *testing.T) {
	e := newEnv(1 << 20)
	for name, req := range map[string]*http.Request{
		"not csv":       upload(t, "file", "notes.txt", "x"),
		"missing field": upload(t, "other", "a.csv", "x"),
		"not multipart": httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader("x")),
	} {
		t.Run(name, func(t *testing.T) {
			rec := do(e.h, req)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d", rec.Code)
			}
			errorBody(t, rec)
		})
	}
	if len(e.store.objects) != 0 || len(e.jobs.jobs) != 0 || len(e.queue.bodies) != 0 {
		t.Fatal("rejected uploads must have no side effects")
	}
}

func TestUploadTooLarge(t *testing.T) {
	e := newEnv(100)
	rec := do(e.h, upload(t, "file", "big.csv", strings.Repeat("x", 1000)))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d", rec.Code)
	}
	errorBody(t, rec)
	if len(e.jobs.jobs) != 0 || len(e.queue.bodies) != 0 {
		t.Fatal("oversize upload must not create a job")
	}
}

func TestUploadStoreFailure(t *testing.T) {
	e := newEnv(1 << 20)
	e.store.putErr = errors.New("s3 down")
	rec := do(e.h, upload(t, "file", "a.csv", "x"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(errorBody(t, rec), "s3 down") {
		t.Fatal("internal error leaked")
	}
	if len(e.jobs.jobs) != 0 || len(e.queue.bodies) != 0 {
		t.Fatal("failed upload must not create a job")
	}
}

func TestUploadCreateFailureRemovesObject(t *testing.T) {
	e := newEnv(1 << 20)
	e.jobs.createErr = errors.New("db down")
	rec := do(e.h, upload(t, "file", "a.csv", "x"))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(e.store.deleted) != 1 {
		t.Fatalf("deleted = %v", e.store.deleted)
	}
}

func TestUploadQueueFailure(t *testing.T) {
	e := newEnv(1 << 20)
	e.queue.err = errors.New("sqs down")
	rec := do(e.h, upload(t, "file", "a.csv", "x"))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(errorBody(t, rec), "sqs down") {
		t.Fatal("internal error leaked")
	}
	if len(e.jobs.failed) != 1 {
		t.Fatalf("failed jobs = %v", e.jobs.failed)
	}
	for id := range e.jobs.failed {
		if e.jobs.jobs[id].Status != jobs.StatusFailed {
			t.Fatal("job not marked failed")
		}
	}
}

func TestGetJob(t *testing.T) {
	e := newEnv(1024)
	j, _ := e.jobs.Create(context.Background(), uuid.NewString(), "a.csv", "k")
	rec := do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+j.ID, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var got map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got["status"] != "PENDING" || got["id"] != j.ID {
		t.Fatalf("unexpected body: %v", got)
	}
	if _, leaked := got["input_key"]; leaked {
		t.Fatal("object key must not be exposed")
	}
	for _, field := range []string{"total_rows", "valid_rows", "invalid_rows", "attempts", "error_msg", "created_at", "updated_at", "started_at", "finished_at"} {
		if _, ok := got[field]; !ok {
			t.Errorf("missing field %q", field)
		}
	}

	rec = do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/"+uuid.NewString(), nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown id status = %d", rec.Code)
	}
	errorBody(t, rec)

	rec = do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs/nope", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid id status = %d", rec.Code)
	}
	errorBody(t, rec)
}

func TestListJobs(t *testing.T) {
	e := newEnv(1024)

	rec := do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != "[]" {
		t.Fatalf("empty list: %d %q", rec.Code, rec.Body.String())
	}
	if e.jobs.listLimit != 50 {
		t.Fatalf("default limit = %d", e.jobs.listLimit)
	}

	var ids []string
	for range 3 {
		j, _ := e.jobs.Create(context.Background(), uuid.NewString(), "a.csv", "k")
		ids = append(ids, j.ID)
	}
	rec = do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs?limit=2", nil))
	var list []jobResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list) != 2 || list[0].ID != ids[2] || list[1].ID != ids[1] {
		t.Fatalf("unexpected list: %+v", list)
	}

	do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs?limit=100000", nil))
	if e.jobs.listLimit != 200 {
		t.Fatalf("limit not clamped: %d", e.jobs.listLimit)
	}

	for _, bad := range []string{"abc", "0", "-1"} {
		rec = do(e.h, httptest.NewRequest(http.MethodGet, "/api/jobs?limit="+bad, nil))
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("limit=%s status = %d", bad, rec.Code)
		}
		errorBody(t, rec)
	}
}

func TestRequestID(t *testing.T) {
	h := newEnv(1024).h
	rec := do(h, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing generated request id")
	}
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "client-id.1")
	if got := do(h, req).Header().Get("X-Request-ID"); got != "client-id.1" {
		t.Fatalf("request id = %q", got)
	}
	req = httptest.NewRequest(http.MethodGet, "/healthz", nil)
	req.Header.Set("X-Request-ID", "bad id\twith junk")
	if got := do(h, req).Header().Get("X-Request-ID"); got == "bad id\twith junk" || got == "" {
		t.Fatalf("unsafe request id accepted: %q", got)
	}
}

func TestRecoverPanics(t *testing.T) {
	boom := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") })
	h := requestID(recoverPanics(slog.New(slog.DiscardHandler), boom))
	rec := do(h, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if strings.Contains(errorBody(t, rec), "boom") {
		t.Fatal("panic value leaked")
	}
	if rec.Header().Get("X-Request-ID") == "" {
		t.Fatal("missing request id")
	}
}
