package httpapi

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestStalledUploadTimesOut(t *testing.T) {
	store, repo, q := &fakeStore{}, &fakeJobs{}, &fakeQueue{}
	srv := httptest.NewServer(NewHandler(Deps{
		Store: store, Jobs: repo, Queue: q, Logger: slog.New(slog.DiscardHandler),
		MaxUploadBytes: 1 << 20, UploadIdleTimeout: 300 * time.Millisecond, UploadMaxDuration: time.Minute,
	}))
	defer srv.Close()

	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// Announce a large body, send a fraction of it, then go quiet.
	req := "POST /api/jobs HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=B\r\nContent-Length: 100000\r\n\r\n" +
		"--B\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.csv\"\r\n\r\nid,name\n1,a\n"
	start := time.Now()
	if _, err := conn.Write([]byte(req)); err != nil {
		t.Fatal(err)
	}
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response to a stalled upload: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusRequestTimeout || !strings.Contains(string(body), "upload timed out") {
		t.Fatalf("status = %d body %q, want 408 upload timed out", resp.StatusCode, body)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("stalled upload held for %v", elapsed)
	}
	if len(store.objects) != 0 || len(repo.jobs) != 0 || len(q.bodies) != 0 {
		t.Fatal("a timed-out upload must leave no object, job or message behind")
	}
}

func TestUploadMaxDuration(t *testing.T) {
	// A client that keeps trickling data still hits the overall cap.
	srv := httptest.NewServer(NewHandler(Deps{
		Store: &fakeStore{}, Jobs: &fakeJobs{}, Queue: &fakeQueue{},
		MaxUploadBytes: 1 << 20, UploadIdleTimeout: time.Minute, UploadMaxDuration: 400 * time.Millisecond,
	}))
	defer srv.Close()
	conn, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	head := "POST /api/jobs HTTP/1.1\r\nHost: x\r\nContent-Type: multipart/form-data; boundary=B\r\nContent-Length: 100000\r\n\r\n" +
		"--B\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.csv\"\r\n\r\n"
	_, _ = conn.Write([]byte(head))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 40; i++ {
			if _, err := conn.Write([]byte("x")); err != nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("no response: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusRequestTimeout {
		t.Fatalf("status = %d, want 408", resp.StatusCode)
	}
	conn.Close()
	<-done
}

// blockingStore holds Put until released, to keep an upload "in progress".
type blockingStore struct {
	fakeStore
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (b *blockingStore) Put(ctx context.Context, key string, r io.Reader) error {
	b.once.Do(func() { close(b.started) })
	<-b.release
	return b.fakeStore.Put(ctx, key, r)
}

func TestConcurrentUploadCap(t *testing.T) {
	store := &blockingStore{started: make(chan struct{}), release: make(chan struct{})}
	h := NewHandler(Deps{Store: store, Jobs: &fakeJobs{}, Queue: &fakeQueue{}, MaxUploadBytes: 1 << 20, MaxConcurrentUploads: 1})
	e := &env{h: h}
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- uploadWithCookie(t, e, "") }()
	<-store.started

	rec := uploadWithCookie(t, e, "")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" {
		t.Fatalf("second concurrent upload: status %d, Retry-After %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	errorBody(t, rec)

	close(store.release)
	if r := <-first; r.Code != http.StatusAccepted {
		t.Fatalf("first upload status = %d", r.Code)
	}
	// The slot is free again.
	if r := uploadWithCookie(t, e, ""); r.Code != http.StatusAccepted {
		t.Fatalf("upload after release status = %d", r.Code)
	}
}

func intp(n int) *int { return &n }

func envWithCaps(global, perClient int) *env {
	e := &env{store: &fakeStore{}, jobs: &fakeJobs{}, queue: &fakeQueue{}}
	e.h = NewHandler(Deps{
		Store: e.store, Jobs: e.jobs, Queue: e.queue, MaxUploadBytes: 1 << 20,
		MaxUnfinishedJobs: global, MaxUnfinishedPerClient: perClient,
	})
	return e
}

func TestGlobalUnfinishedCap(t *testing.T) {
	e := envWithCaps(5, 0)
	e.jobs.unfinishedTotal, e.jobs.unfinishedClient = intp(5), intp(0)
	rec := uploadWithCookie(t, e, "")
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") == "" ||
		errorBody(t, rec) != "the processing queue is full, try again later" {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if len(e.store.objects) != 0 || len(e.jobs.jobs) != 0 {
		t.Fatal("a refused upload must not be read or stored")
	}
	e.jobs.unfinishedTotal = intp(4)
	if rec := uploadWithCookie(t, e, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("below the cap: status %d", rec.Code)
	}
}

func TestPerClientUnfinishedCap(t *testing.T) {
	e := envWithCaps(50, 3)
	e.jobs.unfinishedTotal, e.jobs.unfinishedClient = intp(3), intp(3)
	rec := uploadWithCookie(t, e, "")
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") == "" ||
		errorBody(t, rec) != "you have too many jobs in progress, try again when one finishes" {
		t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
	}
	if len(e.store.objects) != 0 {
		t.Fatal("a refused upload must not be stored")
	}
}

func TestUnfinishedCapsDisabledWithZero(t *testing.T) {
	e := envWithCaps(0, 0)
	e.jobs.unfinishedTotal, e.jobs.unfinishedClient = intp(1000), intp(1000)
	if rec := uploadWithCookie(t, e, ""); rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202 with both caps disabled", rec.Code)
	}
	if e.jobs.unfinishedCalls != 0 {
		t.Fatal("disabled caps must not query the repository")
	}
}

func TestPerClientCapCountsOnlyThatClient(t *testing.T) {
	e := envWithCaps(50, 2)
	post := func(ip string) int {
		req := upload(t, "file", "a.csv", "id,name\n1,a\n")
		req.RemoteAddr = ip + ":1000"
		return do(e.h, req).Code
	}
	for i := 0; i < 2; i++ {
		if code := post("192.0.2.1"); code != http.StatusAccepted {
			t.Fatalf("upload %d: %d", i+1, code)
		}
	}
	if code := post("192.0.2.1"); code != http.StatusTooManyRequests {
		t.Fatalf("third upload from the same client: %d, want 429", code)
	}
	if code := post("192.0.2.2"); code != http.StatusAccepted {
		t.Fatalf("another client: %d, want 202", code)
	}
	for id, key := range e.jobs.clients {
		if len(key) != 64 || strings.Contains(key, "192.0.2") {
			t.Fatalf("job %s client key %q is not a hash", id, key)
		}
	}
}

func TestTruncatedMultipartIsBadRequest(t *testing.T) {
	e := newEnv(1 << 20)
	// A body cut off before its closing boundary is reported as incomplete. The multipart
	// reader cannot tell a body cut inside the part headers from an empty one, so those are
	// reported as having no file; either way the client gets a 400, not a 500.
	for name, tc := range map[string]struct{ body, msg string }{
		"missing closing boundary": {"--B\r\nContent-Disposition: form-data; name=\"file\"; filename=\"a.csv\"\r\n\r\nid,name\n1,a\n", "incomplete upload"},
		"cut inside the headers":   {"--B\r\nContent-Disposition: form-data; name=\"fi", ""},
		"only the opening":         {"--B\r\n", ""},
	} {
		t.Run(name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/jobs", strings.NewReader(tc.body))
			req.Header.Set("Content-Type", "multipart/form-data; boundary=B")
			rec := do(e.h, req)
			msg := errorBody(t, rec)
			if rec.Code != http.StatusBadRequest || (tc.msg != "" && msg != tc.msg) {
				t.Fatalf("status %d body %q", rec.Code, rec.Body.String())
			}
		})
	}
	if len(e.store.objects) != 0 || len(e.jobs.jobs) != 0 {
		t.Fatal("a truncated upload must have no side effects")
	}
}

func TestCleanFilename(t *testing.T) {
	long := strings.Repeat("a", 252) + ".csv" // 256 runes
	for _, tc := range []struct {
		in, want string
		wantErr  string
	}{
		{in: "data.csv", want: "data.csv"},
		{in: `C:\temp\..\data.CSV`, want: "data.CSV"},
		{in: "a/b/c.csv", want: "c.csv"},
		{in: "ré\u202esumé.csv", want: "résumé.csv"},
		{in: "x\u200f\u2066y\u2069.csv", want: "xy.csv"},
		{in: "a\x00b\x1b[31m\x7f\u0085.csv", want: "ab[31m.csv"},
		{in: strings.Repeat("a", 251) + ".csv", want: strings.Repeat("a", 251) + ".csv"},
		{in: long, wantErr: "filename too long"},
		{in: strings.Repeat("é", 300) + ".csv", wantErr: "filename too long"},
		{in: "bad\xffname.csv", wantErr: "filename must be valid UTF-8"},
		{in: "notes.txt", wantErr: "file must have a .csv extension"},
		{in: "evil.csv\u202e", want: "evil.csv"},
		{in: "\u202e.csv", want: ".csv"},
	} {
		got, err := cleanFilename(tc.in)
		switch {
		case tc.wantErr != "" && (err == nil || err.Error() != tc.wantErr):
			t.Errorf("cleanFilename(%q) err = %v, want %q", tc.in, err, tc.wantErr)
		case tc.wantErr == "" && (err != nil || got != tc.want):
			t.Errorf("cleanFilename(%q) = %q, %v, want %q", tc.in, got, err, tc.want)
		}
	}
}

func TestUploadFilenameRules(t *testing.T) {
	e := newEnv(1 << 20)
	rec := do(e.h, upload(t, "file", strings.Repeat("a", 300)+".csv", "x"))
	if rec.Code != http.StatusBadRequest || errorBody(t, rec) != "filename too long" {
		t.Fatalf("long name: %d %q", rec.Code, rec.Body.String())
	}
	if len(e.jobs.jobs) != 0 {
		t.Fatal("rejected name created a job")
	}
	rec = do(e.h, upload(t, "file", "bad\xffname.csv", "x"))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid UTF-8 name: %d %q", rec.Code, rec.Body.String())
	}
	rec = do(e.h, upload(t, "file", "rtl\u202etxt.csv", "x"))
	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d", rec.Code)
	}
	for _, j := range e.jobs.jobs {
		if j.Filename != "rtltxt.csv" {
			t.Fatalf("stored filename = %q, want control characters stripped", j.Filename)
		}
	}
}

func TestMetricsNotOnPublicHandler(t *testing.T) {
	rec := do(newEnv(1024).h, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("/metrics status = %d, want 404", rec.Code)
	}
}
