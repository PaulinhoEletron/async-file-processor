package httpapi

import (
	"encoding/csv"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"async-file-processor/internal/validate"
)

func newUIEnv() http.Handler {
	ui := fstest.MapFS{
		"index.html": {Data: []byte("<!doctype html><title>x</title>")},
		"app.js":     {Data: []byte("console.log(1)")},
		"style.css":  {Data: []byte("body{}")},
	}
	return NewHandler(Deps{Store: &fakeStore{}, Jobs: &fakeJobs{}, Queue: &fakeQueue{}, Logger: slog.New(slog.DiscardHandler), MaxUploadBytes: 1024, UI: ui})
}

func TestUIServed(t *testing.T) {
	h := newUIEnv()
	for _, tc := range []struct{ path, ctype string }{
		{"/", "text/html"},
		{"/app.js", "javascript"},
		{"/style.css", "text/css"},
	} {
		rec := do(h, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), tc.ctype) {
			t.Errorf("%s: status %d, Content-Type %q", tc.path, rec.Code, rec.Header().Get("Content-Type"))
		}
		if rec.Header().Get("Cache-Control") != "no-cache" {
			t.Errorf("%s: Cache-Control = %q", tc.path, rec.Header().Get("Cache-Control"))
		}
	}
	for _, p := range []string{"/nope", "/index.html.bak", "/api/nope"} {
		if rec := do(h, httptest.NewRequest(http.MethodGet, p, nil)); rec.Code != http.StatusNotFound {
			t.Errorf("%s: status %d, want 404", p, rec.Code)
		}
	}
}

func TestNoUIWithoutFS(t *testing.T) {
	rec := do(newEnv(1024).h, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestSampleCSV(t *testing.T) {
	rec := do(newEnv(1024).h, httptest.NewRequest(http.MethodGet, "/api/sample.csv", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/csv") {
		t.Errorf("Content-Type = %q", ct)
	}
	if cd := rec.Header().Get("Content-Disposition"); cd != `attachment; filename="sample.csv"` {
		t.Errorf("Content-Disposition = %q", cd)
	}
	r := csv.NewReader(rec.Body)
	r.FieldsPerRecord = -1
	header, err := r.Read()
	if err != nil || validate.ValidateHeader(header) != nil {
		t.Fatalf("bad header %v (%v)", header, err)
	}
	v := validate.NewValidator()
	var total, invalid int
	rules := map[string]bool{}
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		total++
		if errs := v.ValidateRow(rec); len(errs) > 0 {
			invalid++
			for _, e := range errs {
				rules[e.Rule] = true
			}
		}
	}
	if total != sampleRows || invalid != sampleRows/sampleBadEvery {
		t.Errorf("rows = %d invalid = %d, want %d and %d", total, invalid, sampleRows, sampleRows/sampleBadEvery)
	}
	for _, rule := range []string{validate.RuleRequired, validate.RuleNotInteger, validate.RuleDuplicate, validate.RuleTooLong,
		validate.RuleInvalidEmail, validate.RuleInvalidDecimal, validate.RuleNegative, validate.RuleInvalidDate,
		validate.RuleInvalidCountry, validate.RuleColumnCount} {
		if !rules[rule] {
			t.Errorf("sample never triggers rule %q", rule)
		}
	}
}

func TestSecurityHeaders(t *testing.T) {
	h := newUIEnv()
	for _, path := range []string{"/", "/app.js", "/api/jobs", "/nope"} {
		rec := do(h, httptest.NewRequest(http.MethodGet, path, nil))
		for name, want := range map[string]string{
			"Content-Security-Policy": "default-src 'self'; frame-ancestors 'none'",
			"X-Content-Type-Options":  "nosniff",
			"X-Frame-Options":         "DENY",
			"Referrer-Policy":         "no-referrer",
		} {
			if got := rec.Header().Get(name); got != want {
				t.Errorf("%s %s = %q, want %q", path, name, got, want)
			}
		}
	}
}
