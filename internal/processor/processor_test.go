package processor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"

	"async-file-processor/internal/jobs"
)

type fakeStore struct {
	mu      sync.Mutex
	objects map[string]string
	getErr  error
	putErr  error
	putKeys []string
}

func newStore(key, content string) *fakeStore {
	return &fakeStore{objects: map[string]string{key: content}}
}

func (s *fakeStore) Get(_ context.Context, key string) (io.ReadCloser, error) {
	if s.getErr != nil {
		return nil, s.getErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.objects[key]
	if !ok {
		return nil, errors.New("no such key")
	}
	return io.NopCloser(strings.NewReader(c)), nil
}

func (s *fakeStore) Put(_ context.Context, key string, r io.Reader) error {
	var buf bytes.Buffer
	if _, err := io.Copy(&buf, r); err != nil {
		return err
	}
	if s.putErr != nil {
		return s.putErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[key] = buf.String()
	s.putKeys = append(s.putKeys, key)
	return nil
}

type progressCall struct{ total, valid, invalid int64 }

type fakeProgress struct{ calls []progressCall }

func (p *fakeProgress) UpdateProgress(_ context.Context, _ string, total, valid, invalid int64) error {
	p.calls = append(p.calls, progressCall{total, valid, invalid})
	return nil
}

func run(t *testing.T, store *fakeStore, prog Progress) (Result, error) {
	t.Helper()
	p := New(store, prog, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return p.Process(context.Background(), jobs.Job{ID: "job1", InputKey: "in"})
}

const header = "id,name,email,amount,date,country\n"

func TestProcessSample(t *testing.T) {
	sample, err := os.ReadFile("../../scripts/sample.csv")
	if err != nil {
		t.Fatal(err)
	}
	store := newStore("in", string(sample))
	res, err := run(t, store, &fakeProgress{})
	if err != nil {
		t.Fatal(err)
	}
	want := Result{ErrorsKey: "job1/errors.csv", Total: 10, Valid: 4, Invalid: 6}
	if res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	wantCSV := "row,field,error,raw\n" +
		"5,email,must be a valid email address,\"5,Eva Novak,not-an-email,10.00,2024-05-05,CZ\"\n" +
		"6,name,is required,\"6,,frank@example.com,15.00,2024-06-01,DE\"\n" +
		"7,amount,must be greater than or equal to 0,\"7,Grace Lee,grace@example.com,-5.00,2024-07-09,US\"\n" +
		"8,date,must be a valid date in YYYY-MM-DD format,\"8,Hiro Tanaka,hiro@example.com,42.00,2024-13-45,JP\"\n" +
		"9,country,must be two uppercase letters,\"9,Ines Silva,ines@example.com,12.30,2024-08-19,brazil\"\n" +
		"10,id,is a duplicate of an earlier row,\"2,Duplicate Id,dup@example.com,9.99,2024-09-30,PT\"\n"
	if got := store.objects["job1/errors.csv"]; got != wantCSV {
		t.Fatalf("errors.csv:\n%s\nwant:\n%s", got, wantCSV)
	}
}

func TestProcessAllValidWritesHeaderOnly(t *testing.T) {
	store := newStore("in", header+"1,A,a@example.com,1,2024-01-01,US\n2,B,b@example.com,2,2024-01-02,BR\n")
	res, err := run(t, store, &fakeProgress{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 2 || res.Valid != 2 || res.Invalid != 0 || res.ErrorsKey != "job1/errors.csv" || res.ReportKey != "" {
		t.Fatalf("result = %+v", res)
	}
	if got := store.objects["job1/errors.csv"]; got != "row,field,error,raw\n" {
		t.Fatalf("errors.csv = %q", got)
	}
}

func TestProcessHeaderOnlyFile(t *testing.T) {
	res, err := run(t, newStore("in", header), &fakeProgress{})
	if err != nil || res.Total != 0 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestProcessBOMAndCRLF(t *testing.T) {
	store := newStore("in", "\uFEFFid,name,email,amount,date,country\r\n1,A,a@example.com,1,2024-01-01,US\r\n")
	res, err := run(t, store, &fakeProgress{})
	if err != nil || res.Valid != 1 {
		t.Fatalf("res=%+v err=%v", res, err)
	}
}

func TestProcessRowErrors(t *testing.T) {
	in := header +
		"1,A,a@example.com,1,2024-01-01\n" + // too few fields
		"2,\"Quoted, Name\",q@example.com,1,2024-01-01,US\n" + // valid, embedded comma
		"3,x\"y,c@example.com,1,2024-01-01,US\n" + // bare quote
		"4,,bad,-1,nope,xx\n" + // every rule except id
		"5,Z,z@example.com,1,2024-01-01,US\n"
	store := newStore("in", in)
	res, err := run(t, store, &fakeProgress{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 5 || res.Valid != 2 || res.Invalid != 3 {
		t.Fatalf("result = %+v", res)
	}
	lines := strings.Split(strings.TrimSpace(store.objects["job1/errors.csv"]), "\n")
	// header + width (1) + bare quote (1) + five field errors for row 4
	if len(lines) != 1+1+1+5 {
		t.Fatalf("errors.csv has %d lines:\n%s", len(lines), store.objects["job1/errors.csv"])
	}
	if want := `1,row,"expected 6 fields, got 5","1,A,a@example.com,1,2024-01-01"`; lines[1] != want {
		t.Fatalf("width error line = %q, want %q", lines[1], want)
	}
	if !strings.HasPrefix(lines[2], `3,row,"malformed CSV at line 4`) {
		t.Fatalf("parse error line = %q", lines[2])
	}
	for _, l := range lines[3:] {
		if !strings.HasPrefix(l, "4,") {
			t.Fatalf("unexpected line %q", l)
		}
	}
}

func TestProcessUnterminatedQuoteAtEOF(t *testing.T) {
	store := newStore("in", header+"1,A,a@example.com,1,2024-01-01,US\n2,\"open,b@example.com,1,2024-01-01,US\n")
	res, err := run(t, store, &fakeProgress{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != 2 || res.Valid != 1 || res.Invalid != 1 {
		t.Fatalf("result = %+v", res)
	}
}

func TestProcessInvalidInput(t *testing.T) {
	for name, content := range map[string]string{
		"empty":        "",
		"wrong header": "id,name,mail,amount,date,country\n1,A,a@example.com,1,2024-01-01,US\n",
		"no header":    "1,A,a@example.com,1,2024-01-01,US\n",
		"reordered":    "name,id,email,amount,date,country\n",
		"bad quote":    "id,na\"me,email,amount,date,country\n",
	} {
		t.Run(name, func(t *testing.T) {
			store := newStore("in", content)
			_, err := run(t, store, &fakeProgress{})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("err = %v, want ErrInvalidInput", err)
			}
			if len(store.putKeys) != 0 {
				t.Fatal("errors file must not be written for invalid input")
			}
		})
	}
}

func TestProcessProgress(t *testing.T) {
	var b strings.Builder
	b.WriteString(header)
	const rows = 2*ProgressEvery + 10
	for i := 1; i <= rows; i++ {
		if i%100 == 0 {
			fmt.Fprintf(&b, "%d,,a@example.com,1,2024-01-01,US\n", i)
		} else {
			fmt.Fprintf(&b, "%d,N,a@example.com,1,2024-01-01,US\n", i)
		}
	}
	prog := &fakeProgress{}
	res, err := run(t, newStore("in", b.String()), prog)
	if err != nil {
		t.Fatal(err)
	}
	if res.Total != rows || res.Invalid != rows/100 {
		t.Fatalf("result = %+v", res)
	}
	want := []progressCall{
		{ProgressEvery, ProgressEvery - ProgressEvery/100, ProgressEvery / 100},
		{2 * ProgressEvery, 2*ProgressEvery - 2*ProgressEvery/100, 2 * ProgressEvery / 100},
	}
	if len(prog.calls) != len(want) || prog.calls[0] != want[0] || prog.calls[1] != want[1] {
		t.Fatalf("progress calls = %v, want %v", prog.calls, want)
	}
}

func TestProcessGetFailure(t *testing.T) {
	boom := errors.New("s3 down")
	store := newStore("in", "")
	store.getErr = boom
	_, err := run(t, store, &fakeProgress{})
	if !errors.Is(err, boom) || errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v, want wrapped %v and not ErrInvalidInput", err, boom)
	}
}

func TestProcessPutFailure(t *testing.T) {
	boom := errors.New("upload refused")
	for name, rows := range map[string]int{"small": 3, "large": 50000} {
		t.Run(name, func(t *testing.T) {
			var b strings.Builder
			b.WriteString(header)
			for i := 1; i <= rows; i++ {
				fmt.Fprintf(&b, "%d,,bad,1,2024-01-01,US\n", i) // every row produces errors
			}
			store := newStore("in", b.String())
			store.putErr = boom
			_, err := run(t, store, &fakeProgress{})
			if !errors.Is(err, boom) {
				t.Fatalf("err = %v, want wrapped %v", err, boom)
			}
		})
	}
}

// earlyExitStore stops reading after the first read, as an uploader that gives up mid-stream would.
type earlyExitStore struct{ *fakeStore }

func (s earlyExitStore) Put(_ context.Context, _ string, r io.Reader) error {
	buf := make([]byte, 16)
	_, _ = r.Read(buf)
	return errors.New("upload aborted")
}

func TestProcessUploaderStopsEarly(t *testing.T) {
	var b strings.Builder
	b.WriteString(header)
	for i := 1; i <= 50000; i++ {
		fmt.Fprintf(&b, "%d,,bad,1,2024-01-01,US\n", i)
	}
	base := newStore("in", b.String())
	p := New(earlyExitStore{base}, &fakeProgress{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	_, err := p.Process(context.Background(), jobs.Job{ID: "job1", InputKey: "in"})
	if err == nil || !strings.Contains(err.Error(), "upload aborted") {
		t.Fatalf("err = %v, want upload aborted", err)
	}
}
