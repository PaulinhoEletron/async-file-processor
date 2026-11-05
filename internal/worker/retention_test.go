package worker

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"testing"
	"time"

	"async-file-processor/internal/jobs"
)

type retJobs struct {
	expired   []jobs.Job
	deleted   []string
	cutoff    time.Time
	limit     int
	deleteErr map[string]error
}

func (f *retJobs) ListExpired(_ context.Context, cutoff time.Time, limit int) ([]jobs.Job, error) {
	f.cutoff, f.limit = cutoff, limit
	return f.expired, nil
}

func (f *retJobs) DeleteFinished(_ context.Context, id string) (bool, error) {
	if err := f.deleteErr[id]; err != nil {
		return false, err
	}
	f.deleted = append(f.deleted, id)
	return true, nil
}

type retStore struct {
	deleted []string
	err     map[string]error
}

func (f *retStore) Delete(_ context.Context, key string) error {
	if err := f.err[key]; err != nil {
		return err
	}
	f.deleted = append(f.deleted, key)
	return nil
}

func TestSweepDeletesObjectsThenRows(t *testing.T) {
	report, errs := "a/summary.json", "a/errors.csv"
	j := &retJobs{expired: []jobs.Job{
		{ID: "a", InputKey: "a/input.csv", ReportKey: &report, ErrorsKey: &errs},
		{ID: "b", InputKey: "b/input.csv"}, // failed job: no output keys recorded
	}}
	st := &retStore{}
	now := time.Unix(100000, 0)
	s := NewSweeper(j, st, 24*time.Hour, time.Minute, nil, slog.New(slog.DiscardHandler))
	s.now = func() time.Time { return now }

	if n := s.Sweep(context.Background()); n != 2 {
		t.Fatalf("deleted = %d, want 2", n)
	}
	if want := now.Add(-24 * time.Hour); !j.cutoff.Equal(want) || j.limit != sweepBatch {
		t.Fatalf("cutoff/limit = %v/%d, want %v/%d", j.cutoff, j.limit, want, sweepBatch)
	}
	sort.Strings(st.deleted)
	want := []string{"a/errors.csv", "a/input.csv", "a/summary.json", "b/errors.csv", "b/input.csv", "b/summary.json"}
	if len(st.deleted) != len(want) {
		t.Fatalf("objects deleted = %v, want %v", st.deleted, want)
	}
	for i := range want {
		if st.deleted[i] != want[i] {
			t.Fatalf("objects deleted = %v, want %v", st.deleted, want)
		}
	}
	if len(j.deleted) != 2 {
		t.Fatalf("rows deleted = %v", j.deleted)
	}
}

func TestSweepKeepsRowWhenObjectDeleteFails(t *testing.T) {
	j := &retJobs{expired: []jobs.Job{{ID: "a", InputKey: "a/input.csv"}, {ID: "b", InputKey: "b/input.csv"}}}
	st := &retStore{err: map[string]error{"a/input.csv": errors.New("s3 down")}}
	s := NewSweeper(j, st, time.Hour, time.Minute, nil, slog.New(slog.DiscardHandler))
	if n := s.Sweep(context.Background()); n != 1 {
		t.Fatalf("deleted = %d, want 1", n)
	}
	if len(j.deleted) != 1 || j.deleted[0] != "b" {
		t.Fatalf("only job b may be deleted, got %v", j.deleted)
	}
}

func TestSweepRowDeleteErrorIsNotCounted(t *testing.T) {
	j := &retJobs{expired: []jobs.Job{{ID: "a", InputKey: "a/input.csv"}}, deleteErr: map[string]error{"a": errors.New("db down")}}
	s := NewSweeper(j, &retStore{}, time.Hour, time.Minute, nil, slog.New(slog.DiscardHandler))
	if n := s.Sweep(context.Background()); n != 0 {
		t.Fatalf("deleted = %d, want 0", n)
	}
}

func TestSweeperRunStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	s := NewSweeper(&retJobs{}, &retStore{}, time.Hour, 10*time.Millisecond, nil, slog.New(slog.DiscardHandler))
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	time.Sleep(30 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after cancel")
	}
}
