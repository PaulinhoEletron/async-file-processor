package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/processor"
	"async-file-processor/internal/queue"
)

type fakeQueue struct {
	mu        sync.Mutex
	pending   []queue.Msg
	deleted   map[string]bool
	maxLimit  int
	delivered int
}

func newFakeQueue(bodies ...string) *fakeQueue {
	q := &fakeQueue{deleted: map[string]bool{}}
	for i, b := range bodies {
		q.pending = append(q.pending, queue.Msg{ID: fmt.Sprint(i), Body: b, Receipt: fmt.Sprintf("r%d", i)})
	}
	return q
}

func (q *fakeQueue) Receive(ctx context.Context, limit int, _ time.Duration) ([]queue.Msg, error) {
	q.mu.Lock()
	if len(q.pending) > 0 {
		n := min(limit, len(q.pending))
		batch := q.pending[:n]
		q.pending = q.pending[n:]
		q.delivered += n
		q.maxLimit = max(q.maxLimit, limit)
		q.mu.Unlock()
		return batch, nil
	}
	q.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return nil, nil
	}
}

func (q *fakeQueue) Delete(_ context.Context, receipt string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.deleted[receipt] = true
	return nil
}

func (q *fakeQueue) deletedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.deleted)
}

type fakeJobs struct {
	claimErr  error
	mu        sync.Mutex
	completed map[string]bool
}

func (j *fakeJobs) MarkProcessing(_ context.Context, id string) (jobs.Job, error) {
	if j.claimErr != nil {
		return jobs.Job{}, j.claimErr
	}
	return jobs.Job{ID: id}, nil
}

func (j *fakeJobs) Complete(_ context.Context, id, _, _ string, _, _, _ int64) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.completed == nil {
		j.completed = map[string]bool{}
	}
	j.completed[id] = true
	return nil
}

func (j *fakeJobs) completedCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return len(j.completed)
}

type fakeProc struct {
	calls, inFlight, maxInFlight atomic.Int64
	started                      chan struct{}
	delay                        time.Duration
	err                          error
}

func (p *fakeProc) Process(ctx context.Context, _ jobs.Job) (processor.Result, error) {
	p.calls.Add(1)
	n := p.inFlight.Add(1)
	defer p.inFlight.Add(-1)
	for {
		m := p.maxInFlight.Load()
		if n <= m || p.maxInFlight.CompareAndSwap(m, n) {
			break
		}
	}
	if p.started != nil {
		select {
		case p.started <- struct{}{}:
		default:
		}
	}
	select {
	case <-time.After(p.delay):
	case <-ctx.Done():
		return processor.Result{}, ctx.Err()
	}
	return processor.Result{}, p.err
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func body(i int) string { return fmt.Sprintf(`{"job_id":"job-%d"}`, i) }

func start(t *testing.T, q Queue, j Jobs, p Processor, concurrency int) (cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := New(q, j, p, Config{Concurrency: concurrency, PollWait: time.Second, ShutdownTimeout: 2 * time.Second}, quiet())
	res := make(chan error, 1)
	go func() { res <- w.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel, res
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met in time")
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestProcessesAllMessagesWithinConcurrency(t *testing.T) {
	const total, concurrency = 30, 3
	var bodies []string
	for i := range total {
		bodies = append(bodies, body(i))
	}
	q, j, p := newFakeQueue(bodies...), &fakeJobs{}, &fakeProc{delay: 10 * time.Millisecond}
	cancel, result := start(t, q, j, p, concurrency)
	waitFor(t, func() bool { return q.deletedCount() == total })
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := j.completedCount(); got != total {
		t.Fatalf("completed %d jobs, want %d", got, total)
	}
	if got := p.maxInFlight.Load(); got > concurrency || got < 2 {
		t.Fatalf("max in-flight = %d, want between 2 and %d", got, concurrency)
	}
	if q.maxLimit > concurrency {
		t.Fatalf("requested batch of %d with concurrency %d", q.maxLimit, concurrency)
	}
}

func TestNotClaimableIsDeletedWithoutProcessing(t *testing.T) {
	for _, err := range []error{jobs.ErrNotClaimable, jobs.ErrNotFound} {
		q, p := newFakeQueue(body(1)), &fakeProc{}
		cancel, result := start(t, q, &fakeJobs{claimErr: err}, p, 2)
		waitFor(t, func() bool { return q.deletedCount() == 1 })
		cancel()
		<-result
		if p.calls.Load() != 0 {
			t.Fatalf("%v: processor ran %d times", err, p.calls.Load())
		}
	}
}

func TestMalformedBodyIsDeleted(t *testing.T) {
	q, p := newFakeQueue("not json", `{}`), &fakeProc{}
	cancel, result := start(t, q, &fakeJobs{}, p, 2)
	waitFor(t, func() bool { return q.deletedCount() == 2 })
	cancel()
	<-result
	if p.calls.Load() != 0 {
		t.Fatal("processor ran for malformed message")
	}
}

func TestProcessorErrorLeavesMessage(t *testing.T) {
	q, j, p := newFakeQueue(body(1)), &fakeJobs{}, &fakeProc{err: errors.New("boom")}
	cancel, result := start(t, q, j, p, 2)
	waitFor(t, func() bool { return p.calls.Load() == 1 })
	time.Sleep(20 * time.Millisecond)
	cancel()
	<-result
	if q.deletedCount() != 0 || j.completedCount() != 0 {
		t.Fatal("failed job must not be deleted or completed")
	}
}

func TestShutdownFinishesInFlightAndSkipsQueued(t *testing.T) {
	const concurrency = 2
	var bodies []string
	for i := range 6 {
		bodies = append(bodies, body(i))
	}
	q, j := newFakeQueue(bodies...), &fakeJobs{}
	p := &fakeProc{delay: 150 * time.Millisecond, started: make(chan struct{}, 8)}
	cancel, result := start(t, q, j, p, concurrency)
	<-p.started
	<-p.started
	cancel()
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if got := p.calls.Load(); got != concurrency {
		t.Fatalf("processor ran %d times, want only the %d in-flight jobs", got, concurrency)
	}
	if j.completedCount() != concurrency || q.deletedCount() != concurrency {
		t.Fatalf("in-flight jobs did not finish cleanly: completed=%d deleted=%d", j.completedCount(), q.deletedCount())
	}
}

func TestShutdownTimeoutCancelsInFlight(t *testing.T) {
	q, j, p := newFakeQueue(body(1)), &fakeJobs{}, &fakeProc{delay: time.Minute, started: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	w := New(q, j, p, Config{Concurrency: 1, PollWait: time.Second, ShutdownTimeout: 50 * time.Millisecond}, quiet())
	res := make(chan error, 1)
	go func() { res <- w.Run(ctx) }()
	<-p.started
	cancel()
	if err := <-res; err == nil {
		t.Fatal("expected shutdown timeout error")
	}
	if q.deletedCount() != 0 {
		t.Fatal("cancelled job must not be deleted")
	}
}
