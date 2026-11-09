package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/metrics"
	"async-file-processor/internal/processor"
	"async-file-processor/internal/queue"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeQueue struct {
	mu        sync.Mutex
	pending   []queue.Msg
	deleted   map[string]bool
	maxLimit  int
	delivered int
	events    []string // "delete" and "vis:<duration>" in call order
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
	q.events = append(q.events, "delete")
	return nil
}

func (q *fakeQueue) ChangeVisibility(_ context.Context, _ string, timeout time.Duration) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.events = append(q.events, "vis:"+timeout.String())
	return nil
}

func (q *fakeQueue) snapshot() []string {
	q.mu.Lock()
	defer q.mu.Unlock()
	return append([]string(nil), q.events...)
}

func (q *fakeQueue) deletedCount() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.deleted)
}

type fakeJobs struct {
	claimErr  error
	attempts  int // attempt number reported by MarkProcessing; 0 means 1
	mu        sync.Mutex
	completed map[string]bool
	failed    map[string]string
}

func (j *fakeJobs) Fail(_ context.Context, id, msg string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failed == nil {
		j.failed = map[string]string{}
	}
	j.failed[id] = msg
	return nil
}

func (j *fakeJobs) MarkProcessing(_ context.Context, id string) (jobs.Job, error) {
	if j.claimErr != nil {
		return jobs.Job{}, j.claimErr
	}
	return jobs.Job{ID: id, Attempts: max(j.attempts, 1)}, nil
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

func (j *fakeJobs) failedMsg(id string) (string, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	m, ok := j.failed[id]
	return m, ok
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

func startCfg(t *testing.T, q Queue, j Jobs, p Processor, cfg Config) (cancel context.CancelFunc, result <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	w := New(q, j, p, cfg, quiet())
	res := make(chan error, 1)
	go func() { res <- w.Run(ctx) }()
	t.Cleanup(cancel)
	return cancel, res
}

func TestBackoff(t *testing.T) {
	for attempt, want := range map[int]time.Duration{
		1: 10 * time.Second, 2: 20 * time.Second, 3: 40 * time.Second, 5: 160 * time.Second,
		6: 5 * time.Minute, 40: 5 * time.Minute, 100: 5 * time.Minute,
	} {
		if got := backoff(attempt); got != want {
			t.Errorf("backoff(%d) = %v, want %v", attempt, got, want)
		}
	}
}

func TestTransientErrorBacksOffWithoutDeleteOrFail(t *testing.T) {
	q, j, p := newFakeQueue(body(1)), &fakeJobs{attempts: 1}, &fakeProc{err: errors.New("boom")}
	cancel, result := start(t, q, j, p, 2)
	waitFor(t, func() bool { return len(q.snapshot()) == 1 })
	cancel()
	<-result
	if ev := q.snapshot(); len(ev) != 1 || ev[0] != "vis:10s" {
		t.Fatalf("events = %v, want [vis:10s]", ev)
	}
	if _, failed := j.failedMsg("job-1"); failed || q.deletedCount() != 0 || j.completedCount() != 0 {
		t.Fatal("transient failure must not fail, delete or complete the job")
	}
}

func TestFinalAttemptFailsJobAndReleasesMessage(t *testing.T) {
	q, j, p := newFakeQueue(body(1)), &fakeJobs{attempts: 3}, &fakeProc{err: errors.New("boom")}
	cancel, result := startCfg(t, q, j, p, Config{Concurrency: 1, PollWait: time.Second, ShutdownTimeout: time.Second, MaxAttempts: 3})
	waitFor(t, func() bool { return len(q.snapshot()) == 1 })
	cancel()
	<-result
	if ev := q.snapshot(); len(ev) != 1 || ev[0] != "vis:0s" {
		t.Fatalf("events = %v, want [vis:0s]", ev)
	}
	msg, failed := j.failedMsg("job-1")
	if !failed || msg != "processing failed after 3 attempts" {
		t.Fatalf("failure message = %q (failed=%v)", msg, failed)
	}
}

func TestFinalDeliveryUsesReceiveCount(t *testing.T) {
	// The job was claimed twice, but the queue has delivered the message three times (one
	// delivery was lost before it was claimed). This is the last delivery before redrive.
	q, j, p := newFakeQueue(body(1)), &fakeJobs{attempts: 2}, &fakeProc{err: errors.New("boom")}
	q.pending[0].ReceiveCount = 3
	cancel, result := startCfg(t, q, j, p, Config{Concurrency: 1, PollWait: time.Second, ShutdownTimeout: time.Second, MaxAttempts: 3})
	waitFor(t, func() bool { return len(q.snapshot()) == 1 })
	cancel()
	<-result
	if ev := q.snapshot(); len(ev) != 1 || ev[0] != "vis:0s" {
		t.Fatalf("events = %v, want [vis:0s]", ev)
	}
	if msg, failed := j.failedMsg("job-1"); !failed || msg != "processing failed after 3 attempts" {
		t.Fatalf("failure message = %q (failed=%v)", msg, failed)
	}
}

func TestSuccessDeletesMessage(t *testing.T) {
	q, j, p := newFakeQueue(body(1)), &fakeJobs{}, &fakeProc{}
	cancel, result := start(t, q, j, p, 1)
	waitFor(t, func() bool { return q.deletedCount() == 1 })
	cancel()
	<-result
	if ev := q.snapshot(); len(ev) != 1 || ev[0] != "delete" {
		t.Fatalf("events = %v, want [delete]", ev)
	}
}

func TestHeartbeatRunsDuringProcessAndStopsBeforeDelete(t *testing.T) {
	q, j, p := newFakeQueue(body(1)), &fakeJobs{}, &fakeProc{delay: 120 * time.Millisecond}
	cancel, result := startCfg(t, q, j, p, Config{
		Concurrency: 1, PollWait: time.Second, ShutdownTimeout: time.Second,
		VisibilityTimeout: 42 * time.Second, HeartbeatInterval: 10 * time.Millisecond,
	})
	waitFor(t, func() bool { return q.deletedCount() == 1 })
	time.Sleep(50 * time.Millisecond) // a leaked heartbeat would show up after the delete
	cancel()
	<-result
	ev := q.snapshot()
	if len(ev) < 3 || ev[len(ev)-1] != "delete" {
		t.Fatalf("events = %v, want several heartbeats then delete last", ev)
	}
	for _, e := range ev[:len(ev)-1] {
		if e != "vis:42s" {
			t.Fatalf("unexpected event %q in %v", e, ev)
		}
	}
}

func TestShutdownCancelDoesNotFailJob(t *testing.T) {
	q, j, p := newFakeQueue(body(1)), &fakeJobs{attempts: 3}, &fakeProc{delay: time.Minute, started: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	w := New(q, j, p, Config{Concurrency: 1, PollWait: time.Second, ShutdownTimeout: 50 * time.Millisecond, MaxAttempts: 3}, quiet())
	res := make(chan error, 1)
	go func() { res <- w.Run(ctx) }()
	<-p.started
	cancel()
	<-res
	if _, failed := j.failedMsg("job-1"); failed {
		t.Fatal("shutdown cancellation must not mark the job FAILED")
	}
	if ev := q.snapshot(); len(ev) != 0 {
		t.Fatalf("events = %v, want none", ev)
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
	if got := q.snapshot(); len(got) != concurrency {
		t.Fatalf("events = %v, want only the %d deletes of in-flight jobs", got, concurrency)
	}
}

// blockingQueue holds Receive open until unblock is closed, like a server-side long poll.
type blockingQueue struct {
	*fakeQueue
	entered      chan struct{}
	unblock      chan struct{}
	ctxCancelled atomic.Bool
}

func (q *blockingQueue) Receive(ctx context.Context, _ int, _ time.Duration) ([]queue.Msg, error) {
	close(q.entered)
	<-q.unblock
	q.ctxCancelled.Store(ctx.Err() != nil)
	return []queue.Msg{{ID: "late", Body: body(9), Receipt: "late"}}, nil
}

func TestShutdownLetsReceiveFinishAndReleasesItsMessages(t *testing.T) {
	q := &blockingQueue{fakeQueue: newFakeQueue(), entered: make(chan struct{}), unblock: make(chan struct{})}
	j, p := &fakeJobs{}, &fakeProc{}
	cancel, result := start(t, q, j, p, 2)
	<-q.entered
	cancel()
	select {
	case err := <-result:
		t.Fatalf("Run returned (%v) while a receive was still in flight", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(q.unblock)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if q.ctxCancelled.Load() {
		t.Fatal("shutdown cancelled the in-flight receive")
	}
	if ev := q.snapshot(); len(ev) != 1 || ev[0] != "vis:0s" {
		t.Fatalf("events = %v, want [vis:0s]", ev)
	}
	if p.calls.Load() != 0 || j.completedCount() != 0 {
		t.Fatal("a message received after shutdown began must not be processed")
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

func TestInvalidInputFailsJobAndDeletesMessage(t *testing.T) {
	q, j := newFakeQueue(body(1)), &fakeJobs{}
	p := &fakeProc{err: fmt.Errorf("%w: invalid header", processor.ErrInvalidInput)}
	cancel, result := start(t, q, j, p, 2)
	waitFor(t, func() bool { return q.deletedCount() == 1 })
	cancel()
	<-result
	j.mu.Lock()
	defer j.mu.Unlock()
	if msg := j.failed["job-1"]; !strings.Contains(msg, "invalid header") {
		t.Fatalf("failure message = %q", msg)
	}
	if len(j.completed) != 0 {
		t.Fatal("invalid input must not complete the job")
	}
}

// shutdownOnReceive cancels the worker's context while delivering a batch, so every message in
// it is received but not started when shutdown begins.
type shutdownOnReceive struct {
	*fakeQueue
	cancel context.CancelFunc
}

func (q *shutdownOnReceive) Receive(ctx context.Context, limit int, wait time.Duration) ([]queue.Msg, error) {
	batch, err := q.fakeQueue.Receive(ctx, limit, wait)
	if len(batch) > 0 {
		q.cancel()
	}
	return batch, err
}

func TestUnstartedMessagesAreReleasedAtShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	q := &shutdownOnReceive{fakeQueue: newFakeQueue(body(1), body(2), body(3)), cancel: cancel}
	j, p := &fakeJobs{}, &fakeProc{}
	w := New(q, j, p, Config{Concurrency: 3, PollWait: time.Second, ShutdownTimeout: time.Second}, quiet())
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	ev := q.snapshot()
	if len(ev) != 3 || ev[0] != "vis:0s" || ev[1] != "vis:0s" || ev[2] != "vis:0s" {
		t.Fatalf("events = %v, want three vis:0s", ev)
	}
	if p.calls.Load() != 0 || q.deletedCount() != 0 {
		t.Fatal("unstarted messages must be neither processed nor deleted")
	}
}

func TestMetricsTrackJobOutcomes(t *testing.T) {
	m := metrics.New()
	q := newFakeQueue(body(1), body(2))
	j := &fakeJobs{}
	p := &fakeProc{delay: 80 * time.Millisecond, started: make(chan struct{}, 4)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := New(q, j, p, Config{Concurrency: 2, PollWait: time.Second, ShutdownTimeout: 2 * time.Second, Metrics: m}, quiet())
	res := make(chan error, 1)
	go func() { res <- w.Run(ctx) }()

	<-p.started
	waitFor(t, func() bool { return testutil.ToFloat64(m.WorkerBusy) >= 1 })
	waitFor(t, func() bool { return q.deletedCount() == 2 })
	waitFor(t, func() bool { return testutil.ToFloat64(m.WorkerBusy) == 0 })
	cancel()
	if err := <-res; err != nil {
		t.Fatal(err)
	}
	if got := testutil.ToFloat64(m.JobsTotal.WithLabelValues(metrics.StatusCompleted)); got != 2 {
		t.Fatalf("jobs_total{completed} = %v, want 2", got)
	}
	if got := testutil.CollectAndCount(m.JobDuration); got != 1 {
		t.Fatalf("job_duration_seconds series = %d", got)
	}
}

func TestMetricsCountFailedJobs(t *testing.T) {
	m := metrics.New()
	// Permanent failure.
	q := newFakeQueue(body(1))
	p := &fakeProc{err: fmt.Errorf("bad header: %w", processor.ErrInvalidInput)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := New(q, &fakeJobs{}, p, Config{Concurrency: 1, PollWait: time.Second, ShutdownTimeout: 2 * time.Second, Metrics: m}, quiet())
	res := make(chan error, 1)
	go func() { res <- w.Run(ctx) }()
	waitFor(t, func() bool { return q.deletedCount() == 1 })
	cancel()
	<-res
	if got := testutil.ToFloat64(m.JobsTotal.WithLabelValues(metrics.StatusFailed)); got != 1 {
		t.Fatalf("jobs_total{failed} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.JobsTotal.WithLabelValues(metrics.StatusCompleted)); got != 0 {
		t.Fatalf("jobs_total{completed} = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.WorkerBusy); got != 0 {
		t.Fatalf("worker_busy = %v, want 0", got)
	}
}
