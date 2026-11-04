package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"async-file-processor/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

type fakeDLQJobs struct {
	mu      sync.Mutex
	active  map[string]bool // job id -> still PROCESSING
	failed  map[string]string
	failErr error
}

func (j *fakeDLQJobs) FailIfActive(_ context.Context, id, msg string) (bool, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.failErr != nil {
		return false, j.failErr
	}
	if !j.active[id] {
		return false, nil
	}
	j.active[id] = false
	if j.failed == nil {
		j.failed = map[string]string{}
	}
	j.failed[id] = msg
	return true, nil
}

func (j *fakeDLQJobs) failedMsg(id string) (string, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	m, ok := j.failed[id]
	return m, ok
}

func runDLQ(t *testing.T, q DLQQueue, j DLQJobs, m *metrics.Metrics) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	c := NewDLQConsumer(q, j, m, 3, time.Second, quiet())
	go func() { c.Run(ctx); close(done) }()
	stop = func() { cancel(); <-done }
	t.Cleanup(stop)
	return stop
}

func TestDLQConsumerFailsStuckJobAndDeletes(t *testing.T) {
	q := newFakeQueue(`{"job_id":"stuck"}`, `{"job_id":"done"}`, `not json`, `{"job_id":""}`)
	j := &fakeDLQJobs{active: map[string]bool{"stuck": true}}
	m := metrics.New()
	stop := runDLQ(t, q, j, m)
	waitFor(t, func() bool { return q.deletedCount() == 4 })
	stop()

	msg, ok := j.failedMsg("stuck")
	if !ok || msg != "moved to dead-letter queue after 3 receives" {
		t.Fatalf("stuck job failure = %q, %v", msg, ok)
	}
	if _, ok := j.failedMsg("done"); ok {
		t.Fatal("an already finished job must not be changed")
	}
	if got := testutil.ToFloat64(m.JobsDeadLettered); got != 2 {
		t.Fatalf("jobs_dead_lettered_total = %v, want 2 (malformed messages do not count)", got)
	}
	if got := testutil.ToFloat64(m.JobsTotal.WithLabelValues(metrics.StatusFailed)); got != 1 {
		t.Fatalf("jobs_total{failed} = %v, want 1 (only the job that changed)", got)
	}
}

func TestDLQConsumerKeepsMessageWhenDatabaseFails(t *testing.T) {
	q := newFakeQueue(`{"job_id":"x"}`)
	j := &fakeDLQJobs{failErr: errors.New("db down")}
	stop := runDLQ(t, q, j, nil)
	waitFor(t, func() bool {
		q.mu.Lock()
		defer q.mu.Unlock()
		return q.delivered == 1
	})
	time.Sleep(50 * time.Millisecond)
	stop()
	if q.deletedCount() != 0 {
		t.Fatal("message must stay on the queue when the job could not be updated")
	}
}
