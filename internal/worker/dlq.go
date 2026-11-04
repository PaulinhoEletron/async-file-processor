package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"async-file-processor/internal/metrics"
	"async-file-processor/internal/queue"
)

const dlqHandleTimeout = 30 * time.Second

// DLQQueue is the subset of the dead-letter queue the consumer uses.
type DLQQueue interface {
	Receive(ctx context.Context, limit int, wait time.Duration) ([]queue.Msg, error)
	Delete(ctx context.Context, receipt string) error
}

// DLQJobs is the subset of the job repository the consumer uses.
type DLQJobs interface {
	// FailIfActive fails a PENDING or PROCESSING job and reports whether it changed anything.
	FailIfActive(ctx context.Context, id, msg string) (bool, error)
}

// DLQConsumer drains the dead-letter queue. A message lands there when the jobs queue's redrive
// policy gives up on it, for example because a worker died on the final attempt and left the job
// PROCESSING. The consumer fails any such job that is still active and deletes the message.
type DLQConsumer struct {
	queue       DLQQueue
	jobs        DLQJobs
	m           *metrics.Metrics
	logger      *slog.Logger
	maxAttempts int
	pollWait    time.Duration
}

// NewDLQConsumer returns a consumer. maxAttempts is only used in the recorded failure message.
// A nil m uses a private set of metrics.
func NewDLQConsumer(q DLQQueue, j DLQJobs, m *metrics.Metrics, maxAttempts int, pollWait time.Duration, logger *slog.Logger) *DLQConsumer {
	if m == nil {
		m = metrics.New()
	}
	return &DLQConsumer{queue: q, jobs: j, m: m, logger: logger, maxAttempts: maxAttempts, pollWait: pollWait}
}

// Run polls until ctx is cancelled. Like the main receive loop, a receive already in flight is
// allowed to finish so a message is never handed to a dead request.
func (c *DLQConsumer) Run(ctx context.Context) {
	for ctx.Err() == nil {
		recvCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), c.pollWait+receiveGrace)
		batch, err := c.queue.Receive(recvCtx, maxBatch, c.pollWait)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			c.logger.Error("dead-letter receive failed", "error", err)
			select {
			case <-time.After(retryBackoff):
			case <-ctx.Done():
				return
			}
			continue
		}
		for _, msg := range batch {
			hctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), dlqHandleTimeout)
			c.handle(hctx, msg)
			cancel()
		}
	}
}

func (c *DLQConsumer) handle(ctx context.Context, m queue.Msg) {
	var p payload
	if err := json.Unmarshal([]byte(m.Body), &p); err != nil || p.JobID == "" {
		c.logger.Warn("discarding malformed dead-letter message", "message_id", m.ID, "error", err)
		c.delete(ctx, m, "")
		return
	}
	log := c.logger.With("job_id", p.JobID, "message_id", m.ID)

	msg := fmt.Sprintf("moved to dead-letter queue after %d receives", c.maxAttempts)
	changed, err := c.jobs.FailIfActive(ctx, p.JobID, msg)
	if err != nil {
		log.Error("fail dead-lettered job; leaving message for retry", "error", err)
		return
	}
	c.m.JobsDeadLettered.Inc()
	if changed {
		c.m.JobsTotal.WithLabelValues(metrics.StatusFailed).Inc()
	}
	log.Warn("consumed dead-lettered message", "job_marked_failed", changed)
	c.delete(ctx, m, p.JobID)
}

func (c *DLQConsumer) delete(ctx context.Context, m queue.Msg, jobID string) {
	if err := c.queue.Delete(ctx, m.Receipt); err != nil {
		c.logger.Error("delete dead-letter message failed", "job_id", jobID, "message_id", m.ID, "error", err)
	}
}
