// Package worker consumes job messages from a queue and runs them on a fixed pool of goroutines.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/metrics"
	"async-file-processor/internal/processor"
	"async-file-processor/internal/queue"
)

const (
	maxBatch       = 10
	retryBackoff   = time.Second
	receiveGrace   = 5 * time.Second
	releaseTimeout = 5 * time.Second

	backoffBase = 5 * time.Second
	backoffMax  = 5 * time.Minute

	defaultMaxAttempts       = 3
	defaultVisibilityTimeout = 60 * time.Second
	defaultHeartbeat         = 20 * time.Second
)

// backoff returns how long a message stays hidden after the given failed attempt:
// min(5s * 2^attempt, 5m).
func backoff(attempt int) time.Duration {
	if attempt < 0 || attempt >= 6 { // 5s * 2^6 already exceeds the cap
		return backoffMax
	}
	return min(backoffBase<<attempt, backoffMax)
}

// Queue is the subset of the message queue the worker uses.
type Queue interface {
	Receive(ctx context.Context, limit int, wait time.Duration) ([]queue.Msg, error)
	Delete(ctx context.Context, receipt string) error
	ChangeVisibility(ctx context.Context, receipt string, timeout time.Duration) error
}

// Jobs is the subset of the job repository the worker uses.
type Jobs interface {
	MarkProcessing(ctx context.Context, id string) (jobs.Job, error)
	Complete(ctx context.Context, id, reportKey, errorsKey string, total, valid, invalid int64) error
	Fail(ctx context.Context, id, msg string) error
}

// Processor handles the contents of one claimed job.
type Processor interface {
	Process(ctx context.Context, job jobs.Job) (processor.Result, error)
}

// Config tunes the pool.
type Config struct {
	Concurrency     int           // number of processing goroutines
	PollWait        time.Duration // long-poll duration per receive call
	ShutdownTimeout time.Duration // how long in-flight jobs may run after shutdown begins

	// MaxAttempts is the number of processing attempts before a job is failed for good. It must
	// equal the queue's maxReceiveCount so the final message is redriven to the dead-letter queue.
	MaxAttempts       int
	VisibilityTimeout time.Duration // visibility the heartbeat keeps extending while a job runs
	HeartbeatInterval time.Duration // how often the heartbeat extends visibility

	// Metrics receives job counters and gauges. Nil uses a private, unexposed set.
	Metrics *metrics.Metrics
}

// Worker receives messages and dispatches them to a bounded pool.
type Worker struct {
	queue  Queue
	jobs   Jobs
	proc   Processor
	cfg    Config
	logger *slog.Logger
	m      *metrics.Metrics
}

// New returns a Worker. Concurrency below 1 is treated as 1.
func New(q Queue, j Jobs, p Processor, cfg Config, logger *slog.Logger) *Worker {
	cfg.Concurrency = max(cfg.Concurrency, 1)
	if cfg.MaxAttempts < 1 {
		cfg.MaxAttempts = defaultMaxAttempts
	}
	if cfg.VisibilityTimeout <= 0 {
		cfg.VisibilityTimeout = defaultVisibilityTimeout
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeat
	}
	if cfg.Metrics == nil {
		cfg.Metrics = metrics.New()
	}
	return &Worker{queue: q, jobs: j, proc: p, cfg: cfg, logger: logger, m: cfg.Metrics}
}

// Run consumes messages until ctx is cancelled, then waits up to ShutdownTimeout for in-flight
// jobs. In-flight work runs on a context that ctx's cancellation does not reach.
//
// A receive call already in flight when ctx is cancelled is allowed to finish: abandoning it
// would let the queue hand a message to a dead request and hide it for the full visibility
// timeout. Messages received or queued after shutdown began are not processed; they are
// released immediately (visibility 0) so another worker can take them.
//
// A slot is reserved for every message before it is received, so at most Concurrency messages
// are held in memory (queued or running) and none waits long enough to lose its visibility.
func (w *Worker) Run(ctx context.Context) error {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	slots := make(chan struct{}, w.cfg.Concurrency)
	msgs := make(chan queue.Msg, w.cfg.Concurrency)

	var released atomic.Int64
	var wg sync.WaitGroup
	for id := 1; id <= w.cfg.Concurrency; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range msgs {
				if ctx.Err() == nil {
					w.handle(workCtx, id, m)
				} else {
					w.release(workCtx, m)
					released.Add(1)
				}
				<-slots
			}
		}()
	}

	w.receiveLoop(ctx, slots, msgs)
	close(msgs)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	w.logger.Info("worker draining in-flight jobs", "timeout", w.cfg.ShutdownTimeout.String())
	timer := time.NewTimer(w.cfg.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-done:
		w.logger.Info("unstarted messages released", "count", released.Load())
		return nil
	case <-timer.C:
		cancelWork()
		<-done
		w.logger.Info("unstarted messages released", "count", released.Load())
		return fmt.Errorf("shutdown timeout %s exceeded; in-flight jobs were cancelled", w.cfg.ShutdownTimeout)
	}
}

func (w *Worker) receiveLoop(ctx context.Context, slots chan struct{}, msgs chan<- queue.Msg) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Wait for one free slot, then claim any others that are free right now.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
			return
		}
		if ctx.Err() != nil { // both cases may be ready; never start a receive after shutdown
			<-slots
			return
		}
		n := 1
		for n < maxBatch {
			select {
			case slots <- struct{}{}:
				n++
				continue
			default:
			}
			break
		}

		// Shutdown must not cancel a receive in flight; the timeout is only a safety bound.
		recvCtx, cancelRecv := context.WithTimeout(context.WithoutCancel(ctx), w.cfg.PollWait+receiveGrace)
		batch, err := w.queue.Receive(recvCtx, n, w.cfg.PollWait)
		cancelRecv()
		for i := len(batch); i < n; i++ {
			<-slots
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("receive failed", "error", err)
			select {
			case <-time.After(retryBackoff):
			case <-ctx.Done():
				return
			}
			continue
		}
		for _, m := range batch {
			msgs <- m // never blocks: the channel capacity equals the slot count
		}
	}
}

type payload struct {
	JobID string `json:"job_id"`
}

func (w *Worker) handle(ctx context.Context, workerID int, m queue.Msg) {
	var p payload
	if err := json.Unmarshal([]byte(m.Body), &p); err != nil || p.JobID == "" {
		w.logger.Warn("discarding malformed message", "worker", workerID, "message_id", m.ID, "error", err)
		w.delete(ctx, workerID, m, "")
		return
	}
	log := w.logger.With("worker", workerID, "job_id", p.JobID, "message_id", m.ID)

	job, err := w.jobs.MarkProcessing(ctx, p.JobID)
	switch {
	case errors.Is(err, jobs.ErrNotFound):
		log.Warn("discarding message for unknown job")
		w.delete(ctx, workerID, m, p.JobID)
		return
	case errors.Is(err, jobs.ErrNotClaimable):
		log.Info("job already finished; skipping")
		w.delete(ctx, workerID, m, p.JobID)
		return
	case err != nil:
		log.Error("claim job failed; leaving message for redelivery", "error", err)
		return
	}

	log = log.With("attempt", job.Attempts)
	start := time.Now()
	log.Info("job started", "filename", job.Filename)

	w.m.WorkerBusy.Inc()
	stopBeat := w.heartbeat(ctx, log, m.Receipt)
	res, err := w.proc.Process(ctx, job)
	if err == nil {
		err = w.jobs.Complete(ctx, job.ID, res.ReportKey, res.ErrorsKey, res.Total, res.Valid, res.Invalid)
	}
	// The heartbeat must be gone before the message is deleted or its visibility is changed, so
	// it can never overwrite a backoff.
	stopBeat()
	w.m.WorkerBusy.Dec()
	elapsed := time.Since(start)
	duration := elapsed.String()
	if ctx.Err() == nil { // an attempt cut off by shutdown has no outcome to time
		w.m.JobDuration.Observe(elapsed.Seconds())
	}

	switch {
	case err == nil:
		log.Info("job completed", "duration", duration,
			"total_rows", res.Total, "valid_rows", res.Valid, "invalid_rows", res.Invalid)
		w.m.JobsTotal.WithLabelValues(metrics.StatusCompleted).Inc()
		w.m.RowsProcessed.Add(float64(res.Total))
		w.delete(ctx, workerID, m, p.JobID)
	case ctx.Err() != nil:
		// Shutdown is not a failure: leave the message to become visible again.
		log.Warn("job interrupted by shutdown; leaving message for redelivery", "error", err, "duration", duration)
	case errors.Is(err, processor.ErrInvalidInput):
		// Retrying cannot fix the input: record the failure and drop the message.
		if failErr := w.jobs.Fail(ctx, job.ID, err.Error()); failErr != nil {
			log.Error("mark job failed; leaving message for redelivery", "error", failErr)
			return
		}
		log.Warn("job failed: invalid input", "error", err, "duration", duration)
		w.m.JobsTotal.WithLabelValues(metrics.StatusFailed).Inc()
		w.delete(ctx, workerID, m, p.JobID)
	default:
		w.handleTransient(ctx, log, m, job, err, duration)
	}
}

// handleTransient schedules a retry with backoff, or fails the job once attempts are exhausted.
//
// The attempt number is the larger of the job's claim count and the message's receive count:
// a delivery can be lost before it is claimed (for example by a worker stopping mid-poll), and
// the queue's redrive policy counts that receive too. Without this the last delivery could look
// like an earlier attempt, leaving the job PROCESSING while its message moves to the DLQ.
func (w *Worker) handleTransient(ctx context.Context, log *slog.Logger, m queue.Msg, job jobs.Job, err error, duration string) {
	attempt := max(job.Attempts, m.ReceiveCount)
	if attempt >= w.cfg.MaxAttempts {
		msg := fmt.Sprintf("giving up after %d attempts: %v", attempt, err)
		if failErr := w.jobs.Fail(ctx, job.ID, msg); failErr != nil {
			log.Error("mark job failed; leaving message for redelivery", "error", failErr)
			return
		}
		log.Error("job failed: attempts exhausted", "receive_count", m.ReceiveCount, "error", err, "duration", duration)
		w.m.JobsTotal.WithLabelValues(metrics.StatusFailed).Inc()
		// Make the message visible right away without deleting it: the queue's redrive policy
		// moves it to the dead-letter queue on the next receive.
		if vErr := w.queue.ChangeVisibility(ctx, m.Receipt, 0); vErr != nil {
			log.Error("release message for redrive failed", "error", vErr)
		}
		return
	}
	delay := backoff(attempt)
	log.Warn("job attempt failed; retrying after backoff",
		"receive_count", m.ReceiveCount, "backoff", delay.String(), "error", err, "duration", duration)
	if vErr := w.queue.ChangeVisibility(ctx, m.Receipt, delay); vErr != nil {
		log.Error("set retry backoff failed", "error", vErr)
	}
}

// heartbeat extends the message's visibility every HeartbeatInterval until the returned stop
// function is called. Stop blocks until the goroutine has exited. Errors are logged only.
func (w *Worker) heartbeat(ctx context.Context, log *slog.Logger, receipt string) (stop func()) {
	beatCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(w.cfg.HeartbeatInterval)
		defer t.Stop()
		for {
			select {
			case <-beatCtx.Done():
				return
			case <-t.C:
				if err := w.queue.ChangeVisibility(beatCtx, receipt, w.cfg.VisibilityTimeout); err != nil {
					if beatCtx.Err() == nil {
						log.Warn("extend visibility failed", "error", err)
					}
					continue
				}
				log.Info("visibility extended", "timeout", w.cfg.VisibilityTimeout.String())
			}
		}
	}()
	return func() { cancel(); <-done }
}

// release makes an unstarted message visible again right away.
func (w *Worker) release(ctx context.Context, m queue.Msg) {
	ctx, cancel := context.WithTimeout(ctx, releaseTimeout)
	defer cancel()
	if err := w.queue.ChangeVisibility(ctx, m.Receipt, 0); err != nil {
		w.logger.Warn("release message failed", "message_id", m.ID, "error", err)
	}
}

func (w *Worker) delete(ctx context.Context, workerID int, m queue.Msg, jobID string) {
	if err := w.queue.Delete(ctx, m.Receipt); err != nil {
		w.logger.Error("delete message failed", "worker", workerID, "job_id", jobID, "message_id", m.ID, "error", err)
	}
}
