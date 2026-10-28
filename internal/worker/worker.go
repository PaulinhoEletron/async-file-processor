// Package worker consumes job messages from a queue and runs them on a fixed pool of goroutines.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/processor"
	"async-file-processor/internal/queue"
)

const (
	maxBatch     = 10
	retryBackoff = time.Second
)

// Queue is the subset of the message queue the worker uses.
type Queue interface {
	Receive(ctx context.Context, limit int, wait time.Duration) ([]queue.Msg, error)
	Delete(ctx context.Context, receipt string) error
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
}

// Worker receives messages and dispatches them to a bounded pool.
type Worker struct {
	queue  Queue
	jobs   Jobs
	proc   Processor
	cfg    Config
	logger *slog.Logger
}

// New returns a Worker. Concurrency below 1 is treated as 1.
func New(q Queue, j Jobs, p Processor, cfg Config, logger *slog.Logger) *Worker {
	cfg.Concurrency = max(cfg.Concurrency, 1)
	return &Worker{queue: q, jobs: j, proc: p, cfg: cfg, logger: logger}
}

// Run consumes messages until ctx is cancelled, then waits up to ShutdownTimeout for in-flight
// jobs. In-flight work runs on a context that ctx's cancellation does not reach; messages
// received but not yet started are left on the queue to become visible again.
//
// A slot is reserved for every message before it is received, so at most Concurrency messages
// are held in memory (queued or running) and none waits long enough to lose its visibility.
func (w *Worker) Run(ctx context.Context) error {
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	slots := make(chan struct{}, w.cfg.Concurrency)
	msgs := make(chan queue.Msg, w.cfg.Concurrency)

	var wg sync.WaitGroup
	for id := 1; id <= w.cfg.Concurrency; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for m := range msgs {
				if ctx.Err() == nil {
					w.handle(workCtx, id, m)
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
		return nil
	case <-timer.C:
		cancelWork()
		<-done
		return fmt.Errorf("shutdown timeout %s exceeded; in-flight jobs were cancelled", w.cfg.ShutdownTimeout)
	}
}

func (w *Worker) receiveLoop(ctx context.Context, slots chan struct{}, msgs chan<- queue.Msg) {
	for {
		// Wait for one free slot, then claim any others that are free right now.
		select {
		case slots <- struct{}{}:
		case <-ctx.Done():
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

		batch, err := w.queue.Receive(ctx, n, w.cfg.PollWait)
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

	start := time.Now()
	log.Info("job started", "filename", job.Filename)
	res, err := w.proc.Process(ctx, job)
	if errors.Is(err, processor.ErrInvalidInput) {
		// Retrying cannot fix the input: record the failure and drop the message.
		if failErr := w.jobs.Fail(ctx, job.ID, err.Error()); failErr != nil {
			log.Error("mark job failed; leaving message for redelivery", "error", failErr)
			return
		}
		log.Warn("job failed: invalid input", "error", err, "duration", time.Since(start).String())
		w.delete(ctx, workerID, m, p.JobID)
		return
	}
	if err == nil {
		err = w.jobs.Complete(ctx, job.ID, res.ReportKey, res.ErrorsKey, res.Total, res.Valid, res.Invalid)
	}
	if err != nil {
		log.Error("job failed; leaving message for redelivery", "error", err, "duration", time.Since(start).String())
		return
	}
	log.Info("job completed", "duration", time.Since(start).String(),
		"total_rows", res.Total, "valid_rows", res.Valid, "invalid_rows", res.Invalid)
	w.delete(ctx, workerID, m, p.JobID)
}

func (w *Worker) delete(ctx context.Context, workerID int, m queue.Msg, jobID string) {
	if err := w.queue.Delete(ctx, m.Receipt); err != nil {
		w.logger.Error("delete message failed", "worker", workerID, "job_id", jobID, "message_id", m.ID, "error", err)
	}
}
