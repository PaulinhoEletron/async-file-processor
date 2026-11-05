package worker

import (
	"context"
	"log/slog"
	"time"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/metrics"
	"async-file-processor/internal/processor"
)

// sweepBatch bounds the work of one sweep; a backlog is cleared over several sweeps.
const sweepBatch = 200

// RetentionJobs is the subset of the job repository the sweeper uses.
type RetentionJobs interface {
	ListExpired(ctx context.Context, cutoff time.Time, limit int) ([]jobs.Job, error)
	DeleteFinished(ctx context.Context, id string) (bool, error)
}

// RetentionStore deletes objects. Deleting a missing key must not be an error.
type RetentionStore interface {
	Delete(ctx context.Context, key string) error
}

// Sweeper deletes finished jobs, and their stored objects, once they are older than the
// retention period, so a long-running demo cannot fill the disk. Several workers may sweep
// at the same time: deleting objects and rows is idempotent, and a job's row is removed only
// after its objects, so an interrupted sweep is simply retried.
type Sweeper struct {
	jobs      RetentionJobs
	store     RetentionStore
	retention time.Duration
	interval  time.Duration
	m         *metrics.Metrics
	logger    *slog.Logger
	now       func() time.Time
}

// NewSweeper returns a sweeper. m may be nil.
func NewSweeper(j RetentionJobs, s RetentionStore, retention, interval time.Duration, m *metrics.Metrics, logger *slog.Logger) *Sweeper {
	return &Sweeper{jobs: j, store: s, retention: retention, interval: interval, m: m, logger: logger, now: time.Now}
}

// Run sweeps once immediately and then every interval until ctx ends.
func (s *Sweeper) Run(ctx context.Context) {
	s.logger.Info("retention sweeper started", "retention", s.retention.String(), "interval", s.interval.String())
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		s.Sweep(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Sweep deletes one batch of expired jobs and returns how many rows it removed.
func (s *Sweeper) Sweep(ctx context.Context) int {
	expired, err := s.jobs.ListExpired(ctx, s.now().Add(-s.retention), sweepBatch)
	if err != nil {
		if ctx.Err() == nil {
			s.logger.Error("retention: list expired jobs", "error", err)
		}
		return 0
	}
	deleted, failed := 0, 0
	for _, j := range expired {
		if ctx.Err() != nil {
			break
		}
		if err := s.deleteObjects(ctx, j); err != nil {
			s.logger.Error("retention: delete objects", "job_id", j.ID, "error", err)
			failed++
			continue
		}
		ok, err := s.jobs.DeleteFinished(ctx, j.ID)
		if err != nil {
			s.logger.Error("retention: delete job", "job_id", j.ID, "error", err)
			failed++
			continue
		}
		if ok {
			deleted++
		}
	}
	if s.m != nil {
		s.m.JobsExpired.Add(float64(deleted))
	}
	if len(expired) > 0 {
		s.logger.Info("retention sweep", "candidates", len(expired), "deleted", deleted, "failed", failed)
	}
	return deleted
}

// deleteObjects removes every object a job can own. The standard keys are deleted even when
// the row does not record them, because an attempt that was cut short may have written some.
func (s *Sweeper) deleteObjects(ctx context.Context, j jobs.Job) error {
	keys := map[string]struct{}{
		j.InputKey:                 {},
		processor.SummaryKey(j.ID): {},
		processor.ErrorsKey(j.ID):  {},
	}
	for _, k := range []*string{j.ReportKey, j.ErrorsKey} {
		if k != nil {
			keys[*k] = struct{}{}
		}
	}
	for k := range keys {
		if k == "" {
			continue
		}
		if err := s.store.Delete(ctx, k); err != nil {
			return err
		}
	}
	return nil
}
