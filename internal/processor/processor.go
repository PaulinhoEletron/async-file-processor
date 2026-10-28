// Package processor turns an uploaded file into per-row results.
package processor

import (
	"context"
	"fmt"
	"time"

	"async-file-processor/internal/jobs"
)

// Result is the outcome of processing one job.
type Result struct {
	ReportKey string
	ErrorsKey string
	Total     int64
	Valid     int64
	Invalid   int64
}

// Stub simulates processing by waiting for Delay. It reads no input and reports zero rows.
type Stub struct {
	Delay time.Duration
}

// Process waits for the configured delay or until ctx is done.
func (s Stub) Process(ctx context.Context, _ jobs.Job) (Result, error) {
	t := time.NewTimer(s.Delay)
	defer t.Stop()
	select {
	case <-t.C:
		return Result{}, nil
	case <-ctx.Done():
		return Result{}, fmt.Errorf("processing interrupted: %w", ctx.Err())
	}
}
