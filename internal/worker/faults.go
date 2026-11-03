package worker

import (
	"context"
	"errors"
	"math/rand/v2"

	"async-file-processor/internal/jobs"
	"async-file-processor/internal/processor"
)

// ErrInjected is the transient error returned by FaultyProcessor.
var ErrInjected = errors.New("injected transient failure")

// FaultyProcessor wraps a Processor and fails a fraction of calls before processing.
// It exists to demonstrate retries in development and must not be enabled in production.
type FaultyProcessor struct {
	Next Processor
	Rate float64 // probability of failure per call, 0..1
}

// Process fails with ErrInjected with probability Rate, otherwise delegates to Next.
func (f FaultyProcessor) Process(ctx context.Context, job jobs.Job) (processor.Result, error) {
	if rand.Float64() < f.Rate {
		return processor.Result{}, ErrInjected
	}
	return f.Next.Process(ctx, job)
}
