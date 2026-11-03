package worker

import (
	"context"
	"errors"
	"testing"

	"async-file-processor/internal/jobs"
)

func TestFaultyProcessorRates(t *testing.T) {
	for range 200 {
		p := &fakeProc{}
		if _, err := (FaultyProcessor{Next: p, Rate: 0}).Process(context.Background(), jobs.Job{}); err != nil {
			t.Fatalf("rate 0 failed: %v", err)
		}
		if p.calls.Load() != 1 {
			t.Fatal("rate 0 must delegate")
		}
		p = &fakeProc{}
		_, err := (FaultyProcessor{Next: p, Rate: 1}).Process(context.Background(), jobs.Job{})
		if !errors.Is(err, ErrInjected) || p.calls.Load() != 0 {
			t.Fatalf("rate 1: err=%v calls=%d", err, p.calls.Load())
		}
	}
}
