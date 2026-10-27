package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"async-file-processor/internal/config"
)

// The queue receive loop and worker pool are implemented in step 4.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	logger.Info("worker skeleton started; queue processing is not implemented", "concurrency", cfg.WorkerConcurrency)
	<-ctx.Done()
	logger.Info("worker stopped")
}
