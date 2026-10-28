package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"async-file-processor/internal/config"
	"async-file-processor/internal/jobs"
	"async-file-processor/internal/processor"
	"async-file-processor/internal/queue"
	"async-file-processor/internal/storage"
	"async-file-processor/internal/worker"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

// run expects the database schema to exist; the API applies migrations when it starts.
func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Bound startup so an unreachable dependency fails fast instead of hanging.
	startCtx, cancelStart := context.WithTimeout(ctx, 30*time.Second)
	defer cancelStart()

	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(startCtx); err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	jobQueue, err := queue.New(startCtx, queue.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, QueueURL: cfg.SQSQueueURL})
	if err != nil {
		return err
	}

	store, err := storage.New(startCtx, storage.Config{Region: cfg.AWSRegion, Endpoint: cfg.S3Endpoint, Bucket: cfg.S3Bucket})
	if err != nil {
		return err
	}

	repo := jobs.NewRepo(pool)
	w := worker.New(jobQueue, repo, processor.New(store, repo, logger), worker.Config{
		Concurrency:     cfg.WorkerConcurrency,
		PollWait:        queue.MaxWait,
		ShutdownTimeout: cfg.ShutdownTimeout,
	}, logger)
	logger.Info("worker started", "concurrency", cfg.WorkerConcurrency)
	if err := w.Run(ctx); err != nil {
		return err
	}
	logger.Info("worker stopped")
	return nil
}
