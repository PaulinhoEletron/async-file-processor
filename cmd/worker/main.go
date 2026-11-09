package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"async-file-processor/internal/config"
	"async-file-processor/internal/healthcheck"
	"async-file-processor/internal/jobs"
	"async-file-processor/internal/metrics"
	"async-file-processor/internal/processor"
	"async-file-processor/internal/queue"
	"async-file-processor/internal/storage"
	"async-file-processor/internal/worker"

	"github.com/jackc/pgx/v5/pgxpool"
)

// heartbeatInterval is how often a running job extends its message's visibility.
const heartbeatInterval = 20 * time.Second

func main() {
	probe := flag.Bool("healthcheck", false, "probe the local /healthz endpoint and exit 0 if healthy (for container health checks)")
	flag.Parse()
	if *probe {
		os.Exit(runHealthcheck())
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("worker stopped", "error", err)
		os.Exit(1)
	}
}

func runHealthcheck() int {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := healthcheck.Probe(healthcheck.URL(cfg.MetricsAddr, "/healthz"), 3*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
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

	warnOnAttemptMismatch(startCtx, logger, jobQueue, cfg.MaxAttempts)

	m := metrics.New()
	repo := jobs.NewRepo(pool)
	var proc worker.Processor = processor.New(store, repo, logger,
		processor.WithMaxErrorLines(cfg.MaxErrorLines), processor.WithMaxRecordBytes(cfg.MaxRecordBytes))
	if cfg.FailRate > 0 {
		logger.Warn("fault injection enabled; for development only", "fail_rate", cfg.FailRate)
		proc = worker.FaultyProcessor{Next: proc, Rate: cfg.FailRate}
	}
	w := worker.New(jobQueue, repo, proc, worker.Config{
		Concurrency:     cfg.WorkerConcurrency,
		PollWait:        queue.MaxWait,
		ShutdownTimeout: cfg.ShutdownTimeout,
		MaxAttempts:     cfg.MaxAttempts,
		// Matches the jobs queue's defaultVisibilityTimeout in deploy/elasticmq.conf.
		VisibilityTimeout: 60 * time.Second,
		HeartbeatInterval: heartbeatInterval,
		Metrics:           m,
	}, logger)

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler())
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	metricsServer := &http.Server{Addr: cfg.MetricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	metricsErrors := make(chan error, 1)
	go func() {
		logger.Info("metrics listening", "addr", cfg.MetricsAddr)
		metricsErrors <- metricsServer.ListenAndServe()
	}()
	// The metrics server stays up while in-flight jobs drain.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			_ = metricsServer.Close()
		}
	}()

	// runCtx also ends if the metrics server cannot run, so a port clash is not silent.
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()
	var metricsErr error
	metricsStopped := make(chan struct{})
	go func() {
		defer close(metricsStopped)
		if err := <-metricsErrors; err != nil && !errors.Is(err, http.ErrServerClosed) {
			metricsErr = err
			cancelRun()
		}
	}()

	var dlqDone chan struct{}
	if cfg.SQSDLQURL != "" {
		dlq, err := queue.New(startCtx, queue.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, QueueURL: cfg.SQSDLQURL})
		if err != nil {
			return err
		}
		consumer := worker.NewDLQConsumer(dlq, repo, m, cfg.MaxAttempts, queue.MaxWait, logger)
		dlqDone = make(chan struct{})
		go func() { defer close(dlqDone); consumer.Run(runCtx) }()
	} else {
		logger.Warn("SQS_DLQ_URL is empty; dead-lettered jobs will not be failed automatically")
	}

	var sweepDone chan struct{}
	if cfg.Retention > 0 {
		sweeper := worker.NewSweeper(repo, store, cfg.Retention, cfg.RetentionInterval, m, logger)
		sweepDone = make(chan struct{})
		go func() { defer close(sweepDone); sweeper.Run(runCtx) }()
	} else {
		logger.Info("RETENTION is 0; finished jobs are kept forever")
	}

	logger.Info("worker started", "concurrency", cfg.WorkerConcurrency)
	err = w.Run(runCtx)
	cancelRun()
	if dlqDone != nil {
		<-dlqDone
	}
	if sweepDone != nil {
		<-sweepDone
	}
	if err != nil {
		return err
	}
	select {
	case <-metricsStopped:
		if metricsErr != nil {
			return fmt.Errorf("metrics server: %w", metricsErr)
		}
	default:
	}
	logger.Info("worker stopped")
	return nil
}

// warnOnAttemptMismatch logs a warning when MAX_ATTEMPTS differs from the jobs queue's
// maxReceiveCount, since exhausted jobs would then not reach the dead-letter queue as intended.
func warnOnAttemptMismatch(ctx context.Context, logger *slog.Logger, q *queue.Queue, maxAttempts int) {
	n, err := q.MaxReceiveCount(ctx)
	switch {
	case err != nil:
		logger.Warn("could not read queue redrive policy", "error", err)
	case n != maxAttempts:
		logger.Warn("MAX_ATTEMPTS does not match the queue's maxReceiveCount",
			"max_attempts", maxAttempts, "max_receive_count", n)
	}
}
