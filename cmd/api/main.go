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
	"async-file-processor/internal/httpapi"
	"async-file-processor/internal/jobs"
	"async-file-processor/internal/metrics"
	"async-file-processor/internal/queue"
	"async-file-processor/internal/storage"
	"async-file-processor/migrations"
	"async-file-processor/web"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	probe := flag.Bool("healthcheck", false, "probe the local /readyz endpoint and exit 0 if ready (for container health checks)")
	flag.Parse()
	if *probe {
		os.Exit(runHealthcheck())
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("api stopped", "error", err)
		os.Exit(1)
	}
}

func runHealthcheck() int {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	if err := healthcheck.Probe(healthcheck.URL(cfg.APIAddr, "/readyz"), 3*time.Second); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	return 0
}

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
	if err := migrations.Run(startCtx, pool); err != nil {
		return fmt.Errorf("run migrations: %w", err)
	}
	logger.Info("migrations applied")

	store, err := storage.New(startCtx, storage.Config{Region: cfg.AWSRegion, Endpoint: cfg.S3Endpoint, Bucket: cfg.S3Bucket})
	if err != nil {
		return err
	}
	if err := store.EnsureBucket(startCtx); err != nil {
		return err
	}
	logger.Info("bucket ready", "bucket", cfg.S3Bucket)

	jobQueue, err := queue.New(startCtx, queue.Config{Region: cfg.AWSRegion, Endpoint: cfg.SQSEndpoint, QueueURL: cfg.SQSQueueURL})
	if err != nil {
		return err
	}

	repo := jobs.NewRepo(pool)
	m := metrics.New()
	// No Read/WriteTimeout: large uploads stream for as long as the client keeps sending; the
	// handler enforces an idle and an overall deadline on the upload body itself.
	server := &http.Server{
		Addr: cfg.APIAddr,
		Handler: httpapi.NewHandler(httpapi.Deps{
			Store: store, Jobs: repo, Queue: jobQueue, Logger: logger, MaxUploadBytes: cfg.MaxUploadBytes, UI: web.FS(),
			Metrics:          m,
			UploadRatePerMin: cfg.UploadRatePerMin, UploadRateBurst: cfg.UploadRateBurst,
			TrustCFConnectingIP: cfg.TrustCFConnectingIP, CookieSecure: cfg.CookieSecure,
			UploadIdleTimeout: cfg.UploadIdleTimeout, UploadMaxDuration: cfg.UploadMaxDuration,
			MaxConcurrentUploads: cfg.MaxConcurrentUploads,
			MaxUnfinishedJobs:    cfg.MaxUnfinishedJobs, MaxUnfinishedPerClient: cfg.MaxUnfinishedPerClient,
			Checks: map[string]httpapi.Checker{"db": pool.Ping, "s3": store.Check, "sqs": jobQueue.Check},
		}),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	// Metrics are served on their own listener, which is not published outside the container
	// network, so the public API never exposes them.
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", m.Handler())
	metricsServer := &http.Server{Addr: cfg.APIMetricsAddr, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}

	serverErrors := make(chan error, 2)
	go func() {
		logger.Info("api listening", "addr", cfg.APIAddr)
		serverErrors <- server.ListenAndServe()
	}()
	go func() {
		logger.Info("metrics listening", "addr", cfg.APIMetricsAddr)
		serverErrors <- metricsServer.ListenAndServe()
	}()
	var runErr error
	select {
	case runErr = <-serverErrors:
	case <-ctx.Done():
		stop()
	}
	logger.Info("api shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()
	for _, srv := range []*http.Server{server, metricsServer} {
		if err := srv.Shutdown(shutdownCtx); err != nil {
			_ = srv.Close()
			runErr = errors.Join(runErr, err)
		}
	}
	// Both servers have stopped; collect what they reported.
	for range 2 {
		select {
		case err := <-serverErrors:
			if !errors.Is(err, http.ErrServerClosed) {
				runErr = errors.Join(runErr, err)
			}
		default:
		}
	}
	if runErr != nil && !errors.Is(runErr, http.ErrServerClosed) {
		return runErr
	}
	logger.Info("api stopped")
	return nil
}
