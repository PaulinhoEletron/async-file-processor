package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	for _, key := range []string{
		"API_ADDR", "SHUTDOWN_TIMEOUT", "DATABASE_URL", "AWS_REGION",
		"S3_ENDPOINT", "S3_BUCKET", "SQS_ENDPOINT", "SQS_QUEUE_URL",
		"WORKER_CONCURRENCY", "MAX_UPLOAD_BYTES", "MAX_ATTEMPTS", "FAIL_RATE",
		"UPLOAD_RATE_PER_MIN", "UPLOAD_RATE_BURST", "RETENTION", "RETENTION_INTERVAL",
		"TRUST_CF_CONNECTING_IP", "COOKIE_SECURE", "UPLOAD_IDLE_TIMEOUT", "UPLOAD_MAX_DURATION",
		"MAX_CONCURRENT_UPLOADS", "MAX_UNFINISHED_JOBS", "MAX_UNFINISHED_PER_CLIENT",
		"MAX_ERROR_LINES", "MAX_RECORD_BYTES", "API_METRICS_ADDR",
	} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIAddr != ":8080" || cfg.WorkerConcurrency != 4 || cfg.S3Bucket != "uploads" {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
	if cfg.FailRate != 0 {
		t.Fatalf("FAIL_RATE default = %v, want 0", cfg.FailRate)
	}
	if cfg.MetricsAddr != ":9091" || !strings.HasSuffix(cfg.SQSDLQURL, "/jobs-dlq") {
		t.Fatalf("unexpected metrics/DLQ defaults: %+v", cfg)
	}
	if cfg.UploadRatePerMin != 10 || cfg.UploadRateBurst != 5 || cfg.Retention != 24*time.Hour || cfg.RetentionInterval != 10*time.Minute {
		t.Fatalf("unexpected limit/retention defaults: %+v", cfg)
	}
	if cfg.TrustCFConnectingIP || cfg.CookieSecure || cfg.UploadIdleTimeout != 30*time.Second ||
		cfg.UploadMaxDuration != 10*time.Minute || cfg.MaxConcurrentUploads != 16 ||
		cfg.MaxUnfinishedJobs != 50 || cfg.MaxUnfinishedPerClient != 3 ||
		cfg.MaxErrorLines != 50000 || cfg.MaxRecordBytes != 1<<20 || cfg.APIMetricsAddr != ":9090" {
		t.Fatalf("unexpected safeguard defaults: %+v", cfg)
	}
	t.Setenv("SQS_DLQ_URL", "")
	if cfg, err = Load(); err != nil || cfg.SQSDLQURL != "" {
		t.Fatalf("empty SQS_DLQ_URL must disable the consumer: %q, %v", cfg.SQSDLQURL, err)
	}
	t.Setenv("API_ADDR", ":9090")
	t.Setenv("WORKER_CONCURRENCY", "8")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIAddr != ":9090" || cfg.WorkerConcurrency != 8 {
		t.Fatalf("environment overrides were not loaded: %+v", cfg)
	}
	for _, tc := range []struct{ key, value string }{
		{"SHUTDOWN_TIMEOUT", "invalid"},
		{"SHUTDOWN_TIMEOUT", "0s"},
		{"WORKER_CONCURRENCY", "0"},
		{"WORKER_CONCURRENCY", "many"},
		{"MAX_UPLOAD_BYTES", "-1"},
		{"MAX_UPLOAD_BYTES", "9223372036854775808"},
		{"MAX_ATTEMPTS", "0"},
		{"FAIL_RATE", "1.5"},
		{"FAIL_RATE", "-0.1"},
		{"FAIL_RATE", "abc"},
		{"UPLOAD_RATE_PER_MIN", "-1"},
		{"UPLOAD_RATE_PER_MIN", "fast"},
		{"UPLOAD_RATE_BURST", "0"},
		{"RETENTION", "-1h"},
		{"RETENTION", "soon"},
		{"RETENTION_INTERVAL", "0s"},
		{"TRUST_CF_CONNECTING_IP", "maybe"},
		{"COOKIE_SECURE", "2x"},
		{"UPLOAD_IDLE_TIMEOUT", "-1s"},
		{"UPLOAD_MAX_DURATION", "soon"},
		{"MAX_CONCURRENT_UPLOADS", "-1"},
		{"MAX_UNFINISHED_JOBS", "many"},
		{"MAX_UNFINISHED_PER_CLIENT", "-2"},
		{"MAX_ERROR_LINES", "0"},
		{"MAX_RECORD_BYTES", "0"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatalf("expected error for %s=%s", tc.key, tc.value)
			}
		})
	}
}
