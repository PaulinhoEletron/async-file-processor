package config

import (
	"testing"
	"time"
)

func TestLoad(t *testing.T) {
	for _, key := range []string{
		"API_ADDR", "SHUTDOWN_TIMEOUT", "DATABASE_URL", "AWS_REGION",
		"S3_ENDPOINT", "S3_BUCKET", "SQS_ENDPOINT", "SQS_QUEUE_URL",
		"WORKER_CONCURRENCY", "MAX_UPLOAD_BYTES", "MAX_ATTEMPTS", "STUB_PROCESS_DELAY",
	} {
		t.Setenv(key, "")
	}
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.APIAddr != ":8080" || cfg.WorkerConcurrency != 4 || cfg.S3Bucket != "uploads" || cfg.StubDelay != 500*time.Millisecond {
		t.Fatalf("unexpected defaults: %+v", cfg)
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
		{"STUB_PROCESS_DELAY", "-1s"},
		{"STUB_PROCESS_DELAY", "slow"},
	} {
		t.Run(tc.key+"/"+tc.value, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatalf("expected error for %s=%s", tc.key, tc.value)
			}
		})
	}
}
