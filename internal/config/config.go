// Package config loads shared application settings from the environment.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

type Config struct {
	APIAddr           string
	ShutdownTimeout   time.Duration
	DatabaseURL       string
	AWSRegion         string
	S3Endpoint        string
	S3Bucket          string
	SQSEndpoint       string
	SQSQueueURL       string
	WorkerConcurrency int
	MaxUploadBytes    int64
	MaxAttempts       int
}

// Load reads settings once at startup. AWS credentials are left to the SDK's
// standard environment credential provider when adapters are introduced.
func Load() (Config, error) {
	c := Config{
		APIAddr:     value("API_ADDR", ":8080"),
		DatabaseURL: value("DATABASE_URL", "postgres://processor:processor@localhost:5433/processor?sslmode=disable"),
		AWSRegion:   value("AWS_REGION", "us-east-1"),
		S3Endpoint:  value("S3_ENDPOINT", "http://localhost:9000"),
		S3Bucket:    value("S3_BUCKET", "uploads"),
		SQSEndpoint: value("SQS_ENDPOINT", "http://localhost:9324"),
		SQSQueueURL: value("SQS_QUEUE_URL", "http://localhost:9324/000000000000/jobs"),
	}
	var err error
	c.ShutdownTimeout, err = time.ParseDuration(value("SHUTDOWN_TIMEOUT", "15s"))
	if err != nil || c.ShutdownTimeout <= 0 {
		return Config{}, fmt.Errorf("SHUTDOWN_TIMEOUT must be a positive duration")
	}
	for _, setting := range []struct {
		name     string
		fallback int
		target   *int
	}{
		{"WORKER_CONCURRENCY", 4, &c.WorkerConcurrency},
		{"MAX_ATTEMPTS", 3, &c.MaxAttempts},
	} {
		n, err := strconv.Atoi(value(setting.name, strconv.Itoa(setting.fallback)))
		if err != nil || n <= 0 {
			return Config{}, fmt.Errorf("%s must be a positive integer", setting.name)
		}
		*setting.target = n
	}
	c.MaxUploadBytes, err = strconv.ParseInt(value("MAX_UPLOAD_BYTES", "524288000"), 10, 64)
	if err != nil || c.MaxUploadBytes <= 0 {
		return Config{}, fmt.Errorf("MAX_UPLOAD_BYTES must be a positive integer")
	}
	return c, nil
}

func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
