// Package config loads shared application settings from the environment.
package config

import (
	"fmt"
	"math"
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
	SQSDLQURL         string // dead-letter queue consumed by the worker; empty disables the consumer
	MetricsAddr       string // worker metrics listen address
	APIMetricsAddr    string // API metrics listen address; kept off the public listener
	WorkerConcurrency int
	MaxUploadBytes    int64
	MaxAttempts       int
	FailRate          float64 // dev-only fault injection probability, 0..1

	UploadRatePerMin  float64       // sustained POST /api/jobs rate per client; 0 disables the limit
	UploadRateBurst   int           // largest burst of uploads per client
	Retention         time.Duration // finished jobs older than this are deleted; 0 disables cleanup
	RetentionInterval time.Duration // how often the worker sweeps for expired jobs

	// TrustCFConnectingIP makes the API key rate limits and job caps on the CF-Connecting-IP
	// header. Enable it only when the API is reachable solely through Cloudflare.
	TrustCFConnectingIP bool
	CookieSecure        bool // mark the owner cookie Secure (enable behind HTTPS)

	UploadIdleTimeout      time.Duration // an upload is cut off after this long without body data; 0 disables
	UploadMaxDuration      time.Duration // upper bound on the total time of one upload; 0 disables
	MaxConcurrentUploads   int           // uploads streaming at once; 0 disables the cap
	MaxUnfinishedJobs      int           // PENDING+PROCESSING jobs allowed in total; 0 disables the cap
	MaxUnfinishedPerClient int           // PENDING+PROCESSING jobs allowed per client; 0 disables the cap

	MaxErrorLines  int64 // error lines written to errors.csv per job; counting continues past it
	MaxRecordBytes int   // largest CSV record the worker accepts
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
	c.MetricsAddr = value("METRICS_ADDR", ":9091")
	c.APIMetricsAddr = value("API_METRICS_ADDR", ":9090")
	c.SQSDLQURL = "http://localhost:9324/000000000000/jobs-dlq"
	if v, ok := os.LookupEnv("SQS_DLQ_URL"); ok {
		c.SQSDLQURL = v // set but empty disables the consumer
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
	c.FailRate, err = strconv.ParseFloat(value("FAIL_RATE", "0"), 64)
	if err != nil || math.IsNaN(c.FailRate) || c.FailRate < 0 || c.FailRate > 1 {
		return Config{}, fmt.Errorf("FAIL_RATE must be a number between 0 and 1")
	}
	c.UploadRatePerMin, err = strconv.ParseFloat(value("UPLOAD_RATE_PER_MIN", "10"), 64)
	if err != nil || math.IsNaN(c.UploadRatePerMin) || math.IsInf(c.UploadRatePerMin, 0) || c.UploadRatePerMin < 0 {
		return Config{}, fmt.Errorf("UPLOAD_RATE_PER_MIN must be a number of 0 or more")
	}
	c.UploadRateBurst, err = strconv.Atoi(value("UPLOAD_RATE_BURST", "5"))
	if err != nil || c.UploadRateBurst <= 0 {
		return Config{}, fmt.Errorf("UPLOAD_RATE_BURST must be a positive integer")
	}
	c.Retention, err = time.ParseDuration(value("RETENTION", "24h"))
	if err != nil || c.Retention < 0 {
		return Config{}, fmt.Errorf("RETENTION must be a duration of 0 or more")
	}
	c.RetentionInterval, err = time.ParseDuration(value("RETENTION_INTERVAL", "10m"))
	if err != nil || c.RetentionInterval <= 0 {
		return Config{}, fmt.Errorf("RETENTION_INTERVAL must be a positive duration")
	}
	if c.TrustCFConnectingIP, err = boolSetting("TRUST_CF_CONNECTING_IP"); err != nil {
		return Config{}, err
	}
	if c.CookieSecure, err = boolSetting("COOKIE_SECURE"); err != nil {
		return Config{}, err
	}
	for _, setting := range []struct {
		name     string
		fallback string
		target   *time.Duration
	}{
		{"UPLOAD_IDLE_TIMEOUT", "30s", &c.UploadIdleTimeout},
		{"UPLOAD_MAX_DURATION", "10m", &c.UploadMaxDuration},
	} {
		d, err := time.ParseDuration(value(setting.name, setting.fallback))
		if err != nil || d < 0 {
			return Config{}, fmt.Errorf("%s must be a duration of 0 or more", setting.name)
		}
		*setting.target = d
	}
	for _, setting := range []struct {
		name     string
		fallback int
		target   *int
	}{
		{"MAX_CONCURRENT_UPLOADS", 16, &c.MaxConcurrentUploads},
		{"MAX_UNFINISHED_JOBS", 50, &c.MaxUnfinishedJobs},
		{"MAX_UNFINISHED_PER_CLIENT", 3, &c.MaxUnfinishedPerClient},
	} {
		n, err := strconv.Atoi(value(setting.name, strconv.Itoa(setting.fallback)))
		if err != nil || n < 0 {
			return Config{}, fmt.Errorf("%s must be an integer of 0 or more", setting.name)
		}
		*setting.target = n
	}
	c.MaxErrorLines, err = strconv.ParseInt(value("MAX_ERROR_LINES", "50000"), 10, 64)
	if err != nil || c.MaxErrorLines <= 0 {
		return Config{}, fmt.Errorf("MAX_ERROR_LINES must be a positive integer")
	}
	c.MaxRecordBytes, err = strconv.Atoi(value("MAX_RECORD_BYTES", "1048576"))
	if err != nil || c.MaxRecordBytes <= 0 {
		return Config{}, fmt.Errorf("MAX_RECORD_BYTES must be a positive integer")
	}
	return c, nil
}

// boolSetting reads a boolean environment variable that defaults to false.
func boolSetting(name string) (bool, error) {
	v := value(name, "false")
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be true or false", name)
	}
	return b, nil
}

func value(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}
