// Package metrics defines the Prometheus collectors shared by the API and the worker.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Job status label values for JobsTotal.
const (
	StatusCreated   = "created"
	StatusCompleted = "completed"
	StatusFailed    = "failed"
)

// Metrics owns every collector on a private registry, so tests and processes never share state.
type Metrics struct {
	Registry *prometheus.Registry

	JobsTotal        *prometheus.CounterVec // jobs_total{status}
	RowsProcessed    prometheus.Counter     // rows_processed_total
	JobDuration      prometheus.Histogram   // job_duration_seconds
	WorkerBusy       prometheus.Gauge       // worker_busy
	UploadBytes      prometheus.Counter     // upload_bytes_total
	JobsDeadLettered prometheus.Counter     // jobs_dead_lettered_total
}

// New creates the collectors, registers them with the Go runtime and process collectors, and
// touches every jobs_total label so the series exist from the first scrape.
func New() *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		JobsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobs_total", Help: "Jobs by lifecycle event: created (upload accepted), completed, failed.",
		}, []string{"status"}),
		RowsProcessed: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "rows_processed_total", Help: "Data rows processed by completed jobs.",
		}),
		JobDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "job_duration_seconds",
			Help:    "Processing time of each job attempt that ran to an outcome.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}),
		WorkerBusy: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "worker_busy", Help: "Jobs currently being processed.",
		}),
		UploadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "upload_bytes_total", Help: "Bytes of CSV data accepted by the upload endpoint.",
		}),
		JobsDeadLettered: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobs_dead_lettered_total", Help: "Messages consumed from the dead-letter queue.",
		}),
	}
	m.Registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.JobsTotal, m.RowsProcessed, m.JobDuration, m.WorkerBusy, m.UploadBytes, m.JobsDeadLettered,
	)
	for _, s := range []string{StatusCreated, StatusCompleted, StatusFailed} {
		m.JobsTotal.WithLabelValues(s)
	}
	return m
}

// Handler serves the registry in the Prometheus text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{})
}
