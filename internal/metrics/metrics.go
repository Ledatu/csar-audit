// Package metrics provides Prometheus instrumentation for csar-audit.
package metrics

import "github.com/prometheus/client_golang/prometheus"

// Metrics holds all csar-audit Prometheus collectors.
type Metrics struct {
	IngestTotal       *prometheus.CounterVec
	IngestDuration    *prometheus.HistogramVec
	BufferDepth       prometheus.GaugeFunc
	EventsPublished   prometheus.Counter
	EventsDropped     *prometheus.CounterVec
	BatchSize         prometheus.Histogram
	BatchFlushSeconds prometheus.Histogram
	EventsWritten     prometheus.Counter
	ConsumerErrors    *prometheus.CounterVec
}

// DepthFunc returns the current buffer depth.
type DepthFunc func() float64

// New creates and registers all metrics on the given registry.
func New(reg *prometheus.Registry, depthFn DepthFunc) *Metrics {
	m := &Metrics{
		IngestTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "audit",
			Name:      "ingest_total",
			Help:      "Total audit events received by ingest handlers.",
		}, []string{"transport", "service"}),

		IngestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "audit",
			Name:      "ingest_duration_seconds",
			Help:      "Ingest handler latency.",
			Buckets:   []float64{.0005, .001, .005, .01, .05, .1},
		}, []string{"transport"}),

		BufferDepth: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "audit",
			Name:      "buffer_depth",
			Help:      "Number of events in the in-memory pipeline buffer.",
		}, depthFn),

		EventsPublished: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "audit",
			Name:      "events_published_total",
			Help:      "Events successfully published to RabbitMQ.",
		}),

		EventsDropped: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "audit",
			Name:      "events_dropped_total",
			Help:      "Events dropped before reaching RabbitMQ.",
		}, []string{"reason"}),

		BatchSize: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "audit",
			Name:      "batch_size",
			Help:      "Number of events per consumer batch write.",
			Buckets:   []float64{1, 5, 10, 25, 50, 100, 200, 500},
		}),

		BatchFlushSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "audit",
			Name:      "batch_flush_duration_seconds",
			Help:      "Time to write a batch to PostgreSQL.",
			Buckets:   []float64{.005, .01, .025, .05, .1, .25, .5, 1, 5},
		}),

		EventsWritten: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "audit",
			Name:      "events_written_total",
			Help:      "Events successfully persisted to PostgreSQL.",
		}),

		ConsumerErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "audit",
			Name:      "consumer_errors_total",
			Help:      "Consumer-side errors by type.",
		}, []string{"type"}),
	}

	reg.MustRegister(
		m.IngestTotal,
		m.IngestDuration,
		m.BufferDepth,
		m.EventsPublished,
		m.EventsDropped,
		m.BatchSize,
		m.BatchFlushSeconds,
		m.EventsWritten,
		m.ConsumerErrors,
	)

	return m
}
