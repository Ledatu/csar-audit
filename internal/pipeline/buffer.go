// Package pipeline bridges validated ingest events to RabbitMQ publishers.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ledatu/csar-audit/internal/rmq"
	"github.com/ledatu/csar-core/audit"
	"github.com/prometheus/client_golang/prometheus"
)

// ErrFull is returned when the in-memory buffer cannot accept another event.
var ErrFull = errors.New("audit pipeline buffer full")

// BufferMetrics is the subset of metrics the buffer writes to.
type BufferMetrics struct {
	EventsPublished prometheus.Counter
	EventsDropped   *prometheus.CounterVec
}

// Buffer is a bounded channel of event pointers drained by workers that publish to RabbitMQ.
// Callers must not mutate an Event after Submit returns successfully.
type Buffer struct {
	ch       chan *audit.Event
	capacity int
	pub      *rmq.Publisher
	logger   *slog.Logger
	metrics  *BufferMetrics

	wg sync.WaitGroup
}

// NewBuffer starts publisher workers that drain the buffer until Close.
// metrics may be nil (no-op instrumentation).
func NewBuffer(depth int, workers int, pub *rmq.Publisher, logger *slog.Logger, m *BufferMetrics) *Buffer {
	if depth < 1 {
		depth = 1
	}
	if workers < 1 {
		workers = 1
	}
	if logger == nil {
		logger = slog.Default()
	}
	b := &Buffer{
		ch:       make(chan *audit.Event, depth),
		capacity: depth,
		pub:      pub,
		logger:   logger.With("component", "audit_pipeline"),
		metrics:  m,
	}
	for range workers {
		b.wg.Add(1)
		go b.worker()
	}
	return b
}

// Depth returns the number of events waiting in the buffer.
func (b *Buffer) Depth() int {
	return len(b.ch)
}

// Capacity returns the buffer channel capacity.
func (b *Buffer) Capacity() int {
	return b.capacity
}

// Submit enqueues a non-blocking event. Returns ErrFull when the buffer is saturated.
func (b *Buffer) Submit(e *audit.Event) error {
	if e == nil {
		return errors.New("nil audit event")
	}
	select {
	case b.ch <- e:
		return nil
	default:
		b.logger.Warn("audit buffer full, event dropped",
			"action", e.Action,
			"service", e.Service,
		)
		if b.metrics != nil {
			b.metrics.EventsDropped.WithLabelValues("buffer_full").Inc()
		}
		return ErrFull
	}
}

func (b *Buffer) worker() {
	defer b.wg.Done()
	for e := range b.ch {
		b.publishWithRetry(e)
	}
}

func (b *Buffer) publishWithRetry(e *audit.Event) {
	if e == nil {
		return
	}
	body, err := json.Marshal(e)
	if err != nil {
		b.logger.Error("audit encode failed", "error", err)
		if b.metrics != nil {
			b.metrics.EventsDropped.WithLabelValues("encode_error").Inc()
		}
		return
	}
	const maxAttempts = 4
	backoff := 100 * time.Millisecond
	for attempt := range maxAttempts {
		if attempt > 0 {
			time.Sleep(backoff)
			backoff = min(backoff*2, 5*time.Second)
		}
		pctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err = b.pub.PublishRaw(pctx, body)
		cancel()
		if err == nil {
			if b.metrics != nil {
				b.metrics.EventsPublished.Inc()
			}
			return
		}
		b.logger.Warn("audit publish failed", "error", err, "attempt", attempt+1)
	}
	b.logger.Error("audit publish exhausted retries", "error", err)
	if b.metrics != nil {
		b.metrics.EventsDropped.WithLabelValues("publish_error").Inc()
	}
}

// Close drains the buffer then stops workers.
func (b *Buffer) Close() {
	close(b.ch)
	b.wg.Wait()
}
