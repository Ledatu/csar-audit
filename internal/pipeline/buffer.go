// Package pipeline bridges validated ingest events to confirmed RabbitMQ publication.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/ledatu/csar-core/audit"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrFull   = errors.New("audit pipeline buffer full")
	ErrClosed = errors.New("audit pipeline closed")
)

type BufferMetrics struct {
	EventsPublished prometheus.Counter
	EventsDropped   *prometheus.CounterVec
}

type Publisher interface {
	PublishRaw(context.Context, []byte) error
}

type request struct {
	ctx    context.Context
	event  *audit.Event
	result chan error
}

// Buffer owns pending events; Submit returns only after a confirmed publication.
type Buffer struct {
	ch       chan request
	capacity int
	pub      Publisher
	logger   *slog.Logger
	metrics  *BufferMetrics
	timeout  time.Duration
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool
	wg       sync.WaitGroup
}

func NewBuffer(depth, workers int, pub Publisher, logger *slog.Logger, m *BufferMetrics, timeout time.Duration) *Buffer {
	if depth < 1 {
		depth = 1
	}
	if workers < 1 {
		workers = 1
	}
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if logger == nil {
		logger = slog.Default()
	}
	ctx, cancel := context.WithCancel(context.Background())
	b := &Buffer{ch: make(chan request, depth), capacity: depth, pub: pub, logger: logger.With("component", "audit_pipeline"), metrics: m, timeout: timeout, ctx: ctx, cancel: cancel}
	for range workers {
		b.wg.Add(1)
		go b.worker()
	}
	return b
}

func (b *Buffer) Depth() int    { return len(b.ch) }
func (b *Buffer) Capacity() int { return b.capacity }

// Submit waits for broker confirmation, bounded by both caller and receipt timeout.
// A failed/lost receipt can mean uncertain delivery: retry with the same event ID.
func (b *Buffer) Submit(ctx context.Context, e *audit.Event) error {
	prepared, err := audit.PrepareEvent(e)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	stop := context.AfterFunc(b.ctx, cancel)
	defer stop()
	req := request{ctx: ctx, event: prepared, result: make(chan error, 1)}
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		b.mu.Unlock()
		return err
	}
	select {
	case b.ch <- req:
		b.mu.Unlock()
	default:
		b.mu.Unlock()
		b.rejected("buffer_full")
		return ErrFull
	}
	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Buffer) rejected(reason string) {
	if b.metrics != nil {
		b.metrics.EventsDropped.WithLabelValues(reason).Inc()
	}
}

func (b *Buffer) worker() {
	defer b.wg.Done()
	for req := range b.ch {
		err := b.publish(req)
		req.result <- err
	}
}

func (b *Buffer) publish(req request) error {
	if err := req.ctx.Err(); err != nil {
		return err
	}
	body, err := json.Marshal(req.event)
	if err != nil {
		b.rejected("encode_error")
		return err
	}
	err = b.pub.PublishRaw(req.ctx, body)
	if err != nil {
		b.rejected("publish_uncertain")
		b.logger.Warn("audit publication not confirmed", "error", err)
		return err
	}
	if b.metrics != nil {
		b.metrics.EventsPublished.Inc()
	}
	return nil
}

// Close cancels pending receipts and joins workers. Concurrent Submit is safe.
func (b *Buffer) Close() {
	b.mu.Lock()
	if !b.closed {
		b.closed = true
		b.cancel()
		close(b.ch)
	}
	b.mu.Unlock()
	b.wg.Wait()
}
