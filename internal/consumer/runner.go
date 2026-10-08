// Package consumer runs the RabbitMQ batch worker that flushes to Postgres.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ledatu/csar-audit/internal/config"
	"github.com/ledatu/csar-audit/internal/ingest"
	"github.com/ledatu/csar-audit/internal/rmq"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/audit"
	"github.com/prometheus/client_golang/prometheus"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

// BatchInserter is satisfied by store.Postgres.
type BatchInserter interface {
	BatchInsert(ctx context.Context, events []audit.Event) error
}

// ConsumerMetrics is the subset of metrics the consumer writes to.
type ConsumerMetrics struct {
	BatchSize         prometheus.Histogram
	BatchFlushSeconds prometheus.Histogram
	EventsWritten     prometheus.Counter
	ConsumerErrors    *prometheus.CounterVec
}

// Run reconnects and processes batches until ctx is cancelled.
func Run(ctx context.Context, cm *rmq.ConnectionManager, st BatchInserter, cfg *config.Config, logger *slog.Logger, m *ConsumerMetrics) {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With("component", "audit_consumer")
	ccfg := &cfg.Consumer

	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		if err := runSession(ctx, cm, st, ccfg, logger, m); err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return
			}
			logger.Error("consumer session ended", "error", err, "retry_in", backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
	}
}

func runSession(ctx context.Context, cm *rmq.ConnectionManager, st BatchInserter, ccfg *config.ConsumerConfig, logger *slog.Logger, m *ConsumerMetrics) error {
	queueCfg := rmq.QueueConfig{
		Name:     ccfg.Queue.Name,
		Durable:  ccfg.Queue.Durable,
		Prefetch: ccfg.Queue.Prefetch,
		Type:     ccfg.Queue.Type,
	}

	sess, err := rmq.OpenConsumerSession(ctx, cm, queueCfg)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close() }()

	dlq := rmq.NewPublisher(cm, ccfg.DLQ.Name)
	for {
		deliveries, err := sess.BatchConsume(ctx, ccfg.BatchSize, ccfg.FlushInterval.Std())
		if err != nil {
			return err
		} // Closing the session requeues every unacknowledged delivery.
		if err := processBatch(ctx, st, dlq, ccfg.Queue.Name, deliveries, logger, m); err != nil {
			return err
		}
	}
}

type quarantinePublisher interface {
	Publish(context.Context, *amqp091.Publishing) error
}

// processBatch never ACKs until PG commits or the quarantine copy is confirmed.
func processBatch(ctx context.Context, st BatchInserter, dlq quarantinePublisher, source string, batch []amqp091.Delivery, logger *slog.Logger, m *ConsumerMetrics) error {
	events := make([]audit.Event, 0, len(batch))
	good := make([]amqp091.Delivery, 0, len(batch))
	for i := range batch {
		d := &batch[i]
		var event audit.Event
		err := json.Unmarshal(d.Body, &event)
		if err == nil {
			err = ingest.Validate(&event)
		}
		if err != nil {
			if err := quarantine(ctx, dlq, source, d, "invalid_event", m); err != nil {
				return err
			}
			continue
		}
		prepared, err := audit.PrepareEvent(&event)
		if err != nil {
			return err
		}
		events = append(events, *prepared)
		good = append(good, *d)
	}
	if len(events) == 0 {
		return nil
	}
	if m != nil {
		m.BatchSize.Observe(float64(len(events)))
	}
	err := insertWithRetry(ctx, st, events, logger, m)
	if permanentEventError(err) {
		// The conflicted batch rolled back atomically. Persist independent events;
		// conflicting contents remain recoverable in quarantine, never overwritten.
		for i := range events {
			err := insertWithRetry(ctx, st, events[i:i+1], logger, m)
			if permanentEventError(err) {
				reason := "pg_invalid_event"
				var conflict *store.EventConflictError
				if errors.As(err, &conflict) {
					reason = "event_id_conflict"
				}
				if err := quarantine(ctx, dlq, source, &good[i], reason, m); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			if err := acknowledge(&good[i], m); err != nil {
				return err
			}
		}
		return nil
	}
	if err != nil {
		return err
	}
	for i := range good {
		if err := acknowledge(&good[i], m); err != nil {
			return err
		}
	}
	return nil
}

func insertWithRetry(ctx context.Context, st BatchInserter, events []audit.Event, logger *slog.Logger, m *ConsumerMetrics) error {
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		started := time.Now()
		attemptCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err := st.BatchInsert(attemptCtx, events)
		cancel()
		if m != nil {
			m.BatchFlushSeconds.Observe(time.Since(started).Seconds())
		}
		if err == nil || permanentEventError(err) {
			return err
		}
		if m != nil {
			m.ConsumerErrors.WithLabelValues("pg_error").Inc()
		}
		logger.Warn("PG write failed; retaining unacknowledged audit batch", "batch_len", len(events), "retry_in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func acknowledge(d *amqp091.Delivery, m *ConsumerMetrics) error {
	if err := d.Ack(false); err != nil {
		return err
	}
	if m != nil {
		m.EventsWritten.Inc()
	}
	return nil
}

func quarantine(ctx context.Context, dlq quarantinePublisher, source string, d *amqp091.Delivery, reason string, m *ConsumerMetrics) error {
	pubCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	// Original bytes and identity are retained, without logging their contents.
	message := amqp091.Publishing{
		Body: d.Body, ContentType: d.ContentType, ContentEncoding: d.ContentEncoding,
		MessageId: d.MessageId, CorrelationId: d.CorrelationId, Timestamp: d.Timestamp, Type: d.Type,
		Headers: amqp091.Table{"audit-quarantine-reason": reason, "audit-source-queue": source},
	}
	if err := dlq.Publish(pubCtx, &message); err != nil {
		return err
	}
	if err := d.Ack(false); err != nil {
		return err
	}
	if m != nil {
		m.ConsumerErrors.WithLabelValues("quarantine").Inc()
	}
	return nil
}

// PostgreSQL data exceptions (e.g. JSONB numeric overflow or escaped NUL)
// are payload failures. Connection, permission and schema failures keep retrying.
func permanentEventError(err error) bool {
	var conflict *store.EventConflictError
	if errors.As(err, &conflict) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && len(pgErr.Code) >= 2 && pgErr.Code[:2] == "22"
}
