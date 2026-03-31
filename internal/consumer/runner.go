// Package consumer runs the RabbitMQ batch worker that flushes to Postgres.
package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/ledatu/csar-audit/internal/config"
	"github.com/ledatu/csar-audit/internal/rmq"
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
		Args: amqp091.Table{
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": ccfg.DLQ.Name,
		},
	}

	sess, err := rmq.OpenConsumerSession(ctx, cm, queueCfg)
	if err != nil {
		return err
	}
	defer func() { _ = sess.Close() }()

	flush := ccfg.FlushInterval.Std()
	batchSize := ccfg.BatchSize
	maxRedeliver := ccfg.MaxRedeliveries

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		deliveries, err := sess.BatchConsume(ctx, batchSize, flush)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return err
			}
			return err
		}
		if len(deliveries) == 0 {
			continue
		}

		events, good, poison := decodeBatch(deliveries, maxRedeliver, logger, m)

		for i := range poison {
			_ = poison[i].Nack(false, false)
		}

		if len(events) == 0 {
			continue
		}

		if m != nil {
			m.BatchSize.Observe(float64(len(events)))
		}

		insertStart := time.Now()
		insertCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		err = st.BatchInsert(insertCtx, events)
		cancel()

		if m != nil {
			m.BatchFlushSeconds.Observe(time.Since(insertStart).Seconds())
		}

		if err != nil {
			logger.Error("batch insert failed", "error", err, "batch_len", len(events))
			if m != nil {
				m.ConsumerErrors.WithLabelValues("pg_error").Inc()
			}
			for i := range good {
				_ = good[i].Nack(false, true)
			}
			continue
		}

		if m != nil {
			m.EventsWritten.Add(float64(len(events)))
		}

		if err := good[len(good)-1].Ack(true); err != nil {
			logger.Error("batch ack failed", "error", err, "batch_len", len(good))
		}
	}
}

func decodeBatch(batch []amqp091.Delivery, maxRedeliver int, logger *slog.Logger, m *ConsumerMetrics) ([]audit.Event, []amqp091.Delivery, []amqp091.Delivery) {
	events := make([]audit.Event, 0, len(batch))
	good := make([]amqp091.Delivery, 0, len(batch))
	var poison []amqp091.Delivery

	for i := range batch {
		if maxRedeliver > 0 && deathCount(&batch[i]) >= maxRedeliver {
			logger.Warn("message exceeded max redeliveries, sending to DLQ",
				"delivery_tag", batch[i].DeliveryTag,
				"deaths", deathCount(&batch[i]),
			)
			if m != nil {
				m.ConsumerErrors.WithLabelValues("dlq").Inc()
			}
			poison = append(poison, batch[i])
			continue
		}

		var e audit.Event
		if err := json.Unmarshal(batch[i].Body, &e); err != nil {
			logger.Warn("audit message decode failed, sending to DLQ", "error", err)
			if m != nil {
				m.ConsumerErrors.WithLabelValues("decode_error").Inc()
			}
			poison = append(poison, batch[i])
			continue
		}
		events = append(events, e)
		good = append(good, batch[i])
	}
	return events, good, poison
}

func deathCount(d *amqp091.Delivery) int {
	xDeath, ok := d.Headers["x-death"]
	if !ok {
		if d.Redelivered {
			return 1
		}
		return 0
	}
	deaths, ok := xDeath.([]interface{})
	if !ok || len(deaths) == 0 {
		return 0
	}
	first, ok := deaths[0].(amqp091.Table)
	if !ok {
		return 0
	}
	count, ok := first["count"].(int64)
	if !ok {
		return 0
	}
	return int(count)
}
