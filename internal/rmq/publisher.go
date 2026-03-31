package rmq

import (
	"context"
	"fmt"

	amqp091 "github.com/rabbitmq/amqp091-go"
	"go.opentelemetry.io/otel"
)

// Publisher sends messages to a RabbitMQ queue with publisher confirms.
type Publisher struct {
	cm    *ConnectionManager
	queue string
}

// NewPublisher creates a publisher for the named queue (default exchange, routing key = queue name).
func NewPublisher(cm *ConnectionManager, queue string) *Publisher {
	return &Publisher{cm: cm, queue: queue}
}

// PublishRaw sends raw bytes with trace context in headers and waits for broker ack.
func (p *Publisher) PublishRaw(ctx context.Context, body []byte) error {
	ch, err := p.cm.Channel()
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer func() { _ = ch.Close() }()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable confirms: %w", err)
	}

	confirmCh := ch.NotifyPublish(make(chan amqp091.Confirmation, 1))

	headers := amqp091.Table{}
	otel.GetTextMapPropagator().Inject(ctx, amqpCarrier(headers))

	err = ch.PublishWithContext(ctx,
		"",
		p.queue,
		false,
		false,
		amqp091.Publishing{
			DeliveryMode: amqp091.Persistent,
			ContentType:  "application/json",
			Headers:      headers,
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", p.queue, err)
	}

	select {
	case confirm := <-confirmCh:
		if !confirm.Ack {
			return fmt.Errorf("publish to %s: message nacked by broker", p.queue)
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	return nil
}
