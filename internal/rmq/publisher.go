package rmq

import (
	"context"
	"fmt"

	"github.com/ledatu/csar-core/amqpconfirm"
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
	return p.Publish(ctx, &amqp091.Publishing{Body: body, ContentType: "application/json"})
}

// Publish requires mandatory routing and positive confirmation for one message.
func (p *Publisher) Publish(ctx context.Context, input *amqp091.Publishing) error {
	message := *input
	ch, stop, err := p.cm.publisherChannel(ctx)
	if err != nil {
		return fmt.Errorf("open channel: %w", err)
	}
	defer stop()
	defer func() { _ = ch.Close() }()

	if err := ch.Confirm(false); err != nil {
		return fmt.Errorf("enable confirms: %w", err)
	}

	confirmCh := ch.NotifyPublish(make(chan amqp091.Confirmation, 1))
	returnCh := ch.NotifyReturn(make(chan amqp091.Return, 1))

	headers := amqp091.Table{}
	for k, v := range message.Headers {
		headers[k] = v
	}
	message.Headers = headers
	otel.GetTextMapPropagator().Inject(ctx, amqpCarrier(message.Headers))
	message.DeliveryMode = amqp091.Persistent

	err = ch.PublishWithContext(ctx,
		"",
		p.queue,
		true,
		false,
		message,
	)
	if err != nil {
		return fmt.Errorf("publish to %s: %w", p.queue, err)
	}

	return amqpconfirm.Await(ctx, fmt.Sprintf("publish to %q", p.queue), confirmCh, returnCh)
}
