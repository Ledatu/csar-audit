package rmq

import (
	"go.opentelemetry.io/otel/propagation"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

type amqpCarrier amqp091.Table

func (c amqpCarrier) Get(key string) string {
	if v, ok := amqp091.Table(c)[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (c amqpCarrier) Set(key, value string) {
	amqp091.Table(c)[key] = value
}

func (c amqpCarrier) Keys() []string {
	keys := make([]string, 0, len(amqp091.Table(c)))
	for k := range amqp091.Table(c) {
		keys = append(keys, k)
	}
	return keys
}

func extractTraceHeaders(headers amqp091.Table) propagation.TextMapCarrier {
	if headers == nil {
		headers = amqp091.Table{}
	}
	return amqpCarrier(headers)
}
