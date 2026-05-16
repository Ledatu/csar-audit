package config

import "testing"

func TestLoadFromBytesDefaultsQueueTypesToQuorum(t *testing.T) {
	t.Parallel()

	cfg, err := LoadFromBytes([]byte(`
database:
  dsn: postgres://audit
rabbitmq:
  url: amqp://audit
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Consumer.Queue.Type != "quorum" {
		t.Fatalf("queue type=%q want quorum", cfg.Consumer.Queue.Type)
	}
	if !cfg.Consumer.Queue.Durable {
		t.Fatal("queue durable=false want true")
	}
	if cfg.Consumer.DLQ.Type != "quorum" {
		t.Fatalf("dlq type=%q want quorum", cfg.Consumer.DLQ.Type)
	}
	if !cfg.Consumer.DLQ.Durable {
		t.Fatal("dlq durable=false want true")
	}
}

func TestLoadFromBytesRejectsUnsupportedQueueType(t *testing.T) {
	t.Parallel()

	_, err := LoadFromBytes([]byte(`
database:
  dsn: postgres://audit
rabbitmq:
  url: amqp://audit
consumer:
  queue:
    type: classic
`))
	if err == nil {
		t.Fatal("expected unsupported queue type error")
	}
}
