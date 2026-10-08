package config

import (
	"os"
	"testing"
	"time"
)

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
	if cfg.Archive.Enabled {
		t.Fatal("archive unexpectedly enabled by default")
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

func TestReceiptTimeoutAndDistinctQuarantine(t *testing.T) {
	for _, tail := range []string{
		"ingest:\n  receipt_timeout: -1s\n",
		"consumer:\n  dlq:\n    name: audit.events\n",
	} {
		if _, err := LoadFromBytes([]byte("database:\n  dsn: postgres://audit\nrabbitmq:\n  url: amqp://audit\n" + tail)); err == nil {
			t.Fatal("unsafe receipt/quarantine config accepted")
		}
	}
}

func TestProductionConfig(t *testing.T) {
	t.Setenv("AUDIT_DATABASE_URL", "postgres://local-test")
	t.Setenv("RABBITMQ_URL", "amqp://local-test")
	body, err := os.ReadFile("../../../csar-configs/prod/csar-audit/config.yaml")
	if os.IsNotExist(err) {
		t.Skip("sibling deployment checkout absent")
	}
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadFromBytes(body)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Ingest.ReceiptTimeout.Std() != 30*time.Second || cfg.HTTP.AllowedClientCN != "csar-client" {
		t.Fatal("prod receipt/trust settings mismatch")
	}
}

func TestArchiveRequiresExplicitDestination(t *testing.T) {
	for _, extra := range []string{
		"archive:\n  enabled: true\n",
		"archive:\n  enabled: true\n  bucket: archive\n  endpoint: https://s3.example.com\n  region: test\n  environment: ../other\n",
	} {
		if _, err := LoadFromBytes([]byte("database:\n  dsn: postgres://test\nrabbitmq:\n  url: amqp://test\n" + extra)); err == nil {
			t.Fatal("invalid archive destination accepted")
		}
	}
}
