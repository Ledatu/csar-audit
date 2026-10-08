package rmq

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func testBroker(t *testing.T) *ConnectionManager {
	t.Helper()
	raw := os.Getenv("AUDIT_TEST_AMQP_URL")
	if raw == "" {
		t.Skip("set AUDIT_TEST_AMQP_URL for isolated broker tests")
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "amqp" || u.Hostname() != "127.0.0.1" || u.Path != "/rabbitmq" || u.User == nil || u.User.Username() != "audit-local" {
		t.Fatal("broker test requires the isolated localhost audit-local/rabbitmq fixture")
	}
	cm := NewConnectionManager(ConnectionConfig{URL: raw, ReconnectDelay: 10 * time.Millisecond}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cm.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := cm.Close(); err != nil {
			t.Error(err)
		}
	})
	return cm
}

func TestPublisherMandatoryRouting(t *testing.T) {
	cm := testBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch, err := cm.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	name := fmt.Sprintf("audit-test-%d", time.Now().UnixNano())
	if _, err := ch.QueueDeclare(name, true, false, false, false, QueueDeclareArgs(QueueTypeQuorum, nil)); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = ch.QueueDelete(name, false, false, false) }()
	pub := NewPublisher(cm, name)
	if err := pub.PublishRaw(ctx, []byte("confirmed")); err != nil {
		t.Fatal(err)
	}
	d, ok, err := ch.Get(name, false)
	if err != nil || !ok || string(d.Body) != "confirmed" || d.DeliveryMode != amqp091.Persistent {
		t.Fatalf("confirmed payload missing: ok=%v err=%v", ok, err)
	}
	if err := d.Ack(false); err != nil {
		t.Fatal(err)
	}
	if err := NewPublisher(cm, name+"-missing").PublishRaw(ctx, []byte("unroutable")); err == nil || !strings.Contains(err.Error(), "returned") {
		t.Fatalf("unroutable accepted: %v", err)
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := pub.PublishRaw(canceled, []byte("canceled")); err == nil {
		t.Fatal("canceled publication accepted")
	}
}

func TestAuditBacklogSurvivesRepeatedRequeues(t *testing.T) {
	for _, name := range []string{"audit.events", "audit.events.dlq"} {
		t.Run(name, func(t *testing.T) {
			cm := testBroker(t)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			pub := NewPublisher(cm, name)
			ch, err := cm.Channel()
			if err != nil {
				t.Fatal(err)
			}
			q, err := ch.QueueDeclarePassive(name, true, false, false, false, QueueDeclareArgs(QueueTypeQuorum, nil))
			if err != nil || q.Messages != 0 || q.Consumers != 0 {
				t.Fatal("audit.events test fixture must be empty and idle")
			}
			_ = ch.Close()
			body := []byte("requeue-retention-proof")
			if err := pub.PublishRaw(ctx, body); err != nil {
				t.Fatal(err)
			}
			for attempt := range 30 {
				ch, err := cm.Channel()
				if err != nil {
					t.Fatal(err)
				}
				var d amqp091.Delivery
				var ok bool
				for !ok && ctx.Err() == nil {
					d, ok, err = ch.Get(name, false)
					if err != nil {
						t.Fatal(err)
					}
					if !ok {
						time.Sleep(10 * time.Millisecond)
					}
				}
				if !ok || !bytes.Equal(d.Body, body) {
					t.Fatalf("backlog lost at redelivery %d", attempt)
				}
				if attempt == 29 {
					if err := d.Ack(false); err != nil {
						t.Fatal(err)
					}
				} else {
					if err := d.Nack(false, true); err != nil {
						t.Fatal(err)
					}
				}
				if err := ch.Close(); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}

func TestBrokerRestartFixture(t *testing.T) {
	stage := os.Getenv("AUDIT_TEST_RESTART_STAGE")
	if stage == "" {
		t.Skip("explicit before/after restart proof only")
	}
	cm := testBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const name = "audit-test-restart"
	ch, err := cm.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ch.Close() }()
	if stage == "before" {
		q, err := ch.QueueDeclare(name, true, false, false, false, QueueDeclareArgs(QueueTypeQuorum, nil))
		if err != nil || q.Messages != 0 {
			t.Fatal("restart fixture not empty")
		}
		if err := NewPublisher(cm, name).PublishRaw(ctx, []byte("durable-restart-proof")); err != nil {
			t.Fatal(err)
		}
		return
	}
	if stage != "after" {
		t.Fatal("unknown restart stage")
	}
	d, ok, err := ch.Get(name, false)
	if err != nil || !ok || string(d.Body) != "durable-restart-proof" {
		t.Fatalf("confirmed payload lost across restart: %v", err)
	}
	if err := d.Ack(false); err != nil {
		t.Fatal(err)
	}
	if _, err := ch.QueueDelete(name, false, false, false); err != nil {
		t.Fatal(err)
	}
}

func TestPublisherBlockedDeadline(t *testing.T) {
	if os.Getenv("AUDIT_TEST_BLOCKED_BROKER") != "true" {
		t.Skip("explicit isolated memory-alarm proof only")
	}
	cm := testBroker(t)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := NewPublisher(cm, "audit.events").PublishRaw(ctx, make([]byte, 1<<20))
	if err == nil {
		t.Fatal("blocked broker accepted a receipt")
	}
	if time.Since(started) > 2*time.Second {
		t.Fatal("publication did not respect receipt deadline")
	}
}
