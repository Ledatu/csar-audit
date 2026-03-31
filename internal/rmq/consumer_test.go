package rmq

import (
	"context"
	"testing"
	"time"

	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestCollectBatch_maxOne(t *testing.T) {
	t.Parallel()
	ch := make(chan amqp091.Delivery, 2)
	go func() {
		ch <- amqp091.Delivery{Body: []byte("a")}
		ch <- amqp091.Delivery{Body: []byte("b")}
	}()

	batch, err := CollectBatch(context.Background(), ch, 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 1 {
		t.Fatalf("len=%d want 1", len(batch))
	}
	if string(batch[0].Body) != "a" {
		t.Fatalf("body=%q", batch[0].Body)
	}
}

func TestCollectBatch_timeoutFlushesPartial(t *testing.T) {
	t.Parallel()
	ch := make(chan amqp091.Delivery, 3)
	go func() {
		time.Sleep(20 * time.Millisecond)
		ch <- amqp091.Delivery{Body: []byte("1")}
		time.Sleep(20 * time.Millisecond)
		ch <- amqp091.Delivery{Body: []byte("2")}
	}()

	batch, err := CollectBatch(context.Background(), ch, 10, 80*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) < 1 {
		t.Fatalf("expected at least 1, got %d", len(batch))
	}
}

func TestCollectBatch_ctxCancel(t *testing.T) {
	t.Parallel()
	ch := make(chan amqp091.Delivery)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch <- amqp091.Delivery{Body: []byte("x")}
		cancel()
	}()
	_, err := CollectBatch(ctx, ch, 5, time.Second)
	if err == nil {
		t.Fatal("expected context error")
	}
}
