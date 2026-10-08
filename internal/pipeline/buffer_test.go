package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ledatu/csar-core/audit"
)

type blockingPublisher struct {
	started chan []byte
	receipt chan error
}

func (p *blockingPublisher) PublishRaw(ctx context.Context, body []byte) error {
	select {
	case p.started <- body:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-p.receipt:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSubmitWaitsForConfirmationAndOwnsEvent(t *testing.T) {
	pub := &blockingPublisher{started: make(chan []byte, 1), receipt: make(chan error, 1)}
	b := NewBuffer(1, 1, pub, nil, nil, time.Second)
	defer b.Close()
	event := &audit.Event{Metadata: json.RawMessage(`{"x":1}`)}
	done := make(chan error, 1)
	go func() { done <- b.Submit(context.Background(), event) }()
	body := <-pub.started
	event.Metadata[5] = '9'
	var queued audit.Event
	if err := json.Unmarshal(body, &queued); err != nil {
		t.Fatal(err)
	}
	if queued.ID == "" || queued.CreatedAt.IsZero() || string(queued.Metadata) != `{"x":1}` {
		t.Fatal("missing identity or owned payload")
	}
	select {
	case <-done:
		t.Fatal("accepted without confirmation")
	default:
	}
	pub.receipt <- nil
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	go func() { done <- b.Submit(context.Background(), &queued) }()
	body = <-pub.started
	var retry audit.Event
	if err := json.Unmarshal(body, &retry); err != nil {
		t.Fatal(err)
	}
	if retry.ID != queued.ID || !retry.CreatedAt.Equal(queued.CreatedAt) {
		t.Fatal("replay changed identity")
	}
	pub.receipt <- errors.New("NACK")
	if err := <-done; err == nil {
		t.Fatal("NACK accepted")
	}
	if err := b.Submit(context.Background(), &audit.Event{ID: "invalid"}); err == nil {
		t.Fatal("invalid ID accepted")
	}
}

func TestFullCancellationAndClose(t *testing.T) {
	pub := &blockingPublisher{started: make(chan []byte, 1), receipt: make(chan error)}
	b := NewBuffer(1, 1, pub, nil, nil, time.Second)
	done := make(chan error, 2)
	go func() { done <- b.Submit(context.Background(), &audit.Event{}) }()
	<-pub.started
	go func() { done <- b.Submit(context.Background(), &audit.Event{}) }()
	deadline := time.Now().Add(time.Second)
	for b.Depth() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if b.Depth() != 1 {
		t.Fatal("second request did not queue")
	}
	if err := b.Submit(context.Background(), &audit.Event{}); !errors.Is(err, ErrFull) {
		t.Fatalf("full: %v", err)
	}
	b.Close()
	for range 2 {
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("close: %v", err)
		}
	}
	if err := b.Submit(context.Background(), &audit.Event{}); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed: %v", err)
	}
	b.Close()
}

func TestReceiptTimeout(t *testing.T) {
	pub := &blockingPublisher{started: make(chan []byte, 1), receipt: make(chan error)}
	b := NewBuffer(1, 1, pub, nil, nil, 20*time.Millisecond)
	defer b.Close()
	if err := b.Submit(context.Background(), &audit.Event{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timeout: %v", err)
	}
}

func TestConcurrentSubmitClose(t *testing.T) {
	pub := &blockingPublisher{started: make(chan []byte, 100), receipt: make(chan error)}
	b := NewBuffer(100, 4, pub, nil, nil, time.Second)
	var wg sync.WaitGroup
	for range 100 {
		wg.Add(1)
		go func() { defer wg.Done(); _ = b.Submit(context.Background(), &audit.Event{}) }()
	}
	b.Close()
	wg.Wait()
}
