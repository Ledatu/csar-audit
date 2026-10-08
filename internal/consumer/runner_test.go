package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/audit"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

type recorder struct {
	ack      []uint64
	multiple bool
	nacks    int
}

func (r *recorder) Ack(tag uint64, multiple bool) error {
	r.ack = append(r.ack, tag)
	r.multiple = r.multiple || multiple
	return nil
}
func (r *recorder) Nack(_ uint64, _, _ bool) error { r.nacks++; return nil }
func (r *recorder) Reject(_ uint64, _ bool) error  { r.nacks++; return nil }

type fakeStore struct {
	insert func(context.Context, []audit.Event) error
}

func (s fakeStore) BatchInsert(ctx context.Context, events []audit.Event) error {
	return s.insert(ctx, events)
}

type fakeDLQ struct {
	publish func(context.Context, *amqp091.Publishing) error
}

func (p fakeDLQ) Publish(ctx context.Context, m *amqp091.Publishing) error { return p.publish(ctx, m) }

func delivery(t *testing.T, r *recorder, tag uint64) amqp091.Delivery {
	t.Helper()
	event := audit.Event{Actor: "user", Action: "update", TargetType: "campaign", ScopeType: "tenant"}
	body, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	return amqp091.Delivery{Acknowledger: r, DeliveryTag: tag, Body: body, Headers: amqp091.Table{"x-delivery-count": int64(99)}}
}
func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPGOutageRetainsDeliveryUntilCommit(t *testing.T) {
	r := &recorder{}
	calls := 0
	var id string
	st := fakeStore{insert: func(_ context.Context, events []audit.Event) error {
		calls++
		if id == "" {
			id = events[0].ID
		} else if id != events[0].ID {
			t.Fatal("ID changed during PG retry")
		}
		if len(r.ack) > 0 || r.nacks > 0 {
			t.Fatal("delivery settled before commit")
		}
		if calls == 1 {
			return errors.New("PG unavailable")
		}
		return nil
	}}
	dlq := fakeDLQ{publish: func(context.Context, *amqp091.Publishing) error {
		t.Fatal("PG outage quarantined valid event")
		return nil
	}}
	if err := processBatch(context.Background(), st, dlq, "audit.events", []amqp091.Delivery{delivery(t, r, 1)}, quiet(), nil); err != nil {
		t.Fatal(err)
	}
	if calls != 2 || len(r.ack) != 1 || r.multiple || r.nacks != 0 {
		t.Fatalf("unsafe settlement: %+v calls=%d", r, calls)
	}
}

func TestFailedQuarantineDoesNotAcknowledgeOriginal(t *testing.T) {
	r := &recorder{}
	d := delivery(t, r, 1)
	d.Body = []byte("{")
	st := fakeStore{insert: func(context.Context, []audit.Event) error { t.Fatal("invalid event reached PG"); return nil }}
	dlq := fakeDLQ{publish: func(_ context.Context, m *amqp091.Publishing) error {
		if string(m.Body) != "{" || len(r.ack) != 0 {
			t.Fatal("original lost or ACKed before confirmation")
		}
		return errors.New("DLQ unavailable")
	}}
	if err := processBatch(context.Background(), st, dlq, "audit.events", []amqp091.Delivery{d}, quiet(), nil); err == nil {
		t.Fatal("unconfirmed quarantine accepted")
	}
	if len(r.ack) != 0 || r.nacks != 0 {
		t.Fatal("original settled after failed quarantine")
	}
}

func TestConflictIsolationAndIndividualAcks(t *testing.T) {
	r := &recorder{}
	batch := []amqp091.Delivery{delivery(t, r, 1), delivery(t, r, 2), delivery(t, r, 3)}
	conflict := &store.EventConflictError{ID: "conflicted"}
	calls := 0
	st := fakeStore{insert: func(_ context.Context, events []audit.Event) error {
		calls++
		if len(events) > 1 || calls == 3 {
			return conflict
		}
		return nil
	}}
	quarantined := 0
	dlq := fakeDLQ{publish: func(_ context.Context, m *amqp091.Publishing) error {
		quarantined++
		if m.Headers["audit-quarantine-reason"] != "event_id_conflict" || len(r.ack) != 1 {
			t.Fatal("wrong quarantine ordering")
		}
		return nil
	}}
	if err := processBatch(context.Background(), st, dlq, "audit.events", batch, quiet(), nil); err != nil {
		t.Fatal(err)
	}
	if quarantined != 1 || calls != 4 || len(r.ack) != 3 || r.multiple || r.nacks != 0 {
		t.Fatalf("conflict handling: %+v calls=%d", r, calls)
	}
}

func TestPGOutageCancellationLeavesOriginalUnsettled(t *testing.T) {
	r := &recorder{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	st := fakeStore{insert: func(context.Context, []audit.Event) error { return errors.New("offline") }}
	if err := processBatch(ctx, st, nil, "audit.events", []amqp091.Delivery{delivery(t, r, 1)}, quiet(), nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel: %v", err)
	}
	if len(r.ack) != 0 || r.nacks != 0 {
		t.Fatal("original settled during outage")
	}
}

func TestOnlyPayloadFailuresArePermanent(t *testing.T) {
	for _, test := range []struct {
		code      string
		permanent bool
	}{
		{"22P05", true}, {"22003", true}, {"08006", false}, {"42501", false}, {"42P01", false}, {"40001", false},
	} {
		if got := permanentEventError(&pgconn.PgError{Code: test.code}); got != test.permanent {
			t.Fatalf("SQLSTATE %s: permanent=%v", test.code, got)
		}
	}
}
