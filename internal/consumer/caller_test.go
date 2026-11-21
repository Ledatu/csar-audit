package consumer

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ledatu/csar-core/audit"
	"github.com/ledatu/csar-core/caller"
	amqp091 "github.com/rabbitmq/amqp091-go"
)

func TestAMQPCallerSurvivesPreparationAndInvalidContextIsQuarantined(t *testing.T) {
	e := audit.Event{Actor: "owner", Action: "report.read", TargetType: "report", ScopeType: "tenant", OperationID: "op:1",
		Caller: &caller.Identity{Version: caller.Version, CallerKind: caller.Service, CallerID: "svc:reporting",
			EffectiveSubject: "svc:reporting", CredentialKind: caller.STS, Assurance: caller.Verified},
		OriginCaller: &caller.Identity{Version: caller.Version, CallerKind: caller.Agent, CallerID: "agent:1",
			OwnerSubject: "user:1", EffectiveSubject: "user:1", CredentialKind: caller.APIKey, CredentialID: "key:1", Assurance: caller.Verified}}
	for _, malformed := range []bool{false, true} {
		if malformed {
			e.OriginCaller.Version = 2
		}
		body, err := json.Marshal(e)
		if err != nil {
			t.Fatal(err)
		}
		r := &recorder{}
		inserted, quarantined := false, false
		st := fakeStore{insert: func(_ context.Context, events []audit.Event) error {
			inserted = true
			if malformed || len(events) != 1 || !reflect.DeepEqual(events[0].Caller, e.Caller) || !reflect.DeepEqual(events[0].OriginCaller, e.OriginCaller) ||
				events[0].OperationID != e.OperationID || events[0].Actor != e.Actor || len(r.ack) != 0 {
				t.Fatal("AMQP caller lost, malformed event stored or ACK preceded commit")
			}
			return nil
		}}
		dlq := fakeDLQ{publish: func(_ context.Context, m *amqp091.Publishing) error {
			quarantined = true
			if !malformed || !bytes.Equal(m.Body, body) || len(r.ack) != 0 {
				t.Fatal("quarantine changed original caller bytes or ACK preceded confirmation")
			}
			return nil
		}}
		d := amqp091.Delivery{Acknowledger: r, DeliveryTag: 1, Body: body}
		if err := processBatch(t.Context(), st, dlq, "audit.events", []amqp091.Delivery{d}, quiet(), nil); err != nil {
			t.Fatal(err)
		}
		if inserted == malformed || quarantined != malformed || len(r.ack) != 1 || r.nacks != 0 {
			t.Fatal("caller validation selected wrong AMQP settlement")
		}
	}
}
