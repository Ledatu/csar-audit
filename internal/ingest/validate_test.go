package ingest

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ledatu/csar-core/audit"
	auditv1 "github.com/ledatu/csar-proto/csar/audit/v1"
)

func TestValidateRejectsOversizedPayloads(t *testing.T) {
	ev := &audit.Event{
		Actor:       "user",
		Action:      "POST /x",
		TargetType:  "path",
		ScopeType:   "platform",
		BeforeState: json.RawMessage(`"` + strings.Repeat("a", maxBeforeStateBytes+1) + `"`),
	}
	if err := Validate(ev); err == nil {
		t.Fatal("expected before_state size error")
	}

	ev.BeforeState = nil
	ev.Metadata = json.RawMessage(`"` + strings.Repeat("b", maxMetadataBytes+1) + `"`)
	if err := Validate(ev); err == nil {
		t.Fatal("expected metadata size error")
	}
}

func TestFromProtoPreservesEventIdentity(t *testing.T) {
	event, err := audit.PrepareEvent(&audit.Event{Actor: "user", Action: "update", TargetType: "campaign", ScopeType: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	received, err := FromProto(audit.EventToProto(event))
	if err != nil {
		t.Fatal(err)
	}
	if received.ID != event.ID || !received.CreatedAt.Equal(event.CreatedAt) {
		t.Fatal("gRPC ingest lost event identity")
	}
	if _, err := FromProto(&auditv1.AuditEvent{Id: "invalid", Actor: "user", Action: "update", TargetType: "campaign", ScopeType: "tenant"}); err == nil {
		t.Fatal("invalid gRPC event ID was accepted")
	}
}
