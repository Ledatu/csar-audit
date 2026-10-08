package ingest

import (
	"fmt"
	"time"

	"github.com/ledatu/csar-core/audit"
	auditv1 "github.com/ledatu/csar-proto/csar/audit/v1"
)

const (
	maxBeforeStateBytes = audit.MaxStateBytes
	maxMetadataBytes    = audit.MaxMetadataBytes
)

// Validate checks mandatory audit fields.
func Validate(e *audit.Event) error { return audit.ValidateEvent(e) }

// FromProto maps a protobuf AuditEvent to the shared core type.
func FromProto(pb *auditv1.AuditEvent) (audit.Event, error) {
	if pb == nil {
		return audit.Event{}, fmt.Errorf("nil event")
	}
	e := audit.Event{
		ID:          pb.Id,
		Service:     pb.Service,
		Actor:       pb.Actor,
		Action:      pb.Action,
		TargetType:  pb.TargetType,
		TargetID:    pb.TargetId,
		ScopeType:   pb.ScopeType,
		ScopeID:     pb.ScopeId,
		BeforeState: pb.BeforeState,
		AfterState:  pb.AfterState,
		Metadata:    pb.Metadata,
		RequestID:   pb.RequestId,
		ClientIP:    pb.ClientIp,
	}
	if ts := pb.Timestamp; ts != nil {
		e.CreatedAt = ts.AsTime()
	} else {
		e.CreatedAt = time.Now().UTC()
	}
	if err := Validate(&e); err != nil {
		return audit.Event{}, err
	}
	return e, nil
}
