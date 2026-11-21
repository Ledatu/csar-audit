package ingest

import (
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
	e, err := audit.EventFromProto(pb)
	if err != nil {
		return audit.Event{}, err
	}
	return *e, nil
}
