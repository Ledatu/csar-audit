package ingest

import (
	"context"

	"github.com/ledatu/csar-audit/internal/pipeline"
	"github.com/ledatu/csar-core/audit"
	auditv1 "github.com/ledatu/csar-proto/csar/audit/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxEventsPerBatch = 1000

// GRPC implements AuditIngestServiceServer.
type GRPC struct {
	auditv1.UnimplementedAuditIngestServiceServer
	buf *pipeline.Buffer
}

// NewGRPC builds a gRPC ingest handler.
func NewGRPC(buf *pipeline.Buffer) *GRPC {
	return &GRPC{buf: buf}
}

// RecordEvent implements AuditIngestService.
func (s *GRPC) RecordEvent(_ context.Context, req *auditv1.RecordEventRequest) (*auditv1.RecordEventResponse, error) {
	if req == nil || req.Event == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}
	e, err := FromProto(req.Event)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if err := s.buf.Submit(&e); err != nil {
		return nil, status.Error(codes.ResourceExhausted, "ingest buffer full")
	}
	return &auditv1.RecordEventResponse{}, nil
}

// RecordEvents implements AuditIngestService.
func (s *GRPC) RecordEvents(_ context.Context, req *auditv1.RecordEventsRequest) (*auditv1.RecordEventsResponse, error) {
	if req == nil || len(req.Events) == 0 {
		return nil, status.Error(codes.InvalidArgument, "events required")
	}
	if len(req.Events) > maxEventsPerBatch {
		return nil, status.Errorf(codes.InvalidArgument, "batch size %d exceeds maximum %d", len(req.Events), maxEventsPerBatch)
	}

	events := make([]audit.Event, 0, len(req.Events))
	for _, ev := range req.Events {
		e, err := FromProto(ev)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		events = append(events, e)
	}

	var accepted int32
	for i := range events {
		if err := s.buf.Submit(&events[i]); err != nil {
			return &auditv1.RecordEventsResponse{Accepted: accepted},
				status.Errorf(codes.ResourceExhausted, "ingest buffer full after %d of %d events", accepted, len(events))
		}
		accepted++
	}
	return &auditv1.RecordEventsResponse{Accepted: accepted}, nil
}
