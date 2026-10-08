package ingest

import (
	"context"
	"errors"
	"time"

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
	buf     submitter
	timeout time.Duration
}

// NewGRPC builds a gRPC ingest handler.
func NewGRPC(buf submitter, timeout time.Duration) *GRPC {
	return &GRPC{buf: buf, timeout: timeout}
}

// RecordEvent implements AuditIngestService.
func (s *GRPC) RecordEvent(ctx context.Context, req *auditv1.RecordEventRequest) (*auditv1.RecordEventResponse, error) {
	if req == nil || req.Event == nil {
		return nil, status.Error(codes.InvalidArgument, "event is required")
	}
	e, err := FromProto(req.Event)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	ctx, cancel := s.receiptContext(ctx)
	defer cancel()
	if err := s.buf.Submit(ctx, &e); err != nil {
		return nil, receiptError(err)
	}
	return &auditv1.RecordEventResponse{}, nil
}

// RecordEvents implements AuditIngestService.
func (s *GRPC) RecordEvents(ctx context.Context, req *auditv1.RecordEventsRequest) (*auditv1.RecordEventsResponse, error) {
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

	ctx, cancel := s.receiptContext(ctx)
	defer cancel()
	var accepted int32
	for i := range events {
		if err := s.buf.Submit(ctx, &events[i]); err != nil {
			return &auditv1.RecordEventsResponse{Accepted: accepted},
				receiptError(err)
		}
		accepted++
	}
	return &auditv1.RecordEventsResponse{Accepted: accepted}, nil
}

func (s *GRPC) receiptContext(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := s.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return context.WithTimeout(ctx, timeout)
}

func receiptError(err error) error {
	code := codes.Unavailable
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, pipeline.ErrFull):
		code = codes.ResourceExhausted
	}
	return status.Error(code, "audit publication not confirmed; retry with retained event IDs")
}
