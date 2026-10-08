package ingest

import (
	"context"
	"testing"

	"github.com/ledatu/csar-audit/internal/pipeline"
	auditv1 "github.com/ledatu/csar-proto/csar/audit/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReceiptFailureStatus(t *testing.T) {
	for _, test := range []struct {
		err  error
		code codes.Code
	}{
		{pipeline.ErrFull, codes.ResourceExhausted}, {pipeline.ErrClosed, codes.Unavailable},
		{context.Canceled, codes.Canceled}, {context.DeadlineExceeded, codes.DeadlineExceeded},
	} {
		if got := status.Code(receiptError(test.err)); got != test.code {
			t.Fatalf("got %s want %s", got, test.code)
		}
	}
}
func TestGRPCDoesNotAcceptUnconfirmedBatch(t *testing.T) {
	buf := &stubSubmitter{failAfter: 1}
	handler := NewGRPC(buf, 0)
	event := &auditv1.AuditEvent{Actor: "user", Action: "update", TargetType: "campaign", ScopeType: "tenant"}
	_, err := handler.RecordEvents(context.Background(), &auditv1.RecordEventsRequest{Events: []*auditv1.AuditEvent{event, event}})
	if status.Code(err) != codes.Unavailable || len(buf.submitted) != 1 {
		t.Fatalf("unexpected partial receipt: %v", err)
	}
}
