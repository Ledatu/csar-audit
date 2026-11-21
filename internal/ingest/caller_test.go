package ingest

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/ledatu/csar-core/audit"
	"github.com/ledatu/csar-core/caller"
	auditv1 "github.com/ledatu/csar-proto/csar/audit/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHTTPCallerContextReachesConfirmedQueue(t *testing.T) {
	e := &audit.Event{Actor: "owner", Action: "report.read", TargetType: "report", ScopeType: "tenant", OperationID: "op:1",
		Caller: &caller.Identity{Version: caller.Version, CallerKind: caller.Application, CallerID: "app:1",
			OwnerSubject: "user:1", EffectiveSubject: "user:1", CredentialKind: caller.APIKey, CredentialID: "key:1", Assurance: caller.Verified}}
	buf := &stubSubmitter{failAfter: -1}
	h := &HTTPHandler{buf: buf}
	send := func() int {
		r := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(mustMarshalJSON(t, httpIngestBody{Events: []*audit.Event{e}})))
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if code := send(); code != http.StatusAccepted || len(buf.submitted) != 1 || !reflect.DeepEqual(buf.submitted[0], e) {
		t.Fatal("HTTP lost caller context before confirmed queue", code)
	}
	e.Caller.Version = 2
	if code := send(); code != http.StatusBadRequest || len(buf.submitted) != 1 {
		t.Fatal("malformed HTTP caller reached queue", code)
	}
}

func TestGRPCCallerContextReachesConfirmedQueue(t *testing.T) {
	e := &audit.Event{Actor: "owner", Action: "report.read", TargetType: "report", ScopeType: "tenant", OperationID: "op:1",
		Caller: &caller.Identity{Version: caller.Version, CallerKind: caller.Agent, CallerID: "agent:1",
			OwnerSubject: "user:1", EffectiveSubject: "user:1", CredentialKind: caller.APIKey, CredentialID: "key:1", Assurance: caller.Verified}}
	buf := &stubSubmitter{failAfter: 2}
	h := NewGRPC(buf, 0)
	_, err := h.RecordEvent(context.Background(), &auditv1.RecordEventRequest{Event: audit.EventToProto(e)})
	if err != nil || len(buf.submitted) != 1 || buf.submitted[0].Caller == nil || *buf.submitted[0].Caller != *e.Caller || buf.submitted[0].OperationID != "op:1" {
		t.Fatalf("caller context lost before confirmed queue: %v", err)
	}
	e.Caller.Version = 2
	_, err = h.RecordEvent(context.Background(), &auditv1.RecordEventRequest{Event: audit.EventToProto(e)})
	if status.Code(err) != codes.InvalidArgument || len(buf.submitted) != 1 {
		t.Fatal("malformed caller reached queue")
	}
}
