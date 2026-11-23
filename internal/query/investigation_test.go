package query

import (
	"context"
	"encoding/json"
	"github.com/ledatu/csar-audit/internal/store"
	"github.com/ledatu/csar-core/gatewayctx"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type investigationLister struct {
	filter  *store.ListFilter
	bounded bool
}

func (s *investigationLister) List(ctx context.Context, f *store.ListFilter) (*store.ListResult, error) {
	s.filter = f
	deadline, ok := ctx.Deadline()
	s.bounded = ok && time.Until(deadline) <= 10*time.Second
	return &store.ListResult{Events: []store.Event{}}, nil
}
func (s *investigationLister) ListGroups(ctx context.Context, f *store.ListFilter) (*store.GroupResult, error) {
	_, _ = s.List(ctx, f)
	return &store.GroupResult{Groups: []store.Group{}}, nil
}
func queryInvestigation(st *investigationLister, path string) *httptest.ResponseRecorder {
	mux := http.NewServeMux()
	New(st).Register(mux)
	request := httptest.NewRequest(http.MethodGet, path, nil)
	request.Header.Set(gatewayctx.HeaderSubject, "operator")
	response := httptest.NewRecorder()
	gatewayctx.Middleware(mux).ServeHTTP(response, request)
	return response
}
func TestInvestigationHTTPContractAndBothEndpoints(t *testing.T) {
	for _, path := range []string{"/admin/audit", "/admin/audit/groups"} {
		st := &investigationLister{}
		q := url.Values{"initiator_kind": {"agent"}, "caller_id": {"svc:api"}, "origin_caller_client_id": {"client:1"}, "trace_id": {"trace:1"}, "cache_status": {"HIT"}, "audit_phase": {"release_authorized"}}
		response := queryInvestigation(st, path+"?"+q.Encode())
		if response.Code != 200 || !st.bounded || st.filter.Initiator.Kind != "agent" || st.filter.Caller.ID != "svc:api" || st.filter.OriginCaller.ClientID != "client:1" || st.filter.TraceID != "trace:1" || st.filter.Since == nil {
			t.Fatal(response.Code, response.Body.String(), st)
		}
		var body struct {
			Contract string                     `json:"filter_contract"`
			Window   *store.InvestigationWindow `json:"window"`
		}
		if json.Unmarshal(response.Body.Bytes(), &body) != nil || body.Contract != "caller-v1" || body.Window == nil {
			t.Fatal(response.Body.String())
		}
	}
}
func TestInvestigationInvalidFiltersNeverQuery(t *testing.T) {
	for _, query := range []string{"initiator_kind=robot", "caller_kind=&caller_kind=agent", "caller_typo=agent", "caller_kind=agent&caller_kind=human", "origin_caller_credential_kind=mcp", "caller_version=0", "trace_id=test&since=invalid", "trace_id=test&since=2026-01-01T00:00:00Z&until=2026-03-01T00:00:00Z", "caller_client_id=" + strings.Repeat("a", 513)} {
		st := &investigationLister{}
		response := queryInvestigation(st, "/admin/audit?"+query)
		if response.Code != 400 || st.filter != nil {
			t.Fatal(query, response.Code, response.Body.String())
		}
	}
	st := &investigationLister{}
	response := queryInvestigation(st, "/admin/audit?actor=legacy")
	if response.Code != 200 || st.filter.Since != nil || st.bounded {
		t.Fatal("legacy query changed", response.Code)
	}
}
