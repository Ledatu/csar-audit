package ingest

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ledatu/csar-core/audit"
)

type stubSubmitter struct {
	submitted []*audit.Event
	failAfter int
}

func (s *stubSubmitter) Submit(e *audit.Event) error {
	if s.failAfter >= 0 && len(s.submitted) >= s.failAfter {
		return errors.New("buffer full")
	}
	s.submitted = append(s.submitted, e)
	return nil
}

func TestHTTPHandlerServeHTTPAcceptsBatchPayload(t *testing.T) {
	t.Parallel()

	buf := &stubSubmitter{failAfter: -1}
	handler := &HTTPHandler{
		buf:    buf,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(mustMarshalJSON(t, map[string]any{
		"events": []map[string]any{
			{
				"actor":       "user-1",
				"action":      "created",
				"target_type": "campaign",
				"scope_type":  "tenant",
			},
			{
				"actor":       "user-2",
				"action":      "updated",
				"target_type": "campaign",
				"scope_type":  "tenant",
			},
		},
	})))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("expected %d, got %d: %s", http.StatusAccepted, rec.Code, rec.Body.String())
	}
	if len(buf.submitted) != 2 {
		t.Fatalf("expected 2 submitted events, got %d", len(buf.submitted))
	}
	if buf.submitted[0].Action != "created" || buf.submitted[1].Action != "updated" {
		t.Fatalf("unexpected actions submitted: %q, %q", buf.submitted[0].Action, buf.submitted[1].Action)
	}
}

func TestHTTPHandlerServeHTTPRejectsEmptyBatch(t *testing.T) {
	t.Parallel()

	buf := &stubSubmitter{failAfter: -1}
	handler := &HTTPHandler{
		buf:    buf,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(mustMarshalJSON(t, map[string]any{
		"events": []map[string]any{},
	})))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
	if len(buf.submitted) != 0 {
		t.Fatalf("expected no submitted events, got %d", len(buf.submitted))
	}
}

func TestHTTPHandlerServeHTTPRejectsBatchWithInvalidEvent(t *testing.T) {
	t.Parallel()

	buf := &stubSubmitter{failAfter: -1}
	handler := &HTTPHandler{
		buf:    buf,
		logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	req := httptest.NewRequest(http.MethodPost, "/ingest", bytes.NewReader(mustMarshalJSON(t, map[string]any{
		"events": []map[string]any{
			{
				"actor":       "user-1",
				"action":      "created",
				"target_type": "campaign",
				"scope_type":  "tenant",
			},
			{
				"actor":       "",
				"action":      "updated",
				"target_type": "campaign",
				"scope_type":  "tenant",
			},
		},
	})))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected %d, got %d: %s", http.StatusBadRequest, rec.Code, rec.Body.String())
	}
	if len(buf.submitted) != 0 {
		t.Fatalf("expected no submitted events, got %d", len(buf.submitted))
	}
}

func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()

	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal json: %v", err)
	}
	return b
}
