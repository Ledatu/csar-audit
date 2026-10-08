package ingest

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/ledatu/csar-audit/internal/pipeline"
	"github.com/ledatu/csar-core/audit"
	csarerrors "github.com/ledatu/csar-core/errors"
	"github.com/ledatu/csar-core/httpx"
)

type submitter interface {
	Submit(context.Context, *audit.Event) error
}

type httpIngestBody struct {
	Events []*audit.Event `json:"events"`
}

// HTTPHandler serves POST /ingest with a JSON batch body.
type HTTPHandler struct {
	buf     submitter
	logger  *slog.Logger
	timeout time.Duration
}

// NewHTTPHandler constructs the HTTP ingest handler.
func NewHTTPHandler(buf *pipeline.Buffer, logger *slog.Logger, timeout time.Duration) *HTTPHandler {
	if logger == nil {
		logger = slog.Default()
	}
	return &HTTPHandler{buf: buf, logger: logger.With("component", "audit_ingest_http"), timeout: timeout}
}

// ServeHTTP returns 202 only after every event has a confirmed broker receipt.
func (h *HTTPHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body httpIngestBody
	if err := httpx.ReadJSON(r, &body); err != nil {
		httpx.WriteError(w, err)
		return
	}
	if len(body.Events) == 0 || len(body.Events) > maxEventsPerBatch {
		httpx.WriteError(w, csarerrors.Validation("1 to %d events required", maxEventsPerBatch))
		return
	}

	for _, event := range body.Events {
		if err := Validate(event); err != nil {
			httpx.WriteError(w, csarerrors.Validation("%v", err))
			return
		}
	}

	timeout := h.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()
	for idx, event := range body.Events {
		if err := h.buf.Submit(ctx, event); err != nil {
			h.logger.Warn("audit ingest not confirmed", "action", event.Action, "accepted", idx, "total", len(body.Events))
			httpx.WriteError(w, csarerrors.Unavailable("audit ingest not confirmed after %d of %d events; retry with retained event IDs", idx, len(body.Events)))
			return
		}
	}

	httpx.WriteJSON(w, http.StatusAccepted, map[string]any{
		"status":   "accepted",
		"accepted": len(body.Events),
	})
}
